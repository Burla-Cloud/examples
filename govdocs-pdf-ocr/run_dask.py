import argparse
import json
import os
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

import distributed
from dask.distributed import Client

from benchmark_infra import (
    REGION,
    collect_metrics,
    ensure_instance_profile,
    ensure_security_group,
    tagged_instances,
)
from benchmark_runner import (
    add_workload_arguments,
    finalize_run,
    prepare_run,
    save_partial,
)
from dask_ec2_cluster import DaskEc2Cluster
from run_burla import process_document

REQUEST_STARTED_AT = datetime(2026, 8, 19, 4, 20, 22, tzinfo=UTC)
SCHEDULER_INSTANCE_TYPE = "m7i.2xlarge"
WORKER_INSTANCE_TYPE = "m7i.16xlarge"
WORKER_COUNT = 32


def parse_args():
    parser = argparse.ArgumentParser(
        description="Run the GovDocs1 PDF workload on Dask on AWS."
    )
    add_workload_arguments(parser, "dask")
    parser.add_argument("--image", required=True)
    parser.add_argument("--skip-setup-pilot", action="store_true")
    parser.add_argument("--worker-count", type=int, default=WORKER_COUNT)
    parser.add_argument("--metrics-delay-seconds", type=int, default=90)
    return parser.parse_args()


def setup_pilot(
    args,
    security_group_id: str,
) -> dict:
    run_id = f"{args.run_id or args.default_run_id}-setup"
    namespace = f"GovDocsBenchmark/Dask/{run_id}"
    started_at = datetime.now(UTC)
    cluster = None
    try:
        cluster = DaskEc2Cluster(
            profile=args.profile,
            run_id=run_id,
            image=args.image,
            namespace=namespace,
            security_group_id=security_group_id,
            scheduler_instance_type=SCHEDULER_INSTANCE_TYPE,
            worker_instance_type="m7i.large",
            worker_count=2,
        )
        scheduler_address = cluster.start()
        cluster.wait_for_scheduler()
        with Client(scheduler_address) as client:
            client.wait_for_workers(2, timeout=1_800)
            workers = client.run(
                lambda: {
                    "hostname": os.uname().nodename,
                    "result": sum(range(100)),
                }
            )
            if len(workers) != 2 or {value["result"] for value in workers.values()} != {
                4_950
            }:
                raise RuntimeError(f"Dask setup pilot failed: {workers}")
        completed_at = datetime.now(UTC)
        return {
            "framework": "dask",
            "started_at": started_at.isoformat(),
            "completed_at": completed_at.isoformat(),
            "elapsed_seconds": (completed_at - started_at).total_seconds(),
            "request_to_ready_seconds": (
                completed_at - REQUEST_STARTED_AT
            ).total_seconds(),
            "workers": workers,
        }
    finally:
        if cluster is not None:
            cluster.close()


def run_full(
    args,
    security_group_id: str,
) -> dict:
    context = prepare_run(args, "dask", distributed.__version__)
    namespace = f"GovDocsBenchmark/Dask/{context.run_id}"
    cluster_started_at = datetime.now(UTC)
    cluster = DaskEc2Cluster(
        profile=args.profile,
        run_id=context.run_id,
        image=args.image,
        namespace=namespace,
        security_group_id=security_group_id,
        scheduler_instance_type=SCHEDULER_INSTANCE_TYPE,
        worker_instance_type=WORKER_INSTANCE_TYPE,
        worker_count=args.worker_count,
    )
    new_results = []
    instances = []
    try:
        scheduler_address = cluster.start()
        cluster.wait_for_scheduler()
        with Client(scheduler_address) as client:
            client.wait_for_workers(args.worker_count * 8, timeout=1_800)
            cluster_ready_at = datetime.now(UTC)
            client.upload_file(str(Path(__file__).with_name("run_burla.py")))
            archive_urls = client.scatter([context.archive_urls], broadcast=True)[0]
            output_post = client.scatter([context.output_post], broadcast=True)[0]
            job_started_at = datetime.now(UTC)
            job_started = time.perf_counter()
            futures = client.map(
                process_document,
                context.pending_documents,
                archive_urls=archive_urls,
                output_post=output_post,
                output_prefix=context.output_prefix,
                pure=False,
            )
            new_results = client.gather(futures)
        job_completed_at = datetime.now(UTC)
        wall_seconds = time.perf_counter() - job_started
    except Exception:
        save_partial(context, new_results)
        raise
    finally:
        instances = tagged_instances(args.profile, "dask", context.run_id)
        cluster.close()

    time.sleep(args.metrics_delay_seconds)
    metrics = collect_metrics(
        args.profile,
        namespace,
        instances,
        job_started_at - timedelta(minutes=1),
        job_completed_at + timedelta(minutes=1),
    )
    cluster_summary = {
        "region": REGION,
        "scheduler_instance_type": SCHEDULER_INSTANCE_TYPE,
        "worker_instance_type": WORKER_INSTANCE_TYPE,
        "maximum_workers": args.worker_count,
        "maximum_worker_vcpus": args.worker_count * 64,
        "cluster_started_at": cluster_started_at.isoformat(),
        "scheduler_ready_at": cluster_ready_at.isoformat(),
        "cluster_bootstrap_seconds": (
            cluster_ready_at - cluster_started_at
        ).total_seconds(),
        "instances": [
            {
                "instance_id": instance["InstanceId"],
                "instance_type": instance["InstanceType"],
                "launch_time": instance["LaunchTime"].astimezone(UTC).isoformat(),
            }
            for instance in instances
        ],
    }
    return finalize_run(
        context,
        new_results,
        job_started_at,
        wall_seconds,
        cluster_summary,
        metrics,
    )


def main() -> None:
    args = parse_args()
    os.environ["AWS_PROFILE"] = args.profile
    session = __import__("boto3").Session(profile_name=args.profile)
    account_id = session.client("sts").get_caller_identity()["Account"]
    bucket = args.bucket or f"burla-govdocs1-corpus-{account_id}"
    ensure_instance_profile(args.profile, bucket)
    security_group_id = ensure_security_group(args.profile)

    setup = None
    if not args.skip_setup_pilot:
        setup = setup_pilot(args, security_group_id)
        print(json.dumps({"setup": setup}, indent=2), flush=True)
    result = run_full(args, security_group_id)
    result["setup"] = setup
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
