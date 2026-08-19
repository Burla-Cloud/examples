import argparse
import json
import shlex
import socket
import subprocess
import tempfile
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

import boto3
import ray
import yaml
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

from benchmark_infra import (
    REGION,
    SUBNET_ID,
    allow_ray_head_worker_ssh,
    collect_metrics,
    ensure_instance_profile,
    ensure_security_group,
    metrics_bootstrap,
    revoke_ray_head_worker_ssh,
    tagged_instances,
)
from benchmark_runner import (
    add_workload_arguments,
    finalize_run,
    prepare_run,
    save_partial,
)
from run_burla import process_document

REQUEST_STARTED_AT = datetime(2026, 8, 19, 4, 20, 22, tzinfo=UTC)
FRAMEWORK_WORK_STARTED_AT = datetime(2026, 8, 19, 7, 26, 21, tzinfo=UTC)
BASE_AMI_ID = "ami-042d92d7f194cd14c"
SCHEDULER_INSTANCE_TYPE = "m7i.2xlarge"
WORKER_INSTANCE_TYPE = "m7i.16xlarge"
WORKER_COUNT = 32


def parse_args():
    parser = argparse.ArgumentParser(
        description="Run the GovDocs1 PDF workload on Ray on AWS."
    )
    add_workload_arguments(parser, "ray")
    parser.add_argument("--image")
    parser.add_argument("--driver", action="store_true")
    parser.add_argument("--pilot", action="store_true")
    parser.add_argument("--skip-setup-pilot", action="store_true")
    parser.add_argument("--worker-count", type=int, default=2)
    parser.add_argument("--full-worker-count", type=int, default=WORKER_COUNT)
    parser.add_argument("--metrics-delay-seconds", type=int, default=90)
    return parser.parse_args()


def ray_cluster_config(
    image: str,
    run_id: str,
    namespace: str,
    instance_profile_arn: str,
    security_group_id: str,
    worker_instance_type: str,
    minimum_workers: int,
    maximum_workers: int,
) -> dict:
    bootstrap = ["sudo chmod 666 /var/run/docker.sock"] + [
        f"sudo bash -lc {shlex.quote(command)}"
        for command in metrics_bootstrap(namespace)
    ]

    def node_config(instance_type: str, node_kind: str) -> dict:
        return {
            "ImageId": BASE_AMI_ID,
            "InstanceType": instance_type,
            "IamInstanceProfile": {"Arn": instance_profile_arn},
            "SecurityGroupIds": [security_group_id],
            "SubnetId": SUBNET_ID,
            "Monitoring": {"Enabled": True},
            "BlockDeviceMappings": [
                {
                    "DeviceName": "/dev/sda1",
                    "Ebs": {
                        "VolumeSize": 100,
                        "VolumeType": "gp3",
                        "DeleteOnTermination": True,
                    },
                }
            ],
            "TagSpecifications": [
                {
                    "ResourceType": "instance",
                    "Tags": [
                        {
                            "Key": "project",
                            "Value": "govdocs-distributed-benchmark",
                        },
                        {"Key": "benchmark-framework", "Value": "ray"},
                        {"Key": "benchmark-run-id", "Value": run_id},
                        {"Key": "benchmark-node-kind", "Value": node_kind},
                    ],
                }
            ],
        }

    return {
        "cluster_name": f"govdocs-{run_id}"[:64],
        "max_workers": maximum_workers,
        "upscaling_speed": 999,
        "idle_timeout_minutes": 5,
        "provider": {
            "type": "aws",
            "region": REGION,
            "availability_zone": "us-east-1a",
            "cache_stopped_nodes": False,
            "use_internal_ips": False,
        },
        "auth": {"ssh_user": "ubuntu"},
        "docker": {
            "image": image,
            "container_name": "ray_container",
            "pull_before_run": True,
            "run_options": ["--shm-size=16g", "--ulimit=nofile=65536:65536"],
        },
        "available_node_types": {
            "ray.head.default": {
                "resources": {"CPU": 0},
                "node_config": node_config(SCHEDULER_INSTANCE_TYPE, "head"),
                "min_workers": 0,
                "max_workers": 0,
            },
            "ray.worker.default": {
                "resources": {"CPU": 64},
                "node_config": node_config(worker_instance_type, "worker"),
                "min_workers": minimum_workers,
                "max_workers": maximum_workers,
            },
        },
        "head_node_type": "ray.head.default",
        "file_mounts": {
            "~/govdocs-pdf-ocr/run_ray.py": str(Path(__file__).resolve()),
            "~/govdocs-pdf-ocr/run_burla.py": str(
                Path(__file__).with_name("run_burla.py")
            ),
            "~/govdocs-pdf-ocr/benchmark_infra.py": str(
                Path(__file__).with_name("benchmark_infra.py")
            ),
            "~/govdocs-pdf-ocr/benchmark_runner.py": str(
                Path(__file__).with_name("benchmark_runner.py")
            ),
        },
        "initialization_commands": bootstrap,
        "setup_commands": [],
        "head_setup_commands": [],
        "worker_setup_commands": [],
        "head_start_ray_commands": [
            "ray stop",
            (
                "ray start --head --port=6379 --object-manager-port=8076 "
                "--autoscaling-config=~/ray_bootstrap_config.yaml "
                "--dashboard-host=0.0.0.0 --ray-client-server-port=10001"
            ),
        ],
        "worker_start_ray_commands": [
            "ray stop",
            ("ray start --address=$RAY_HEAD_IP:6379 --object-manager-port=8076"),
        ],
    }


def write_config(config: dict, run_id: str) -> Path:
    path = Path(tempfile.gettempdir()) / f"{run_id}-ray-cluster.yaml"
    path.write_text(yaml.safe_dump(config, sort_keys=False))
    return path


def ray_cli(config_path: Path, *arguments: str, capture: bool = False):
    return subprocess.run(
        ["ray", *arguments, str(config_path)]
        if arguments and arguments[0] in {"up", "down"}
        else ["ray", *arguments],
        check=True,
        text=True,
        capture_output=capture,
    )


def launch(config_path: Path) -> None:
    subprocess.run(
        ["ray", "up", "-y", "--no-config-cache", str(config_path)],
        check=True,
    )


def execute(config_path: Path, command: str) -> None:
    subprocess.run(
        ["ray", "exec", str(config_path), command],
        check=True,
    )


def terminate(config_path: Path) -> None:
    subprocess.run(
        ["ray", "down", "-y", str(config_path)],
        check=True,
    )


def pilot(args) -> None:
    ray.init(address="auto")
    deadline = time.monotonic() + 1_800
    worker_nodes = []
    while time.monotonic() < deadline:
        worker_nodes = [
            node
            for node in ray.nodes()
            if node["Alive"] and node["Resources"].get("CPU", 0) > 0
        ]
        if len(worker_nodes) >= args.worker_count:
            break
        time.sleep(2)
    if len(worker_nodes) < args.worker_count:
        raise RuntimeError(f"Ray setup pilot found {len(worker_nodes)} workers")

    remote_probe = ray.remote(num_cpus=1)(
        lambda: {"hostname": socket.gethostname(), "result": sum(range(100))}
    )
    probes = [
        remote_probe.options(
            scheduling_strategy=NodeAffinitySchedulingStrategy(
                node_id=node["NodeID"], soft=False
            )
        ).remote()
        for node in worker_nodes[: min(2, args.worker_count)]
    ]
    results = ray.get(probes)
    if {value["result"] for value in results} != {4_950}:
        raise RuntimeError(f"Ray setup pilot failed: {results}")
    print(json.dumps({"workers": results}), flush=True)


def driver(args) -> None:
    args.profile = None
    context = prepare_run(args, "ray", ray.__version__)
    namespace = f"GovDocsBenchmark/Ray/{context.run_id}"
    remote_process = ray.remote(num_cpus=1)(process_document)
    ray.init(address="auto")
    archive_urls = ray.put(context.archive_urls)
    output_post = ray.put(context.output_post)
    new_results = []
    task_iter = iter(context.pending_documents)
    pending = []
    job_started_at = datetime.now(UTC)
    job_started = time.perf_counter()
    try:
        for _ in range(min(args.max_in_flight, len(context.pending_documents))):
            task = next(task_iter)
            pending.append(
                remote_process.remote(
                    task,
                    archive_urls,
                    output_post,
                    context.output_prefix,
                )
            )

        while pending:
            batch_size = min(256, len(pending))
            completed, pending = ray.wait(pending, num_returns=batch_size)
            new_results.extend(ray.get(completed))
            for _ in range(batch_size):
                try:
                    task = next(task_iter)
                except StopIteration:
                    break
                pending.append(
                    remote_process.remote(
                        task,
                        archive_urls,
                        output_post,
                        context.output_prefix,
                    )
                )
    except Exception:
        save_partial(context, new_results)
        raise

    job_completed_at = datetime.now(UTC)
    wall_seconds = time.perf_counter() - job_started
    instances = tagged_instances(None, "ray", context.run_id)
    time.sleep(args.metrics_delay_seconds)
    metrics = collect_metrics(
        None,
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
        "instances": [
            {
                "instance_id": instance["InstanceId"],
                "instance_type": instance["InstanceType"],
                "launch_time": instance["LaunchTime"].astimezone(UTC).isoformat(),
            }
            for instance in instances
        ],
    }
    result = finalize_run(
        context,
        new_results,
        job_started_at,
        wall_seconds,
        cluster_summary,
        metrics,
    )
    print(json.dumps(result, indent=2))


def remote_command(mode: str, arguments: list[str] | None = None) -> str:
    values = [
        "cd ~/govdocs-pdf-ocr",
        "&&",
        "python",
        "run_ray.py",
        f"--{mode}",
    ]
    if arguments:
        values.extend(shlex.quote(value) for value in arguments)
    return " ".join(values)


def orchestrate(args) -> None:
    if not args.image:
        raise ValueError("--image is required")
    session = boto3.Session(profile_name=args.profile, region_name=REGION)
    account_id = session.client("sts").get_caller_identity()["Account"]
    bucket = args.bucket or f"burla-govdocs1-corpus-{account_id}"
    instance_profile_arn = ensure_instance_profile(args.profile, bucket)
    security_group_id = ensure_security_group(args.profile)
    setup = None

    if not args.skip_setup_pilot:
        setup_run_id = f"{args.run_id or args.default_run_id}-setup"
        setup_namespace = f"GovDocsBenchmark/Ray/{setup_run_id}"
        setup_config = write_config(
            ray_cluster_config(
                args.image,
                setup_run_id,
                setup_namespace,
                instance_profile_arn,
                security_group_id,
                "m7i.large",
                2,
                2,
            ),
            setup_run_id,
        )
        setup_started_at = datetime.now(UTC)
        setup_head_cidr = None
        try:
            launch(setup_config)
            setup_head_cidr = allow_ray_head_worker_ssh(
                args.profile,
                security_group_id,
                f"govdocs-{setup_run_id}"[:64],
            )
            execute(setup_config, remote_command("pilot"))
            setup_completed_at = datetime.now(UTC)
            setup = {
                "framework": "ray",
                "started_at": setup_started_at.isoformat(),
                "completed_at": setup_completed_at.isoformat(),
                "elapsed_seconds": (
                    setup_completed_at - setup_started_at
                ).total_seconds(),
                "request_to_ready_seconds": (
                    setup_completed_at - REQUEST_STARTED_AT
                ).total_seconds(),
                "framework_work_to_ready_seconds": (
                    setup_completed_at - FRAMEWORK_WORK_STARTED_AT
                ).total_seconds(),
            }
            print(json.dumps({"setup": setup}, indent=2), flush=True)
        finally:
            terminate(setup_config)
            revoke_ray_head_worker_ssh(
                args.profile, security_group_id, setup_head_cidr
            )

    run_id = args.run_id or args.default_run_id
    namespace = f"GovDocsBenchmark/Ray/{run_id}"
    full_config = write_config(
        ray_cluster_config(
            args.image,
            run_id,
            namespace,
            instance_profile_arn,
            security_group_id,
            WORKER_INSTANCE_TYPE,
            args.full_worker_count,
            args.full_worker_count,
        ),
        run_id,
    )
    cluster_started_at = datetime.now(UTC)
    full_head_cidr = None
    try:
        launch(full_config)
        full_head_cidr = allow_ray_head_worker_ssh(
            args.profile, security_group_id, f"govdocs-{run_id}"[:64]
        )
        execute(
            full_config,
            remote_command("pilot", ["--worker-count", str(args.full_worker_count)]),
        )
        cluster_ready_at = datetime.now(UTC)
        driver_arguments = [
            "--run-id",
            run_id,
            "--bucket",
            bucket,
            "--corpus-run-id",
            args.corpus_run_id,
            "--max-parallelism",
            str(args.max_parallelism),
            "--max-in-flight",
            str(args.max_in_flight),
            "--url-expiration-seconds",
            str(args.url_expiration_seconds),
            "--metrics-delay-seconds",
            str(args.metrics_delay_seconds),
            "--worker-count",
            str(args.full_worker_count),
        ]
        if args.limit is not None:
            driver_arguments.extend(["--limit", str(args.limit)])
        if args.archives:
            driver_arguments.append("--archives")
            driver_arguments.extend(args.archives)
        execute(full_config, remote_command("driver", driver_arguments))
        setup_payload = {
            "setup": setup,
            "full_cluster_started_at": cluster_started_at.isoformat(),
            "full_cluster_ready_at": cluster_ready_at.isoformat(),
            "full_cluster_bootstrap_seconds": (
                cluster_ready_at - cluster_started_at
            ).total_seconds(),
        }
        session.client("s3").put_object(
            Bucket=bucket,
            Key=f"runs/{run_id}/setup.json",
            Body=json.dumps(setup_payload, indent=2).encode(),
            ContentType="application/json",
        )
    finally:
        terminate(full_config)
        revoke_ray_head_worker_ssh(args.profile, security_group_id, full_head_cidr)


def main() -> None:
    args = parse_args()
    if args.pilot:
        pilot(args)
    elif args.driver:
        driver(args)
    else:
        orchestrate(args)


if __name__ == "__main__":
    main()
