import argparse
import json
import time
from collections import Counter
from dataclasses import dataclass
from datetime import UTC, datetime

import boto3
from botocore.exceptions import ClientError

from run_burla import (
    jsonl_payload,
    load_jsonl,
    process_document,
    select_documents,
)


@dataclass
class RunContext:
    args: argparse.Namespace
    framework: str
    framework_version: str
    s3: object
    bucket: str
    run_id: str
    output_prefix: str
    partial_key: str
    prior_results: list[dict]
    pending_documents: list[dict]
    archive_urls: dict[str, str]
    output_post: dict


def add_workload_arguments(parser: argparse.ArgumentParser, framework: str) -> None:
    parser.add_argument("--profile", default="burla-test")
    parser.add_argument("--bucket")
    parser.add_argument("--corpus-run-id", default="govdocs1-v1")
    parser.add_argument("--run-id")
    parser.add_argument("--archives", nargs="*")
    parser.add_argument("--limit", type=int)
    parser.add_argument("--max-parallelism", type=int, default=2_000)
    parser.add_argument("--max-in-flight", type=int, default=4_000)
    parser.add_argument("--url-expiration-seconds", type=int, default=43_200)
    parser.set_defaults(
        default_run_id=f"{framework}-text-{datetime.now(UTC):%Y%m%dT%H%M%SZ}"
    )


def prepare_run(
    args: argparse.Namespace, framework: str, framework_version: str
) -> RunContext:
    session = boto3.Session(profile_name=args.profile)
    account_id = session.client("sts").get_caller_identity()["Account"]
    bucket = args.bucket or f"burla-govdocs1-corpus-{account_id}"
    s3 = session.client("s3")
    member_key = f"manifests/{args.corpus_run_id}/pdf-members.jsonl"
    documents = select_documents(load_jsonl(s3, bucket, member_key), args)
    run_id = args.run_id or args.default_run_id
    output_prefix = f"runs/{run_id}"
    partial_key = f"{output_prefix}/results.partial.jsonl"
    try:
        prior_results = load_jsonl(s3, bucket, partial_key)
    except ClientError as error:
        if error.response["Error"]["Code"] not in {"NoSuchKey", "404"}:
            raise
        prior_results = []
    completed_ids = {result["document_id"] for result in prior_results}
    pending_documents = [
        document
        for document in documents
        if document["document_id"] not in completed_ids
    ]
    archive_names = sorted({document["archive_name"] for document in pending_documents})
    archive_urls = {
        archive_name: s3.generate_presigned_url(
            "get_object",
            Params={"Bucket": bucket, "Key": f"raw/zipfiles/{archive_name}"},
            ExpiresIn=args.url_expiration_seconds,
        )
        for archive_name in archive_names
    }
    output_post = s3.generate_presigned_post(
        Bucket=bucket,
        Key=f"{output_prefix}/text/${{filename}}",
        Fields={"x-amz-server-side-encryption": "AES256"},
        Conditions=[
            ["starts-with", "$key", f"{output_prefix}/text/"],
            {"x-amz-server-side-encryption": "AES256"},
        ],
        ExpiresIn=args.url_expiration_seconds,
    )
    return RunContext(
        args=args,
        framework=framework,
        framework_version=framework_version,
        s3=s3,
        bucket=bucket,
        run_id=run_id,
        output_prefix=output_prefix,
        partial_key=partial_key,
        prior_results=prior_results,
        pending_documents=pending_documents,
        archive_urls=archive_urls,
        output_post=output_post,
    )


def run_one(context: RunContext, task: dict) -> dict:
    return process_document(
        task,
        context.archive_urls,
        context.output_post,
        context.output_prefix,
    )


def save_partial(context: RunContext, new_results: list[dict]) -> None:
    context.s3.put_object(
        Bucket=context.bucket,
        Key=context.partial_key,
        Body=jsonl_payload(context.prior_results + new_results),
    )


def finalize_run(
    context: RunContext,
    new_results: list[dict],
    started_at: datetime,
    wall_seconds: float,
    cluster: dict,
    metrics: dict,
) -> dict:
    results = context.prior_results + new_results
    results.sort(key=lambda result: result["document_id"])
    result_key = f"{context.output_prefix}/results.jsonl"
    context.s3.put_object(
        Bucket=context.bucket,
        Key=result_key,
        Body=jsonl_payload(results),
    )
    if context.prior_results:
        context.s3.delete_object(
            Bucket=context.bucket,
            Key=context.partial_key,
        )

    succeeded = [result for result in results if result["status"] == "succeeded"]
    failed = [result for result in results if result["status"] == "failed"]
    summary = {
        "schema_version": 2,
        "framework": context.framework,
        "framework_version": context.framework_version,
        "run_id": context.run_id,
        "started_at": started_at.isoformat(),
        "completed_at": datetime.now(UTC).isoformat(),
        "corpus_run_id": context.args.corpus_run_id,
        "max_parallelism": context.args.max_parallelism,
        "max_in_flight": context.args.max_in_flight,
        "cluster": cluster,
        "documents": len(results),
        "resumed_documents": len(context.prior_results),
        "processed_documents": len(new_results),
        "succeeded": len(succeeded),
        "failed": len(failed),
        "pages": sum(result["page_count"] for result in succeeded),
        "direct_text_pages": sum(result["direct_text_pages"] for result in succeeded),
        "ocr_pages": sum(result["ocr_pages"] for result in succeeded),
        "text_bytes": sum(result["text_bytes"] for result in succeeded),
        "wall_seconds": wall_seconds,
        "processed_documents_per_second": len(new_results) / wall_seconds,
        "documents_by_worker_container": dict(
            sorted(Counter(result["worker_container"] for result in results).items())
        ),
        "failure_types": dict(
            sorted(Counter(result["error_type"] for result in failed).items())
        ),
        "results_key": result_key,
    }
    summary_key = f"{context.output_prefix}/summary.json"
    metrics_key = f"{context.output_prefix}/node-metrics.json"
    context.s3.put_object(
        Bucket=context.bucket,
        Key=summary_key,
        Body=json.dumps(summary, indent=2).encode(),
        ContentType="application/json",
    )
    context.s3.put_object(
        Bucket=context.bucket,
        Key=metrics_key,
        Body=json.dumps(metrics, indent=2).encode(),
        ContentType="application/json",
    )
    return {
        "summary": f"s3://{context.bucket}/{summary_key}",
        "metrics": f"s3://{context.bucket}/{metrics_key}",
        "documents": len(results),
        "succeeded": len(succeeded),
        "failed": len(failed),
        "pages": summary["pages"],
        "wall_seconds": round(wall_seconds, 2),
    }


def timed(callable_):
    started_at = datetime.now(UTC)
    started = time.perf_counter()
    value = callable_()
    return value, started_at, time.perf_counter() - started
