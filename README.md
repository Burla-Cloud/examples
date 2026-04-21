# Build Simple Python ETL Pipelines Without Airflow

Run a daily ETL over 10,000 files into Postgres with a 30-line Python script. No Airflow DAGs. No Dagster assets. No Prefect flows. No Kubernetes.

## The Problem

You have a daily drop of 10,000 JSON log files on S3. You want to transform them and load into Postgres. Airflow means a scheduler, a metadata DB, a webserver, DAG files, and operators. Dagster and Prefect have their own DAG concepts and deployment surfaces. You don't have a platform team. You have one laptop and a deadline.

You also don't want to run 10,000 `INSERT`s from one machine, and you don't want to DOS your Postgres instance with 10,000 concurrent connections.

## The Solution (Burla)

Extract + transform happens in parallel on 10,000 workers. Load is capped at 1,000 concurrent Postgres connections via `max_parallelism=1000`. Results stream back with `generator=True` so you can print progress to the terminal.

One Python file. Runs locally. Runs in CI. Runs on 10,000 machines.

## Example

```python
import boto3
import psycopg2  # noqa: F401 -- top-level import so Burla installs psycopg2 on workers
from burla import remote_parallel_map

BUCKET = "my-events-bucket"
DATE = "2025-04-19"

s3 = boto3.client("s3")
keys = []
paginator = s3.get_paginator("list_objects_v2")
for page in paginator.paginate(Bucket=BUCKET, Prefix=f"raw/{DATE}/"):
    for obj in page.get("Contents", []):
        if obj["Key"].endswith(".json.gz"):
            keys.append(obj["Key"])

print(f"ETL for {DATE}: {len(keys)} files")


def etl_one_file(key: str) -> dict:
    import gzip
    import json
    import os
    import boto3
    import psycopg2
    from psycopg2.extras import execute_values

    s3 = boto3.client("s3")
    body = s3.get_object(Bucket="my-events-bucket", Key=key)["Body"].read()
    rows_in = [json.loads(line) for line in gzip.decompress(body).splitlines() if line]

    rows_out = [
        (
            r["event_id"],
            r["user_id"],
            r["event_type"],
            r["ts"],
            float(r.get("amount") or 0),
            r.get("country", "XX").upper(),
        )
        for r in rows_in
        if r.get("event_type") in ("click", "purchase", "signup")
    ]

    conn = psycopg2.connect(os.environ["DATABASE_URL"])
    try:
        with conn, conn.cursor() as cur:
            execute_values(
                cur,
                """
                INSERT INTO events (event_id, user_id, event_type, ts, amount, country)
                VALUES %s
                ON CONFLICT (event_id) DO NOTHING
                """,
                rows_out,
                page_size=1000,
            )
    finally:
        conn.close()

    return {"key": key, "rows_in": len(rows_in), "rows_out": len(rows_out)}


# Burla grows the cluster on demand, capped at 1,000 concurrent workers (protects the DB)
done = 0
total_rows = 0
for r in remote_parallel_map(
    etl_one_file,
    keys,
    func_cpu=1,
    func_ram=2,
    max_parallelism=1000,
    generator=True,
    grow=True,
):
    done += 1
    total_rows += r["rows_out"]
    if done % 100 == 0:
        print(f"{done}/{len(keys)} files, {total_rows:,} rows loaded")

print(f"done: {total_rows:,} rows loaded")
```

## Why This Is Better

**vs Airflow** — Airflow is a scheduler, a metadata DB, a webserver, and a DAG library. For a daily cron over a file drop, it's five moving parts too many. Burla is a Python script you run from cron or CI.

**vs Dagster / Prefect** — same shape. Both are excellent for complex, shared orchestration, but both introduce a deploy target and a DSL. You don't need them for "fan out over files, write to Postgres."

**vs AWS Glue** — Glue needs a job, a crawler, a catalog, and Spark. Heavy for small-to-medium ETL.

**vs AWS Batch** — you'd still have to build a container and set up a queue and compute environment. Burla skips all of that.

## How It Works

Burla runs `etl_one_file` on up to 10,000 workers, capped at 1,000 concurrent via `max_parallelism`. The cap protects your Postgres connection pool. `generator=True` yields results as each file finishes so you can print progress or write a summary incrementally. Exceptions from any file are re-raised on your client with the traceback.

## When To Use This

- Daily/hourly file-drop ETL with 100 to 100,000 files.
- One-off backfills ("reprocess the last 90 days").
- Simple pipelines where each file is independent.
- Data loads where you control the load concurrency at the database.

## When NOT To Use This

- DAGs with real cross-task dependencies, retries-with-state, and SLAs — use Airflow/Dagster.
- Streaming pipelines — use Kafka + a streaming runtime.
- Complex scheduling with calendars, sensors, and SLA alerts — use a scheduler that's built for it.
