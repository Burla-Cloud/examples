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


# 10,000 files -> up to 1,000 workers writing to Postgres at once (protects the DB)
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
