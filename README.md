# Process Thousands of Parquet Files in Parallel in Python

Read and aggregate thousands of Parquet files on S3 at the same time, using thousands of machines, with one function call.

## The Problem

You have 5,000+ Parquet files in S3 (daily partitions, user shards, event logs). You want per-file stats: row counts, column sums, null rates, schema checks, deduped IDs.

A single-process `pyarrow` loop takes hours. `pandas.read_parquet("s3://...")` globbing brings every file through your laptop. Spark is overkill and slow to start. Threads help a little, then you hit CPU and network limits on one box.

You don't want a cluster. You just want one function to run on every file at the same time.

## The Solution (Burla)

`remote_parallel_map` runs your Python function on thousands of cloud VMs at the same time. One file per worker. You get back a list of results.

No Dockerfile. No cluster config. No job YAML. The exact same function runs locally and on 5,000 machines.

## Example

```python
import boto3
import pandas as pd
import pyarrow.parquet as pq
from burla import remote_parallel_map

s3 = boto3.client("s3")
BUCKET = "my-events-bucket"

response = s3.list_objects_v2(Bucket=BUCKET, Prefix="events/2025/")
parquet_keys = [obj["Key"] for obj in response["Contents"] if obj["Key"].endswith(".parquet")]
while response.get("IsTruncated"):
    response = s3.list_objects_v2(
        Bucket=BUCKET, Prefix="events/2025/", ContinuationToken=response["NextContinuationToken"]
    )
    parquet_keys += [obj["Key"] for obj in response["Contents"] if obj["Key"].endswith(".parquet")]

print(f"found {len(parquet_keys)} parquet files")


def scan_parquet_file(key: str) -> dict:
    import boto3
    import pyarrow.parquet as pq

    s3 = boto3.client("s3")
    obj = s3.get_object(Bucket=BUCKET, Key=key)
    table = pq.read_table(obj["Body"])

    return {
        "key": key,
        "rows": table.num_rows,
        "bytes": obj["ContentLength"],
        "distinct_users": table.column("user_id").combine_chunks().unique().length(),
        "revenue_sum": float(table.column("revenue").to_pandas().sum()),
        "null_user_rate": table.column("user_id").null_count / max(table.num_rows, 1),
    }


# 5,000 parquet files -> 5,000 workers running in parallel
stats = remote_parallel_map(scan_parquet_file, parquet_keys, func_cpu=1, func_ram=4)

df = pd.DataFrame(stats)
print(df.describe())
df.to_csv("parquet_scan_report.csv", index=False)
```

## Why This Is Better

**vs Ray** — no `ray.init`, no head node, no actor classes. `scan_parquet_file` is a plain function. No tuning the Ray scheduler to not OOM on 5,000 tasks.

**vs Dask** — `dask.dataframe.read_parquet` is great for one logical table, not for independent per-file jobs. You don't need a scheduler process, cluster manager, or `dask-worker` fleet for this.

**vs AWS Batch** — no Dockerfile, no ECR push, no job definition, no compute environment, no queue. Cold starts on Batch are minutes; Burla workers are already warm.

## How It Works

You call `remote_parallel_map(fn, keys)`. Burla ships your function and the input list to a pool of pre-warmed cloud workers. Each worker pulls one key, runs `fn(key)`, returns the result. Exceptions on the worker are re-raised on your laptop with the full traceback.

## When To Use This

- 1,000+ Parquet files in S3/GCS and you want per-file stats or per-file transforms.
- Daily data QA jobs over partitioned event logs.
- Rewriting thousands of Parquet files to a new schema or compression.
- Building a per-file index (min/max, distinct counts, bloom filters).

## When NOT To Use This

- You need to shuffle or join across files — use Spark or DuckDB.
- The files are small (<1 MB each) and there are only a few hundred — local threads are faster.
- You need sub-second interactive queries — use a query engine, not a batch map.
