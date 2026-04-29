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


# 5,000 parquet files -> Burla grows the cluster on demand and runs them in parallel
stats = remote_parallel_map(scan_parquet_file, parquet_keys, func_cpu=1, func_ram=4, grow=True)

df = pd.DataFrame(stats)
print(df.describe())
df.to_csv("parquet_scan_report.csv", index=False)
