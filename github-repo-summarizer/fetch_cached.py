"""Fetch the already-completed BQ job result via the Storage Read API.

The prepare.py query ran for 2.96 TB scan but the REST download was too slow
to complete. BQ caches the query result in a temp destination table for 24h;
we read it back via bqstorage which is ~100x faster.
"""
import sys, time
from pathlib import Path
from google.cloud import bigquery
from google.cloud import bigquery_storage

JOB_ID = sys.argv[1] if len(sys.argv) > 1 else "36fb00f5-fd28-4878-b7b6-9a5fb7c02eb9"
PROJECT = "burla-testing"
OUT = Path(__file__).parent / "samples" / "readmes.parquet"


def main() -> None:
    t0 = time.time()
    client = bigquery.Client(project=PROJECT)
    bq_storage = bigquery_storage.BigQueryReadClient()

    job = client.get_job(JOB_ID)
    print(f"[0] job {JOB_ID} state={job.state}")
    print(f"    destination: {job.destination}")

    # Pull via BQStorage (stream)
    print("[1] streaming results via BigQuery Storage Read API...")
    result = job.result()  # QueryJob.result() returns a RowIterator
    df = result.to_dataframe(bqstorage_client=bq_storage)
    print(f"[2] fetched {len(df):,} rows in {time.time()-t0:.1f}s")

    OUT.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(OUT, index=False, compression="zstd")
    print(f"[3] wrote {OUT} ({OUT.stat().st_size/1e6:.1f} MB)")
    print(f"    total elapsed: {time.time()-t0:.1f}s")
    if "lang" in df.columns:
        print("\nlang top 15:")
        print(df["lang"].value_counts().head(15).to_string())
    print(f"\navg size: {df['size'].mean():.0f} chars")
    print(f"total elapsed: {time.time()-t0:.1f}s")


if __name__ == "__main__":
    main()
