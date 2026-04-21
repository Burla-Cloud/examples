# Parallelize pandas `.apply()` Across Large Datasets in Python

Run your `df.apply(fn)` across 1,200 cloud workers at the same time. No `swifter`, no `modin`, no Dask cluster.

## The Problem

You have a 1 TB Parquet dataset and a real `apply` function: regex parse, call a scoring function, enrich with an API lookup, compute a per-row embedding.

`df.apply(fn, axis=1)` on one machine takes 12+ hours and pegs one core. `swifter` helps a little — it parallelizes across local cores, but not across machines. `modin` requires Ray or Dask running underneath. Moving to Spark means rewriting `fn` as a UDF, dealing with Arrow serialization, and debugging JVM stack traces.

You just want `apply` to run 1,200x faster without rewriting it.

## The Solution (Burla)

Split the Parquet into 1,200 row-group chunks by a cheap key. Hand Burla the chunk keys. Each worker reads its chunk, runs `apply` on a real pandas DataFrame, returns the result. Concat on the client.

The function is unchanged pandas code. No UDF rewrite. No cluster.

## Example

```python
import pandas as pd
import pyarrow.dataset as ds
from burla import remote_parallel_map

DATASET = "s3://my-bucket/events/"

dataset = ds.dataset(DATASET, format="parquet")
all_users = dataset.to_table(columns=["user_id"]).column("user_id").unique().to_pylist()

N_CHUNKS = 1200
chunks = [all_users[i::N_CHUNKS] for i in range(N_CHUNKS)]
print(f"splitting {len(all_users):,} users into {N_CHUNKS} chunks")


def apply_on_chunk(user_ids: list[str]) -> pd.DataFrame:
    import re
    import pandas as pd
    import pyarrow.dataset as ds

    dataset = ds.dataset("s3://my-bucket/events/", format="parquet")
    df = dataset.filter(ds.field("user_id").isin(user_ids)).to_table().to_pandas()

    utm_re = re.compile(r"utm_source=([^&]+)")

    def enrich(row):
        src = utm_re.search(row["url"] or "")
        return pd.Series({
            "utm_source": src.group(1) if src else None,
            "url_len": len(row["url"] or ""),
            "is_mobile": "Mobile" in (row["user_agent"] or ""),
            "revenue_bucket": "high" if row["revenue"] > 100 else "low",
        })

    enriched = df.apply(enrich, axis=1)
    return pd.concat([df, enriched], axis=1)


# 1,200 chunks -> Burla grows the cluster on demand and runs pandas.apply in parallel
frames = remote_parallel_map(apply_on_chunk, chunks, func_cpu=2, func_ram=8, grow=True)

final = pd.concat(frames, ignore_index=True)
final.to_parquet("enriched.parquet")
print(final.shape)
```

## Why This Is Better

**vs Ray / modin** — `modin.pandas` requires a running Ray/Dask cluster and silently falls back to pandas for unsupported ops. You still debug Ray shutdowns and memory pressure.

**vs Dask DataFrame** — Dask `.apply` with `meta=` and `map_partitions` is close, but you pay for cluster startup, the scheduler, and the Dask dialect. `apply` with arbitrary Python (regex, dicts, external calls) is slow on Dask.

**vs Spark / PySpark** — Spark UDFs force you to rewrite the function or accept Arrow UDFs that still have overhead per batch. No one wants to debug Py4J.

**vs `swifter`** — single-machine parallelism only. Won't get you 1,200x.

## How It Works

You pick a cheap partition key (user_id, date, hash). You build a list of chunks of that key. Burla starts 1,200 workers, each pulls its chunk, loads just those rows from Parquet, runs normal pandas `.apply`, returns a DataFrame. The client concatenates.

## When To Use This

- `apply` with custom Python (regex, API enrichment, `json.loads`, per-row scoring).
- Big datasets that don't fit in one machine's RAM but each row is cheap to process.
- One-off data prep jobs where rewriting to a SQL engine is overkill.
- Per-row embedding or feature computation where the function is heavy.

## When NOT To Use This

- The operation is a simple groupby or aggregation — use DuckDB or SQL directly.
- The full dataset fits in RAM and the apply is vectorizable — just use `pandas` or `polars`.
- You need windowed operations across chunk boundaries (rolling across user history) — partition so related rows land in the same chunk first.
