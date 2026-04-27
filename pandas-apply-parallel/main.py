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

    # Force numpy-backed strings instead of arrow-backed (pandas 3.x default)
    # so downstream .apply / .values paths stay purely numpy.
    pd.set_option("future.infer_string", False)

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
