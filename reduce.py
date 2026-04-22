"""Two-stage reduce v2: phrases + tokens + samples, merged locally."""
from __future__ import annotations

import argparse
import io
import json
import os
import pickle
import time
from collections import Counter, defaultdict
from pathlib import Path
from typing import Dict, List

from burla import remote_parallel_map


AGG_DIR = "/workspace/shared/wpi/agg"


def _load_agg_shards() -> List[str]:
    return sorted(f for f in os.listdir(AGG_DIR) if f.endswith(".json"))


def reduce_bucket(shard_names: List[str]) -> bytes:
    country_photos = Counter()
    country_phrases: Dict[str, Counter] = defaultdict(Counter)
    country_tokens: Dict[str, Counter] = defaultdict(Counter)
    admin_phrases: Dict[str, Counter] = defaultdict(Counter)
    city_phrases: Dict[str, Counter] = defaultdict(Counter)
    country_samples: Dict[str, list] = defaultdict(list)
    total_rows = 0

    for name in shard_names:
        path = os.path.join(AGG_DIR, name)
        try:
            with open(path) as f:
                d = json.load(f)
        except Exception:
            continue
        total_rows += d.get("n_rows", 0)
        for cc, n in d.get("country_photos", {}).items():
            country_photos[cc] += n
        for cc, m in d.get("country_phrases", {}).items():
            country_phrases[cc].update(m)
        for cc, m in d.get("country_tokens", {}).items():
            country_tokens[cc].update(m)
        for k, m in d.get("admin_phrases", {}).items():
            admin_phrases[k].update(m)
        for k, m in d.get("city_phrases", {}).items():
            city_phrases[k].update(m)
        for cc, samples in d.get("country_samples", {}).items():
            if len(country_samples[cc]) < 48:
                country_samples[cc].extend(samples[: 48 - len(country_samples[cc])])

    payload = {
        "country_photos": country_photos,
        "country_phrases": country_phrases,
        "country_tokens": country_tokens,
        "admin_phrases": admin_phrases,
        "city_phrases": city_phrases,
        "country_samples": dict(country_samples),
        "total_rows": total_rows,
    }
    buf = io.BytesIO()
    pickle.dump(payload, buf, protocol=4)
    return buf.getvalue()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--buckets", type=int, default=64)
    args = ap.parse_args()

    def _list_shards(_: int) -> List[str]:
        return _load_agg_shards()

    print("listing agg shards from cluster ...")
    shard_names = remote_parallel_map(_list_shards, [0], func_cpu=1, grow=True, spinner=True)[0]
    print(f"  found {len(shard_names)} agg files")

    n = args.buckets
    buckets = [shard_names[i::n] for i in range(n)]
    print(f"partitioning into {n} buckets of ~{len(buckets[0])} files each")

    t0 = time.time()
    bucket_blobs = remote_parallel_map(
        reduce_bucket,
        buckets,
        func_cpu=1,
        func_ram=4,
        grow=True,
        max_parallelism=n,
        spinner=True,
    )
    print(f"got {len(bucket_blobs)} partial aggregates in {time.time() - t0:.1f}s")

    country_photos = Counter()
    country_phrases: Dict[str, Counter] = defaultdict(Counter)
    country_tokens: Dict[str, Counter] = defaultdict(Counter)
    admin_phrases: Dict[str, Counter] = defaultdict(Counter)
    city_phrases: Dict[str, Counter] = defaultdict(Counter)
    country_samples: Dict[str, list] = defaultdict(list)
    total_rows = 0

    for blob in bucket_blobs:
        p = pickle.loads(blob)
        country_photos.update(p["country_photos"])
        for cc, c in p["country_phrases"].items():
            country_phrases[cc].update(c)
        for cc, c in p["country_tokens"].items():
            country_tokens[cc].update(c)
        for k, c in p["admin_phrases"].items():
            admin_phrases[k].update(c)
        for k, c in p["city_phrases"].items():
            city_phrases[k].update(c)
        for cc, samples in p["country_samples"].items():
            if len(country_samples[cc]) < 96:
                country_samples[cc].extend(samples[: 96 - len(country_samples[cc])])
        total_rows += p["total_rows"]

    reduced = {
        "n_rows_total": total_rows,
        "n_countries": len(country_photos),
        "country_photos": dict(country_photos.most_common()),
        "country_top_phrases": {cc: dict(c.most_common(120)) for cc, c in country_phrases.items()},
        "country_top_tokens": {cc: dict(c.most_common(120)) for cc, c in country_tokens.items()},
        "admin_top_phrases": {
            k: dict(v.most_common(40))
            for k, v in admin_phrases.items()
            if sum(v.values()) >= 100
        },
        "city_top_phrases": {
            k: dict(v.most_common(25))
            for k, v in city_phrases.items()
            if sum(v.values()) >= 50
        },
        "country_samples": dict(country_samples),
    }

    out_path = Path(__file__).parent / "samples" / "wpi_reduced_v2.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(reduced))
    print(f"wrote {out_path} ({out_path.stat().st_size / 1024 / 1024:.1f} MB)")
    print(f"  countries: {len(country_photos)}  |  total_rows: {total_rows:,}")
    print(f"  top 10 countries:")
    for cc, n in country_photos.most_common(10):
        print(f"    {cc}: {n:,}")


if __name__ == "__main__":
    main()
