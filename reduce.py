"""Reduce all per-chunk ARD outputs on /workspace/shared/ard/shards
into one JSON per category plus a global findings JSON.

Strategy: fan out one worker per category (30+). Each category worker reads
all its shard files, merges heaps per signal, aggregates rating counts, and
writes /workspace/shared/ard/reduced/{category}.json. Client reads and merges.
"""
from __future__ import annotations

import argparse
import io
import json
import os
import pickle
import time
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List

from burla import remote_parallel_map


SHARD_DIR = "/workspace/shared/ard/shards"
OUT_DIR = "/workspace/shared/ard/reduced"
TOP_K_GLOBAL = 100


def list_categories() -> Dict[str, List[str]]:
    """Group shard file names by category prefix."""
    names = [f for f in os.listdir(SHARD_DIR) if f.endswith(".json")]
    by_cat: Dict[str, List[str]] = {}
    for n in names:
        # filename pattern: "{category}_{idx:03d}.json"
        base = n[:-5]
        if "_" in base:
            cat = base.rsplit("_", 1)[0]
        else:
            cat = base
        by_cat.setdefault(cat, []).append(n)
    return by_cat


def reduce_category(category: str, names: List[str]) -> bytes:
    """Called on cluster worker — merges all shard outputs for one category."""
    n_parsed = 0
    n_profane = 0
    rating_counts = {1: 0, 2: 0, 3: 0, 4: 0, 5: 0}
    length_sum = 0

    # signal -> sorted list of {score, review}
    signals: Dict[str, List[Dict[str, Any]]] = {}
    seen_ids = set()  # dedupe by (asin + user_id + text-hash) since byte-range
                      # boundaries can double-count a handful of lines

    for name in names:
        path = os.path.join(SHARD_DIR, name)
        try:
            with open(path) as f:
                d = json.load(f)
        except Exception:
            continue
        n_parsed += d.get("n_parsed", 0)
        n_profane += d.get("n_profane", 0)
        length_sum += d.get("length_sum", 0)
        for k, v in (d.get("rating_counts") or {}).items():
            rating_counts[int(k)] = rating_counts.get(int(k), 0) + v
        for sig, items in (d.get("top") or {}).items():
            for it in items:
                r = it.get("review") or {}
                # Dedupe key
                text_hash = hash((r.get("asin"), r.get("user_id"), (r.get("text") or "")[:200]))
                if text_hash in seen_ids:
                    continue
                seen_ids.add(text_hash)
                signals.setdefault(sig, []).append({
                    "score": it.get("score"),
                    "review": r,
                })

    # Keep top-K per signal
    for sig in signals:
        signals[sig].sort(key=lambda x: -x["score"])
        signals[sig] = signals[sig][:TOP_K_GLOBAL]

    payload = {
        "category": category,
        "n_parsed": n_parsed,
        "n_profane": n_profane,
        "rating_counts": rating_counts,
        "length_sum": length_sum,
        "mean_length": round(length_sum / n_parsed, 1) if n_parsed else 0,
        "profanity_rate": round(n_profane / max(n_parsed, 1), 4),
        "top": signals,
    }

    # Write to shared disk + return a pickled blob to client
    os.makedirs(OUT_DIR, exist_ok=True)
    with open(os.path.join(OUT_DIR, f"{category}.json"), "w") as f:
        json.dump(payload, f)
    buf = io.BytesIO()
    pickle.dump(payload, buf, protocol=4)
    return buf.getvalue()


def main() -> None:
    ap = argparse.ArgumentParser()
    args = ap.parse_args()

    def _discover(_: int) -> Dict[str, List[str]]:
        return list_categories()

    print("discovering shard files on cluster ...")
    by_cat = remote_parallel_map(_discover, [0], func_cpu=1, grow=True, spinner=True)[0]
    n = sum(len(v) for v in by_cat.values())
    print(f"  {len(by_cat)} categories, {n} shard files")

    jobs = [(cat, names) for cat, names in sorted(by_cat.items())]

    t0 = time.time()
    blobs = remote_parallel_map(
        reduce_category,
        jobs,
        func_cpu=1,
        func_ram=8,
        grow=True,
        max_parallelism=len(jobs),
        spinner=True,
    )
    print(f"reduce took {time.time() - t0:.1f}s")

    total_rc = defaultdict(int)
    merged = {
        "n_categories": len(jobs),
        "total_parsed": 0,
        "total_profane": 0,
        "total_rating_counts": {},
        "categories": {},
    }
    for blob in blobs:
        p = pickle.loads(blob)
        cat = p["category"]
        merged["total_parsed"] += p["n_parsed"]
        merged["total_profane"] += p["n_profane"]
        for k, v in (p["rating_counts"] or {}).items():
            total_rc[int(k)] += v
        merged["categories"][cat] = p
    merged["total_rating_counts"] = dict(sorted(total_rc.items()))

    out_path = Path(__file__).parent / "samples" / "ard_reduced.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(merged))
    print(f"wrote {out_path} ({out_path.stat().st_size / 1024 / 1024:.1f} MB)")
    print(f"  total_parsed:  {merged['total_parsed']:,}")
    print(f"  total_profane: {merged['total_profane']:,} "
          f"({100 * merged['total_profane'] / max(merged['total_parsed'], 1):.2f}%)")
    print(f"  rating distribution:")
    for r in sorted(merged["total_rating_counts"]):
        print(f"    {r}★: {merged['total_rating_counts'][r]:,}")


if __name__ == "__main__":
    main()
