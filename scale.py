"""Scale-out: partition 275 GB of Amazon review jsonls into ~600 byte chunks,
fan out to Burla workers, and distill the most unhinged reviews per category.

Target: >=500 concurrent workers (plan floor). Cluster capacity is 1,040 slots
so we go as wide as remote_parallel_map allows.
"""
from __future__ import annotations

import argparse
import json
import math
import time
from pathlib import Path
from typing import List, Tuple

from burla import remote_parallel_map
from huggingface_hub import HfApi

from pipeline import process_chunk


def plan_jobs(chunk_mb: int = 500) -> List[Tuple[str, int, int, str]]:
    """Emit (file, byte_start, byte_end, chunk_id) tuples for every chunk."""
    api = HfApi()
    infos = list(api.list_repo_tree(
        "McAuley-Lab/Amazon-Reviews-2023",
        path_in_repo="raw/review_categories",
        repo_type="dataset",
        recursive=False,
    ))
    files = sorted(
        [(i.path, i.size) for i in infos if getattr(i, "size", 0) > 0],
        key=lambda kv: -kv[1],
    )
    chunk_bytes = chunk_mb * 1024 * 1024
    jobs: List[Tuple[str, int, int, str]] = []
    for path, size in files:
        n = max(1, math.ceil(size / chunk_bytes))
        # Re-balance to even sizes
        span = size // n
        for i in range(n):
            start = i * span
            end = (i + 1) * span if i < n - 1 else size
            cat = path.rsplit("/", 1)[-1].replace(".jsonl", "")
            chunk_id = f"{cat}_{i:03d}"
            jobs.append((path, start, end, chunk_id))
    return jobs


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--chunk-mb", type=int, default=500)
    ap.add_argument("--max-parallelism", type=int, default=1000)
    ap.add_argument("--limit", type=int, default=0,
                    help="debug: process only first N chunks")
    args = ap.parse_args()

    jobs = plan_jobs(args.chunk_mb)
    if args.limit:
        jobs = jobs[: args.limit]
    total_gb = sum(j[2] - j[1] for j in jobs) / 1e9
    print(f"planned {len(jobs)} chunks across {len({j[0] for j in jobs})} files"
          f", total {total_gb:.1f} GB")
    print(f"  biggest chunk: {max(j[2] - j[1] for j in jobs) / 1e6:.1f} MB")
    print(f"  smallest chunk: {min(j[2] - j[1] for j in jobs) / 1e6:.1f} MB")

    t0 = time.time()
    results = remote_parallel_map(
        process_chunk,
        jobs,
        func_cpu=1,
        func_ram=4,
        grow=True,
        max_parallelism=args.max_parallelism,
        spinner=True,
    )
    elapsed = time.time() - t0

    successes = [r for r in results if "error" not in r]
    failures = [r for r in results if "error" in r]

    total_parsed = sum(r.get("n_parsed", 0) for r in successes)
    total_profane = sum(r.get("n_profane", 0) for r in successes)
    by_cat = {}
    for r in successes:
        c = r.get("category", "?")
        by_cat.setdefault(c, {"n_parsed": 0, "n_profane": 0, "rating_counts": {}})
        by_cat[c]["n_parsed"] += r.get("n_parsed", 0)
        by_cat[c]["n_profane"] += r.get("n_profane", 0)
        for k, v in (r.get("rating_counts") or {}).items():
            by_cat[c]["rating_counts"][str(k)] = by_cat[c]["rating_counts"].get(str(k), 0) + v

    summary = {
        "elapsed_seconds": round(elapsed, 2),
        "elapsed_minutes": round(elapsed / 60, 2),
        "chunks_submitted": len(jobs),
        "chunks_succeeded": len(successes),
        "chunks_failed": len(failures),
        "total_reviews_parsed": total_parsed,
        "total_profane_reviews": total_profane,
        "throughput_reviews_per_sec": round(total_parsed / elapsed, 1) if elapsed else 0,
        "by_category_parsed": {c: v["n_parsed"] for c, v in sorted(by_cat.items(), key=lambda kv: -kv[1]["n_parsed"])},
        "by_category_profane": {c: v["n_profane"] for c, v in sorted(by_cat.items(), key=lambda kv: -kv[1]["n_profane"])},
        "first_failures": failures[:10],
    }

    out = Path(__file__).parent / "samples" / "scale_summary.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(summary, indent=2) + "\n")
    print()
    print("=" * 80)
    print(f"elapsed: {summary['elapsed_minutes']} min  |  "
          f"reviews/sec: {summary['throughput_reviews_per_sec']:,.0f}")
    print(f"parsed: {total_parsed:,}  profane: {total_profane:,}  "
          f"({100 * total_profane / max(total_parsed, 1):.2f}%)")
    print(f"succeeded: {len(successes)}  |  failed: {len(failures)}")
    print(f"wrote {out}")


if __name__ == "__main__":
    main()
