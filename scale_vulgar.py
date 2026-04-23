"""Fan out the vulgar-hunter across Burla at 500+ CPUs.

Reuses the same byte-range planning as scale.py but targets the new
hunt_vulgar.process_chunk worker.
"""
from __future__ import annotations

import argparse
import json
import math
import time
from collections import defaultdict
from pathlib import Path
from typing import List, Tuple

from burla import remote_parallel_map
from huggingface_hub import HfApi

from hunt_vulgar import process_chunk, HARD_ROOTS


def plan_jobs(chunk_mb: int = 500) -> List[Tuple[str, int, int, str]]:
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
        span = size // n
        for i in range(n):
            s = i * span
            e = (i + 1) * span if i < n - 1 else size
            cat = path.rsplit("/", 1)[-1].replace(".jsonl", "")
            jobs.append((path, s, e, f"{cat}_{i:03d}"))
    return jobs


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--chunk-mb", type=int, default=500)
    ap.add_argument("--max-parallelism", type=int, default=1000)
    ap.add_argument("--limit", type=int, default=0)
    args = ap.parse_args()

    jobs = plan_jobs(args.chunk_mb)
    if args.limit:
        jobs = jobs[: args.limit]
    total_gb = sum(j[2] - j[1] for j in jobs) / 1e9
    print(f"vulgar hunt: {len(jobs)} chunks, {total_gb:.1f} GB streaming, "
          f"up to {args.max_parallelism} CPUs")
    print(f"hard roots targeted: {', '.join(HARD_ROOTS)}")

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

    ok = [r for r in results if "error" not in r]
    bad = [r for r in results if "error" in r]
    total_parsed = sum(r.get("n_parsed", 0) for r in ok)
    total_hits = sum(r.get("n_hard_hits", 0) for r in ok)
    root_totals: dict = defaultdict(int)
    by_cat: dict = defaultdict(lambda: {"n_parsed": 0, "n_hard_hits": 0})
    for r in ok:
        c = r.get("category", "?")
        by_cat[c]["n_parsed"] += r.get("n_parsed", 0)
        by_cat[c]["n_hard_hits"] += r.get("n_hard_hits", 0)
        for rt, n in (r.get("root_counts") or {}).items():
            root_totals[rt] += n

    summary = {
        "elapsed_seconds": round(elapsed, 2),
        "elapsed_minutes": round(elapsed / 60, 2),
        "chunks_submitted": len(jobs),
        "chunks_succeeded": len(ok),
        "chunks_failed": len(bad),
        "total_reviews_parsed": total_parsed,
        "total_hard_hits": total_hits,
        "hits_per_million": round(1e6 * total_hits / max(total_parsed, 1), 2),
        "root_totals": dict(sorted(root_totals.items(), key=lambda kv: -kv[1])),
        "by_category": {c: v for c, v in sorted(by_cat.items(), key=lambda kv: -kv[1]["n_hard_hits"])},
        "first_failures": bad[:10],
    }
    out = Path(__file__).parent / "samples" / "vulgar_summary.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(summary, indent=2) + "\n")

    print()
    print("=" * 72)
    print(f"elapsed: {summary['elapsed_minutes']} min  |  "
          f"parsed: {total_parsed:,}  hard hits: {total_hits:,}  "
          f"({summary['hits_per_million']}/M)")
    print(f"succeeded: {len(ok)}  failed: {len(bad)}")
    print(f"root breakdown: {summary['root_totals']}")
    print(f"wrote {out}")


if __name__ == "__main__":
    main()
