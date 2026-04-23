"""Fan out hunt_worst.process_chunk across Burla at 1000 CPUs."""
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

from hunt_worst import process_chunk
from hate_lexicon import CATEGORIES


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
    print(f"worst-of-worst hunt: {len(jobs)} chunks, {total_gb:.1f} GB streaming, "
          f"up to {args.max_parallelism} CPUs")
    print(f"slur categories: {list(CATEGORIES)}")

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
    total_hits = sum(r.get("n_hits", 0) for r in ok)
    cat_totals: dict = defaultdict(int)
    by_cat: dict = defaultdict(lambda: {"n_parsed": 0, "n_hits": 0, "cat_totals": defaultdict(int)})
    for r in ok:
        c = r.get("category", "?")
        by_cat[c]["n_parsed"] += r.get("n_parsed", 0)
        by_cat[c]["n_hits"] += r.get("n_hits", 0)
        for slur_cat, n in (r.get("cat_totals") or {}).items():
            cat_totals[slur_cat] += n
            by_cat[c]["cat_totals"][slur_cat] += n

    summary = {
        "elapsed_seconds": round(elapsed, 2),
        "elapsed_minutes": round(elapsed / 60, 2),
        "chunks_submitted": len(jobs),
        "chunks_succeeded": len(ok),
        "chunks_failed": len(bad),
        "total_reviews_parsed": total_parsed,
        "total_hits": total_hits,
        "hits_per_million": round(1e6 * total_hits / max(total_parsed, 1), 2),
        "cat_totals": dict(sorted(cat_totals.items(), key=lambda kv: -kv[1])),
        "by_category": {
            c: {"n_parsed": v["n_parsed"], "n_hits": v["n_hits"],
                "cat_totals": dict(v["cat_totals"])}
            for c, v in sorted(by_cat.items(), key=lambda kv: -kv[1]["n_hits"])
        },
        "first_failures": bad[:10],
    }
    out = Path(__file__).parent / "samples" / "worst_summary.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(summary, indent=2) + "\n")

    print()
    print("=" * 72)
    print(f"elapsed: {summary['elapsed_minutes']} min  |  "
          f"parsed: {total_parsed:,}  hits: {total_hits:,}  "
          f"({summary['hits_per_million']}/M)")
    print(f"succeeded: {len(ok)}  failed: {len(bad)}")
    print(f"category totals: {summary['cat_totals']}")
    print(f"wrote {out}")


if __name__ == "__main__":
    main()
