"""Reduce all vulgar-hunter shard outputs into per-category JSONs and a
global top list.

One Burla worker per category merges every shard output whose filename starts
with that category, keeping the global top 200 per category by variety_score.
A final in-memory pass then builds a cross-category global top 500.
"""
from __future__ import annotations

import argparse
import heapq
import json
import time
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Tuple

from burla import remote_parallel_map


SHARDS_DIR = "/workspace/shared/ard_vulgar/shards"
REDUCED_DIR = "/workspace/shared/ard_vulgar/reduced"
K_PER_CATEGORY = 200


def reduce_category(category: str, shard_files: List[str]) -> Dict[str, Any]:
    import os, json, heapq

    os.makedirs(REDUCED_DIR, exist_ok=True)

    n_parsed = 0
    n_hard_hits = 0
    root_counts: Dict[str, int] = {}
    heap: List[Tuple[float, int, Dict[str, Any]]] = []
    tie = 0

    for fp in shard_files:
        try:
            with open(fp) as f:
                d = json.load(f)
        except Exception:
            continue
        n_parsed += d.get("n_parsed", 0)
        n_hard_hits += d.get("n_hard_hits", 0)
        for rt, n in (d.get("root_counts") or {}).items():
            root_counts[rt] = root_counts.get(rt, 0) + n
        for item in d.get("top", []):
            s = float(item.get("score") or 0)
            if s <= 0:
                continue
            tie += 1
            rev = item["review"]
            if len(heap) < K_PER_CATEGORY:
                heapq.heappush(heap, (s, tie, rev))
            elif s > heap[0][0]:
                heapq.heapreplace(heap, (s, tie, rev))

    heap.sort(key=lambda x: -x[0])
    payload = {
        "category": category,
        "n_parsed": n_parsed,
        "n_hard_hits": n_hard_hits,
        "hits_per_million": round(1e6 * n_hard_hits / max(n_parsed, 1), 2),
        "root_counts": dict(sorted(root_counts.items(), key=lambda kv: -kv[1])),
        "top": [{"score": round(s, 3), "review": r} for s, _k, r in heap],
    }
    out = os.path.join(REDUCED_DIR, f"{category}.json")
    with open(out, "w") as f:
        json.dump(payload, f)
    # Return the full payload so the client can build a global top list
    # without needing to read back from /workspace/shared locally.
    return payload


def list_shards(_dummy: int) -> Dict[str, List[str]]:
    """Run on a Burla worker to enumerate shard files in the GCS-backed FS."""
    import os as _os
    shard_files = sorted(_os.path.join(SHARDS_DIR, f)
                         for f in _os.listdir(SHARDS_DIR)
                         if f.endswith(".json"))
    partition: Dict[str, List[str]] = {}
    for fp in shard_files:
        base = _os.path.basename(fp).rsplit(".json", 1)[0]
        cat = "_".join(base.split("_")[:-1])
        partition.setdefault(cat, []).append(fp)
    return {"partition": partition, "total": len(shard_files)}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--max-parallelism", type=int, default=40)
    args = ap.parse_args()

    print("listing shards via burla worker ...")
    listing = remote_parallel_map(list_shards, [0], grow=True, spinner=True)[0]
    shard_total = listing["total"]
    print(f"reducing {shard_total} shard files")

    by_cat = listing["partition"]
    jobs = [(cat, files) for cat, files in by_cat.items()]
    print(f"dispatching {len(jobs)} category-reduce jobs")

    t0 = time.time()
    results = remote_parallel_map(
        reduce_category,
        jobs,
        func_cpu=2,
        func_ram=8,
        grow=True,
        max_parallelism=min(args.max_parallelism, len(jobs)),
        spinner=True,
    )
    elapsed = time.time() - t0

    # results is a list of per-category payloads; merge into global top 500.
    global_heap: List[Tuple[float, int, Dict[str, Any]]] = []
    tie = 0
    total_parsed = 0
    total_hits = 0
    cat_summaries: List[Dict[str, Any]] = []
    for d in results:
        if not d:
            continue
        total_parsed += d.get("n_parsed", 0)
        total_hits += d.get("n_hard_hits", 0)
        cat_summaries.append({
            "category": d["category"],
            "n_parsed": d["n_parsed"],
            "n_hard_hits": d["n_hard_hits"],
            "hits_per_million": d["hits_per_million"],
            "root_counts": d["root_counts"],
        })
        for item in d.get("top", []):
            s = float(item.get("score") or 0)
            tie += 1
            if len(global_heap) < 500:
                heapq.heappush(global_heap, (s, tie, {**item["review"], "_score": s}))
            elif s > global_heap[0][0]:
                heapq.heapreplace(global_heap, (s, tie, {**item["review"], "_score": s}))

    global_heap.sort(key=lambda x: -x[0])
    global_top = [r for _s, _k, r in global_heap]

    summary = {
        "elapsed_seconds": round(elapsed, 2),
        "total_reviews_parsed": total_parsed,
        "total_hard_hits": total_hits,
        "hits_per_million": round(1e6 * total_hits / max(total_parsed, 1), 2),
        "categories": sorted(cat_summaries, key=lambda c: -c["n_hard_hits"]),
        "global_top_500_head": [
            {"score": r.get("_score"), "title": r.get("title"),
             "category": r.get("category"), "roots": r.get("score", {}).get("roots")}
            for r in global_top[:20]
        ],
    }

    out = Path(__file__).parent / "samples" / "vulgar_reduce_summary.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(summary, indent=2) + "\n")

    global_out = Path(__file__).parent / "samples" / "ard_vulgar.json"
    global_out.write_text(json.dumps({
        "total_reviews_parsed": total_parsed,
        "total_hard_hits": total_hits,
        "hits_per_million": round(1e6 * total_hits / max(total_parsed, 1), 2),
        "categories": cat_summaries,
        "global_top": global_top,
    }))
    print()
    print("=" * 72)
    print(f"elapsed: {summary['elapsed_seconds']} s  |  "
          f"parsed: {total_parsed:,}  hard hits: {total_hits:,}  "
          f"({summary['hits_per_million']}/M)")
    print(f"wrote {global_out} ({global_out.stat().st_size / 1e6:.2f} MB)")


if __name__ == "__main__":
    main()
