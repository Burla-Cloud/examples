"""Reduce hunt_worst shards into a single global corpus via ONE Burla worker.

Consolidated into one call so we don't hit the cluster's scheduling race.
"""
from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Dict

from burla import remote_parallel_map


SHARDS_DIR = "/workspace/shared/ard_worst/shards"
K_PER_CATEGORY = 250
K_GLOBAL = 500


def reduce_all(_dummy: int) -> Dict[str, Any]:
    import os, json, heapq
    from typing import Dict as _D, List as _L, Any as _A, Tuple as _T

    shard_files = sorted(os.path.join(SHARDS_DIR, f)
                         for f in os.listdir(SHARDS_DIR)
                         if f.endswith(".json"))

    by_cat: _D[str, _L[str]] = {}
    for fp in shard_files:
        base = os.path.basename(fp).rsplit(".json", 1)[0]
        cat = "_".join(base.split("_")[:-1])
        by_cat.setdefault(cat, []).append(fp)

    categories: _L[_D[str, _A]] = []
    total_parsed = 0
    total_hits = 0

    global_heap: _L[_T[float, int, _D[str, _A]]] = []
    global_tie = 0

    for cat, files in by_cat.items():
        n_parsed = 0
        n_hits = 0
        cat_totals: _D[str, int] = {}
        slur_cat_totals: _D[str, _D[str, int]] = {}
        heap: _L[_T[float, int, _D[str, _A]]] = []
        tie = 0
        for fp in files:
            try:
                with open(fp) as f:
                    d = json.load(f)
            except Exception:
                continue
            n_parsed += d.get("n_parsed", 0)
            n_hits += d.get("n_hits", 0)
            for k, n in (d.get("cat_totals") or {}).items():
                cat_totals[k] = cat_totals.get(k, 0) + n
            for c, words in (d.get("slur_cat_totals") or {}).items():
                slur_cat_totals.setdefault(c, {})
                for w, n in words.items():
                    slur_cat_totals[c][w] = slur_cat_totals[c].get(w, 0) + n
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
                global_tie += 1
                if len(global_heap) < K_GLOBAL:
                    heapq.heappush(global_heap, (s, global_tie, rev))
                elif s > global_heap[0][0]:
                    heapq.heapreplace(global_heap, (s, global_tie, rev))

        heap.sort(key=lambda x: -x[0])
        total_parsed += n_parsed
        total_hits += n_hits
        categories.append({
            "category": cat,
            "n_parsed": n_parsed,
            "n_hits": n_hits,
            "hits_per_million": round(1e6 * n_hits / max(n_parsed, 1), 2),
            "cat_totals": dict(sorted(cat_totals.items(), key=lambda kv: -kv[1])),
            "slur_cat_totals": slur_cat_totals,
            "top_count": len(heap),
        })

    global_heap.sort(key=lambda x: -x[0])
    global_top = [{**r, "_score": s} for s, _k, r in global_heap]

    return {
        "total_reviews_parsed": total_parsed,
        "total_hits": total_hits,
        "hits_per_million": round(1e6 * total_hits / max(total_parsed, 1), 2),
        "shards": len(shard_files),
        "categories": categories,
        "global_top": global_top,
    }


def main() -> None:
    print(f"dispatching single-worker reduce across all shards ...")
    t0 = time.time()
    result = remote_parallel_map(reduce_all, [0], grow=True, spinner=True)[0]
    elapsed = time.time() - t0

    out = Path(__file__).parent / "samples" / "ard_worst.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(result))

    print()
    print("=" * 72)
    print(f"elapsed: {elapsed:.1f}s  |  parsed: {result['total_reviews_parsed']:,}  "
          f"hits: {result['total_hits']:,}  ({result['hits_per_million']}/M)")
    print(f"shards: {result['shards']}  categories: {len(result['categories'])}  "
          f"global top: {len(result['global_top'])}")
    print(f"wrote {out} ({out.stat().st_size / 1e6:.2f} MB)")


if __name__ == "__main__":
    main()
