"""GRS reduce stage — runs on Burla, one worker per bucket.

Inputs: /workspace/shared/grs/shards/*.json (~600 files of per-shard summaries)

We split the reduction into 16 parallel buckets: each reducer scans a subset of
shard files, then we do a final local merge.

Output: samples/grs_reduced.json
  - per_category: for each category, top-N repos (by simple "interestingness"
    score) plus category histograms
  - global_tokens: merged doc_freq counter (for TF-IDF in analysis.py)
  - per_lang: histogram
  - per_install: histogram
  - meta: n_repos, n_shards, timing, etc.
"""
from __future__ import annotations

import argparse
import heapq
import json
import os
import time
from pathlib import Path
from typing import Any, Dict, List

from burla import remote_parallel_map


SHARD_DIR = "/workspace/shared/grs/shards"
TOP_PER_CAT = 400        # keep top-400 repos per category
TOP_PER_LANG = 200       # keep top-200 repos per language
KEEP_SAMPLE_REPOS = 6000  # random reservoir sample for the UI search index


def reduce_bucket(bucket_idx: int, n_buckets: int, top_per_cat: int,
                  top_per_lang: int, sample_cap: int) -> Dict[str, Any]:
    """Read shards whose index mod N == bucket_idx."""

    shard_dir = "/workspace/shared/grs/shards"
    files = sorted(f for f in os.listdir(shard_dir) if f.endswith(".json"))
    my_files = [f for i, f in enumerate(files) if i % n_buckets == bucket_idx]

    n_repos = 0
    by_cat: Dict[str, int] = {}
    by_lang: Dict[str, int] = {}
    by_install: Dict[str, int] = {}
    doc_freq: Dict[str, int] = {}

    # Per-category heap of (score, key, row). Score is an "interestingness"
    # signal built from category score, badges, and length.
    cat_heaps: Dict[str, list] = {}
    lang_heaps: Dict[str, list] = {}
    sample: list = []   # reservoir sample for the search index
    rng_state = 12345   # deterministic LCG for reservoir

    def _lcg():
        nonlocal rng_state
        rng_state = (rng_state * 1664525 + 1013904223) % (1 << 32)
        return rng_state

    key_counter = 0
    for fn in my_files:
        path = os.path.join(shard_dir, fn)
        try:
            with open(path) as f:
                d = json.load(f)
        except Exception:
            continue
        rows = d.get("rows", [])
        n_repos += d.get("n_ok", len(rows))
        for k, v in (d.get("by_cat") or {}).items():
            by_cat[k] = by_cat.get(k, 0) + v
        for k, v in (d.get("by_lang") or {}).items():
            by_lang[k] = by_lang.get(k, 0) + v
        for k, v in (d.get("by_install") or {}).items():
            by_install[k] = by_install.get(k, 0) + v
        for k, v in (d.get("doc_freq") or {}).items():
            doc_freq[k] = doc_freq.get(k, 0) + v

        for row in rows:
            cat = row.get("category", "other")
            lang = row.get("lang") or "_unknown"
            key_counter += 1

            # Compact the row for storage (drop big fields)
            cat_score = (row.get("cat_scores") or {}).get(cat, 0)
            size_bonus = min(row.get("chars", 0), 10000) / 10000
            quality = cat_score + row.get("badges", 0) * 1.5 + size_bonus + row.get("code_blocks", 0) * 0.3
            compact = {
                "repo": row["repo"],
                "title": row.get("title", ""),
                "tldr": row.get("tldr", ""),
                "one_line": row.get("one_line", ""),
                "lang": lang,
                "install": row.get("install", "none"),
                "category": cat,
                "badges": row.get("badges", 0),
                "code_blocks": row.get("code_blocks", 0),
                "chars": row.get("chars", 0),
                "cat_score": cat_score,
                "quality": round(quality, 2),
                "tokens": row.get("tokens", {}),
            }
            # Push to per-category heap
            h = cat_heaps.setdefault(cat, [])
            item = (quality, key_counter, compact)
            if len(h) < top_per_cat:
                heapq.heappush(h, item)
            elif quality > h[0][0]:
                heapq.heapreplace(h, item)

            # Push to per-lang heap
            h2 = lang_heaps.setdefault(lang, [])
            if len(h2) < top_per_lang:
                heapq.heappush(h2, item)
            elif quality > h2[0][0]:
                heapq.heapreplace(h2, item)

            # Reservoir sample for the search index
            if len(sample) < sample_cap:
                sample.append(compact)
            else:
                r = _lcg() % key_counter
                if r < sample_cap:
                    sample[r] = compact

    def flat(h):
        return [r for _s, _k, r in sorted(h, key=lambda x: -x[0])]

    return {
        "bucket_idx": bucket_idx,
        "files_read": len(my_files),
        "n_repos": n_repos,
        "by_cat": by_cat,
        "by_lang": by_lang,
        "by_install": by_install,
        "doc_freq": doc_freq,
        "top_per_cat": {c: flat(h) for c, h in cat_heaps.items()},
        "top_per_lang": {l: flat(h) for l, h in lang_heaps.items()},
        "sample": sample,
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--buckets", type=int, default=16)
    ap.add_argument("--top-per-cat", type=int, default=TOP_PER_CAT)
    ap.add_argument("--top-per-lang", type=int, default=TOP_PER_LANG)
    ap.add_argument("--sample-cap", type=int, default=KEEP_SAMPLE_REPOS)
    args = ap.parse_args()

    t0 = time.time()
    print(f"reducing across {args.buckets} buckets...")
    jobs = [(i, args.buckets, args.top_per_cat, args.top_per_lang, args.sample_cap)
            for i in range(args.buckets)]
    results = remote_parallel_map(
        reduce_bucket,
        jobs,
        func_cpu=2,
        func_ram=8,
        max_parallelism=args.buckets,
        spinner=True,
    )
    elapsed = time.time() - t0
    print(f"map-reduce done in {elapsed:.1f}s")

    # Final local merge
    print("merging buckets locally...")
    global_repos = 0
    by_cat: Dict[str, int] = {}
    by_lang: Dict[str, int] = {}
    by_install: Dict[str, int] = {}
    doc_freq: Dict[str, int] = {}
    cat_heaps: Dict[str, list] = {}
    lang_heaps: Dict[str, list] = {}
    sample_all: list = []

    for r in results:
        global_repos += r.get("n_repos", 0)
        for k, v in r.get("by_cat", {}).items():
            by_cat[k] = by_cat.get(k, 0) + v
        for k, v in r.get("by_lang", {}).items():
            by_lang[k] = by_lang.get(k, 0) + v
        for k, v in r.get("by_install", {}).items():
            by_install[k] = by_install.get(k, 0) + v
        for k, v in r.get("doc_freq", {}).items():
            doc_freq[k] = doc_freq.get(k, 0) + v

        for cat, rows in r.get("top_per_cat", {}).items():
            h = cat_heaps.setdefault(cat, [])
            for row in rows:
                q = row.get("quality", 0)
                item = (q, row["repo"], row)
                if len(h) < args.top_per_cat:
                    heapq.heappush(h, item)
                elif q > h[0][0]:
                    heapq.heapreplace(h, item)

        for lang, rows in r.get("top_per_lang", {}).items():
            h = lang_heaps.setdefault(lang, [])
            for row in rows:
                q = row.get("quality", 0)
                item = (q, row["repo"], row)
                if len(h) < args.top_per_lang:
                    heapq.heappush(h, item)
                elif q > h[0][0]:
                    heapq.heapreplace(h, item)

        sample_all.extend(r.get("sample", []))

    # Cap final sample
    if len(sample_all) > args.sample_cap:
        # simple deterministic trim
        sample_all = sample_all[:args.sample_cap]

    def flat(h):
        return [r for _s, _k, r in sorted(h, key=lambda x: -x[0])]

    payload = {
        "generated_at": time.time(),
        "n_repos": global_repos,
        "n_buckets": args.buckets,
        "reduce_elapsed_s": round(elapsed, 2),
        "by_cat": by_cat,
        "by_lang": by_lang,
        "by_install": by_install,
        "doc_freq": doc_freq,
        "top_per_cat": {c: flat(h) for c, h in cat_heaps.items()},
        "top_per_lang": {l: flat(h) for l, h in lang_heaps.items()},
        "sample": sample_all,
    }
    out_path = Path(__file__).parent / "samples" / "grs_reduced.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w") as f:
        json.dump(payload, f)
    size_mb = out_path.stat().st_size / 1e6
    print(f"wrote {out_path} ({size_mb:.1f} MB, {global_repos:,} repos)")
    print("\ncategory histogram:")
    for cat, n in sorted(by_cat.items(), key=lambda kv: -kv[1]):
        print(f"  {cat:<12} {n:>8,}")
    print("\nlang histogram (top 15):")
    for lang, n in sorted(by_lang.items(), key=lambda kv: -kv[1])[:15]:
        print(f"  {lang:<24} {n:>8,}")


if __name__ == "__main__":
    main()
