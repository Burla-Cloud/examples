"""Amazon Review Distiller. Burla map-reduce over 275 GB of review JSONL.

Two scoring passes share one streaming primitive:

  main   scores every review for generic profanity, caps-lock, exclamation
         storms, short-and-brutal takes, 5-star obscene, long rants.
         Output drives the Wall of Rants + the findings page.

  worst  hunts asterisk-censored profanity and categorized slurs
         (racial / homophobic / ableist / gendered / xenophobic).
         Output drives Unhinged Mode.

Each worker receives (file_path, byte_start, byte_end, chunk_id), opens a
single HTTP Range GET against the HF CDN, aligns to the next newline, and
streams reviews row-by-row. Top-K heaps per (category, signal) are kept in
memory and written as one JSON per chunk to `/workspace/shared/ard/{pass}/shards/`.
A reduce pass then merges shards, producing a per-pass rollup JSON that
`analysis.py` turns into the UI artifacts in `data/`.

Reference. no need to re-run. Invoke with `python pipeline.py <stage>`:
  probe        stream ~4 MB of All_Beauty to verify HF + schema.
  map-main     dispatch the main scoring pass across the cluster.
  map-worst    dispatch the worst-of-worst scoring pass.
  reduce-main  single-worker reduce over main shards.
  reduce-worst single-worker reduce over worst shards.
"""
from __future__ import annotations

import argparse
import heapq
import json
import math
import os
import time
from pathlib import Path
from typing import Any, Dict, Iterator, List, Tuple

import requests

from lexicon import (
    STRONG_PROFANE, MEDIUM_PROFANE, MILD_PROFANE,
    WORD_RX, EXCLAM_RX,
    HARD_ROOTS, HARD_WORDS,
    CATEGORIES, CATEGORY_WEIGHT, WORD_TO_CAT,
    CENSORED_PATTERNS, classify_context,
)


HF_BASE = "https://huggingface.co/datasets/McAuley-Lab/Amazon-Reviews-2023/resolve/main/"
SHARED_MAIN = "/workspace/shared/ard/shards"
SHARED_WORST = "/workspace/shared/ard_worst/shards"
TOP_K_MAIN = 40
TOP_K_WORST = 250


# ---------------------------------------------------------------------------
# Streaming primitive: one HTTP Range GET, newline-aligned, yields dicts.
# ---------------------------------------------------------------------------
def stream_reviews(file_path: str, start: int, end: int) -> Iterator[Dict[str, Any]]:
    """Yield parsed review dicts from the byte range [start, end)."""
    resp = requests.get(
        HF_BASE + file_path,
        headers={"Range": f"bytes={start}-{end - 1}"},
        stream=True,
        timeout=300,
    )
    if resp.status_code not in (200, 206):
        raise RuntimeError(f"http_{resp.status_code}")
    try:
        buf = b""
        first_line = True
        for raw in resp.iter_content(chunk_size=1 << 16):
            if not raw:
                continue
            buf += raw
            lines = buf.split(b"\n")
            buf = lines.pop()
            if first_line and start > 0 and lines:
                lines.pop(0)  # discard partial line (byte-range boundary)
            first_line = False
            for line in lines:
                line = line.strip()
                if not line:
                    continue
                try:
                    yield json.loads(line)
                except Exception:
                    continue
    finally:
        resp.close()


def plan_chunks(chunk_mb: int = 500) -> List[Tuple[str, int, int, str]]:
    """Partition every category JSONL into roughly equal byte-range chunks."""
    from huggingface_hub import HfApi
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
        cat = path.rsplit("/", 1)[-1].replace(".jsonl", "")
        for i in range(n):
            s = i * span
            e = (i + 1) * span if i < n - 1 else size
            jobs.append((path, s, e, f"{cat}_{i:03d}"))
    return jobs


def _heappush_topk(h: List, k: int, item: Tuple) -> None:
    if len(h) < k:
        heapq.heappush(h, item)
    elif item[0] > h[0][0]:
        heapq.heapreplace(h, item)


def _is_spam(text: str) -> bool:
    """Cheap filter for 'crap crap crap crap' one-word spam."""
    if not text:
        return True
    tokens = WORD_RX.findall(text.lower())
    if len(tokens) < 3:
        return False
    counts: Dict[str, int] = {}
    for t in tokens:
        counts[t] = counts.get(t, 0) + 1
    _, top_count = max(counts.items(), key=lambda kv: kv[1])
    if len(tokens) >= 15 and top_count / len(tokens) > 0.50:
        return True
    return top_count / len(tokens) > 0.70


# ---------------------------------------------------------------------------
# Main pass. profanity + caps + rants scoring.
# ---------------------------------------------------------------------------
def _score_main(text: str) -> Dict[str, Any]:
    if not text:
        return {"strong": 0, "medium": 0, "mild": 0, "profanity_total": 0,
                "word_count": 0, "caps_ratio": 0, "exclam_count": 0, "unhinged": 0}
    words = WORD_RX.findall(text)
    nw = len(words) or 1
    strong = medium = mild = caps = 0
    for w in words:
        lw = w.lower()
        if lw in STRONG_PROFANE:
            strong += 1
        elif lw in MEDIUM_PROFANE:
            medium += 1
        elif lw in MILD_PROFANE:
            mild += 1
        if len(w) >= 4 and w.isupper():
            caps += 1
    exclam = sum(len(m.group()) for m in EXCLAM_RX.finditer(text))
    caps_ratio = caps / nw
    unhinged = strong * 3.0 + medium * 1.5 + mild * 0.4 + caps_ratio * 6 + min(exclam, 50) * 0.08
    return {
        "strong": strong, "medium": medium, "mild": mild,
        "profanity_total": strong + medium + mild,
        "word_count": nw, "caps_ratio": round(caps_ratio, 3),
        "exclam_count": exclam, "unhinged": round(unhinged, 3),
    }


def process_main(file_path: str, start: int, end: int, chunk_id: str) -> Dict[str, Any]:
    t0 = time.time()
    os.makedirs(SHARED_MAIN, exist_ok=True)
    category = file_path.rsplit("/", 1)[-1].replace(".jsonl", "")

    heaps: Dict[str, List] = {k: [] for k in (
        "profane_strong", "rant", "screaming", "exclamation",
        "short_brutal", "five_star_obscene", "five_star_one_word",
    )}
    n_parsed = n_profane = length_sum = 0
    rating_counts = {1: 0, 2: 0, 3: 0, 4: 0, 5: 0}
    tie = 0

    try:
        for r in stream_reviews(file_path, start, end):
            n_parsed += 1
            text = r.get("text") or ""
            rating = float(r.get("rating") or 0)
            rating_counts[int(rating)] = rating_counts.get(int(rating), 0) + 1
            length_sum += len(text)

            s = _score_main(text)
            if s["profanity_total"]:
                n_profane += 1

            tiny = {
                "text": text, "title": (r.get("title") or "")[:200],
                "rating": rating, "asin": r.get("asin"),
                "user_id": (r.get("user_id") or "")[:30],
                "helpful_vote": r.get("helpful_vote"),
                "verified": r.get("verified_purchase"),
                "ts": r.get("timestamp"), "category": category, "score": s,
            }
            tie += 1
            nw = s["word_count"]
            short_brutal = (s["strong"] * 4 + s["medium"] * 2 + s["mild"] * 0.5) / max(nw, 1) if nw <= 30 else 0.0
            rant = (min(nw, 1500) / 80.0 + s["strong"] * 2.0 + s["caps_ratio"] * 8.0
                    + min(s["exclam_count"], 80) * 0.1) if nw >= 80 else 0.0
            fso = (s["strong"] * 3 + s["medium"] * 1.2) if rating >= 5 else 0.0
            fow = (1.0 if nw == 0 else 1.0 / nw if nw <= 3 else 0.0) if rating >= 5 else 0.0

            _heappush_topk(heaps["profane_strong"], TOP_K_MAIN, (s["strong"] + s["medium"] * 0.4, tie, tiny))
            _heappush_topk(heaps["rant"],           TOP_K_MAIN, (rant, tie, tiny))
            _heappush_topk(heaps["screaming"],      TOP_K_MAIN, (s["caps_ratio"] * nw ** 0.5, tie, tiny))
            _heappush_topk(heaps["exclamation"],    TOP_K_MAIN, (s["exclam_count"], tie, tiny))
            _heappush_topk(heaps["short_brutal"],   TOP_K_MAIN, (short_brutal, tie, tiny))
            _heappush_topk(heaps["five_star_obscene"],  TOP_K_MAIN, (fso, tie, tiny))
            _heappush_topk(heaps["five_star_one_word"], TOP_K_MAIN, (fow, tie, tiny))
    except Exception as e:
        return {"chunk_id": chunk_id, "error": f"{type(e).__name__}: {e}",
                "n_parsed": n_parsed}

    payload = {
        "chunk_id": chunk_id, "category": category,
        "n_parsed": n_parsed, "n_profane": n_profane,
        "rating_counts": rating_counts, "length_sum": length_sum,
        "elapsed_s": round(time.time() - t0, 2),
        "top": {
            sig: [{"score": round(s, 3), "review": r} for s, _, r in sorted(h, key=lambda x: -x[0]) if s > 0]
            for sig, h in heaps.items()
        },
    }
    with open(os.path.join(SHARED_MAIN, f"{chunk_id}.json"), "w") as f:
        json.dump(payload, f)

    return {k: payload[k] for k in
            ("chunk_id", "category", "n_parsed", "n_profane", "rating_counts", "elapsed_s")}


# ---------------------------------------------------------------------------
# Worst-of-worst pass. slurs + censored profanity.
# ---------------------------------------------------------------------------
PREFILTER_ROOTS: List[str] = sorted({
    *HARD_ROOTS.keys(),
    "nigg", "chink", "gook", "spic", "beaner", "wetback", "kike", "yid",
    "coon", "faggot", "fag", "dyke", "tranny", "retard", "shemale",
    "chinaman", "raghead", "towelhead", "sandnigger",
    "thot", "skank", "tramp", "incel", "cuck",
    "f**", "f*k", "s**", "b**", "n**", "c**", "sh*", "sh!", "sh1",
    "b!t", "b1t", "c*n", "f@g", "f*g", "p***",
}, key=len, reverse=True)


def _scan_categories(words: List[str], blob: str) -> Dict[str, Dict[str, int]]:
    """Merge exact-word and censored-regex hits into {category: {word: count}}."""
    out: Dict[str, Dict[str, int]] = {}
    for w in words:
        lw = w.lower()
        cat = WORD_TO_CAT.get(lw) or ("VULG" if lw in HARD_WORDS else None)
        if cat:
            out.setdefault(cat, {})
            out[cat][lw] = out[cat].get(lw, 0) + 1
    for cat, root, pat in CENSORED_PATTERNS:
        hits = pat.findall(blob)
        if hits:
            key = f"{root}*"
            out.setdefault(cat, {})
            out[cat][key] = out[cat].get(key, 0) + len(hits)
    return out


def _score_worst(blob: str, cats: Dict[str, Dict[str, int]]) -> Dict[str, Any]:
    """Severity = tier-weighted hit count with variety bonus + caps/exclam energy."""
    if not cats:
        return {"severity": 0.0, "total_hits": 0, "categories": {}}
    words = WORD_RX.findall(blob)
    nw = len(words) or 1
    caps_ratio = sum(1 for w in words if len(w) >= 4 and w.isupper()) / nw
    exclam = sum(len(m.group()) for m in EXCLAM_RX.finditer(blob))

    severity = 0.0
    total_hits = 0
    for cat, d in cats.items():
        hits = sum(d.values())
        total_hits += hits
        severity += CATEGORY_WEIGHT.get(cat, 1.0) * (hits + 0.5 * (len(d) - 1))
    severity += min(caps_ratio, 0.25) * 8.0 + min(exclam, 40) * 0.05

    return {
        "severity": round(severity, 3), "total_hits": total_hits,
        "categories": cats, "caps_ratio": round(caps_ratio, 3),
        "exclam_count": exclam, "word_count": nw,
    }


CTX_MULT = {"deploy": 1.25, "quote_crit": 0.35, "reclaim": 0.25, "ambiguous": 0.85}


def process_worst(file_path: str, start: int, end: int, chunk_id: str) -> Dict[str, Any]:
    t0 = time.time()
    os.makedirs(SHARED_WORST, exist_ok=True)
    category = file_path.rsplit("/", 1)[-1].replace(".jsonl", "")

    heap: List[Tuple] = []
    n_parsed = n_hits = tie = 0
    cat_totals: Dict[str, int] = {}
    slur_cat_totals: Dict[str, Dict[str, int]] = {}

    try:
        for r in stream_reviews(file_path, start, end):
            n_parsed += 1
            text = r.get("text") or ""
            title = r.get("title") or ""
            blob = f"{title} {text}"
            blob_lo = blob.lower()
            if not any(a in blob_lo for a in PREFILTER_ROOTS):
                continue

            cats = _scan_categories(WORD_RX.findall(blob), blob)
            if not cats:
                continue
            sc = _score_worst(blob, cats)
            if sc["severity"] <= 0 or _is_spam(text):
                continue

            ctx = classify_context(blob)
            sc["context"] = ctx
            sc["severity_adj"] = round(sc["severity"] * CTX_MULT[ctx], 3)

            n_hits += 1
            for cat, d in cats.items():
                cat_totals[cat] = cat_totals.get(cat, 0) + sum(d.values())
                slur_cat_totals.setdefault(cat, {})
                for k, n in d.items():
                    slur_cat_totals[cat][k] = slur_cat_totals[cat].get(k, 0) + n

            tie += 1
            tiny = {
                "text": text, "title": title[:200],
                "rating": float(r.get("rating") or 0), "asin": r.get("asin"),
                "helpful_vote": r.get("helpful_vote"),
                "verified": r.get("verified_purchase"),
                "ts": r.get("timestamp"), "category": category, "score": sc,
            }
            _heappush_topk(heap, TOP_K_WORST, (sc["severity_adj"], tie, tiny))
    except Exception as e:
        return {"chunk_id": chunk_id, "error": f"{type(e).__name__}: {e}",
                "n_parsed": n_parsed}

    heap.sort(key=lambda x: -x[0])
    payload = {
        "chunk_id": chunk_id, "category": category,
        "n_parsed": n_parsed, "n_hits": n_hits,
        "cat_totals": cat_totals, "slur_cat_totals": slur_cat_totals,
        "elapsed_s": round(time.time() - t0, 2),
        "top": [{"score": round(s, 3), "review": r} for s, _, r in heap],
    }
    with open(os.path.join(SHARED_WORST, f"{chunk_id}.json"), "w") as f:
        json.dump(payload, f)
    return {k: payload[k] for k in
            ("chunk_id", "category", "n_parsed", "n_hits", "cat_totals", "elapsed_s")}


# ---------------------------------------------------------------------------
# Reduce. one Burla worker merges every shard for its pass.
# ---------------------------------------------------------------------------
def reduce_main(_dummy: int = 0) -> Dict[str, Any]:
    """Read every shard in SHARED_MAIN and roll up per-category top-K heaps."""
    names = [f for f in os.listdir(SHARED_MAIN) if f.endswith(".json")]
    by_cat: Dict[str, List[str]] = {}
    for n in names:
        base = n[:-5]
        cat = base.rsplit("_", 1)[0] if "_" in base else base
        by_cat.setdefault(cat, []).append(n)

    categories: Dict[str, Dict[str, Any]] = {}
    total_parsed = total_profane = 0
    total_rc: Dict[int, int] = {1: 0, 2: 0, 3: 0, 4: 0, 5: 0}

    for cat, shards in by_cat.items():
        n_parsed = n_profane = length_sum = 0
        rc = {1: 0, 2: 0, 3: 0, 4: 0, 5: 0}
        sigs: Dict[str, List[Dict[str, Any]]] = {}
        seen = set()
        for name in shards:
            try:
                with open(os.path.join(SHARED_MAIN, name)) as f:
                    d = json.load(f)
            except Exception:
                continue
            n_parsed += d.get("n_parsed", 0)
            n_profane += d.get("n_profane", 0)
            length_sum += d.get("length_sum", 0)
            for k, v in (d.get("rating_counts") or {}).items():
                rc[int(k)] = rc.get(int(k), 0) + v
            for sig, items in (d.get("top") or {}).items():
                for it in items:
                    rev = it.get("review") or {}
                    k = hash((rev.get("asin"), rev.get("user_id"), (rev.get("text") or "")[:200]))
                    if k in seen:
                        continue
                    seen.add(k)
                    sigs.setdefault(sig, []).append({"score": it.get("score"), "review": rev})
        for sig in sigs:
            sigs[sig].sort(key=lambda x: -x["score"])
            sigs[sig] = sigs[sig][:100]
        categories[cat] = {
            "category": cat, "n_parsed": n_parsed, "n_profane": n_profane,
            "rating_counts": rc, "length_sum": length_sum,
            "mean_length": round(length_sum / n_parsed, 1) if n_parsed else 0,
            "profanity_rate": round(n_profane / max(n_parsed, 1), 4),
            "top": sigs,
        }
        total_parsed += n_parsed
        total_profane += n_profane
        for k, v in rc.items():
            total_rc[k] += v

    return {
        "n_categories": len(categories),
        "total_parsed": total_parsed,
        "total_profane": total_profane,
        "total_rating_counts": dict(sorted(total_rc.items())),
        "categories": categories,
    }


def reduce_worst(_dummy: int = 0) -> Dict[str, Any]:
    """Read every shard in SHARED_WORST, keep per-category + global top lists."""
    names = [f for f in os.listdir(SHARED_WORST) if f.endswith(".json")]
    by_cat: Dict[str, List[str]] = {}
    for n in names:
        base = n[:-5]
        cat = base.rsplit("_", 1)[0] if "_" in base else base
        by_cat.setdefault(cat, []).append(n)

    categories: List[Dict[str, Any]] = []
    total_parsed = total_hits = 0
    gheap: List[Tuple] = []
    gtie = 0
    K_CAT = 250
    K_GLOBAL = 500

    for cat, shards in by_cat.items():
        n_parsed = n_hits = 0
        cat_totals: Dict[str, int] = {}
        slur_cat_totals: Dict[str, Dict[str, int]] = {}
        heap: List[Tuple] = []
        tie = 0
        for name in shards:
            try:
                with open(os.path.join(SHARED_WORST, name)) as f:
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
                _heappush_topk(heap, K_CAT, (s, tie, rev))
                gtie += 1
                _heappush_topk(gheap, K_GLOBAL, (s, gtie, rev))

        categories.append({
            "category": cat, "n_parsed": n_parsed, "n_hits": n_hits,
            "hits_per_million": round(1e6 * n_hits / max(n_parsed, 1), 2),
            "cat_totals": dict(sorted(cat_totals.items(), key=lambda kv: -kv[1])),
            "slur_cat_totals": slur_cat_totals,
            "top_count": len(heap),
        })
        total_parsed += n_parsed
        total_hits += n_hits

    gheap.sort(key=lambda x: -x[0])
    return {
        "total_reviews_parsed": total_parsed,
        "total_hits": total_hits,
        "hits_per_million": round(1e6 * total_hits / max(total_parsed, 1), 2),
        "shards": len(names), "categories": categories,
        "global_top": [{**r, "_score": s} for s, _, r in gheap],
    }


# ---------------------------------------------------------------------------
# CLI. wraps the map/reduce stages with Burla dispatch.
# ---------------------------------------------------------------------------
def _dispatch_map(worker, jobs, limit: int, max_parallelism: int, label: str,
                  summary_path: Path) -> None:
    from burla import remote_parallel_map
    if limit:
        jobs = jobs[:limit]
    total_gb = sum(e - s for _, s, e, _ in jobs) / 1e9
    print(f"{label}: {len(jobs)} chunks, {total_gb:.1f} GB, up to {max_parallelism} CPUs")

    t0 = time.time()
    results = remote_parallel_map(worker, jobs, func_cpu=1, func_ram=4,
                                   grow=True, max_parallelism=max_parallelism,
                                   spinner=True)
    elapsed = time.time() - t0
    ok = [r for r in results if "error" not in r]
    bad = [r for r in results if "error" in r]
    summary = {
        "stage": label, "elapsed_minutes": round(elapsed / 60, 2),
        "chunks_submitted": len(jobs), "chunks_succeeded": len(ok),
        "chunks_failed": len(bad), "first_failures": bad[:5],
    }
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(json.dumps(summary, indent=2) + "\n")
    print(f"{label} done in {summary['elapsed_minutes']} min. "
          f"ok={len(ok)} fail={len(bad)} -> {summary_path}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("stage", choices=[
        "probe", "plan",
        "map-main", "map-worst", "reduce-main", "reduce-worst",
    ])
    ap.add_argument("--chunk-mb", type=int, default=500)
    ap.add_argument("--max-parallelism", type=int, default=1000)
    ap.add_argument("--limit", type=int, default=0)
    args = ap.parse_args()

    here = Path(__file__).parent
    samples = here / "samples"
    samples.mkdir(parents=True, exist_ok=True)

    if args.stage == "probe":
        from probe import probe
        probe()
        return

    if args.stage == "plan":
        jobs = plan_chunks(args.chunk_mb)
        print(f"{len(jobs)} chunks, {sum(e - s for _, s, e, _ in jobs) / 1e9:.1f} GB")
        return

    if args.stage in ("map-main", "map-worst"):
        jobs = plan_chunks(args.chunk_mb)
        worker = process_main if args.stage == "map-main" else process_worst
        label = args.stage
        _dispatch_map(worker, jobs, args.limit, args.max_parallelism, label,
                      samples / f"{label}_summary.json")
        return

    if args.stage in ("reduce-main", "reduce-worst"):
        from burla import remote_parallel_map
        reducer = reduce_main if args.stage == "reduce-main" else reduce_worst
        t0 = time.time()
        [result] = remote_parallel_map(reducer, [0], grow=True, spinner=True)
        out_name = "ard_reduced.json" if args.stage == "reduce-main" else "ard_worst.json"
        out = samples / out_name
        out.write_text(json.dumps(result))
        print(f"{args.stage} done in {time.time() - t0:.1f}s -> "
              f"{out} ({out.stat().st_size / 1e6:.1f} MB)")
        return


if __name__ == "__main__":
    main()
