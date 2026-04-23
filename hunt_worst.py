"""Worst-of-the-worst hunter. Amazon Review Distiller.

Third-pass scan that catches what the first two passes missed:
  1. Asterisk / symbol-censored profanity (f***, sh!t, b*tch, c***, n****r, f@g)
    . which turns out to be the OVERWHELMING majority of strong profanity
     on Amazon since reviewers self-censor. The original tokenizer couldn't
     see any of this.
  2. Categorized slur detection (racial / homophobic / ableist / xenophobic /
     gendered) using hate_lexicon.py. Tiered severity weights.
  3. Context classification per hit. deploy / quote_and_criticize /
     reclaim / ambiguous. so the rescorer can boost genuine deployments
     and down-weight literary criticism that contains the same strings.

Scoring prioritizes:
  hard slurs  >>  other slurs  >>  censored profanity  >>  variety bonus
so a single n-word lands in the top-K far above a 40-fuck screed.

Output per shard: /workspace/shared/ard_worst/shards/{chunk_id}.json
"""
from __future__ import annotations

import heapq
import json
import os
import re
import time
from typing import Any, Dict, List, Tuple

import requests

from hate_lexicon import (
    WORD_TO_CAT, CATEGORY_WEIGHT, CENSORED_PATTERNS, classify_context,
    CATEGORIES,
)
from hunt_vulgar import HARD_WORDS, HARD_ROOTS


HF_BASE = "https://huggingface.co/datasets/McAuley-Lab/Amazon-Reviews-2023/resolve/main/"
OUTPUT_DIR = "/workspace/shared/ard_worst/shards"
K_PER_CHUNK = 250

WORD_RX = re.compile(r"[A-Za-z]+(?:'[A-Za-z]+)?")
EXCLAM_RX = re.compile(r"!+")

# Char-level pre-filter roots. if none of these substrings are present in
# the lowercase blob, we skip the full scan entirely. Kept short / cheap.
PREFILTER_ROOTS: List[str] = sorted({
    # strong profanity roots from the first pass
    *HARD_ROOTS.keys(),
    # slur roots (lowercased)
    "nigg", "chink", "gook", "spic", "beaner", "wetback", "kike", "yid",
    "coon", "faggot", "fag", "dyke", "tranny", "retard", "shemale",
    "chinaman", "raghead", "towelhead", "sandnigger",
    # ambiguous / softer but still flagged
    "thot", "skank", "tramp", "incel", "cuck",
    # censored-substitution anchors. crude but fast
    "f**", "f*k", "s**", "b**", "n**", "c**", "sh*", "sh!", "sh1",
    "b!t", "b1t", "c*n", "f@g", "f*g", "p***",
}, key=len, reverse=True)


def _scan_exact(words: List[str]) -> Dict[str, Dict[str, int]]:
    """Exact word-list match. Returns: {category: {word: count}}."""
    out: Dict[str, Dict[str, int]] = {}
    for w in words:
        lw = w.lower()
        cat = WORD_TO_CAT.get(lw)
        if cat is None:
            cat = "VULG" if lw in HARD_WORDS else None
        if cat is None:
            continue
        out.setdefault(cat, {})
        out[cat][lw] = out[cat].get(lw, 0) + 1
    return out


def _scan_censored(text: str) -> Dict[str, Dict[str, int]]:
    """Regex pass for asterisk / symbol-censored variants."""
    out: Dict[str, Dict[str, int]] = {}
    for cat, root, pat in CENSORED_PATTERNS:
        hits = pat.findall(text)
        if not hits:
            continue
        out.setdefault(cat, {})
        key = f"{root}*"  # star marks the censored variant
        out[cat][key] = out[cat].get(key, 0) + len(hits)
    return out


def _merge_counts(a: Dict[str, Dict[str, int]], b: Dict[str, Dict[str, int]]) -> Dict[str, Dict[str, int]]:
    """Merge nested count dicts in place on a, return a."""
    for cat, d in b.items():
        a.setdefault(cat, {})
        for k, n in d.items():
            a[cat][k] = a[cat].get(k, 0) + n
    return a


def _score(text: str, categories: Dict[str, Dict[str, int]]) -> Dict[str, Any]:
    """Combine slur & profanity hits into a single severity score."""
    if not categories:
        return {"severity": 0.0, "total_hits": 0, "categories": {}}
    words = WORD_RX.findall(text)
    nw = len(words) or 1
    caps = sum(1 for w in words if len(w) >= 4 and w.isupper())
    caps_ratio = caps / nw
    exclam = sum(len(m.group()) for m in EXCLAM_RX.finditer(text))

    severity = 0.0
    total_hits = 0
    for cat, d in categories.items():
        cat_hits = sum(d.values())
        total_hits += cat_hits
        w = CATEGORY_WEIGHT.get(cat, 1.0)
        unique = len(d)
        severity += w * (cat_hits + 0.5 * (unique - 1))

    # Small bonuses for unhinged energy on top of the category weight.
    severity += min(caps_ratio, 0.25) * 8.0
    severity += min(exclam, 40) * 0.05

    return {
        "severity": round(severity, 3),
        "total_hits": total_hits,
        "categories": categories,
        "caps_ratio": round(caps_ratio, 3),
        "exclam_count": exclam,
        "word_count": nw,
    }


def _is_spam(text: str) -> bool:
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
    if top_count / len(tokens) > 0.70:
        return True
    return False


def _heap_push_top_k(h: List, k: int, item: Tuple[float, int, Dict[str, Any]]) -> None:
    if len(h) < k:
        heapq.heappush(h, item)
    elif item[0] > h[0][0]:
        heapq.heapreplace(h, item)


def process_chunk(file_path: str, start: int, end: int, chunk_id: str) -> Dict[str, Any]:
    t0 = time.time()
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    url = HF_BASE + file_path
    headers = {"Range": f"bytes={start}-{end - 1}"}
    try:
        resp = requests.get(url, headers=headers, stream=True, timeout=300)
        if resp.status_code not in (200, 206):
            return {"chunk_id": chunk_id, "error": f"http_{resp.status_code}"}
    except Exception as e:
        return {"chunk_id": chunk_id, "error": f"http_fail: {type(e).__name__}: {e}"}

    category = file_path.rsplit("/", 1)[-1].replace(".jsonl", "")

    heap: List[Tuple[float, int, Dict[str, Any]]] = []
    n_parsed = 0
    n_hits = 0
    n_skipped = 0
    tie = 0
    cat_totals: Dict[str, int] = {}
    slur_cat_totals: Dict[str, Dict[str, int]] = {}  # per-category root totals

    buf = b""
    first_line = True
    try:
        for raw in resp.iter_content(chunk_size=1 << 16):
            if not raw:
                continue
            buf += raw
            lines = buf.split(b"\n")
            buf = lines.pop()
            if first_line and start > 0 and lines:
                lines.pop(0)
            first_line = False

            for line in lines:
                line = line.strip()
                if not line:
                    continue
                try:
                    r = json.loads(line)
                except Exception:
                    n_skipped += 1
                    continue
                n_parsed += 1
                text = r.get("text") or ""
                title = r.get("title") or ""
                blob = (title + " " + text)
                blob_lo = blob.lower()

                # Pre-filter: require SOMETHING interesting before regex burn.
                if not any(anchor in blob_lo for anchor in PREFILTER_ROOTS):
                    continue

                words = WORD_RX.findall(blob)
                cats = _scan_exact(words)
                cats_cen = _scan_censored(blob)
                cats = _merge_counts(cats, cats_cen)
                if not cats:
                    continue

                sc = _score(blob, cats)
                if sc["severity"] <= 0:
                    continue
                if _is_spam(text):
                    continue

                ctx = classify_context(blob)
                sc["context"] = ctx
                sc["severity_adj"] = round(
                    sc["severity"] * {
                        "deploy": 1.25,
                        "quote_crit": 0.35,
                        "reclaim": 0.25,
                        "ambiguous": 0.85,
                    }[ctx],
                    3,
                )

                n_hits += 1
                for cat, d in cats.items():
                    cat_totals[cat] = cat_totals.get(cat, 0) + sum(d.values())
                    slur_cat_totals.setdefault(cat, {})
                    for k, n in d.items():
                        slur_cat_totals[cat][k] = slur_cat_totals[cat].get(k, 0) + n

                tie += 1
                tiny = {
                    "text": text[:1400],
                    "title": title[:200],
                    "rating": float(r.get("rating", 0)),
                    "asin": r.get("asin"),
                    "helpful_vote": r.get("helpful_vote"),
                    "verified": r.get("verified_purchase"),
                    "ts": r.get("timestamp"),
                    "category": category,
                    "score": sc,
                }
                _heap_push_top_k(heap, K_PER_CHUNK, (sc["severity_adj"], tie, tiny))
    except Exception as e:
        resp.close()
        return {"chunk_id": chunk_id, "error": f"stream_fail: {type(e).__name__}: {e}",
                "n_parsed": n_parsed}
    resp.close()

    heap.sort(key=lambda x: -x[0])
    payload = {
        "chunk_id": chunk_id,
        "file_path": file_path,
        "category": category,
        "n_parsed": n_parsed,
        "n_hits": n_hits,
        "n_skipped": n_skipped,
        "cat_totals": cat_totals,
        "slur_cat_totals": slur_cat_totals,
        "elapsed_s": round(time.time() - t0, 2),
        "top": [{"score": round(s, 3), "review": r} for s, _k, r in heap],
    }

    out_path = os.path.join(OUTPUT_DIR, f"{chunk_id}.json")
    with open(out_path, "w") as f:
        json.dump(payload, f)

    return {
        "chunk_id": chunk_id,
        "category": category,
        "n_parsed": n_parsed,
        "n_hits": n_hits,
        "cat_totals": cat_totals,
        "elapsed_s": payload["elapsed_s"],
    }
