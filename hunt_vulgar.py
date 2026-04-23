"""Amazon Review Distiller — truly-vulgar hunter.

Re-scans the full 275 GB Amazon Reviews 2023 dataset (571M reviews) looking
specifically for reviews that contain HARD profanity — the stuff Amazon
reviewers would normally self-censor. Explicitly excludes soft curses like
"crap" / "crappy" that dominated the original top-K heaps.

Per-chunk worker ranks every hit by (variety_of_hard_words × count × length_bonus)
and keeps only the top K_PER_CHUNK. A global reduce then keeps the top-N per
category + a global top-500.

Output:
  /workspace/shared/ard_vulgar/shards/{chunk_id}.json
"""
from __future__ import annotations

import heapq
import json
import os
import re
import time
from typing import Any, Dict, List, Tuple

import requests


HF_BASE = "https://huggingface.co/datasets/McAuley-Lab/Amazon-Reviews-2023/resolve/main/"
OUTPUT_DIR = "/workspace/shared/ard_vulgar/shards"
K_PER_CHUNK = 200  # keep top 200 hard-profane reviews per byte-range chunk


# --- HARD profanity only. No "crap", no "damn", no "hell", no "stupid". ---
# Grouped by root so we can count unique roots (variety matters more than volume)
HARD_ROOTS: Dict[str, List[str]] = {
    "fuck":         ["fuck", "fucks", "fucked", "fucking", "fucker", "fuckers",
                     "fuckin", "fuckn", "fucken", "fuckall", "fuckery",
                     "motherfuck", "motherfucker", "motherfuckers", "motherfucking",
                     "muthafucka", "muthafucker", "muthafuckin"],
    "shit":         ["shit", "shits", "shitty", "shittier", "shittiest",
                     "shitshow", "shithole", "shitbag", "shitstorm",
                     "bullshit", "horseshit", "batshit", "dogshit",
                     "shithead", "shitheads"],
    "bitch":        ["bitch", "bitches", "bitchy", "bitching", "bitched",
                     "sonofabitch", "sonsofbitches"],
    "cunt":         ["cunt", "cunts", "cunty", "cuntish"],
    "whore":        ["whore", "whores", "whorish", "whoring", "manwhore"],
    "slut":         ["slut", "sluts", "slutty"],
    "asshole":      ["asshole", "assholes", "asshat", "asshats",
                     "dumbass", "jackass", "smartass", "fatass", "lazyass"],
    "dick":         ["dick", "dicks", "dickhead", "dickheads", "dickish",
                     "dickwad", "dickweed"],
    "cock":         ["cock", "cocks", "cocksucker", "cocksuckers", "cockblock"],
    "pussy":        ["pussy", "pussies"],
    "twat":         ["twat", "twats", "twatty"],
    "douche":       ["douche", "douches", "douchebag", "douchebags", "douchey"],
    "bastard":      ["bastard", "bastards"],
    "prick":        ["prick", "pricks", "prickish"],
    "wanker":       ["wanker", "wankers", "wanking"],
    "piss":         ["piss", "pissed", "pissing", "pisser", "pissy", "pissoff"],
    "bollocks":     ["bollocks", "bollox"],
    "nutsack":      ["nutsack", "ballsack"],
    "arse":         ["arse", "arsehole", "arses"],
}

HARD_WORDS: Dict[str, str] = {}  # word -> root
for root, variants in HARD_ROOTS.items():
    for v in variants:
        HARD_WORDS[v] = root

WORD_RX = re.compile(r"[A-Za-z]+(?:'[A-Za-z]+)?")
EXCLAM_RX = re.compile(r"!+")


def _score_hard(text: str) -> Dict[str, Any]:
    """Score a review purely on hard-profanity content.

    Returns dict with:
      total_hits: total hard-profane words
      unique_roots: count of distinct hard roots (fuck/shit/cunt/etc.)
      roots: {root: count}
      words: {word: count}
      word_count, caps_ratio, exclam_count
      variety_score: headline rank metric
    """
    if not text:
        return {"total_hits": 0, "unique_roots": 0, "variety_score": 0}
    words = WORD_RX.findall(text)
    nw = len(words) or 1
    roots: Dict[str, int] = {}
    hits: Dict[str, int] = {}
    caps = 0
    for w in words:
        lw = w.lower()
        if lw in HARD_WORDS:
            root = HARD_WORDS[lw]
            roots[root] = roots.get(root, 0) + 1
            hits[lw] = hits.get(lw, 0) + 1
        if len(w) >= 4 and w.isupper():
            caps += 1
    total_hits = sum(hits.values())
    unique_roots = len(roots)
    caps_ratio = caps / nw
    exclam_count = sum(len(m.group()) for m in EXCLAM_RX.finditer(text))

    # Variety × intensity — reward reviews that swear across multiple root words.
    # variety_score climbs steeply with unique_roots because it's rare to see
    # 3+ distinct hard roots in a single review (those are the gold).
    variety_score = (
        unique_roots ** 1.8              # 1->1, 2->3.5, 3->7.2, 4->12.1, 5->18.1
        + total_hits * 0.6
        + caps_ratio * 2.0
        + min(exclam_count, 40) * 0.05
    )
    return {
        "total_hits": total_hits,
        "unique_roots": unique_roots,
        "roots": roots,
        "words": hits,
        "word_count": nw,
        "caps_ratio": round(caps_ratio, 3),
        "exclam_count": exclam_count,
        "variety_score": round(variety_score, 3),
    }


def _is_spam(text: str) -> bool:
    """Cheap filter: reject "crap crap crap crap" style one-word spam."""
    if not text:
        return True
    tokens = WORD_RX.findall(text.lower())
    if len(tokens) < 3:
        return False
    counts: Dict[str, int] = {}
    for t in tokens:
        counts[t] = counts.get(t, 0) + 1
    top_word, top_count = max(counts.items(), key=lambda kv: kv[1])
    if len(tokens) >= 15 and top_count / len(tokens) > 0.50:
        return True
    if top_count / len(tokens) > 0.70:
        return True
    return False


def _heap_push_top_k(h: List, k: int, item: Tuple[float, int, Dict[str, Any]]):
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
    n_hard_hits = 0
    root_counts: Dict[str, int] = {r: 0 for r in HARD_ROOTS}
    n_skipped = 0
    tie = 0

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
                lines.pop(0)  # discard byte-misaligned partial line
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
                # Quick char-level pre-filter before tokenizing.
                blob = (title + " " + text).lower()
                if not any(root in blob for root in HARD_ROOTS):
                    continue
                s = _score_hard(title + " " + text)
                if s["total_hits"] == 0:
                    continue
                if _is_spam(text):
                    continue
                n_hard_hits += 1
                for rt, n in (s.get("roots") or {}).items():
                    root_counts[rt] = root_counts.get(rt, 0) + n

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
                    "score": s,
                }
                _heap_push_top_k(heap, K_PER_CHUNK, (s["variety_score"], tie, tiny))
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
        "n_hard_hits": n_hard_hits,
        "n_skipped": n_skipped,
        "root_counts": root_counts,
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
        "n_hard_hits": n_hard_hits,
        "root_counts": root_counts,
        "elapsed_s": payload["elapsed_s"],
    }
