"""Amazon Review Distiller — worker pipeline.

Each Burla worker processes one byte-range chunk of one review JSONL file:
  (file_path, byte_start, byte_end, chunk_id)

Flow:
  1. HTTP Range GET from HF CDN
  2. Align to next newline after start; stop at end, letting current line finish
  3. Stream-parse JSONL line-by-line, never buffering the full chunk
  4. Score every review across 6 signals: profanity, screaming (caps), rant,
     punctuation-storm, five-star-overshare, short-and-brutal
  5. Maintain top-K heaps per (category, signal); also count aggregates
     (rating distribution, total profanity, unique ASINs, etc.)
  6. Write per-chunk JSON to /workspace/shared/ard/shards/{chunk_id}.json

NO LLM on the critical path. NO sanitization. Raw review text is preserved
verbatim — profanity, typos, foreign characters, emojis, all intact.
"""
from __future__ import annotations

import heapq
import io
import json
import os
import re
import sys
import time
from typing import Any, Dict, List, Tuple

import requests


HF_BASE = "https://huggingface.co/datasets/McAuley-Lab/Amazon-Reviews-2023/resolve/main/"
OUTPUT_DIR = "/workspace/shared/ard/shards"
TOP_K_PER_SIGNAL = 40   # keep top 40 reviews per (category, signal)


# Profanity patterns — matched as whole words or inside compounds.
# Weighted: strong = 3, medium = 2, mild = 1. Score bias shown.
STRONG_PROFANE = {
    "fuck", "fucks", "fucked", "fucking", "fucker", "fuckers", "fuckin",
    "shit", "shits", "shitty", "shittier", "shittiest", "shitshow", "shithole",
    "bitch", "bitches", "bitching", "bitchy",
    "asshole", "assholes", "ass-hole", "asshat", "dumbass", "jackass",
    "cunt", "cunts",
    "bastard", "bastards",
    "motherfucker", "motherfuckers", "motherfucking",
    "dick", "dicks", "dickhead", "dickheads",
    "cock", "cocks", "cocksucker",
    "pussy", "pussies",
    "whore", "whores", "whorish",
    "piss", "pissed", "pissing", "pissoff",
    "crap", "craptastic", "crappy",
}
MEDIUM_PROFANE = {
    "damn", "damned", "damnit", "goddamn", "goddamnit",
    "hell", "hellish",
    "screwed", "screwing",
    "bullshit", "horseshit",
    "wtf", "stfu", "fubar",
    "douche", "douchebag", "douchy",
    "moron", "morons", "moronic",
    "idiot", "idiots", "idiotic",
    "retard", "retarded", "retards",
    "garbage", "rubbish", "trash",
}
MILD_PROFANE = {
    "suck", "sucked", "sucks", "sucky", "sucking", "sucker", "suckers",
    "stupid", "stupidity",
    "lame", "lamely",
    "terrible", "horrible", "awful", "horrid",
    "worst", "hate", "hated", "hates", "hating", "hatred",
    "pathetic", "useless", "worthless",
}
WORD_RX = re.compile(r"[A-Za-z]+(?:'[A-Za-z]+)?")
EXCLAM_RX = re.compile(r"!+")


def _score(text: str) -> Dict[str, Any]:
    if not text:
        return {
            "strong": 0, "medium": 0, "mild": 0, "profanity_total": 0,
            "word_count": 0, "caps_ratio": 0, "exclam_count": 0,
            "unhinged": 0,
        }
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
        # "all-caps words" — at least 4 chars and ALL uppercase (letters only)
        if len(w) >= 4 and w.isupper():
            caps += 1
    profanity_total = strong + medium + mild
    exclam_count = sum(len(m.group()) for m in EXCLAM_RX.finditer(text))
    caps_ratio = caps / nw
    # composite "unhinged score": profanity + caps + exclam, bias toward strong
    unhinged = strong * 3.0 + medium * 1.5 + mild * 0.4 + caps_ratio * 6 + min(exclam_count, 50) * 0.08
    return {
        "strong": strong, "medium": medium, "mild": mild,
        "profanity_total": profanity_total,
        "word_count": nw, "caps_ratio": round(caps_ratio, 3),
        "exclam_count": exclam_count,
        "unhinged": round(unhinged, 3),
    }


def _short_brutal_score(s: Dict[str, Any]) -> float:
    """High score = short + vulgar. 1-3 words with profanity is perfect."""
    if s["word_count"] == 0:
        return 0.0
    if s["word_count"] > 30:
        return 0.0
    return (s["strong"] * 4 + s["medium"] * 2 + s["mild"] * 0.5) / max(s["word_count"], 1)


def _rant_score(s: Dict[str, Any]) -> float:
    """High score = long, screaming, profane. Full Karen energy."""
    if s["word_count"] < 80:
        return 0.0
    return (
        min(s["word_count"], 1500) / 80.0
        + s["strong"] * 2.0
        + s["caps_ratio"] * 8.0
        + min(s["exclam_count"], 80) * 0.1
    )


def _five_star_obscene(rating: float, s: Dict[str, Any]) -> float:
    """5-star review with heavy profanity — the 'this product fucking slaps' genre."""
    if rating < 5:
        return 0.0
    return s["strong"] * 3 + s["medium"] * 1.2


def _five_star_boring(rating: float, s: Dict[str, Any]) -> float:
    """5-star review with 0 words / 1-2 word reviews — Amazon's bleakest genre."""
    if rating < 5:
        return 0.0
    if s["word_count"] == 0:
        return 1.0  # tied top
    if s["word_count"] > 3:
        return 0.0
    return 1.0 / s["word_count"]


# Heap helpers — we keep the top-K smallest (so heapreplace pops the worst).
def _heap_push_top_k(h: List, k: int, item: Tuple[float, int, Dict[str, Any]]):
    if len(h) < k:
        heapq.heappush(h, item)
    else:
        if item[0] > h[0][0]:
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

    # Category name from filename
    category = file_path.rsplit("/", 1)[-1].replace(".jsonl", "")

    # Heaps per signal
    heaps: Dict[str, List] = {
        "profane_strong": [],
        "rant":           [],
        "screaming":      [],
        "exclamation":    [],
        "short_brutal":   [],
        "five_star_obscene": [],
        "five_star_one_word": [],
    }
    n_parsed = 0
    n_profane = 0
    n_skipped = 0
    rating_counts = {1: 0, 2: 0, 3: 0, 4: 0, 5: 0}
    length_sum = 0

    # Streaming parse — accumulate bytes, split on newline, align first line
    buf = b""
    first_line = True
    chunk_id_counter = 0

    try:
        for raw in resp.iter_content(chunk_size=1 << 16):
            if not raw:
                continue
            buf += raw
            # Split complete lines
            lines = buf.split(b"\n")
            buf = lines.pop()  # keep partial
            if first_line and start > 0:
                # Discard first (partial) line — byte alignment safety
                if lines:
                    lines.pop(0)
                first_line = False
            elif first_line:
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
                rating = float(r.get("rating", 0))
                rating_counts[int(rating)] = rating_counts.get(int(rating), 0) + 1
                length_sum += len(text)
                s = _score(text)
                if s["profanity_total"]:
                    n_profane += 1
                tiny = {
                    "text": text[:1400],
                    "title": title[:200],
                    "rating": rating,
                    "asin": r.get("asin"),
                    "user_id": (r.get("user_id") or "")[:30],
                    "helpful_vote": r.get("helpful_vote"),
                    "verified": r.get("verified_purchase"),
                    "ts": r.get("timestamp"),
                    "category": category,
                    "score": s,
                }
                chunk_id_counter += 1
                key = chunk_id_counter  # tie-breaker
                _heap_push_top_k(heaps["profane_strong"],
                                 TOP_K_PER_SIGNAL,
                                 (s["strong"] + s["medium"] * 0.4, key, tiny))
                _heap_push_top_k(heaps["rant"],
                                 TOP_K_PER_SIGNAL,
                                 (_rant_score(s), key, tiny))
                _heap_push_top_k(heaps["screaming"],
                                 TOP_K_PER_SIGNAL,
                                 (s["caps_ratio"] * s["word_count"] ** 0.5, key, tiny))
                _heap_push_top_k(heaps["exclamation"],
                                 TOP_K_PER_SIGNAL,
                                 (s["exclam_count"], key, tiny))
                _heap_push_top_k(heaps["short_brutal"],
                                 TOP_K_PER_SIGNAL,
                                 (_short_brutal_score(s), key, tiny))
                _heap_push_top_k(heaps["five_star_obscene"],
                                 TOP_K_PER_SIGNAL,
                                 (_five_star_obscene(rating, s), key, tiny))
                _heap_push_top_k(heaps["five_star_one_word"],
                                 TOP_K_PER_SIGNAL,
                                 (_five_star_boring(rating, s), key, tiny))
    except Exception as e:
        resp.close()
        return {"chunk_id": chunk_id, "error": f"stream_fail: {type(e).__name__}: {e}",
                "n_parsed": n_parsed}
    resp.close()

    # Flatten heaps: sorted descending, drop zero-score entries
    def flat(h):
        out = sorted(h, key=lambda x: -x[0])
        return [item for item in out if item[0] > 0]

    payload = {
        "chunk_id": chunk_id,
        "file_path": file_path,
        "category": category,
        "byte_start": start,
        "byte_end": end,
        "n_parsed": n_parsed,
        "n_profane": n_profane,
        "n_skipped": n_skipped,
        "rating_counts": rating_counts,
        "length_sum": length_sum,
        "elapsed_s": round(time.time() - t0, 2),
        "top": {
            sig: [{"score": round(s, 3), "review": r}
                  for s, _k, r in flat(h)]
            for sig, h in heaps.items()
        },
    }

    out_path = os.path.join(OUTPUT_DIR, f"{chunk_id}.json")
    with open(out_path, "w") as f:
        json.dump(payload, f)

    # Return a small summary (heaps are too big to return in-memory)
    return {
        "chunk_id": chunk_id,
        "category": category,
        "n_parsed": n_parsed,
        "n_profane": n_profane,
        "rating_counts": rating_counts,
        "elapsed_s": payload["elapsed_s"],
        "bytes": end - start,
    }


if __name__ == "__main__":
    # Local smoke test on a tiny in-memory sample
    sample = json.dumps({
        "rating": 1.0, "title": "WTF",
        "text": "This is the most FUCKING RIDICULOUS product I've ever bought!!! GARBAGE GARBAGE GARBAGE.",
        "asin": "B000", "user_id": "u1", "helpful_vote": 0,
        "verified_purchase": True, "timestamp": 123,
    })
    s = _score(json.loads(sample)["text"])
    print(json.dumps(s, indent=2))
