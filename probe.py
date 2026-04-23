"""Phase 2a: Amazon Review Distiller data access probe.

Streams just the first ~4 MB of the All_Beauty.jsonl file directly from
HuggingFace's CDN (HTTP Range via requests iter_lines). Never downloads
the full file. laptop disk is tight and we only need to prove:
  - file is reachable
  - schema matches expectations
  - 1-star and 5-star reviews exist
  - samples actually look unhinged enough to justify the demo

The full files will only ever land on `/workspace/shared` on the Burla
cluster during the scale-up phase.
"""
from __future__ import annotations

import json
import random
import sys
from pathlib import Path


REPO_ID = "McAuley-Lab/Amazon-Reviews-2023"
RAW_REVIEW_FILE = "raw/review_categories/All_Beauty.jsonl"
HF_URL = f"https://huggingface.co/datasets/{REPO_ID}/resolve/main/{RAW_REVIEW_FILE}"
MAX_BYTES = 4 * 1024 * 1024  # 4 MB window, plenty for 5000+ rows


def probe() -> dict:
    try:
        import requests
    except ImportError:
        print("BLOCKED: `pip install requests` required", file=sys.stderr)
        sys.exit(2)

    print(f"streaming {HF_URL}")
    print(f"  window: first {MAX_BYTES // 1024 // 1024} MB (not downloading full file)")

    headers = {"Range": f"bytes=0-{MAX_BYTES - 1}"}
    resp = requests.get(HF_URL, headers=headers, stream=True, timeout=30)
    if resp.status_code not in (200, 206):
        print(f"BLOCKED: HTTP {resp.status_code}. {resp.text[:300]}", file=sys.stderr)
        sys.exit(3)
    content_length = int(resp.headers.get("content-length", 0))
    full_length = resp.headers.get("content-range", "")
    print(f"  http_status={resp.status_code} content-length={content_length} content-range={full_length}")

    rows: list[dict] = []
    buf = b""
    for chunk in resp.iter_content(chunk_size=65536):
        if not chunk:
            continue
        buf += chunk
        lines = buf.split(b"\n")
        buf = lines.pop()  # keep trailing partial line for next iter
        for line in lines:
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except Exception:
                pass
        if len(rows) >= 10000:
            break
    resp.close()

    if not rows:
        print("BLOCKED: zero rows parsed from stream", file=sys.stderr)
        sys.exit(4)

    required = {"rating", "text", "title", "asin", "user_id", "timestamp", "verified_purchase"}
    cols = set(rows[0].keys())
    missing = required - cols
    if missing:
        print(f"BLOCKED: schema missing fields {missing} (have {cols})", file=sys.stderr)
        sys.exit(5)

    ones = [r for r in rows if r.get("rating") in (1, 1.0)]
    fives = [r for r in rows if r.get("rating") in (5, 5.0)]
    if not ones or not fives:
        print(f"BLOCKED: no 1-star ({len(ones)}) or 5-star ({len(fives)}) in {len(rows)}-row sample", file=sys.stderr)
        sys.exit(6)

    rng = random.Random(1337)
    sampled_ones = rng.sample(ones, min(3, len(ones)))
    sampled_fives = rng.sample(fives, min(2, len(fives)))

    out_path = Path(__file__).parent / "samples" / "beauty_probe.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps({
        "repo": REPO_ID,
        "file": RAW_REVIEW_FILE,
        "stream_url": HF_URL,
        "bytes_window": MAX_BYTES,
        "columns": sorted(cols),
        "rows_inspected": len(rows),
        "counts_in_sample": {"1_star": len(ones), "5_star": len(fives)},
        "samples": {"one_star": sampled_ones, "five_star": sampled_fives},
    }, indent=2, default=str) + "\n")

    print("=" * 70)
    print(f"PROBE_OK: {len(rows)} rows parsed from first {MAX_BYTES // 1024 // 1024} MB")
    print(f"  columns: {sorted(cols)}")
    print(f"  in sample: {len(ones)} one-star, {len(fives)} five-star")
    print()
    print("--- Sample 1-star reviews ---")
    for i, r in enumerate(sampled_ones, 1):
        print(f"[{i}] rating={r['rating']} verified={r.get('verified_purchase')} asin={r['asin']}")
        print(f"    title: {r.get('title', '')[:140]}")
        print(f"    text: {r.get('text', '')[:400]}")
        print()
    print("--- Sample 5-star reviews ---")
    for i, r in enumerate(sampled_fives, 1):
        print(f"[{i}] rating={r['rating']} verified={r.get('verified_purchase')} asin={r['asin']}")
        print(f"    title: {r.get('title', '')[:140]}")
        print(f"    text: {r.get('text', '')[:400]}")
        print()
    print(f"wrote {out_path}")
    return {"status": "ok"}


if __name__ == "__main__":
    probe()
