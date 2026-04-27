"""World Photo Index — per-shard worker for Burla `remote_parallel_map`.

Per-shard workflow (no ML on the critical path for speed):
  1. Download metadata (.jsonl.gz, 7KB–1.4MB) for the shard from HF Hub.
  2. Filter to geotagged rows.
  3. Reverse-geocode (lat, lon) -> (country_cc, admin1, city) in one batched
     call via `reverse_geocoder` (embedded KD-tree, ~16MB, in-memory).
  4. Extract signal text per photo from user tags + title + description.
  5. Write a compact JSONL row per photo to /workspace/shared/wpi/shards/.

This path stays under 30s per shard for 200 rows and keeps the 1000-CPU
fan-out actually usable. CLIP image embedding is available as a separate
optional phase (embed_photos.py) on top-N highlights per country after
the aggregate is computed.
"""
from __future__ import annotations

import gzip
import io
import json
import os
import sys
import time
import zipfile
from typing import Any, Dict

import requests
from huggingface_hub import hf_hub_url
import reverse_geocoder as rg


REPO_ID = "dalle-mini/YFCC100M_OpenAI_subset"
OUTPUT_DIR = "/workspace/shared/wpi/shards"


_RG_READY = {"loaded": False}


def _ensure_rg_loaded() -> None:
    if _RG_READY["loaded"]:
        return
    rg.search([(0.0, 0.0)], mode=2)
    _RG_READY["loaded"] = True


def _extract_key_zip_path(key_hex: str) -> str:
    return f"data/images/{key_hex[:3]}/{key_hex[3:6]}/{key_hex}.jpg"


def process_shard(shard_id: str) -> Dict[str, Any]:
    """Burla worker entry point. `shard_id` is a 3-char hex like '100'."""
    t0 = time.time()
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    meta_url = hf_hub_url(REPO_ID, filename=f"metadata/metadata_{shard_id}.jsonl.gz", repo_type="dataset")
    try:
        m = requests.get(meta_url, timeout=60); m.raise_for_status()
    except Exception as e:
        return {"shard": shard_id, "error": f"meta_fetch_fail: {type(e).__name__}: {e}",
                "rows": 0, "geotagged": 0}

    try:
        rows = [
            json.loads(l)
            for l in gzip.decompress(m.content).decode("utf-8", errors="replace").split("\n")
            if l.strip()
        ]
    except Exception as e:
        return {"shard": shard_id, "error": f"metadata_parse_fail: {e}", "rows": 0, "geotagged": 0}

    geotagged = [r for r in rows if r.get("latitude") and r.get("longitude")]

    points = [(float(r["latitude"]), float(r["longitude"])) for r in geotagged]
    try:
        _ensure_rg_loaded()
        geo_results = rg.search(points, mode=2) if points else []
    except Exception as e:
        return {"shard": shard_id, "error": f"rg_fail: {e}", "rows": len(rows), "geotagged": 0}

    out_path = os.path.join(OUTPUT_DIR, f"{shard_id}.jsonl")
    written = 0
    with open(out_path, "w") as out_f:
        for r, geo in zip(geotagged, geo_results):
            key_hex = r.get("key") or ""
            if len(key_hex) < 6:
                continue
            out_f.write(json.dumps({
                "photoid": r["photoid"],
                "key": key_hex,
                "shard": shard_id,
                "lat": float(r["latitude"]),
                "lon": float(r["longitude"]),
                "country_cc": geo.get("cc"),
                "admin1": geo.get("admin1"),
                "city": geo.get("name"),
                "title": (r.get("title") or "")[:300],
                "usertags": (r.get("usertags") or "")[:400],
                "description": (r.get("description") or "")[:400],
                "datetaken": r.get("datetaken", ""),
                "downloadurl": r.get("downloadurl", ""),
                "zip_path": _extract_key_zip_path(key_hex),
            }) + "\n")
            written += 1

    elapsed = time.time() - t0
    return {
        "shard": shard_id,
        "rows": len(rows),
        "geotagged": len(geotagged),
        "written": written,
        "output_path": out_path,
        "elapsed_s": round(elapsed, 2),
    }


if __name__ == "__main__":
    shard = sys.argv[1] if len(sys.argv) > 1 else "100"
    print(json.dumps(process_shard(shard), indent=2))
