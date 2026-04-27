"""Aggregation pass v2: clean tokens + multi-word phrases per photo.

Why v2: user tags in YFCC are comma-separated multi-word phrases joined with
"+". Tokenizing the whole thing with a word regex turns "le mans 24 hours"
into ["le", "mans", "hours"], destroying the phrase. We now keep the
phrase as the primary unit, and also extract cleaner unigrams from title/
usertags while aggressively stop-wording Flickr's HTML description cruft
(href, http, fwww, nofollow, rel, fphotos, etc).

Outputs per shard on /workspace/shared/wpi/agg/{shard}.json:
  - country_photos           (cc -> n)
  - country_phrases          (cc -> {phrase: n})
  - country_tokens           (cc -> {token: n})
  - admin_phrases            ("cc|admin1" -> {phrase: n})
  - city_phrases             ("cc|admin1|city" -> {phrase: n})
  - country_samples          (cc -> [up to 8 display-ready rows])
"""
from __future__ import annotations

import argparse
import json
import os
import re
import time
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Dict, List, Tuple

from burla import remote_parallel_map


SHARD_DIR = "/workspace/shared/wpi/shards"
AGG_DIR = "/workspace/shared/wpi/agg"


STOP_TOKENS = frozenset(
    """
    the a an and or of in on at to from by with for as is are was were be been being this that these those
    it its i my me we our you your he she they them his her their our ours mine yours theirs us them

    com www http https href nofollow rel taken where esee fwww fflickr fphotos fgroups fgeotagging fdiscuss
    flickr staticflickr img image images photo photos pic pics picture pictures img_ dsc dscn p p_ imgp
    camera canon nikon pentax sony fujifilm fuji olympus leica samsung apple iphone android digital dslr
    lens shot shots snapshot raw jpg jpeg png mp mmf mb gig resolution iso f_ mm ef exposure lightroom
    photoshop lr ps snap instagram igers igerf

    color colour bw blackandwhite sepia hdr macro bokeh portrait landscape night light lighting natural
    outdoor outside inside indoor studio frame cropped edited

    new old day days night year years month months week weeks hour hours time times morning evening
    afternoon weekend trip visit travel visit vacation holiday holidays

    me myself you us them thing things stuff
    """.split()
)

# Flickr HTML description cruft: any hex-y or unrecognizable fragment
NON_WORD_RX = re.compile(r"[a-z][a-z0-9]+")
HEX_RX = re.compile(r"^[a-f0-9]{2,}$")


def _clean_token(t: str) -> str:
    t = t.strip()
    if not t:
        return ""
    if t in STOP_TOKENS:
        return ""
    if len(t) < 3:
        return ""
    if t.isdigit():
        return ""
    # Hex-only tokens (URL fragments)
    if HEX_RX.fullmatch(t) and not any(v in t for v in "aeiou"):
        return ""
    # Token must have at least one vowel (filters out "bhz" style garbage)
    if not any(v in t for v in "aeiou"):
        return ""
    return t


def _extract_phrases(usertags: str) -> List[str]:
    if not usertags:
        return []
    out: List[str] = []
    for raw in usertags.split(","):
        raw = raw.strip()
        if not raw:
            continue
        phrase = raw.replace("+", " ").strip().lower()
        if not phrase or len(phrase) < 3:
            continue
        # drop pure-digit and hex-id phrases
        words = phrase.split()
        if all(w.isdigit() for w in words):
            continue
        if all(HEX_RX.fullmatch(w) for w in words):
            continue
        # Remove junk words that sometimes sneak into tags
        words = [w for w in words if _clean_token(w)]
        if not words:
            continue
        # reject single-stop-word phrases
        phrase = " ".join(words).strip()
        if not phrase or phrase in STOP_TOKENS:
            continue
        if len(phrase) > 48:
            phrase = phrase[:48].rstrip()
        out.append(phrase)
    return out


def _extract_tokens(row: Dict[str, Any]) -> List[str]:
    """Token extraction from title and description (description is HTML-y)."""
    title = (row.get("title") or "").lower()
    # title is url-encoded (%20 spaces, + spaces, %27 apostrophes) — decode roughly
    title = title.replace("%20", " ").replace("+", " ").replace("%27", "'")
    desc = (row.get("description") or "").lower()
    # description has flickr-generated html — strip angle-bracket tags
    desc = re.sub(r"<[^>]+>", " ", desc)
    combined = " ".join([title, desc])
    raw_tokens = NON_WORD_RX.findall(combined)
    out = []
    for t in raw_tokens:
        c = _clean_token(t)
        if c:
            out.append(c)
    return out


def process_shard_file(shard_id: str) -> Dict[str, Any]:
    t0 = time.time()
    path = os.path.join(SHARD_DIR, f"{shard_id}.jsonl")
    if not os.path.exists(path):
        return {"shard": shard_id, "error": "missing_input"}

    country_photos = Counter()
    country_phrases: Dict[str, Counter] = defaultdict(Counter)
    country_tokens: Dict[str, Counter] = defaultdict(Counter)
    admin_phrases: Dict[str, Counter] = defaultdict(Counter)
    city_phrases: Dict[str, Counter] = defaultdict(Counter)
    country_keep: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    n_rows = 0

    with open(path) as f:
        for line in f:
            try:
                r = json.loads(line)
            except Exception:
                continue
            n_rows += 1
            cc = r.get("country_cc") or "??"
            admin1 = r.get("admin1") or ""
            city = r.get("city") or ""
            country_photos[cc] += 1

            phrases = _extract_phrases(r.get("usertags") or "")
            tokens = _extract_tokens(r)

            if phrases:
                country_phrases[cc].update(phrases)
                admin_phrases[f"{cc}|{admin1}"].update(phrases)
                city_phrases[f"{cc}|{admin1}|{city}"].update(phrases)
            if tokens:
                country_tokens[cc].update(tokens)

            if len(country_keep[cc]) < 8 and (phrases or tokens):
                country_keep[cc].append({
                    "photoid": r.get("photoid"),
                    "key": r.get("key"),
                    "shard": r.get("shard"),
                    "lat": r.get("lat"),
                    "lon": r.get("lon"),
                    "admin1": admin1,
                    "city": city,
                    "title": (r.get("title") or "")[:140],
                    "usertags": (r.get("usertags") or "")[:200],
                    "downloadurl": r.get("downloadurl"),
                    "top_phrases": phrases[:10],
                })

    os.makedirs(AGG_DIR, exist_ok=True)
    agg_path = os.path.join(AGG_DIR, f"{shard_id}.json")
    with open(agg_path, "w") as f:
        json.dump(
            {
                "shard": shard_id,
                "n_rows": n_rows,
                "country_photos": dict(country_photos),
                "country_phrases": {k: dict(v.most_common(180)) for k, v in country_phrases.items()},
                "country_tokens": {k: dict(v.most_common(180)) for k, v in country_tokens.items()},
                "admin_phrases": {k: dict(v.most_common(60)) for k, v in admin_phrases.items()},
                "city_phrases": {k: dict(v.most_common(40)) for k, v in city_phrases.items()},
                "country_samples": dict(country_keep),
            },
            f,
        )

    return {
        "shard": shard_id,
        "rows": n_rows,
        "countries": len(country_photos),
        "agg_path": agg_path,
        "elapsed_s": round(time.time() - t0, 2),
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--max-parallelism", type=int, default=1100)
    args = ap.parse_args()

    from pipeline import REPO_ID  # noqa
    from huggingface_hub import HfApi
    api = HfApi()
    files = api.list_repo_files(REPO_ID, repo_type="dataset")
    shards = sorted(
        f.removeprefix("metadata/metadata_").removesuffix(".jsonl.gz")
        for f in files
        if f.startswith("metadata/metadata_") and f.endswith(".jsonl.gz")
    )
    print(f"aggregating v2 (phrases + tokens) across {len(shards)} shards, max_parallelism={args.max_parallelism}")

    t0 = time.time()
    results = remote_parallel_map(
        process_shard_file,
        shards,
        func_cpu=1,
        func_ram=4,
        grow=True,
        max_parallelism=args.max_parallelism,
        spinner=True,
    )
    elapsed = time.time() - t0

    successes = [r for r in results if "error" not in r]
    failures = [r for r in results if "error" in r]
    total_rows = sum(r.get("rows", 0) for r in successes)

    summary = {
        "elapsed_seconds": round(elapsed, 2),
        "shards_submitted": len(shards),
        "shards_succeeded": len(successes),
        "shards_failed": len(failures),
        "total_rows": total_rows,
        "throughput_rows_per_sec": round(total_rows / elapsed, 1) if elapsed else 0,
    }
    print()
    print("=" * 70)
    print(json.dumps(summary, indent=2))

    out = Path(__file__).parent / "samples" / "wpi_agg_v2_summary.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps({**summary, "first_failures": failures[:10]}, indent=2) + "\n")
    print(f"wrote {out}")


if __name__ == "__main__":
    main()
