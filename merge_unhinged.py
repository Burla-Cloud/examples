"""Merge the two vulgar corpora into one consolidated `unhinged` dataset.

Sources:
  samples/ard_vulgar_ranked.json    — hard profanity (shit/bitch/cunt/...)
  samples/ard_worst_ranked.json     — slurs + censored profanity

Output:
  samples/unhinged_wall.json        — top 120 for the main Unhinged wall
  samples/unhinged_search.json      — top 400 for the search corpus
  samples/unhinged_stats.json       — aggregate stats for the hero blurb

The two corpora scored reviews on different scales, so we normalize
`_rescore` per-source before merging. Dedup is (asin, title, text slice).

Run locally, no Burla needed.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List

HERE = Path(__file__).parent
SAMPLES = HERE / "samples"


def _load(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    d = json.load(open(path))
    if isinstance(d, list):
        return d
    return d.get("rows") or []


def _dedup_key(r: Dict[str, Any]) -> str:
    return f"{r.get('asin','')}|{(r.get('title') or '')[:40].lower()}|{(r.get('text') or '')[:60].lower()}"


def _normalize_per_source(rows: List[Dict[str, Any]], source: str) -> List[Dict[str, Any]]:
    """Scale `_rescore` into [0, 1] per source so they can be merged fairly."""
    if not rows:
        return rows
    scores = [float(r.get("_rescore") or 0) for r in rows]
    top = max(scores) or 1.0
    bottom = min(scores)
    rng = (top - bottom) or 1.0
    for r in rows:
        s = float(r.get("_rescore") or 0)
        r["_source"] = source
        r["_norm"] = (s - bottom) / rng
    return rows


def _category_class(cats: Dict[str, Dict[str, int]]) -> str:
    """Label for UI badges. Priorities: RS_HARD > RS > HOM > ABL > SEX > XEN > VULG."""
    if not cats:
        return ""
    order = ["RS_HARD", "RS", "HOM", "XEN", "ABL", "SEX", "VULG"]
    for k in order:
        if k in cats:
            return {
                "RS_HARD": "RACIAL_SLUR",
                "RS":      "RACIAL_SLUR",
                "HOM":     "HOMOPHOBIC_SLUR",
                "ABL":     "ABLEIST_SLUR",
                "SEX":     "GENDERED_SLUR",
                "XEN":     "XENOPHOBIC_SLUR",
                "VULG":    "PROFANITY",
            }[k]
    return ""


def _enrich(r: Dict[str, Any]) -> Dict[str, Any]:
    sc = r.get("score") or r.get("_score") or {}
    cats = sc.get("categories") or {}
    r.setdefault("_category", r.get("category"))
    r["_badge"] = _category_class(cats)
    r["_slur_categories"] = sorted(cats.keys())
    return r


def main() -> None:
    hard = _load(SAMPLES / "ard_vulgar_ranked.json")
    worst = _load(SAMPLES / "ard_worst_ranked.json")

    _normalize_per_source(hard, source="hard_profanity")
    _normalize_per_source(worst, source="worst_of_worse")

    merged: Dict[str, Dict[str, Any]] = {}
    for r in hard + worst:
        key = _dedup_key(r)
        existing = merged.get(key)
        if existing is None or r["_norm"] > existing["_norm"]:
            merged[key] = r

    rows = [_enrich(r) for r in merged.values()]
    # Combined sort: normalized score (70%), plus bonus for slur categories
    # (so HOM/RS_HARD tier surfaces higher than raw hard profanity).
    def _combined(r: Dict[str, Any]) -> float:
        base = r.get("_norm", 0) * 70
        cats = (r.get("score") or {}).get("categories") or {}
        bump = 0.0
        if "RS_HARD" in cats:
            bump += 15.0
        if "RS" in cats:
            bump += 6.0
        if "HOM" in cats:
            bump += 4.0
        return base + bump

    rows.sort(key=_combined, reverse=True)

    wall_rows = rows[:120]
    search_rows = rows[:400]

    out_wall = SAMPLES / "unhinged_wall.json"
    out_search = SAMPLES / "unhinged_search.json"
    out_stats = SAMPLES / "unhinged_stats.json"

    out_wall.write_text(json.dumps({
        "blurb": (
            "The most unhinged of {total:,}+ Amazon reviews — every f-bomb, "
            "slur, censored rant, and full-caps meltdown the first three passes "
            "could surface. Flip Unhinged Mode off to return to the normal Wall."
        ).format(total=571_544_386),
        "rows": wall_rows,
    }, indent=2))

    out_search.write_text(json.dumps({
        "rows": search_rows,
    }, indent=2))

    cat_counts: Dict[str, int] = {}
    for r in rows:
        for c in r.get("_slur_categories") or []:
            cat_counts[c] = cat_counts.get(c, 0) + 1
    out_stats.write_text(json.dumps({
        "merged_rows": len(rows),
        "hard_profanity_rows": len(hard),
        "worst_of_worse_rows": len(worst),
        "wall_rows": len(wall_rows),
        "search_rows": len(search_rows),
        "category_counts": cat_counts,
    }, indent=2))

    print(f"merged: {len(rows)} unique rows ({len(hard)} hard + {len(worst)} worst after dedup)")
    print(f"wrote unhinged_wall.json ({len(wall_rows)})")
    print(f"wrote unhinged_search.json ({len(search_rows)})")
    print(f"category mix: {cat_counts}")


if __name__ == "__main__":
    main()
