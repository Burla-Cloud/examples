"""
derby_build.py - Build clean CSVs from raw scraped artifacts.

Reads:
  data/raw/wikipedia/{year}.json  (real Derby finishing orders 2010-2025)
  data/raw/hrn_2026.json          (HRN measured 2026 features)
  data/raw/morning_line.json      (real 2026 post draw + morning-line odds)
  data/raw/wapo_winner_beyers.json (winner Beyers 1987-2025)

Writes:
  data/historical_results.csv  (real, year x finish x 20 horses)
  data/field_2026.csv          (real measured 2026 features)

Replaces what derby_scraper.py + derby_features.py:FIELD_2026 used to produce.
"""

import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, List

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent
DATA = ROOT / "data"
RAW = DATA / "raw"


def _load_json(path: Path) -> Any:
    with open(path) as f:
        return json.load(f)


def _normalize_condition(raw: str) -> str:
    """Reduce Wikipedia track-condition strings to the canonical
    ('fast', 'sloppy', 'good', 'muddy', 'wet-fast') vocabulary the model uses."""
    if not raw:
        return "fast"
    r = raw.lower().strip()
    if "sloppy" in r:
        return "sloppy"
    if "muddy" in r:
        return "muddy"
    if "good" in r:
        return "good"
    if "wet" in r:
        return "wet-fast"
    return "fast"


def build_historical_csv() -> pd.DataFrame:
    """Walk raw/wikipedia/*.json and build a real historical_results.csv."""
    wapo = _load_json(RAW / "wapo_winner_beyers.json")["winners"]

    rows = []
    years = sorted(int(p.stem) for p in (RAW / "wikipedia").glob("*.json"))
    for year in years:
        wiki = _load_json(RAW / "wikipedia" / f"{year}.json")
        if "error" in wiki:
            print(f"  skip {year}: {wiki['error'][:60]}")
            continue
        condition = _normalize_condition(wiki.get("track_condition") or "")
        winner_beyer = None
        wapo_year = wapo.get(str(year))
        if wapo_year:
            winner_beyer = wapo_year.get("beyer")
        # Year-level final-quarter split (last quarter mile).
        splits = wiki.get("splits") or []
        try:
            last_q = float(splits[-1]) if splits else None
        except (TypeError, ValueError):
            last_q = None

        import re as _re
        for f in wiki.get("finishers", []):
            finish = f.get("finish")
            if not isinstance(finish, int):
                continue
            if finish < 1 or finish > 25:
                continue
            # Clean horse name of Wikipedia annotations like "(winner)", "[d]", "(JPN)".
            horse = (f.get("horse") or "").strip()
            horse = _re.sub(r"\s*\[\s*[a-zA-Z0-9]+\s*\]\s*", "", horse)
            horse = _re.sub(r"\s*\((?:winner|JPN|GB|IRE|FR)\)\s*$", "", horse, flags=_re.IGNORECASE).strip()
            ml = f.get("ml_odds")
            fo = f.get("final_odds")
            odds_for_pipeline = fo if fo is not None else ml
            rows.append({
                "year": year,
                "finish": finish,
                "post": f.get("post"),
                "horse": horse,
                "trainer": (f.get("trainer") or "").strip(),
                "jockey": (f.get("jockey") or "").strip(),
                "odds": odds_for_pipeline if odds_for_pipeline is not None else 30.0,
                "ml_odds": ml,
                "final_odds": fo,
                "condition": condition,
                "run_style": 3,
                "beyer": winner_beyer if (winner_beyer is not None and finish == 1) else (winner_beyer or 95),
                "dosage": 2.5,
                "is_winner": int(finish == 1),
                "year_winner_beyer": winner_beyer,
                "year_last_quarter_s": last_q,
            })

    df = pd.DataFrame(rows)
    # Sort and write
    df = df.sort_values(["year", "finish"]).reset_index(drop=True)
    out = DATA / "historical_results.csv"
    df.to_csv(out, index=False)
    print(f"  historical_results.csv: {len(df)} rows across {df['year'].nunique()} years -> {out}")
    return df


def build_2026_csv(hist_df: pd.DataFrame) -> pd.DataFrame:
    """Build the real 2026 field CSV from raw/hrn_2026.json + raw/morning_line.json.

    Columns are the same as the existing derby_features.py output so downstream
    derby_model.py / derby_sensitivity.py / derby_montecarlo.py / derby_trillion.py
    don't need schema changes -- only data sources change.
    """
    hrn_raw = _load_json(RAW / "hrn_2026.json")["horses"]
    hrn_by_name = {h["name"]: h for h in hrn_raw}
    ml_data = _load_json(RAW / "morning_line.json")
    morning_line = ml_data["horses"]

    # Compute per-trainer + per-jockey Derby win counts and rates from the real
    # historical CSV (replaces the trainer_dw / jockey_dw hand-typed values).
    trainer_stats: Dict[str, Dict[str, float]] = {}
    for trainer, grp in hist_df.groupby("trainer"):
        starts = len(grp)
        wins = int(grp["is_winner"].sum())
        trainer_stats[trainer] = {
            "starts": starts,
            "wins": wins,
            "win_pct": wins / starts if starts > 0 else 0.0,
        }
    jockey_stats: Dict[str, Dict[str, float]] = {}
    for jockey, grp in hist_df.groupby("jockey"):
        starts = len(grp)
        wins = int(grp["is_winner"].sum())
        jockey_stats[jockey] = {
            "starts": starts,
            "wins": wins,
            "win_pct": wins / starts if starts > 0 else 0.0,
        }
    # Per-post historical win/ITM rates (same shape as current pipeline expects).
    post_stats: Dict[int, Dict[str, float]] = {}
    for post, grp in hist_df.groupby("post"):
        n = len(grp)
        wp = float(grp["is_winner"].sum() / n) * 100.0 if n else 0.0
        itm = float(((grp["finish"] <= 3).sum()) / n) * 100.0 if n else 0.0
        post_stats[int(post)] = {"post_wp": wp, "post_itm": itm}

    rows = []
    for ml in morning_line:
        post = ml["post"]
        if ml.get("also_eligible"):
            continue
        name = ml["name"]
        h = hrn_by_name.get(name, {})
        trainer = ml.get("trainer", "")
        jockey = ml.get("jockey", "")

        # Lookup trainer/jockey stats by approximate name match (Wikipedia uses
        # full names like 'Brad H. Cox', morning line might say 'Brad H. Cox' or
        # 'Cox, Brad H' depending on source).
        def _lookup_person(stats: Dict[str, Dict[str, float]], name_str: str) -> Dict[str, float]:
            """Sum stats across all keys whose last name matches. Wikipedia
            uses inconsistent name formats (e.g. 'Todd Pletcher' in older
            articles vs 'Todd A. Pletcher' in newer ones); we treat them as
            the same person if the last name matches AND the first initial
            matches. Returns aggregated starts/wins.
            """
            if not name_str:
                return {"starts": 0, "wins": 0, "win_pct": 0.0}
            # Normalize "Last, First M" -> "First M Last"
            normalized = name_str.strip()
            if "," in normalized:
                parts = [p.strip() for p in normalized.split(",", 1)]
                if len(parts) == 2:
                    normalized = f"{parts[1]} {parts[0]}"
            tokens = [t for t in normalized.split() if t and t != "Jr." and t != "Sr." and t != "II" and t != "III"]
            if not tokens:
                return {"starts": 0, "wins": 0, "win_pct": 0.0}
            last = tokens[-1].lower().rstrip(".,")
            first_initial = tokens[0][0].lower() if tokens[0] else ""
            agg_starts = 0
            agg_wins = 0
            for key, st in stats.items():
                if not key:
                    continue
                key_tokens = [t for t in key.split() if t not in ("Jr.", "Sr.", "II", "III")]
                if not key_tokens:
                    continue
                key_last = key_tokens[-1].lower().rstrip(".,")
                key_first = key_tokens[0][0].lower() if key_tokens[0] else ""
                if key_last == last and key_first == first_initial:
                    agg_starts += int(st.get("starts", 0))
                    agg_wins += int(st.get("wins", 0))
            win_pct = agg_wins / agg_starts if agg_starts > 0 else 0.0
            return {"starts": agg_starts, "wins": agg_wins, "win_pct": win_pct}

        ts = _lookup_person(trainer_stats, trainer)
        js = _lookup_person(jockey_stats, jockey)

        # Run-style derived from the final-furlong split when measured (lower
        # last-furlong = better closer; higher = front-runner that faded).
        last1f = h.get("last1f")
        last3f = h.get("last3f")
        if last3f is not None:
            if last3f < 36.0:
                style, style_score = 4, 7.0   # Close (best closer fraction)
            elif last3f < 37.0:
                style, style_score = 3, 8.0   # Stalk
            elif last3f < 38.0:
                style, style_score = 2, 8.5   # Press
            else:
                style, style_score = 1, 4.0   # Pace (faded)
        else:
            style, style_score = 3, 6.5

        # Pace fit: best for stalker/closers when many pace types are in the field.
        # Compute as inverse of last3f (faster final 3f = better fit when pace melts).
        pace_fit = max(0.0, min(10.0, 10.0 - (last3f - 35.5) * 2.0)) if last3f is not None else 6.0

        # Beyer / Brisnet / TFUS / HRN: foreign horses (Japan / UAE) don't have
        # comparable speed numbers; use the field-low-end placeholder.
        beyer = h.get("beyer") if h.get("beyer") is not None else 80
        brisnet = h.get("brisnet") if h.get("brisnet") is not None else 88
        tfus = h.get("tfus") if h.get("tfus") is not None else 100
        hrn_rating = h.get("hrn") if h.get("hrn") is not None else 105

        # Trainer + jockey scores: real Derby win count + Churchill historical
        # win pct as the primary signals (replaces hand-typed trainer_dw etc.).
        trainer_score = ts["wins"] * 1.5 + ts["win_pct"] * 30 + (post_stats.get(post, {}).get("post_wp", 5) / 10.0)
        jockey_score = js["wins"] * 2.0 + js["win_pct"] * 30 + 3.0

        ml_odds = ml.get("odds") or 20.0
        implied_prob = 1.0 / (ml_odds + 1) if ml_odds > 0 else 0.05

        ps = post_stats.get(post, {"post_wp": 5.0, "post_itm": 18.0})
        rows.append({
            "post": post,
            "name": name,
            "odds": ml_odds,
            "ml_odds_implied_pct": round(implied_prob * 100, 2),
            "beyer": beyer,
            "beyer_over_100": int(beyer >= 100),
            "brisnet": brisnet,
            "tfus": tfus,
            "hrn_rating": hrn_rating,
            "last1f": last1f if last1f is not None else 13.0,
            "last3f": last3f if last3f is not None else 38.0,
            "dosage": 2.5,  # placeholder; pedigree DI not free-scrapeable for 2026 field
            "dosage_score": 7.0,
            "run_style": style,
            "run_style_score": style_score,
            "pace_fit": pace_fit,
            "trainer": trainer,
            "trainer_dw": int(ts["wins"]),
            "trainer_score": trainer_score,
            "jockey": jockey,
            "jockey_dw": int(js["wins"]),
            "jockey_score": jockey_score,
            "win_rate": 0.5,  # placeholder; per-horse career stats not free-scrapeable
            "itm_rate": 0.75,
            "stamina_test": int(beyer >= 95),
            "post_wp": ps["post_wp"],
            "post_itm": ps["post_itm"],
            "pedigree_dist": 7.0,  # placeholder
            "expert_score": 5.0,    # placeholder; will be overridden by sensitivity weights
            "post_draw_adj": 0.0,
            "sire_won": 0,
            "foreign": int(h.get("foreign", False)),
        })

    df = pd.DataFrame(rows)
    # Normalize continuous features 0-10 (same convention as old derby_features.py).
    def _norm(s: pd.Series) -> pd.Series:
        lo, hi = float(s.min()), float(s.max())
        if hi == lo:
            return pd.Series([5.0] * len(s), index=s.index)
        return (s - lo) / (hi - lo) * 10.0
    df["beyer_norm"] = _norm(df["beyer"])
    df["brisnet_norm"] = _norm(df["brisnet"])
    df["tfus_norm"] = _norm(df["tfus"])
    df["last3f_norm"] = _norm(-df["last3f"])  # invert: lower last3f = higher score
    df["last1f_norm"] = _norm(-df["last1f"])
    df["trainer_score_norm"] = _norm(df["trainer_score"])
    df["jockey_score_norm"] = _norm(df["jockey_score"])
    df["win_rate_norm"] = _norm(df["win_rate"])
    df["itm_rate_norm"] = _norm(df["itm_rate"])
    df["post_wp_norm"] = _norm(df["post_wp"])
    df["post_itm_norm"] = _norm(df["post_itm"])
    df["pace_fit_norm"] = _norm(df["pace_fit"])

    df = df.sort_values("post").reset_index(drop=True)
    out = DATA / "field_2026.csv"
    df.to_csv(out, index=False)
    print(f"  field_2026.csv: {len(df)} horses -> {out}")
    return df


def main():
    print("Building real CSVs from raw/...")
    hist_df = build_historical_csv()
    field_df = build_2026_csv(hist_df)

    # Quick summary so we can see the headline numbers
    print("\nReal historical data summary:")
    print(f"  Years: {sorted(hist_df['year'].unique().tolist())}")
    print(f"  Total starters: {len(hist_df)}")
    print(f"  Track conditions: {hist_df['condition'].value_counts().to_dict()}")
    print(f"  Distinct trainers: {hist_df['trainer'].nunique()}")
    print(f"  Distinct jockeys: {hist_df['jockey'].nunique()}")
    print(f"\nReal 2026 field summary:")
    print(f"  Horses: {len(field_df)}")
    print(f"  Top Beyer: {field_df['beyer'].max()} (post {int(field_df.loc[field_df['beyer'].idxmax(), 'post'])} {field_df.loc[field_df['beyer'].idxmax(), 'name']})")
    print(f"  Morning-line favorite: {field_df.loc[field_df['odds'].idxmin(), 'name']} {field_df['odds'].min()}-1")
    print(f"  Avg trainer Derby wins: {field_df['trainer_dw'].mean():.2f}")
    print(f"  Avg jockey Derby wins: {field_df['jockey_dw'].mean():.2f}")


if __name__ == "__main__":
    main()
