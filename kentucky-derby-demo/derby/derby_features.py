"""
derby_features.py
-----------------
v2 -- reads from real CSVs (data/historical_results.csv and data/field_2026.csv,
both produced by derby_build.py) and writes data/train_features.csv used by
derby_model.py and the rest of the pipeline.

v1 had three hand-coded dicts (EXPERT_SCORES, PACE_FIT, POST_DRAW_ADJUSTMENTS)
plus a FIELD_2026 literal that the audit flagged. All four are gone in v2:
  * field_2026.csv comes from real HRN + morning-line data via derby_build.py
  * pace fit comes from HRN's measured Last-3f times
  * the "expert score" hand-tuning is replaced by the Dirichlet weight search
    in derby_sensitivity.py (no manual prior to bias the model)

Saves: data/train_features.csv
"""

import os
import sys
from pathlib import Path

import numpy as np
import pandas as pd

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

DATA_DIR = Path(__file__).resolve().parent / "data"


def normalize(series: pd.Series) -> pd.Series:
    lo, hi = float(series.min()), float(series.max())
    if hi == lo:
        return pd.Series([5.0] * len(series), index=series.index)
    return (series - lo) / (hi - lo) * 10.0


def build_training_features(hist_df: pd.DataFrame) -> pd.DataFrame:
    """Build feature matrix for ML training from historical data.

    v2 changes from v1:
      * Drop implied_prob (= 1 / (odds + 1)). That feature embedded the
        closing-market signal directly into the training labels and made the
        model relearn the bookmaker. Audit fix #4.
      * Replace post_wp_approx (heuristic 8.0 if mid-pack else 5.0) with
        real per-post historical win rate over 2010-2025.
      * Add trainer_dw_norm, jockey_dw_norm computed from the actual Wikipedia
        finishing orders (no hand-typed dictionaries).
    """
    # Real per-post win rates from the historical data (computed once).
    post_wp_pct = (hist_df.groupby("post")["is_winner"].mean() * 100.0).to_dict()

    # Trainer + jockey Derby-win counts from real data; map back to per-row.
    def _last(name: str) -> str:
        return name.split()[-1].rstrip(".,").lower() if name else ""

    trainer_wins_by_last = {}
    for trainer, grp in hist_df.groupby("trainer"):
        trainer_wins_by_last.setdefault(_last(trainer), 0)
        trainer_wins_by_last[_last(trainer)] += int(grp["is_winner"].sum())
    jockey_wins_by_last = {}
    for jockey, grp in hist_df.groupby("jockey"):
        jockey_wins_by_last.setdefault(_last(jockey), 0)
        jockey_wins_by_last[_last(jockey)] += int(grp["is_winner"].sum())

    rows = []
    for _, r in hist_df.iterrows():
        beyer_val = r.get("beyer") if pd.notna(r.get("beyer")) else 95
        beyer_norm = float(np.clip((beyer_val - 80) / 3.0, 0, 10))
        dosage_val = r.get("dosage") if pd.notna(r.get("dosage")) else 2.5
        dosage_score = float(np.clip(10 - (dosage_val - 1.0) * (10 / 6.0), 0, 10))
        style_map = {1: 4.0, 2: 8.5, 3: 8.0, 4: 7.0, 5: 5.5}
        run_style_score = style_map.get(int(r.get("run_style", 3)), 6.5)

        post_val = r.get("post")
        if pd.isna(post_val):
            post_int = 10
        else:
            post_int = int(post_val)

        post_wp_real = post_wp_pct.get(post_int, 5.0)

        cond = str(r.get("condition", "fast")).lower()
        muddy = int(cond in ("muddy", "sloppy"))

        t_wins = trainer_wins_by_last.get(_last(str(r.get("trainer", ""))), 0)
        j_wins = jockey_wins_by_last.get(_last(str(r.get("jockey", ""))), 0)

        rows.append({
            "year": r["year"],
            "is_winner": int(r["is_winner"]),
            "beyer_norm": beyer_norm,
            "dosage_score": dosage_score,
            "run_style_score": run_style_score,
            "post": post_int,
            "post_wp_real": post_wp_real,
            "muddy": muddy,
            "trainer_dw": t_wins,
            "jockey_dw": j_wins,
        })

    df = pd.DataFrame(rows)
    df["trainer_dw_norm"] = normalize(df["trainer_dw"])
    df["jockey_dw_norm"] = normalize(df["jockey_dw"])
    return df


def main():
    DATA_DIR.mkdir(exist_ok=True)
    hist_path = DATA_DIR / "historical_results.csv"
    if not hist_path.exists():
        print("Historical data not found. Run derby_build.py first.")
        sys.exit(1)

    hist_df = pd.read_csv(hist_path)
    print(f"Loaded {len(hist_df)} historical records from {hist_df['year'].nunique()} years")

    print("Building ML training features...")
    train_df = build_training_features(hist_df)
    train_path = DATA_DIR / "train_features.csv"
    train_df.to_csv(train_path, index=False)
    print(f"Saved training features ({len(train_df)} rows) -> {train_path}")
    print(f"  Win-rate by post (top 5): {dict(sorted(((p, hist_df[hist_df.post == p]['is_winner'].mean()) for p in hist_df.post.dropna().unique()), key=lambda x: -x[1])[:5])}")


if __name__ == "__main__":
    main()
