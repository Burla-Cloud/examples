"""
derby_features.py
-----------------
Merges 2026 field data (from Excel) with historical results.
Builds a unified feature matrix for ML training and 2026 prediction.
Saves: data/field_2026.csv, data/train_features.csv
"""

import os
import sys
import numpy as np
import pandas as pd
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
EXCEL_PATH = r"C:\Users\Jack Rzucidlo\Downloads\Kentucky Derby Information 2026.xlsx"

# ─── 2026 field: manually structured from the user's Excel spreadsheet ────────
# All 18 data points per horse, post-draw final (Prat → Emerging Market)
FIELD_2026 = [
    # post horse           odds  beyer  trainer            trainer_dw jockey               jockey_dw  age ht   wt    sire              sire_won  starts wins 2nds 3rds  style    dosage stam post_wp post_itm pedigree_dist
    # style: 1=pace 2=press 3=stalk 4=close 5=deep  stam: 1=pass 0=fail  pedigree_dist 1-10
    dict(post=1,  name="Renegade",        odds=4.5,  beyer=97,  trainer="Todd Pletcher",      trainer_dw=2, jockey="Irad Ortiz Jr.",     jockey_dw=0, jockey_age=33, ht=64, wt=1080, sire="Into Mischief",    sire_won=0, starts=5, wins=2, places=2, shows=1, style=4, dosage=3.00, stam=1, post_wp=8.3,  post_itm=18.8, post_cdns_wp=19, pedigree_dist=8.5),
    dict(post=2,  name="Albus",           odds=30,   beyer=95,  trainer="Riley Mott",         trainer_dw=0, jockey="Manny Franco",       jockey_dw=0, jockey_age=31, ht=63, wt=1025, sire="Yaupon",          sire_won=0, starts=4, wins=2, places=0, shows=1, style=3, dosage=2.60, stam=1, post_wp=7.3,  post_itm=27.1, post_cdns_wp=19, pedigree_dist=5.0),
    dict(post=3,  name="Intrepido",       odds=50,   beyer=94,  trainer="Jeff Mullins",       trainer_dw=0, jockey="Hector Berrios",     jockey_dw=0, jockey_age=39, ht=64, wt=1070, sire="Maximus Mischief", sire_won=0, starts=6, wins=2, places=1, shows=0, style=2, dosage=3.40, stam=1, post_wp=6.3,  post_itm=22.9, post_cdns_wp=14, pedigree_dist=5.0),
    dict(post=4,  name="Litmus Test",     odds=30,   beyer=96,  trainer="Bob Baffert",        trainer_dw=6, jockey="Martin Garcia",      jockey_dw=0, jockey_age=41, ht=66, wt=1160, sire="Nyquist",         sire_won=1, starts=7, wins=2, places=0, shows=2, style=3, dosage=3.36, stam=1, post_wp=5.2,  post_itm=15.6, post_cdns_wp=20, pedigree_dist=7.0),
    dict(post=5,  name="Commandment",     odds=6.5,  beyer=104, trainer="Brad Cox",           trainer_dw=1, jockey="Luis Saez",          jockey_dw=1, jockey_age=33, ht=66, wt=1150, sire="Into Mischief",    sire_won=0, starts=5, wins=4, places=0, shows=0, style=3, dosage=3.44, stam=1, post_wp=10.4, post_itm=22.9, post_cdns_wp=26, pedigree_dist=8.5),
    dict(post=6,  name="Danon Bourbon",   odds=20,   beyer=94,  trainer="Manabu Ikezoe",      trainer_dw=0, jockey="Atsuya Nishimura",   jockey_dw=0, jockey_age=26, ht=64, wt=1090, sire="Maxfield",        sire_won=0, starts=3, wins=3, places=0, shows=0, style=3, dosage=1.86, stam=1, post_wp=2.1,  post_itm=13.5, post_cdns_wp=10, pedigree_dist=5.5),
    dict(post=7,  name="So Happy",        odds=6.0,  beyer=98,  trainer="Mark Glatt",         trainer_dw=0, jockey="Mike Smith",         jockey_dw=2, jockey_age=60, ht=65, wt=1100, sire="Run Happy",        sire_won=1, starts=4, wins=3, places=0, shows=1, style=3, dosage=7.00, stam=0, post_wp=8.4,  post_itm=22.1, post_cdns_wp=14, pedigree_dist=3.0),
    dict(post=8,  name="The Puma",        odds=10,   beyer=99,  trainer="Gustavo Delgado",    trainer_dw=1, jockey="Javier Castellano",  jockey_dw=1, jockey_age=48, ht=65, wt=1120, sire="Essential Quality", sire_won=0, starts=4, wins=1, places=2, shows=1, style=4, dosage=2.47, stam=1, post_wp=9.5,  post_itm=20.0, post_cdns_wp=22, pedigree_dist=8.5),
    dict(post=9,  name="Wonder Dean",     odds=30,   beyer=95,  trainer="Daisuke Takayanagi", trainer_dw=0, jockey="Ryusei Sakai",       jockey_dw=0, jockey_age=28, ht=65, wt=1130, sire="Dee Majesty",      sire_won=0, starts=6, wins=2, places=2, shows=0, style=3, dosage=2.11, stam=1, post_wp=4.3,  post_itm=19.6, post_cdns_wp=10, pedigree_dist=8.0),
    dict(post=10, name="Incredibolt",     odds=20,   beyer=91,  trainer="Riley Mott",         trainer_dw=0, jockey="Jaime Torres",       jockey_dw=0, jockey_age=27, ht=67, wt=1200, sire="Bolt d'Oro",      sire_won=0, starts=5, wins=3, places=0, shows=0, style=3, dosage=3.00, stam=1, post_wp=10.1, post_itm=29.2, post_cdns_wp=14, pedigree_dist=5.5),
    dict(post=11, name="Chief Wallabee",  odds=8.0,  beyer=93,  trainer="Bill Mott",          trainer_dw=1, jockey="Junior Alvarado",    jockey_dw=1, jockey_age=39, ht=65, wt=1110, sire="Constitution",     sire_won=0, starts=3, wins=1, places=1, shows=1, style=2, dosage=1.92, stam=1, post_wp=2.4,  post_itm=14.1, post_cdns_wp=19, pedigree_dist=9.0),
    dict(post=12, name="Potente",         odds=20,   beyer=96,  trainer="Bob Baffert",        trainer_dw=6, jockey="Juan Hernandez",     jockey_dw=0, jockey_age=34, ht=66, wt=1180, sire="Into Mischief",    sire_won=0, starts=3, wins=2, places=1, shows=0, style=1, dosage=3.44, stam=1, post_wp=3.7,  post_itm=12.3, post_cdns_wp=20, pedigree_dist=7.0),
    dict(post=13, name="Emerging Market", odds=15,   beyer=98,  trainer="Chad Brown",         trainer_dw=0, jockey="Flavien Prat",       jockey_dw=1, jockey_age=33, ht=67, wt=1190, sire="Candy Ride",       sire_won=0, starts=2, wins=2, places=0, shows=0, style=3, dosage=2.20, stam=1, post_wp=6.3,  post_itm=21.5, post_cdns_wp=25, pedigree_dist=8.0),
    dict(post=14, name="Pavlovian",       odds=30,   beyer=93,  trainer="Doug O'Neill",       trainer_dw=2, jockey="Edwin Maldonado",    jockey_dw=0, jockey_age=43, ht=65, wt=1125, sire="Pavel",            sire_won=0, starts=10, wins=2, places=4, shows=1, style=1, dosage=1.29, stam=1, post_wp=2.9,  post_itm=20.3, post_cdns_wp=15, pedigree_dist=5.5),
    dict(post=15, name="Six Speed",       odds=50,   beyer=91,  trainer="Bhupat Seemar",      trainer_dw=0, jockey="Brian Hernandez Jr.", jockey_dw=1, jockey_age=40, ht=64, wt=1100, sire="Not This Time",   sire_won=0, starts=6, wins=3, places=1, shows=1, style=1, dosage=5.00, stam=0, post_wp=9.4,  post_itm=14.1, post_cdns_wp=14, pedigree_dist=6.5),
    dict(post=16, name="Further Ado",     odds=6.5,  beyer=106, trainer="Brad Cox",           trainer_dw=1, jockey="John Velazquez",     jockey_dw=3, jockey_age=54, ht=68, wt=1210, sire="Gunrunner",        sire_won=0, starts=6, wins=3, places=1, shows=1, style=2, dosage=2.08, stam=1, post_wp=9.4,  post_itm=20.8, post_cdns_wp=26, pedigree_dist=9.5),
    dict(post=17, name="Golden Tempo",    odds=30,   beyer=90,  trainer="Cherie DeVaux",      trainer_dw=0, jockey="Jose Ortiz",         jockey_dw=0, jockey_age=32, ht=66, wt=1155, sire="Curlin",           sire_won=0, starts=4, wins=2, places=0, shows=2, style=5, dosage=3.00, stam=1, post_wp=0.0,  post_itm=6.5,  post_cdns_wp=16, pedigree_dist=8.0),
    dict(post=18, name="Great White",     odds=50,   beyer=94,  trainer="John Ennis",         trainer_dw=0, jockey="Alex Achard",        jockey_dw=0, jockey_age=30, ht=70, wt=1320, sire="Volatile",         sire_won=0, starts=4, wins=2, places=0, shows=0, style=3, dosage=3.00, stam=1, post_wp=5.3,  post_itm=15.8, post_cdns_wp=10, pedigree_dist=3.0),
    dict(post=19, name="Ocelli",          odds=50,   beyer=81,  trainer="D Whitworth Beckman", trainer_dw=0, jockey="Joseph D Ramos",    jockey_dw=0, jockey_age=26, ht=67, wt=1195, sire="Connect",          sire_won=0, starts=6, wins=0, places=1, shows=3, style=5, dosage=1.73, stam=1, post_wp=3.1,  post_itm=9.4,  post_cdns_wp=10, pedigree_dist=5.0),
    dict(post=20, name="Robusta",         odds=50,   beyer=93,  trainer="Doug O'Neill",       trainer_dw=2, jockey="Emisael Jaramillo",  jockey_dw=0, jockey_age=49, ht=65, wt=1115, sire="Accelerate",       sire_won=0, starts=5, wins=1, places=1, shows=0, style=1, dosage=3.44, stam=1, post_wp=10.5, post_itm=15.8, post_cdns_wp=15, pedigree_dist=7.0),
]

# Expert consensus scores (0-10): aggregated from Aces&Races, CBS, Yahoo, SportsLine
EXPERT_SCORES = {
    "Further Ado": 9.2, "Commandment": 8.8, "The Puma": 7.8,
    "Renegade": 7.2, "Emerging Market": 7.0, "Chief Wallabee": 6.8,
    "So Happy": 6.2, "Litmus Test": 6.0, "Potente": 5.5,
    "Danon Bourbon": 5.2, "Albus": 4.8, "Incredibolt": 4.5,
    "Wonder Dean": 4.5, "Intrepido": 4.0, "Six Speed": 3.8,
    "Golden Tempo": 3.5, "Pavlovian": 3.5, "Great White": 3.0,
    "Robusta": 2.5, "Ocelli": 2.0,
}

# Pace scenario score: how well each horse benefits from the contested early pace
# (Six Speed, Renegade, Potente all pressing → pace meltdown likely)
PACE_FIT = {
    "Further Ado": 9.0, "Commandment": 8.5, "The Puma": 8.5,
    "Emerging Market": 8.0, "Chief Wallabee": 7.5, "Renegade": 6.0,
    "So Happy": 7.0, "Wonder Dean": 7.0, "Albus": 6.5,
    "Incredibolt": 6.0, "Danon Bourbon": 6.0, "Litmus Test": 7.0,
    "Pavlovian": 3.0, "Six Speed": 2.5, "Potente": 2.5,
    "Golden Tempo": 5.5, "Robusta": 3.0, "Great White": 5.5,
    "Intrepido": 6.5, "Ocelli": 5.0,
}

# Prat jockey switch: +1.5 to Emerging Market; post-17 curse; rail draw penalty
POST_DRAW_ADJUSTMENTS = {
    "Emerging Market": +1.5,
    "Commandment": -0.5,   # lost Prat
    "Golden Tempo": -2.0,  # Post 17 curse (0 wins all-time)
    "Renegade": -1.5,      # Rail draw (last winner from rail: 1986)
}


def build_trainer_stats(hist_df: pd.DataFrame) -> dict:
    """Compute each trainer's historical Derby win rate from historical data."""
    stats = {}
    for trainer, grp in hist_df.groupby("trainer"):
        n_starts = len(grp)
        n_wins = grp["is_winner"].sum()
        stats[trainer] = {
            "derby_starts": n_starts,
            "derby_wins": int(n_wins),
            "derby_win_pct": float(n_wins / n_starts) if n_starts > 0 else 0.0,
        }
    return stats


def build_jockey_stats(hist_df: pd.DataFrame) -> dict:
    stats = {}
    for jockey, grp in hist_df.groupby("jockey"):
        n_starts = len(grp)
        n_wins = grp["is_winner"].sum()
        stats[jockey] = {
            "derby_starts": n_starts,
            "derby_wins": int(n_wins),
            "derby_win_pct": float(n_wins / n_starts) if n_starts > 0 else 0.0,
        }
    return stats


def normalize(series: pd.Series) -> pd.Series:
    lo, hi = series.min(), series.max()
    if hi == lo:
        return pd.Series([5.0] * len(series), index=series.index)
    return (series - lo) / (hi - lo) * 10


def build_2026_features(hist_df: pd.DataFrame) -> pd.DataFrame:
    trainer_stats = build_trainer_stats(hist_df)
    jockey_stats  = build_jockey_stats(hist_df)

    rows = []
    for h in FIELD_2026:
        ts = trainer_stats.get(h["trainer"], {})
        js = jockey_stats.get(h["jockey"], {})

        win_rate = h["wins"] / h["starts"] if h["starts"] > 0 else 0.0
        itm_rate = (h["wins"] + h["places"] + h["shows"]) / h["starts"] if h["starts"] > 0 else 0.0

        # Dosage score: inverse-scaled 0-10 (lower DI = higher score)
        dosage_score = max(0, min(10, 10 - (h["dosage"] - 1.0) * (10 / 6.0)))

        # Run style score given contested early pace
        style_map = {1: 4.0, 2: 8.5, 3: 8.0, 4: 7.0, 5: 5.5}
        run_style_score = style_map.get(h["style"], 6.0)

        # Trainer score: Derby wins + CDns win% + historical Derby win%
        trainer_score = (
            h["trainer_dw"] * 1.5
            + h["post_cdns_wp"] / 10.0
            + ts.get("derby_win_pct", 0.0) * 30
        )

        # Jockey score: Derby wins + CDns base
        jockey_score = h["jockey_dw"] * 2.0 + js.get("derby_win_pct", 0.0) * 30 + 3.0

        rows.append({
            "post": h["post"],
            "name": h["name"],
            "odds": h["odds"],
            "beyer": h["beyer"],
            "beyer_over_100": int(h["beyer"] >= 100),
            "dosage": h["dosage"],
            "dosage_score": dosage_score,
            "run_style": h["style"],
            "run_style_score": run_style_score,
            "pace_fit": PACE_FIT.get(h["name"], 6.0),
            "trainer_dw": h["trainer_dw"],
            "trainer_score": trainer_score,
            "jockey_dw": h["jockey_dw"],
            "jockey_score": jockey_score,
            "win_rate": win_rate,
            "itm_rate": itm_rate,
            "stamina_test": h["stam"],
            "post_wp": h["post_wp"],
            "post_itm": h["post_itm"],
            "pedigree_dist": h["pedigree_dist"],
            "expert_score": EXPERT_SCORES.get(h["name"], 5.0),
            "post_draw_adj": POST_DRAW_ADJUSTMENTS.get(h["name"], 0.0),
            "sire_won": h["sire_won"],
        })

    df = pd.DataFrame(rows)

    # Normalize continuous features to 0-10
    for col in ["beyer", "trainer_score", "jockey_score", "win_rate", "itm_rate", "post_wp", "post_itm"]:
        df[f"{col}_norm"] = normalize(df[col])

    return df


def build_training_features(hist_df: pd.DataFrame) -> pd.DataFrame:
    """Build feature matrix for ML training from historical data."""
    rows = []
    for _, r in hist_df.iterrows():
        beyer_norm = min(10, max(0, (r["beyer"] - 80) / 3.0))
        dosage_score = max(0, min(10, 10 - (r["dosage"] - 1.0) * (10 / 6.0)))
        style_map = {1: 4.0, 2: 8.5, 3: 8.0, 4: 7.0, 5: 5.5}
        run_style_score = style_map.get(r.get("run_style", 3), 6.5)

        # Implied probability from odds (efficient market signal)
        implied_prob = 1.0 / (r["odds"] + 1) if r["odds"] > 0 else 0.5

        rows.append({
            "year": r["year"],
            "is_winner": r["is_winner"],
            "beyer_norm": beyer_norm,
            "dosage_score": dosage_score,
            "run_style_score": run_style_score,
            "implied_prob": implied_prob,
            "post": r["post"],
            "post_wp_approx": 8.0 if 5 <= r["post"] <= 12 else 5.0,  # mid-pack post bias
            "muddy": int(r.get("condition", "fast") == "muddy"),
        })

    return pd.DataFrame(rows)


def main():
    os.makedirs(DATA_DIR, exist_ok=True)

    hist_path = os.path.join(DATA_DIR, "historical_results.csv")
    if not os.path.exists(hist_path):
        print("Historical data not found. Run derby_scraper.py first.")
        sys.exit(1)

    hist_df = pd.read_csv(hist_path)
    print(f"Loaded {len(hist_df)} historical records from {hist_df['year'].nunique()} years")

    print("Building 2026 field features...")
    field_df = build_2026_features(hist_df)
    field_path = os.path.join(DATA_DIR, "field_2026.csv")
    field_df.to_csv(field_path, index=False)
    print(f"Saved 2026 field features ({len(field_df)} horses) -> {field_path}")

    print("Building ML training features...")
    train_df = build_training_features(hist_df)
    train_path = os.path.join(DATA_DIR, "train_features.csv")
    train_df.to_csv(train_path, index=False)
    print(f"Saved training features ({len(train_df)} rows) -> {train_path}")


if __name__ == "__main__":
    main()
