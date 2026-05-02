# Kentucky Derby 2026 on Burla

> **One trillion Monte Carlo simulations of the 2026 Kentucky Derby in 45 minutes**, plus a 5,000-combination weight backtest, 164 ML configurations trained in parallel, and a 2,000-permutation null-test on the backtest score.
>
> Live demo: **[burla-cloud.github.io/examples/kentucky-derby-demo](https://burla-cloud.github.io/examples/kentucky-derby-demo/)**

This is a real handicapping pipeline, not a marketing simulation. Every input is scraped or pulled from a public source. The code that produced the live page is in this directory.

## What the model picks

After four scratches (Silent Tactic, Fulleffort, Right To Party, The Puma), the final 19-horse field runs Saturday May 2 at Churchill Downs. Posts below are the official program posts as published by Churchill Downs, with gaps where horses scratched and the three also-eligibles drawn into deep-outside posts (21, 22, 23).

The model finds **five value bets**, headlined by the chalky horse:

| Horse | Post | ML Odds | Model Win % | Market Implied % | Multiplier |
|---|---|---|---|---|---|
| Further Ado | 18 | 6-1 | 27.9% | 14.3% | **1.95x** |
| Litmus Test | 4 | 30-1 | 6.2% | 3.2% | **1.94x** |
| Intrepido | 3 | 50-1 | 3.7% | 2.0% | **1.85x** |
| Robusta | 23 | 50-1 | 3.7% | 2.0% | **1.85x** |
| Pavlovian | 16 | 30-1 | 5.6% | 3.2% | **1.75x** |

**Further Ado** is the headline play: field-leading 106 Beyer, drew post 18 (the highest historical-win-rate gate in the 2010-2025 sample, where Authentic won in 2020), Cox / Velazquez. The chalky horse at 6-1 is also the value play.

The four longshots behind him are 30-1 and 50-1 saver tickets; multiplier sizes (1.75x to 1.94x) clear Churchill's 17 to 22% takeout but stake sizes have to stay light.

The headline favorite **Renegade** (4-1 ML, post 1) is the model's cleanest fade: 4.2% model vs 20.0% implied (4.7x market over model). Post 1 has not produced a Derby winner in our 2010 to 2025 sample (none since Ferdinand 1986).

## Pipeline

Nine Python scripts. Four use Burla's `remote_parallel_map` for the heavy lifting; the rest are local glue.

```
derby_ingest.py        Burla 114-task scrape of HRN, KY Derby press,
                       Wikipedia, NWS, PedigreeQuery
derby_scraper.py       Burla parallel fetch of historical Derby results
                       (2010 through 2025, Wikipedia)
derby_build.py         Local: builds field_2026.csv and
                       historical_results.csv from raw artifacts
derby_features.py      Local: assembles train_features.csv from real
                       CSVs (no hand-coded expert dicts)
derby_model.py         Burla: trains 164 ML configurations
                       (GBM, RF, LogReg) in parallel, picks top-5
                       ensemble by log-loss
derby_sensitivity.py   Burla: 5,000 Dirichlet weight combinations
                       backtested against 16 Derbies in 7 seconds
derby_montecarlo.py    Local 1-million-sim warmup using the chosen
                       weights and ML ensemble
derby_audit.py         Burla: 2,000-run permutation test that
                       scrambles winners and re-runs the full search
                       (null distribution for the backtest score)
derby_trillion.py      Burla: 1,000,000,000,000 race simulations
                       across 10,000 worker tasks, 1,000 concurrent
                       workers, 45.1 minutes wall clock
```

## Setup

```bash
pip install burla pandas scikit-learn numpy requests beautifulsoup4 lxml
burla login
```

## Run

```bash
python derby/derby_ingest.py        # ~2 min on Burla
python derby/derby_scraper.py       # ~30 sec on Burla
python derby/derby_build.py         # local, instant
python derby/derby_features.py      # local, instant
python derby/derby_model.py         # ~2 min on Burla (164 configs)
python derby/derby_sensitivity.py   # ~7 sec on Burla (5,000 combos)
python derby/derby_montecarlo.py    # ~1 min local
python derby/derby_audit.py         # ~30 sec on Burla
python derby/derby_trillion.py      # ~45 min on Burla (1T sims)
```

Each Burla script falls back to a single-machine implementation if a cluster isn't available, so you can develop locally and only burn cluster time on the final run.

## What the model looks at

Eight features feed the ranking, weighted by the 5,000-combo Dirichlet backtest against 2010 to 2025 Derbies:

| Factor | Weight |
|---|---|
| Stamina test (binary, Beyer >= 95) | 19.2% |
| Year-level Beyer | 16.1% |
| Dosage score | 15.9% |
| Career win-rate (normalized) | 13.2% |
| Post-position historical win % | 12.3% |
| Trainer Derby score | 11.0% |
| Jockey Derby score | 5.2% |
| Post-position historical ITM % | 3.8% |
| Run-style score | 1.8% |
| Pedigree distance aptitude | 1.6% |

The market price (`implied_prob = 1 / (odds + 1)`) is **not** in the feature set. Predictions are independent of the market they are compared against.

## Backtest

The chosen weights score **126 / 160** on a 10-5-2-1-0 ranking metric across 16 Derbies (2010 through 2025), picking the actual winner first in 11 of 16 years.

A **2,000-permutation null test** (run on Burla in `derby_audit.py`) re-ran the full 5,000-combo search after secretly scrambling which horse actually won each historical Derby. None of the 2,000 scrambled runs came within four points of 126. The framework is finding real signal, not search noise.

| Metric | Value |
|---|---|
| Published score | 126 / 160 |
| Null median | 75 / 160 |
| Null max (across 2,000 runs) | 122 / 160 |
| P(null >= 126) | 0.0% |
| Edge over random | +78 points |

## Honest limits

The site has a full audit section at [#audit](https://burla-cloud.github.io/examples/kentucky-derby-demo/#audit). The short version:

1. **Morning line is not the closing tote.** BET / FAIR / FADE compares to the morning line. The closing tote will move; the model's calls reflect the field at posted ML.
2. **Takeout eats edge.** Churchill keeps ~17% of the win pool, ~22% of exotics. Multipliers under ~1.2x do not clear takeout. The five BET-tagged horses (1.75x to 1.95x) all clear it. Further Ado at 6-1 is the only one stake-able at full bankroll; the four longshots (30-1 to 50-1) stay as small saver tickets.
3. **Per-horse historical Beyers for losing finishers are paywalled** behind DRF. The historical training set therefore carries the same year-level winner Beyer for every horse in a given Derby. Per-horse historical signal comes from post position and connections instead.
4. **No Ragozin / Thoro-Graph / Brisnet pace sheets.** Sectional times for losers, current jockey-trainer combo win rates, recent workouts, and live odds movement are all outside the model's view.
5. **Two of the model's top-five weights are placeholder features for the 2026 field.** Dosage score (16% weight) and career win-rate (13% weight) were trained on per-horse historical data, but for the 2026 field every runner currently carries `dosage_score = 7.0` and `win_rate = 0.5`. Those columns earned their weight on 2010 to 2025 winners (where they varied), but they do not differentiate any 2026 horse from any other 2026 horse. The 2026 ranking effectively leans on year-Beyer, the binary stamina test, post-position win %, trainer/jockey Derby scores, and run style.

## Why Burla?

Burla turns Python parallelism into a single function call. Wrap a function, hand it a list of arguments, and it fans out across a cluster:

```python
from burla import remote_parallel_map

def simulate_race_batch(scores, n_sims, seed):
    # ...one batch of Monte Carlo sims...
    return {"counts": counts.tolist()}

# 10,000 worker tasks across 1,000 concurrent workers in ~45 minutes
args = [(log_probs, 100_000_000, seed) for seed in range(10_000)]
results = remote_parallel_map(simulate_race_batch, args, grow=True)
```

No Docker. No Kubernetes. No orchestration glue.

| Task | Sequential local | Burla parallel |
|---|---|---|
| 164 ML configs | ~8 min | ~110 sec |
| 5,000 weight combos | ~20 min | 7 sec |
| 1M Monte Carlo sims | ~6 min | ~90 sec |
| 1T Monte Carlo sims | ~10 hr | 45 min |

Burla docs: [docs.burla.dev](https://docs.burla.dev/)

## Repository layout

```
kentucky-derby-demo/
├── README.md                        # this file
├── docs/
│   └── index.html                   # the deployed site (GitHub Pages)
├── derby/
│   ├── data/
│   │   ├── raw/                     # untouched scrape artifacts
│   │   │   ├── morning_line.json    # post-scratch 19-horse field
│   │   │   ├── hrn_2026.json        # HorseRacingNation HRN data
│   │   │   ├── derby_*.json         # historical Wikipedia results
│   │   │   └── ...
│   │   ├── field_2026.csv           # built from raw, 19 horses
│   │   ├── historical_results.csv   # 2010 to 2025, 305 starters
│   │   ├── train_features.csv       # ML training matrix
│   │   ├── model_results.json       # ML configs + sensitivity
│   │   └── trillion_results.json    # final 1T-sim probabilities
│   ├── derby_ingest.py
│   ├── derby_scraper.py
│   ├── derby_build.py
│   ├── derby_features.py
│   ├── derby_model.py
│   ├── derby_sensitivity.py
│   ├── derby_audit.py
│   ├── derby_montecarlo.py
│   └── derby_trillion.py
└── canvases/                        # exploratory Jupyter / Cursor canvases
```

## Disclaimer

For entertainment and engineering demonstration purposes. Not financial or wagering advice. Post time Saturday May 2, 6:57 PM ET.
