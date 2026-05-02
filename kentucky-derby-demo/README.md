# BurlaKentuckyDerby

A Burla-powered prediction model for the 2026 Kentucky Derby. Five Python scripts run in parallel on Burla's distributed cloud compute to scrape historical data, train ML models, run sensitivity analysis, and simulate 1,000,000 races.

## Pipeline

```
derby_scraper.py      ← Hard-coded historical results (2000-2025) + Burla parallel HRN scraping
derby_features.py     ← Merge 2026 field data (Excel) + historical data into feature matrix
derby_model.py        ← Burla parallel ML training (164 configs: GBM, RF, LogReg)
derby_sensitivity.py  ← Burla parallel weight sensitivity (5,000 Dirichlet combos)
derby_montecarlo.py   ← Burla parallel Monte Carlo (1M sims) → writes canvas
```

## Results

| Horse | Post | Odds | Model Win% | Market Implied% | Signal |
|-------|------|------|-----------|-----------------|--------|
| Further Ado | 16 | 6.5-1 | 31.4% | 13.3% | **VALUE BET** |
| Chief Wallabee | 11 | 8-1 | 26.3% | 11.1% | **VALUE BET** |
| The Puma | 8 | 10-1 | 11.3% | 9.1% | Fair |
| Emerging Market | 13 | 15-1 | 7.6% | 6.3% | Fair |
| Commandment | 5 | 6.5-1 | 5.5% | 13.3% | Fade |

**Longshot:** Litmus Test (30-1) — Bob Baffert's 6 Derby wins give the highest trainer score in the field.

### Exotic Plays
- **Exacta box:** Further Ado / Chief Wallabee / The Puma — $1 box ($6)
- **Trifecta:** Further Ado + Chief Wallabee on top, The Puma / Emerging Market / Commandment for 3rd ($12)
- **Superfecta:** 10-cent box of top-4 ($2.40)

## Key Model Findings

The sensitivity analysis (5,000 weight combos back-tested on 2022-2025) revealed:

| Factor | Empirically Validated Weight |
|--------|------------------------------|
| Jockey quality | **32.5%** |
| Pedigree distance aptitude | 16.6% |
| Post position ITM% | 13.4% |
| Post position win% | 9.7% |
| Run style / pace fit | 8.4% |
| Beyer speed figure | 7.7% |
| Stamina test | 5.5% |
| Trainer quality | 4.8% |
| Dosage index | 0.8% |
| Career win rate | 0.6% |

Jockey quality dominates — far more than the conventional Beyer-first approach. The model correctly called 2023 (Mage) and 2025 (Sovereignty) outright.

## Setup

```bash
pip install burla pandas scikit-learn numpy requests beautifulsoup4 lxml openpyxl
burla login
```

## Run

```bash
python derby/derby_scraper.py
python derby/derby_features.py
python derby/derby_model.py
python derby/derby_sensitivity.py
python derby/derby_montecarlo.py
```

The final script writes `canvases/kentucky-derby-2026.canvas.tsx` with all results embedded inline.

## Why Burla?

| Task | Sequential | Burla Parallel |
|------|-----------|----------------|
| 164 ML configs | ~8 min | ~110 sec |
| 5,000 weight combos | ~20 min | **7 seconds** |
| 1M Monte Carlo sims | ~6 min | ~90 sec |
