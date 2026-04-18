# Global Rainiest Day Ever — a Burla demo

Scan **every single-day rainfall measurement in NOAA GHCN-Daily** (the public
worldwide station archive, one `YYYY.csv.gz` per year, ~276 files) and find
the largest daily precipitation ever recorded, plus a top-500 leaderboard
and a top-by-distinct-station leaderboard.

## Headline result

> **1,750.0 mm (68.9 in) at Koumac, New Caledonia — January 17, 1976.**
> Largest single-day PRCP in NOAA's Global Historical Climatology Network - Daily.

Stats from the actual run (on Burla's `plus-dig` cluster, 2026-04-18):

- **3,177,336,585 rows scanned** across 265 year-files (1763 → 2026).
- **1,090,829,523 valid PRCP observations** (post-filter, mm).
- **Map phase wall-clock: ~90 s**; reduce: ~30 s. Total ≈ **2 minutes**.
- 12 early-modern years (1751–1762) don't exist in the NOAA `by_year/` index —
  only the 1750 file and 1763+ are published. Everything else processed cleanly.

See `burla_results/` for the artifacts produced by the remote run.

## Top-20 distinct stations (each's single wettest day, ever)

| Rank | Station (name / country) | Date | PRCP (mm) |
|:---:|:---|:---:|---:|
|  1 | Koumac, New Caledonia | 1976-01-17 | 1,750.0 |
|  2 | Honomanu Mauka (Maui, HI, USA) | 1950-04-30 | 1,505.0 |
|  3 | Kailua Mauka (Maui, HI, USA) | 1950-04-30 | 1,457.2 |
|  4 | East Honomanu (Maui, HI, USA) | 1955-02-28 | 1,158.0 |
|  5 | Cherrapunji / P.S., India | 1910-07-12 | 997.7 |
|  6 | Cherrapunji, India | 1956-06-05 | 973.8 |
|  7 | Pasighat Aero, India | 1981-06-28 | 912.4 |
|  8 | Puohokamoa 2 (Maui, HI, USA) | 1952-11-30 | 905.3 |
|  9 | Mawsynram, India ("wettest place on Earth") | 1966-06-09 | 877.4 |
| 10 | Opana Mauka (Maui, HI, USA) | 1955-02-28 | 867.7 |
| 11 | Cape Tribulation, Queensland, Australia | 2023-12-18 | 861.2 |
| 12 | Puu Paki (Maui, HI, USA) | 2010-03-31 | 838.2 |
| 13 | Waikamoi (Maui, HI, USA) | 1990-01-01 | 807.7 |
| 14 | Owase, Japan | 1968-09-26 | 806.0 |
| 15 | Lupi Upper (Maui, HI, USA) | 1990-01-01 | 802.6 |
| 16 | Haelaau (Maui, HI, USA) | 1950-04-30 | 787.4 |
| 17 | Panna Obsy, India | 1977-08-05 | 774.7 |
| 18 | Atka Island, Alaska, USA | 2025-05-22 | 758.7 |
| 19 | Doon Doon, NSW, Australia | 2022-02-28 | 758.0 |
| 20 | Paluma, Queensland, Australia | 2025-02-03 | 745.2 |

Tropical cyclones (Koumac, Queensland), windward-slope orographic storms
(Haleakala on Maui!), the Indian summer monsoon (Cherrapunji / Mawsynram),
typhoons near Japan, Aleutian storms — the leaderboard reads like a tour
of every major wet-weather regime on Earth.

## How it works

- One remote CPU per calendar year (`remote_parallel_map`).
- Each worker streams a `YYYY.csv.gz`, filters `PRCP` rows, emits the year's
  top 100 to `/workspace/shared/ghcn/parts/YYYY.json`.
- One reduce worker merges all parts into a global top-500, joins station
  metadata (name / lat-lon / country), and writes:
  - `top_result.json` — the single headline record (with citation + note)
  - `top_500.csv` — raw leaderboard (500 rows — many from the same station)
  - `top_by_station.csv` — each distinct station's best day (deduplicated)
  - `map.html` — single-file Leaflet map of the top-25 pins
  - `run_summary.json` — rows scanned, failures, timings

## Data source

- **Year shards:** `https://www.ncei.noaa.gov/pub/data/ghcn/daily/by_year/YYYY.csv.gz`
- **Station metadata:** `https://www.ncei.noaa.gov/pub/data/ghcn/daily/ghcnd-stations.txt`
- **Country codes:** `https://www.ncei.noaa.gov/pub/data/ghcn/daily/ghcnd-countries.txt`
- **Row schema** per NOAA `readme-by_year.txt`:
  `ID, YYYYMMDD, ELEMENT, DATA VALUE, M-FLAG, Q-FLAG, S-FLAG, OBS-TIME`
- **Units:** PRCP is stored in **tenths of mm**; we divide by 10.

## Filters

- `ELEMENT == "PRCP"` (single-day totals only; multi-day `MDPR` totals excluded).
- Rows with non-empty `Q-FLAG` (NOAA's own QC rejected them) dropped.
- `-9999` and empty values dropped.
- No extra capping — the leaderboard stands on its own. If a value looks
  extreme, the raw row is there in `top_500.csv` with its M/S/Q-flags and
  station ID so anyone can verify.

## How to run

### Prereqs

Uses the canonical starter-kit flow from `agents/burla/burla.md` and the
per-account venv under `~/.burla/joeyper23/.venv/` (Python 3.12,
`burla==1.4.5`).

### 1. Local smoke test (no Burla)

```bash
cd agents/ghcn-rainiest-day
SHARED_DIR=./local_shared \
  /Users/josephperry/.burla/joeyper23/.venv/bin/python local_validate.py 1995 2000
```

Expected: `LOCAL_OK` and a `HEADLINE:` line printed. Artifacts end up under
`./local_shared/ghcn/results/`.

### 2. Full run on Burla (1750 → current year)

```bash
# (a) Ensure cluster is ready (idempotent)
python ../burla-agent-starter-kit/onboard.py --email joeyper23@gmail.com

# (b) Submit the pipeline (map phase ~90 s, reduce ~30 s)
python ../burla-agent-starter-kit/run_job.py \
  --email joeyper23@gmail.com \
  ghcn_pipeline.py

# (c) Pull artifacts back to ./burla_results/
python ../burla-agent-starter-kit/run_job.py \
  --email joeyper23@gmail.com \
  fetch_artifacts.py
```

#### Reduce-only re-runs (skip 90 s map phase)

```bash
REDUCE_ONLY=1 python ../burla-agent-starter-kit/run_job.py \
  --email joeyper23@gmail.com \
  ghcn_pipeline.py
```

Reuses the per-year JSONs already in `/workspace/shared/ghcn/parts/`.

#### Narrow the range

```bash
GHCN_START_YEAR=1950 GHCN_END_YEAR=2025 \
  python ../burla-agent-starter-kit/run_job.py \
  --email joeyper23@gmail.com ghcn_pipeline.py
```

## Honest framing (use this in any public post)

- **The claim:** largest single-day PRCP in NOAA GHCN-Daily (global station
  network, quality-filtered).
- **NOT the claim:** "the wettest day that ever happened on Earth." GHCN is
  a station network with uneven coverage (dense in the US, Europe, parts of
  Asia; sparse in oceans and some tropical regions). Satellite-era TRMM/GPM
  gridded products will disagree with station values at specific locations.
- **Data quality caveat:** station NC000091577 (Koumac) accounts for 151 of
  the top 500 rows in `top_500.csv`, including the top 7 entries all in the
  1,505–1,750 mm range. These are during the Southern-Hemisphere cyclone
  season (January), which is physically consistent with intense tropical
  cyclones, but their clustering at one station is unusual. NOAA's Q-flag
  did not reject them. Publishing them as-is and providing
  `top_by_station.csv` lets readers see both the raw result and the
  geographically-diverse view side by side.
- For a WMO-certified 24-h rainfall world record context, see
  [World Meteorological Organization — extreme precipitation](https://wmo.asu.edu/content/world-greatest-twenty-four-hour-point-precipitation).

## Citation

> Menne, M.J., Durre, I., Vose, R.S., Gleason, B.E., and Houston, T.G., 2012.
> An overview of the Global Historical Climatology Network-Daily Database.
> *J. Atmos. Oceanic Technol.* 29: 897-910.
> Dataset DOI: `10.7289/V5D21VHZ`.

## Client-version note

The `joeyper23` account's `~/.burla/joeyper23/user_config.json` pins
`burla==1.4.5`, whose `remote_parallel_map` signature is
`(function_, inputs, func_cpu=1, func_ram=4, detach=False, generator=False, spinner=True, max_parallelism=None)`.
There is **no `grow=True`** in this client version — the cluster must be ON
before the call. The starter kit's `onboard.py` handles that via a UI
"Start" fallback (it boots ~13 nodes; cold start ≈ 2 min).

## Files

```
agents/ghcn-rainiest-day/
├── ghcn_pipeline.py      # process_year + reduce_years + main()
├── stations.py           # local fixed-width ghcnd-stations.txt parser (for local_validate.py)
├── local_validate.py     # two-year smoke test, no Burla
├── fetch_artifacts.py    # helper to pull /workspace/shared/ghcn/results/* back locally
├── render_map_local.py   # regenerate map.html from top_500.csv locally
├── requirements.txt      # requests, folium, burla
├── README.md             # (this file)
├── .gitignore
├── burla_results/        # artifacts from the latest Burla run
│   ├── top_result.json
│   ├── top_500.csv
│   ├── top_by_station.csv
│   ├── map.html
│   └── run_summary.json
└── local_shared/         # sandbox for local_validate.py (gitignored)
```
