# Global Rainiest Day Ever — a Burla demo

Scan **every single-day rainfall measurement in NOAA GHCN-Daily** (the public
worldwide station archive, one `YYYY.csv.gz` per year, 1750 → today) and
produce:

- a global **top-500 single-day rainfall** leaderboard
- a **top-by-distinct-station** leaderboard (deduplicated, for geographic diversity)
- a per-decade, per-country **climatology** — rainiest and driest countries per decade
- a polished interactive **Leaflet map** of the top-50 events worldwide

## Headline result

> **1,750.0 mm (68.9 in) at Koumac, New Caledonia — January 17, 1976.**
> Largest single-day PRCP in NOAA's Global Historical Climatology Network - Daily.

### Scale (from the latest Burla run, 2026-04-18)

| Metric | Value |
|---|---:|
| Rows scanned across every GHCN-Daily year-file | **3,177,336,585** |
| Valid PRCP observations after filters | **1,090,829,523** |
| Year-files processed | 265 (1750 → 2026) |
| Map phase wall-clock | ~90 s |
| Reduce phase wall-clock | ~30 s |
| Peak parallel workers | 245 |
| Distinct stations in NOAA's gazetteer | 129,657 |
| Countries reporting PRCP | 218 |

See `burla_results/` for the artifacts.

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
typhoons near Japan, Aleutian storms — the leaderboard reads like a tour of
every major wet-weather regime on Earth.

## Rainiest and driest countries per decade

Full tables in `burla_results/rainiest_by_decade.md` / `driest_by_decade.md`;
underlying data in `country_decade_stats.csv` (2,056 country × decade rows —
total rain, observation-days, station-years).

**Metric**: `mean_mm_per_obs_day = total_prcp_mm / total_obs_days`, the average
precipitation on a reporting station-day. Multiplied by 365 gives a
"projected annual mm" for the typical station in that country × decade.

**Quality filters** for ranking: a (country, decade) row must have ≥ 1,000
observation-days and ≥ 3 station-years of coverage. Removes spurious
single-station anomalies from the headlines while still keeping them in the CSV.

### Rainiest country per decade (mean mm per reporting station-day)

| Decade | Country | Mean mm/day | Proj. annual mm | Station-years |
|---:|:---|---:|---:|---:|
| 1750s | Australia ⚠ | 2.31 | 844 | 3,561 |
| 1780s | Germany | 1.88 | 686 | 7 |
| 1790s | Germany | 1.67 | 608 | 8 |
| 1800s | Germany | 1.70 | 620 | 10 |
| 1810s | Italy | 1.60 | 585 | 9 |
| 1820s | Germany | 4.19 | 1,530 | 14 |
| 1830s | Germany | 2.52 | 920 | 20 |
| 1840s | United States | 3.06 | 1,117 | 29 |
| 1850s | Canada | 2.47 | 900 | 10 |
| 1860s | Ireland | 3.12 | 1,138 | 11 |
| 1870s | Russia | 5.62 | 2,052 | 6 |
| 1880s | United States | 3.11 | 1,136 | 1,179 |
| 1890s | Austria | 3.04 | 1,111 | 10 |
| 1900s | Puerto Rico | 4.86 | 1,773 | 102 |
| 1910s | Puerto Rico | 4.59 | 1,677 | 121 |
| 1920s | Puerto Rico | 4.41 | 1,610 | 129 |
| 1930s | Turkey | 7.75 | 2,828 | 49 |
| 1940s | Palau | 10.57 | 3,859 | 6 |
| **1950s** | **New Caledonia** | **24.89** | **9,086** | 9 |
| **1960s** | **New Caledonia** | **27.55** | **10,057** | 10 |
| 1970s | New Caledonia | 19.28 | 7,036 | 17 |
| 1980s | New Caledonia | 14.11 | 5,151 | 20 |
| 1990s | New Caledonia | 15.22 | 5,555 | 20 |
| 2000s | Sudan | 18.52 | 6,760 | 271 |
| 2010s | Guinea | 20.78 | 7,584 | 15 |
| 2020s | Indonesia | 15.40 | 5,620 | 517 |

⚠ The 1750s Australia row is a NOAA backfill artifact — see data-quality note below.

**Patterns:** early decades are dominated by the handful of European stations
that kept records (Hohenpeissenberg, Milan, Dublin). Puerto Rico takes over as
tropical stations come online in the 1900s. **New Caledonia owns the
mid-20th-century because of the same Koumac station that dominates our daily
leaderboard.** The final three decades reflect where the network has grown
into tropical convergence zones (Sudan/Sahel, Guinea/West African monsoon,
Indonesia/Maritime Continent).

### Driest country per decade

| Decade | Country | Mean mm/day | Proj. annual mm | Station-years |
|---:|:---|---:|---:|---:|
| 1750s | Australia ⚠ | 2.31 | 844 | 3,561 |
| 1780s | Germany | 1.88 | 686 | 7 |
| 1790s | Germany | 1.67 | 608 | 8 |
| 1800s–1860s | **Czech Republic** | 1.11–1.39 | 404–508 | 6–10 |
| 1870s | Greenland | 0.59 | 217 | 7 |
| **1880s–1970s** | **Egypt** | 0.11–0.23 | 41–85 | 9–91 |
| 1980s | Macau SAR | 0.00 | 0 | 8 |
| 1990s | Mongolia | 0.18 | 65 | 400 |
| 2000s | Egypt | 0.33 | 121 | 97 |
| 2010s | UAE | 0.43 | 158 | 40 |
| 2020s | UAE | 0.50 | 183 | 22 |

**Patterns:** the Central European continental interior (Prague area) is
"driest" in the 1800s because the 2 data-rich European countries in that era
are Germany and Czechia and Czechia is slightly drier. **Egypt absolutely
dominates the 20th century with projected annual rainfall of 40-80 mm/year
(~1-3 inches).** The desert Gulf (Mongolia → UAE) takes the crown in the 21st
century as coverage there expands.

### Data-quality caveats worth knowing

- **1750s Australia row**: NOAA's `1750.csv.gz` has 344,589 rows, all labeled
  "1750", all from Australian Synoptic Network (`ASN*`) stations. That's
  obviously a backfill/placeholder artifact — there were ~0 weather stations
  in Australia in 1750. We include it for completeness (it passed the
  quality flag) but flag it in the table.
- **Macau 1980s = 0.00 mm/day**: 2,322 observation-days with essentially no
  recorded PRCP. Almost certainly a reporting-convention issue with that
  station rather than a real zero-rainfall decade.
- **Small European samples (1780s–1860s)**: most of the "winners" in those
  decades have only 6-20 station-years of coverage — effectively 1-2 stations
  reporting for the whole decade. The ranking is correct but not statistically
  robust; see `country_decade_stats.csv` for the full field.

## How it works

- One remote CPU per calendar year (`remote_parallel_map`).
- Each worker streams the matching `YYYY.csv.gz`, filters `PRCP`, and emits:
  - a **per-year top-100** row set (for the global leaderboard)
  - a **per-country aggregate** (total mm, obs-days, station count) — feeds the decade analysis
  - both land in `/workspace/shared/ghcn/parts/YYYY.json`
- A single reduce worker then:
  - merges top-100s into a global top-500
  - joins station metadata (name / lat-lon / country / elevation)
  - sums per-country stats across each decade
  - ranks rainiest/driest countries per decade
  - renders a Leaflet `map.html` with sized/color-coded markers, styled popups,
    legend, title overlay, and CartoDB tiles

## Artifacts

All output lives in `burla_results/`:

| File | What it is |
|---|---|
| `top_result.json` | The single headline record with citation + note |
| `top_500.csv` | Full 500-row leaderboard (often many rows per station) |
| `top_by_station.csv` | Each distinct station's single best day (deduplicated) |
| `country_decade_stats.csv` | One row per (country, decade) with totals + means |
| `rainiest_by_decade.md` / `.csv` | Rainiest country per decade (filtered ranking) |
| `driest_by_decade.md` / `.csv` | Driest country per decade (filtered ranking) |
| `map.html` | Single-file polished Leaflet map of the top-50 events |
| `run_summary.json` | Rows scanned, failures, timings, top-result echo |

## Data source

- **Year shards:** `https://www.ncei.noaa.gov/pub/data/ghcn/daily/by_year/YYYY.csv.gz`
- **Station metadata:** `https://www.ncei.noaa.gov/pub/data/ghcn/daily/ghcnd-stations.txt`
- **Country codes:** `https://www.ncei.noaa.gov/pub/data/ghcn/daily/ghcnd-countries.txt`
- **Row schema** per NOAA `readme-by_year.txt`:
  `ID, YYYYMMDD, ELEMENT, DATA VALUE, M-FLAG, Q-FLAG, S-FLAG, OBS-TIME`
- **Units:** PRCP is stored in **tenths of mm**; we divide by 10.

### Bundled station snapshot (`data/`)

Station names / lat-lon / elevation / country come from NOAA's
`ghcnd-stations.txt` (129,657 rows, ~11 MB) and `ghcnd-countries.txt`
(219 rows). Both are committed into `data/` as a point-in-time snapshot so
the demo is reproducible without hitting NOAA.

The reduce worker prefers the bundle → `/workspace/shared/ghcn/meta/` (auto-staged
by `main()` on each Burla run) → NOAA as a last-resort fallback.

**Refreshing the snapshot** when NOAA publishes a newer station file:

```bash
python refresh_station_snapshot.py
git add data/ && git commit -m "refresh station snapshot"
```

## Filters

- `ELEMENT == "PRCP"` only (single-day totals; `MDPR` multiday excluded).
- Rows with non-empty `Q-FLAG` (NOAA's own QC rejected them) dropped.
- `-9999` sentinels and empties dropped.
- Negative values dropped.
- No further capping — the leaderboard stands on its own. Raw flags are
  preserved in `top_500.csv` so anyone can verify a surprising row.

## How to run

### Prereqs

Uses the canonical starter-kit flow from `agents/burla/burla.md` and the
per-account venv under `~/.burla/joeyper23/.venv/` (Python 3.12, `burla==1.4.5`).

### 1. Local smoke test (no Burla)

```bash
cd agents/ghcn-rainiest-day
SHARED_DIR=./local_shared \
  /Users/josephperry/.burla/joeyper23/.venv/bin/python local_validate.py 1995 2000
```

Expected: `LOCAL_OK` and a `HEADLINE:` line. Artifacts under `./local_shared/ghcn/results/`.

### 2. Full run on Burla (1750 → current year)

```bash
# (a) Ensure cluster is ready (idempotent)
python ../burla-agent-starter-kit/onboard.py --email joeyper23@gmail.com

# (b) Submit the pipeline
python ../burla-agent-starter-kit/run_job.py --email joeyper23@gmail.com ghcn_pipeline.py

# (c) Pull artifacts back to ./burla_results/
python ../burla-agent-starter-kit/run_job.py --email joeyper23@gmail.com fetch_artifacts.py
```

### Reduce-only re-runs (skip ~90 s map phase)

```bash
REDUCE_ONLY=1 python ../burla-agent-starter-kit/run_job.py \
  --email joeyper23@gmail.com ghcn_pipeline.py
```

Reuses the per-year JSONs already in `/workspace/shared/ghcn/parts/`.

### Narrow the range

```bash
GHCN_START_YEAR=1950 GHCN_END_YEAR=2025 \
  python ../burla-agent-starter-kit/run_job.py --email joeyper23@gmail.com ghcn_pipeline.py
```

## Viewing the map

Three options, in order of convenience:

```bash
# 1. Open in your default browser
open burla_results/map.html

# 2. View inside Cursor's Simple Browser panel
python -m http.server 8765 --bind 127.0.0.1 &
# then Cmd+Shift+P -> "Simple Browser: Show" -> http://127.0.0.1:8765/map.html

# 3. Host on GitHub Pages (see repo Settings -> Pages -> deploy from `main`)
```

## Honest framing (use this in any public post)

- **The claim:** largest single-day PRCP in NOAA GHCN-Daily (global station
  network, quality-filtered).
- **NOT the claim:** "the wettest day that ever happened on Earth." GHCN is a
  station network with uneven coverage (dense in US/Europe/parts of Asia;
  sparse in oceans and some tropical regions).
- **Koumac (NC000091577) clustering:** 151 of the top-500 rows come from this
  one station, including the top 7 entries. The quality flag didn't reject
  them. Publishing them as-is + the `top_by_station.csv` lets readers see both
  the raw and deduplicated views.
- **For a WMO-certified 24-h world record context**, see the
  [WMO extreme precipitation archive](https://wmo.asu.edu/content/world-greatest-twenty-four-hour-point-precipitation).

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
before the call. The starter kit's `onboard.py` handles that via a UI "Start"
fallback (boots ~13 nodes; cold start ≈ 2 min).

## Files

```
agents/ghcn-rainiest-day/
├── ghcn_pipeline.py             # all core logic: map + reduce + map.html renderer
├── local_validate.py            # smoke test, no Burla
├── fetch_artifacts.py           # pull /workspace/shared/ghcn/results/* back
├── refresh_station_snapshot.py  # refresh data/*.txt from NOAA
├── requirements.txt
├── README.md
├── .gitignore
├── data/                        # bundled NOAA station snapshot
│   ├── ghcnd-stations.txt       # 11 MB, 129,657 rows
│   └── ghcnd-countries.txt      # 4 KB, 219 rows
└── burla_results/               # artifacts from the latest Burla run
    ├── top_result.json
    ├── top_500.csv
    ├── top_by_station.csv
    ├── country_decade_stats.csv
    ├── rainiest_by_decade.md / .csv
    ├── driest_by_decade.md / .csv
    ├── map.html
    └── run_summary.json
```
