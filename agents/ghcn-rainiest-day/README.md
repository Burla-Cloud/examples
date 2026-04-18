# Global Rainiest Day Ever — a Burla demo

Scan **every single-day rainfall measurement in NOAA GHCN-Daily** (the public
global station archive, one `YYYY.csv.gz` per year, 1750 → today) and produce:

- a global **top-500 single-day rainfall** leaderboard and a
  **top-by-distinct-station** view
- a per-decade, per-country **climatology** — rainiest and driest countries
  per decade
- a polished single-file Leaflet **map** of the 100 wettest stations on Earth

## Headline

> **1,750.0 mm (68.9 in) at Koumac, New Caledonia — 17 January 1976.**
> Largest single-day PRCP in NOAA GHCN-Daily.

| | |
|---|---:|
| Rows scanned | **3,177,336,585** |
| Valid PRCP rows kept | **1,090,829,523** |
| Year-files processed | 265 (1750 → 2026) |
| Serial equivalent compute | ~75 min |
| **Burla wall-clock (map + reduce)** | **~2 min** |
| Peak parallel workers | 245 |

## Top-10 distinct stations

| # | Station / Country | Date | PRCP (mm) |
|:---:|:---|:---:|---:|
|  1 | Koumac, New Caledonia | 1976-01-17 | 1,750.0 |
|  2 | Honomanu Mauka (Maui, HI) | 1950-04-30 | 1,505.0 |
|  3 | Kailua Mauka (Maui, HI) | 1950-04-30 | 1,457.2 |
|  4 | East Honomanu (Maui, HI) | 1955-02-28 | 1,158.0 |
|  5 | Cherrapunji / P.S., India | 1910-07-12 | 997.7 |
|  6 | Cherrapunji, India | 1956-06-05 | 973.8 |
|  7 | Pasighat Aero, India | 1981-06-28 | 912.4 |
|  8 | Puohokamoa 2 (Maui, HI) | 1952-11-30 | 905.3 |
|  9 | Mawsynram, India | 1966-06-09 | 877.4 |
| 10 | Opana Mauka (Maui, HI) | 1955-02-28 | 867.7 |

Full list in `burla_results/top_by_station.csv`. The leaderboard reads like a
tour of every major wet-weather regime: Pacific tropical cyclones (Koumac,
Queensland), windward-slope orographic storms (Haleakala on Maui), the Indian
summer monsoon (Cherrapunji, Mawsynram), typhoons near Japan, Aleutian storms.

## Rainiest and driest country per decade

**Metric**: `mean_mm_per_obs_day = total_prcp_mm / total_obs_days` (average
precipitation on a reporting station-day; × 365 → projected annual mm).

**QC filter** for ranking: country-decade must have ≥ 1,000 observation-days
and ≥ 3 station-years. Full tables: `rainiest_by_decade.md`,
`driest_by_decade.md`, `country_decade_stats.csv` (2,056 rows).

### Rainiest

| Decade | Country | mm/day | Proj. annual mm | Station-years |
|---:|:---|---:|---:|---:|
| 1750s | Australia ⚠ | 2.31 | 844 | 3,561 |
| 1780s–1830s | Germany | 1.67–4.19 | 608–1,530 | 7–20 |
| 1840s | United States | 3.06 | 1,117 | 29 |
| 1850s | Canada | 2.47 | 900 | 10 |
| 1860s | Ireland | 3.12 | 1,138 | 11 |
| 1870s | Russia | 5.62 | 2,052 | 6 |
| 1880s | United States | 3.11 | 1,136 | 1,179 |
| 1890s | Austria | 3.04 | 1,111 | 10 |
| 1900s–1920s | **Puerto Rico** | 4.41–4.86 | 1,610–1,773 | 102–129 |
| 1930s | Turkey | 7.75 | 2,828 | 49 |
| 1940s | Palau | 10.57 | 3,859 | 6 |
| **1950s–1990s** | **New Caledonia** | 14.11–**27.55** | 5,151–**10,057** | 9–20 |
| 2000s | Sudan | 18.52 | 6,760 | 271 |
| 2010s | Guinea | 20.78 | 7,584 | 15 |
| 2020s | Indonesia | 15.40 | 5,620 | 517 |

Puerto Rico takes over as the Caribbean stations come online. New Caledonia's
mid-20th-century dominance is driven by the same Koumac station that tops the
daily leaderboard. The modern decades follow tropical network expansion
(Sahel → West African monsoon → Maritime Continent).

### Driest

| Decade | Country | mm/day | Proj. annual mm | Station-years |
|---:|:---|---:|---:|---:|
| 1800s–1860s | **Czech Republic** | 1.11–1.39 | 404–508 | 6–10 |
| 1870s | Greenland | 0.59 | 217 | 7 |
| **1880s–1970s** | **Egypt** | 0.11–0.23 | 41–85 | 9–91 |
| 1980s | Macau SAR ⚠ | 0.00 | 0 | 8 |
| 1990s | Mongolia | 0.18 | 65 | 400 |
| 2000s | Egypt | 0.33 | 121 | 97 |
| 2010s–2020s | UAE | 0.43–0.50 | 158–183 | 22–40 |

Egypt is driest for **nine consecutive decades** (1880s–1970s). Modern desert
dominance shifts to Central Asia (Mongolia) and the Arabian Peninsula (UAE) as
coverage there expands.

## Why GHCN-Daily, and when it isn't enough

**Short answer:** GHCN-Daily is the right source for this demo. It's the
canonical ground truth for station-level PRCP records; the validated
"extreme" datasets in the literature are built on top of it:

- **HYADES** (Papalexiou et al., *Nature Scientific Data* 2024) — the global
  archive of annual-maxima daily precipitation across 39,206 stations — is
  **derived directly from GHCN-Daily**.
- **WMO World Weather and Climate Extremes Archive** — the body that certifies
  records like the 1,825 mm Réunion 24-h world record — pulls from GHCN-Daily
  plus national archives.
- Gridded products (CHIRPS, MSWEP, CPC Unified, GPCC) would actively **hurt**
  this analysis for extremes: they smear a 1,750 mm point value across a
  ~10 km grid cell down to ~150–250 mm. You need station data for peak records.

**Where it's weaker (and our README should say so):**

1. **Network-density bias in decade rankings.** "Egypt driest for 9 decades"
   partly reflects where NOAA has good desert stations — over the Sahara
   interior we literally have no data. A gridded/area-weighted product
   (MSWEP V3, GPCC) would give a more defensible *climatological* ranking
   for modern eras, at the cost of hiding extremes.
2. **Pre-1900 coverage is a handful of European stations.** The 1780s–1860s
   "rainiest/driest" winners are really "which European country kept the best
   books," not climate.
3. **Koumac clustering.** Station `NC000091577` contributes 151 of the top-500
   rows including the top 7. Quality flags didn't reject them; we publish
   them as-is and provide `top_by_station.csv` for the deduplicated view.
4. **1750s Australia anomaly** ⚠. NOAA's `1750.csv.gz` contains 344,589 rows
   all labeled "1750", all from Australian Synoptic Network (`ASN*`) stations
   — obviously a backfill artifact. Passes our QC so we keep it but flag it.
5. **Macau 1980s = 0.00 mm/day** ⚠. 2,322 obs-days with essentially no PRCP
   recorded — a station-level reporting-convention quirk, not a real zero.

Trust `top_by_station.csv` and the map over the raw top-500. Trust
**modern decades with high station-years** (Egypt 2000s: 97 sy; Indonesia
2020s: 517 sy) over early-decade European winners with 6–20 sy.

## Data source

- Year shards: `https://www.ncei.noaa.gov/pub/data/ghcn/daily/by_year/YYYY.csv.gz`
- Station metadata: `ghcnd-stations.txt` (129,657 rows) + `ghcnd-countries.txt`
  — both bundled in `data/` as a point-in-time snapshot
  (refresh with `python refresh_station_snapshot.py`)
- Row schema: `ID, YYYYMMDD, ELEMENT, DATA VALUE, M-FLAG, Q-FLAG, S-FLAG, OBS-TIME`
- Units: PRCP in **tenths of mm**; we divide by 10.
- Filters: `ELEMENT == "PRCP"`, empty `Q-FLAG`, drop `-9999`, drop negatives.
  Multi-day totals (`MDPR`) excluded.

## How it works

- `process_year(year)` — one remote CPU per calendar year. Streams the gzip,
  filters PRCP, maintains a top-100 heap, and aggregates per-country totals.
  Writes `/workspace/shared/ghcn/parts/{year}.json`.
- `reduce_years(parts)` — single worker. Merges top-100s → global top-500,
  joins station metadata, sums country stats by decade, ranks rainiest/driest,
  renders `map.html`. Writes to `/workspace/shared/ghcn/results/`.

## How to run

```bash
# Local smoke test (no Burla; single year, ~30 s)
python local_validate.py 1995

# Full Burla run (1750 → current year, ~2 min wall-clock)
python ../burla-agent-starter-kit/onboard.py  --email joeyper23@gmail.com
python ../burla-agent-starter-kit/run_job.py  --email joeyper23@gmail.com ghcn_pipeline.py
python ../burla-agent-starter-kit/run_job.py  --email joeyper23@gmail.com fetch_artifacts.py

# Reduce-only re-run (skip the ~90 s map phase, reuse existing parts)
REDUCE_ONLY=1 python ../burla-agent-starter-kit/run_job.py \
  --email joeyper23@gmail.com ghcn_pipeline.py

# Narrow the year range
GHCN_START_YEAR=1950 GHCN_END_YEAR=2025 python ../burla-agent-starter-kit/run_job.py ...
```

View the map: `open burla_results/map.html`.

## Artifacts (all in `burla_results/`)

| File | Contents |
|---|---|
| `top_result.json` | Headline record + citation |
| `top_500.csv` | Full 500-row leaderboard |
| `top_by_station.csv` | Deduplicated — each station's best day |
| `country_decade_stats.csv` | 2,056 (country × decade) rows |
| `rainiest_by_decade.{md,csv}` | Rainiest-per-decade ranking |
| `driest_by_decade.{md,csv}` | Driest-per-decade ranking |
| `map.html` | Single-file Leaflet map (top 100 stations) |
| `run_summary.json` | Row counts, failures, timings |

## Files

```
ghcn_pipeline.py             map + reduce + map renderer (all core logic)
local_validate.py            smoke test, no Burla
fetch_artifacts.py           pull /workspace/shared/ghcn/results/* back
refresh_station_snapshot.py  refresh data/*.txt from NOAA
data/                        bundled NOAA station snapshot
burla_results/               artifacts from the latest run
```

## Citation

Menne, M.J., Durre, I., Vose, R.S., Gleason, B.E., and Houston, T.G., 2012.
An overview of the Global Historical Climatology Network-Daily Database.
*J. Atmos. Oceanic Technol.* 29: 897–910. DOI `10.7289/V5D21VHZ`.

---

*Burla client pinned to v1.4.5 (no `grow=True` in this version — `onboard.py`
boots the cluster via the dashboard UI Start fallback; ~2 min cold start.)*
