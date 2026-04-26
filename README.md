# airbnb-burla

Looking at every public Airbnb listing in Inside Airbnb's open data dump, all
at once, on Burla.

- 1,097,241 listings across 116 cities
- 1,406,718 photo URLs scraped from public listing pages
- 1,243,339 images CLIP-scored on CPU
- 48,122 images sent through YOLOv8 for object detection
- 50,686,612 reviews put through a 3-tier funnel ending in Claude Haiku on the
  top 10k
- 5 hypotheses tested with bootstrap 95% CIs

10.8 hours of wall time, ~$361 of compute, 1,000 peak concurrent Burla
workers. The whole thing is a single Python project that runs on Burla via
`remote_parallel_map`.

Live site: https://burla-cloud.github.io/airbnb-burla/

Writeup: [WRITEUP.md](./WRITEUP.md)

## Quickstart

```bash
~/.burla/<your-account>/.venv/bin/pip install -e .

cp .env.example .env
# edit .env, drop in ANTHROPIC_API_KEY

make all
```

Each stage is independently runnable and resume-aware (checkpoints to a shared
`/workspace/shared` filesystem):

```bash
make stage00          # validate cities (Inside Airbnb has 116 cities live)
make stage02a_sample  # 1k-listing scrape sanity check
make stage02b_sample  # 1k-image CLIP sanity check
make stage04          # 3-tier review scoring (50M -> 200k -> 10k)
```

## Pipeline

| Stage | What | In | Out |
|---|---|---|---|
| 00 | Validate every Inside Airbnb city | (none) | `data/outputs/validation_report.json` |
| 01 | Download + clean per-city listings | `validation_report.json` | `listings_clean.parquet` |
| 02a | Scrape photo manifests from `airbnb.com/rooms/<id>` | `listings_clean.parquet` | `photo_manifest.parquet` |
| 02b | CLIP-score every image (CPU) | `photo_manifest.parquet` | `images_cpu.parquet` |
| 03 | YOLOv8 on top weird candidates (GPU) | `images_cpu.parquet` | `images_gpu.parquet` |
| 04 | 3-tier review scoring (heuristic + embed + Claude) | per-city `reviews.csv.gz` | `reviews_scored.parquet` |
| 05 | Correlations with bootstrap 95% CIs | all parquets | `correlations.parquet` |
| 06 | Write site JSON artifacts | all parquets | `data/outputs/*.json` |

## Layout

```
src/
  config.py          # cities, top-N, hypotheses, budgets
  stages/            # each: read parquet, write parquet
  tasks/             # Burla-serializable worker functions
  lib/               # io, budget, http retries, shared FS helpers
site/                # static HTML/CSS/JS, no build step, fed by data/outputs
data/
  raw/               # gitignored
  interim/           # parquet checkpoints between stages
  outputs/           # final JSON the site reads (committed)
```

## How it talks to Burla

Every stage looks roughly like this:

```python
from burla import remote_parallel_map

results = remote_parallel_map(
    worker_fn,                  # Burla pickles this and ships it to workers
    list_of_dataclass_inputs,
    func_cpu=1,
    func_ram=8,
    max_parallelism=1000,
    grow=True,                  # let the cluster scale up to fit the queue
    spinner=True,
)
```

Each worker writes its slice of output to a shared GCS-backed
`/workspace/shared` filesystem as a Parquet file, and the orchestrator merges
slices at the end of each stage. Stage 4 in particular fans out to 1000
concurrent workers reading row-group-aligned slices of a 50.7M-row reviews
parquet.

## Caveats

- `reviews_per_month` from Inside Airbnb is a public demand proxy, not actual
  bookings. The site repeats this caveat under every chart.
- Scrape coverage is whatever public listing pages returned without anti-bot
  blocking. We hit Datadome on a chunk of cities and used the listing-data
  primary photos as fallback (1.4M photo URLs covering 1.1M listings).
- Inside Airbnb anonymizes locations within ~150m; the world map shows
  neighborhoods, not exact addresses.
- Stage 3 GPU success rate landed at 27% because we were paying the YOLO model
  weight download cost on every worker spin-up before pre-staging fixed it
  late in the run. The TV / mirror / plant findings still hold because Stage
  2b CLIP scoring covered all 1.4M photos.

## License

MIT.
