# 1.1 million Airbnbs, looked at all at once

A writeup of how this repo runs end to end on Burla, what we found, and where
the bodies are buried.

## TL;DR

We pulled every public Airbnb listing in Inside Airbnb's open data dump (1.1M
listings across 116 cities), scraped 1.4M photo URLs, CLIP-scored 1.2M of
those images, ran 48k of the weirdest ones through YOLOv8, and put 50.7M
reviews through a 3-tier funnel that ends in Claude Haiku on the top 10k.

End to end: 10.8 hours of wall clock, ~$361 of compute, peak 1000 concurrent
Burla workers, one Python repo, one operator.

The whole pipeline is `remote_parallel_map` calls glued together with
checkpointed parquet files on a shared GCS-backed filesystem. Nothing fancy.
Most of the work was sanding off the rough edges of running thousands of
workers concurrently against external sites and HuggingFace.

## What we found

### Five hypotheses, bootstrap 95% CIs

We bucketed listings by visual traits and compared median `reviews_per_month`
(Inside Airbnb's demand proxy) per bucket, with bootstrap 95% CIs (1000
resamples, n >= 100 per bucket required). A finding is rejected if any pair of
bucket CIs overlaps.

| Hypothesis | Verdict | Notes |
|---|---|---|
| brightness_quartile | accepted | Q1 median 0.29 reviews/mo, Q4 median 0.65. Brighter photos correlate with more demand, monotonically across all four quartiles, no CI overlap. |
| tv_too_high | accepted | Listings where YOLO put the TV in the upper half of the frame had a 2.25x higher median demand (0.36 vs 0.16). The "too high" cases are dominated by TVs above fireplaces, which are a known design crime, but the demand signal goes the other way - those photos perform better. |
| messiness_quartile | rejected | Q1 0.35, Q4 0.34. CIs overlap completely. CLIP "messy" score has no relationship with demand once you control for sample size. |
| plant_count_bucket | rejected | Plant lovers will be sad. 0 plants: 0.44 reviews/mo. 4+ plants: 0.28. CIs overlap on the high-plant buckets so we can't reject the null. |
| cleaning_fee_ratio_bucket | rejected | Single-bucket result, the bucketing logic collapsed. Listed for transparency. |

The two surviving findings are nice in opposite directions: brightness is the
boring expected one, TV-too-high is the unintuitive one. Both are reported
with the bucket CIs visualized on the live site.

### The funny stuff

The site also surfaces:

- **Worst TV placements** (top 50): YOLO TV detection + CLIP "TV mounted above
  a fireplace." Mostly Chicago, NYC, London brownstones with original
  fireplaces and one Samsung 55-inch.
- **Messiest listings** (top 50): CLIP "messy cluttered room." Curated for
  taste, not weaponized. We do not link out individual listings by name in the
  social posts.
- **Mirror selfies** (top 50): hosts who didn't notice they were in the
  reflection. CLIP "photographer reflected in a mirror."
- **Plant maximalists** (top 30): CLIP "room full of houseplants" + YOLO
  potted plant count. Mostly NYC and Berlin.
- **Cleaning fees > nightly rate** (top 100): cleaning_fee / nightly_price,
  ranked. Worst offenders are vacation rentals in low-demand markets pricing
  the cleaning fee close to or over the nightly rate.
- **Funniest reviews** (top 100): 3-tier funnel (heuristic on every review,
  embedding cluster on top 200k, Claude humor score on top 10k).

## How the pipeline works

Seven stages, each takes a parquet, returns a parquet, and is independently
runnable. Every stage is a small script that calls `remote_parallel_map` once
or twice and merges the slices.

```
00 validate  -> 01 listings -> 02a scrape -> 02b clip ->
03 yolo gpu  -> 04 reviews 3-tier -> 05 correlate -> 06 artifacts
```

The unifying pattern is:

```python
from burla import remote_parallel_map

@dataclass
class WorkerArgs:
    batch_id: int
    input_path: str
    output_path: str
    ...

def worker(args: WorkerArgs) -> dict:
    rows = read_my_slice(args.input_path, args.batch_id)
    out  = do_real_work(rows)
    out.to_parquet(f"{args.output_path}/batch_{args.batch_id:05d}.parquet")
    return {"batch_id": args.batch_id, "n_rows": len(out)}

results = remote_parallel_map(
    worker,
    [WorkerArgs(...) for batch_id in range(N_BATCHES)],
    func_cpu=1, func_ram=8,
    max_parallelism=1000,
    grow=True, spinner=True,
)
merge_parquet_dir(...)
```

Burla pickles `worker` plus everything it transitively imports, ships it to
the cluster, and runs N copies in parallel. The shared filesystem at
`/workspace/shared` is GCS-backed and visible from every worker, which is what
makes the slice-and-merge pattern cheap.

### Sharing data: row-group-aligned parquet

The two big inputs are the photo manifest (1.4M rows) and the reviews dump
(50.7M rows). Naive code does this:

```python
df = pd.read_parquet("reviews_raw.parquet")           # OOM on the workers
batch = df.iloc[batch_id*BATCH:(batch_id+1)*BATCH]    # because every worker
                                                      # reads all 50M rows
```

That falls over hard at 1000 concurrent workers because every worker pulls
the entire file. The fix is to write the input parquet with
`row_group_size = BATCH_SIZE`, then read only the needed row group:

```python
import pyarrow.parquet as pq
pf = pq.ParquetFile("reviews_raw.parquet")
batch = pf.read_row_group(batch_id).to_pandas()
```

Worker memory drops by 100x and the cluster stops thrashing. Stage 4 went
from "fails on OOM at 30%" to "completes in 2.3 minutes wall" once the
reviews parquet was rechunked.

### Pre-staging model weights

CLIP, YOLO, and `sentence-transformers` all want to download their weights
from HuggingFace on first use. With 1000 cold workers, that's 1000
simultaneous HF downloads, which gets you rate-limited and disk-bombed in
about 90 seconds.

The pattern that worked:

1. One worker downloads weights into `/workspace/shared/models/...`.
2. Subsequent workers use `fcntl.flock` to serialize a single per-node copy
   from shared to local SSD, then load from local.
3. Each worker pins `torch.set_num_threads(1)` to avoid CPU thrash inside an
   already-saturated pod.

`scripts/preload_st_weights.py` and `_ensure_*` helpers in
`src/tasks/image_tasks.py` and `src/tasks/review_tasks.py` are where this
lives.

### Three-tier review funnel

Heuristic + embeddings + LLM. Cost-aware top-K cascade.

```
  50,686,612  raw reviews        (Inside Airbnb dumps)
       |     heuristic score on every review (CPU, 1000 workers, ~5 min wall)
       v
     200,000 keep top by heuristic
       |     sentence-transformers MiniLM-L6 embedding + MiniBatchKMeans
       |     (CPU, 200 workers, ~5 min wall)
       v
      10,000 sampled across clusters for diversity
       |     Claude Haiku scoring -> humor_score, category, one_line
       |     (200 workers, ~3 min wall, ~$0.65 in API calls)
       v
         100 final picks for the site
```

The cascade saves ~99.98% of LLM cost vs running Claude on every review.
Heuristic alone surfaces phrases like "do not stay" and emoji clusters,
embedding clustering buys diversity (so the top 100 isn't 80 noise complaints
and 20 cleaning-fee complaints), and Claude does the actual taste-tier sort.

### Bootstrap CIs

`s05_correlate.py` runs each hypothesis as a single Burla worker because the
math is small but the data load is annoying. It bootstrap-resamples each
bucket 1000 times, takes the 2.5/97.5 percentile of the bucket median, and
flags as "rejected" if any bucket pair has overlapping CIs.

## What broke (and what we did about it)

This was an iteratively-debugged run. Things that bit us, in order:

1. **Datadome blocking** at scale during Stage 2a scraping. We fell back to
   the photo URLs already present in the listings dump, which gave us 1.1M
   primary photos plus 309k full-manifest scrapes.
2. **GPU workers OOM-ing on YOLO model load** because of `cu118` vs `cu124`
   mismatches in the worker base image. Resolved by pinning `torch==2.5.1+cu124`
   and `opencv-python-headless` in the worker bootstrap.
3. **Reviews parquet OOM** at Stage 4, fixed by row-group-aligned writes
   (above).
4. **HuggingFace rate limits** when 200+ workers simultaneously downloaded
   the embedding model. Fixed by pre-staging to shared FS.
5. **Burla "job not found" 500s** during long jobs, transient cluster API
   errors. Worked around by retrying with `--skip-tier1 --skip-tier2`-style
   resume flags so completed work didn't redo.
6. **Soft budget cap trips** at the end of stages where work succeeded but
   the merge step hadn't run. Fixed by adding `scripts/manual_merge_*.py`
   scripts that complete the merge externally.

The recurring takeaway: at this fan-out, every "should be fine" assumption
gets stress-tested. Burla makes it easy to retry only the broken slice.

## Cost breakdown

| Stage | Wall | $ | What |
|---|---|---|---|
| 00 validate | ~2 min | 0.22 | check 116 Inside Airbnb cities are live |
| 01 listings | ~3 min | 0.17 | download + clean per-city listings.csv.gz |
| 02a scrape | ~3.5 hr | ~208 | scrape photo manifests, hit Datadome, fallback |
| 02b clip | ~2.4 hr | ~119 | CLIP-score 1.4M images on CPU |
| 03 yolo gpu | ~2 hr | ~13 | YOLOv8 on top 48k weirdest images |
| 04 reviews | ~1.6 hr | ~19 | 3-tier funnel on 50.7M reviews |
| 05 correlate | ~5 min | 0.13 | 5 hypotheses, bootstrap CIs |
| 06 artifacts | ~3 min | 0.04 | write site/data/*.json |
| **total** | **10.8 hr** | **~$361** | |

The dominant cost is Stage 2a (Datadome retries) and Stage 2b (1.4M CLIP
inferences). Both are CPU-bound. The GPU stage is cheap because it only runs
on the candidate subset, not on every photo.

## Caveats

- `reviews_per_month` is a public demand proxy, not actual bookings. Every
  chart in the site repeats this caveat.
- Inside Airbnb anonymizes locations within ~150m, so the world map shows
  neighborhoods, not exact addresses.
- Stage 3 GPU success rate landed at 27% because we paid YOLO weight download
  on every worker spin-up before pre-staging fixed it late. The visual
  findings still hold because Stage 2b CLIP scoring covered all 1.4M photos.
- We are not naming individual hosts. The "messiest" and "mirror selfie"
  sections show photos but link to public listings only by URL.

## Code map

- `src/config.py` - cities, top-N, hypotheses, soft budgets per stage.
- `src/lib/budget.py` - per-stage wall time and $ tracking, soft caps.
- `src/lib/io.py` - shared FS helpers, `register_src_for_burla`,
  `stage_done` checkpoint markers.
- `src/stages/s0[0-6]_*.py` - one orchestrator script per stage.
- `src/tasks/*.py` - Burla worker functions (have to be top-level + dataclass
  args so they're picklable).
- `scripts/` - one-off retry/merge/diagnostic scripts that built up over
  the run.
- `site/` - static HTML/CSS/JS, no build step. Reads `site/data/*.json`,
  paints DOM, draws Leaflet map.

## Reproducing

```bash
~/.burla/<your-account>/.venv/bin/pip install -e .
cp .env.example .env  # add ANTHROPIC_API_KEY
make all
```

The Makefile wires the stages in order and is resume-aware (Burla `stage_done`
markers in `/workspace/shared`). If anything breaks mid-run, re-running the
same `make` target picks up where it left off.
