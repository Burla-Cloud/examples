# Burla Examples

These are not toy notebooks. This repo is a gallery of real workloads that
usually force teams to choose a platform: Spark for data, Airflow for
orchestration, SageMaker for inference, Batch for containers, Ray for clusters,
Slurm for science, Kubernetes for everything else.

Burla keeps the shape of the work much smaller:

```python
from burla import remote_parallel_map

results = remote_parallel_map(my_python_function, my_inputs, grow=True)
```

One function, one list, thousands of remote calls. Then, when the job gets
serious, the same call can change the hardware, runtime, concurrency, and
streaming behavior without asking you to rebuild the workload around a new
framework.

<p align="center">
  <a href="https://burla-cloud.github.io/airbnb-burla/">
    <img alt="Airbnb example preview" src="https://a0.muscache.com/pictures/85379587-35fd-42f7-88a4-872581e417ee.jpg" width="48%">
  </a>
  <a href="https://burla-cloud.github.io/examples/met-weirdest-art/">
    <img alt="The Met hidden twins preview" src="https://images.metmuseum.org/CRDImages/es/web-large/DP258669.jpg" width="48%">
  </a>
</p>

Start with the live gallery: <https://burla-cloud.github.io/examples/>

## Why Burla Feels Different

The best part of these examples is not only that they are big. It is that they
stay legible.

| API surface | Where it shows up | What it buys you |
| --- | --- | --- |
| `func_cpu=` and `func_ram=` | Cheap CPU workers for scans, bigger workers for reducers, GPU-sized workers for inference and vision | A pipeline can move from "many tiny workers" to "one big reducer" to "A100 batch inference" without becoming three systems. |
| `image=` | `bioinformatics-alignment` runs BWA, samtools, and AWS CLI inside a custom worker image | Native binaries, CUDA stacks, and awkward system dependencies become part of the function call, not a separate platform migration. |
| `grow=True` | `image-dataset-resize` asks for 5,000 workers; `monte-carlo-simulation` asks for 2,000; `world-photo-index` peaked at 967 | You do not pre-provision a cluster just to run an embarrassingly parallel Python job. |
| `/workspace/shared` | Airbnb, arXiv, GitHub READMEs, GHCN, and World Photo Index write intermediate shards to the cluster filesystem | Map-reduce pipelines can pass large artifacts between stages without dragging everything through your laptop. |
| `max_parallelism=` | API backfills, web scraping, Postgres ETL, and polite CDN fetching | The same fan-out primitive can be aggressive with compute and careful with rate limits. |
| `generator=True` | ETL, scraping, image resizing, and API jobs | Long jobs can report progress, write partial output, and fail visibly instead of disappearing into a queue. |
| Local Python semantics | Worker exceptions and tracebacks surface back on the client | The failure mode feels like debugging Python, not spelunking through a job system. |

The result is a rare thing: examples that look like ordinary Python, but behave
like a serious distributed system.

```python
from burla import remote_parallel_map

# Many small workers for a wide scan.
parts = remote_parallel_map(
    scan_one_file,
    parquet_files,
    func_cpu=1,
    func_ram=4,
    max_parallelism=5_000,
    grow=True,
)

# Different runtime and hardware for a GPU stage.
vectors = remote_parallel_map(
    embed_batch,
    text_batches,
    image="jakezuliani/burla-embedder:latest",
    func_cpu=8,
    func_ram=64,
    max_parallelism=8,
    grow=True,
)

# One larger worker for the reduce.
answer = remote_parallel_map(
    reduce_everything,
    [parts],
    func_cpu=16,
    func_ram=64,
    grow=True,
)[0]
```

## Live Data Stories

These are polished, explorable demos with generated artifacts and writeups.

| Demo | Scale | What makes it a Burla example |
| --- | ---: | --- |
| [Airbnb at continental scale](https://burla-cloud.github.io/airbnb-burla/) | 1.1M listings, 1.4M photos, 50.7M reviews | A multi-stage CPU/GPU pipeline: scrape and clean on cheap workers, CLIP-score images on CPU, run YOLO on GPU-sized workers, and merge Parquet shards through `/workspace/shared`. |
| [Amazon Review Distiller](https://burla-cloud.github.io/examples/amazon-review-distiller/) | 571M reviews, 275 GB JSONL | Deterministic text mining across 500+ CPUs, with shard outputs written remotely and reduced into an interactive site. |
| [The Met's Hidden Twins](https://burla-cloud.github.io/examples/met-weirdest-art/) | 192K public-domain artwork images | Polite CDN-aware image fetching, CLIP embeddings, FAISS search, and visual pair discovery in one Python project. |
| [NYC Ghost Neighborhoods](https://burla-cloud.github.io/examples/nyc-ghost-neighborhoods/) | 2.76B taxi trips in about 15 seconds | One task per monthly Parquet file, streamed directly from public storage, then reduced into city-level recovery patterns. |
| [Fossils of the arXiv](https://burla-cloud.github.io/examples/arxiv-fossils/) | 2.71M abstracts in about 25 minutes | Different stages request different machines: raw-data discovery, CPU embedding shards, and a larger reduce worker for clustering. |
| [World Photo Index](https://burla-cloud.github.io/examples/world-photo-index/) | 9.49M geotagged Flickr photos in about 8 minutes | 967 workers reverse-geocode and tokenize photo metadata, then reduce country-level photographic obsessions. |
| [One Million GitHub READMEs](https://burla-cloud.github.io/examples/github-repo-summarizer/) | 1.2M READMEs, 2.3B file rows scanned | A scatter-gather upload into the shared filesystem, 600 shard workers, then a parallel reduce. No LLM required. |

## Workloads That Usually Become Platforms

These examples are meant to be copied, edited, and turned into real production
scripts.

| Example | The job | Burla move |
| --- | --- | --- |
| [GPU embeddings](gpu-embedding-demo/) | Embed Wikipedia slices with `BAAI/bge-large-en-v1.5` on A100s | CPU download stage, custom CUDA image, GPU embedding stage, GPU query stage, local vector search. |
| [Image dataset resize](image-dataset-resize/) | Resize 5M S3 images into multiple sizes | 5,000 workers stream S3 objects, resize with Pillow, write back to S3, and stream progress. |
| [Bioinformatics alignment](bioinformatics-alignment/) | Align thousands of paired-end FASTQ samples with BWA-MEM | Run native genomics tools inside a custom container using `image=`, with `func_cpu=4` and `func_ram=16`. |
| [GDAL raster processing](gdal-raster-processing/) | Compute NDVI, clip, or reproject Sentinel tiles | Fan out one tile per worker with GDAL/PROJ/rasterio available, no hand-built geospatial cluster. |
| [Batch ML inference](ml-inference-batch/) | Run a HuggingFace model over 10M text rows | Chunk rows, load the model once per worker, and choose CPU or GPU resources from the call site. |
| [Global Rainiest Day Ever](ghcn-rainiest-day/agents/ghcn-rainiest-day/) | Scan 3.18B NOAA weather rows from 1750 to today | One worker per year-file, remote JSON shards, then a larger reducer that renders a map and leaderboards. |

## Everyday Patterns With Teeth

These are the examples for jobs that start as a script and then suddenly need a
thousand computers.

| Example | Use it when... | Burla move |
| --- | --- | --- |
| [Parallel web scraping](parallel-web-scraping/) | You have 1M mostly-static pages to fetch and parse | Hold exactly 1,000 workers live with `max_parallelism`, reuse HTTP clients, and stream rows back. |
| [ETL without Airflow](python-etl-no-airflow/) | You need a simple file-drop ETL without a scheduler stack | Run extract/transform on 10,000 files while capping database load at 1,000 concurrent workers. |
| [Rate-limited API requests](rate-limited-api-requests/) | You need millions of API calls but must respect a global limit | Split work into chunks, cap live workers, and keep per-worker throttling in plain Python. |
| [Pandas apply at cluster scale](pandas-apply-parallel/) | Your row-wise `df.apply(fn)` is correct but painfully slow | Partition the data and run the exact pandas function on 1,200 cloud workers. |
| [Parquet fan-out](parquet-parallel/) | You have thousands of independent Parquet files | Run one file per worker and get local exceptions with remote tracebacks. |
| [Monte Carlo simulation](monte-carlo-simulation/) | You have independent simulations, samples, or pricing paths | Run 1B paths across 2,000 workers, return tiny summaries, aggregate locally. |

## What To Read First

If you want to see the big idea in five minutes, read
[parquet-parallel](parquet-parallel/). It is the purest version of the API:
function, inputs, remote results.

If you want to see why Burla is different from a "parallel map" wrapper, read
[gpu-embedding-demo](gpu-embedding-demo/),
[bioinformatics-alignment](bioinformatics-alignment/), and
[arxiv-fossils](arxiv-fossils/). Those show runtime changes, hardware changes,
GPU work, shared artifacts, and reducers that need more machine than the map
stage.

If you want to be delighted, open the live stories:
[Airbnb](https://burla-cloud.github.io/airbnb-burla/),
[The Met](https://burla-cloud.github.io/examples/met-weirdest-art/),
[World Photo Index](https://burla-cloud.github.io/examples/world-photo-index/),
or [NYC Ghost Neighborhoods](https://burla-cloud.github.io/examples/nyc-ghost-neighborhoods/).

## Why This Repo Exists

Most distributed systems make you translate your work into their worldview:
DAGs, actors, jobs, queues, services, UDFs, manifests, containers, schedulers,
clusters.

Burla's bet is smaller and more developer-friendly: keep your Python function,
keep your data-shaped list of inputs, and add just enough remote execution
control to make the job enormous when it needs to be enormous.

That is why these examples are intentionally concrete. They are full of boring,
useful details: S3 keys, Parquet row groups, model caches, custom Docker images,
rate limits, database connection caps, CDN politeness, worker RAM, GPU quota,
shared filesystem paths, reducers, and artifacts you can inspect.

The magic is not that Burla hides reality. The magic is that it lets you deal
with reality from Python.

## Links

- Burla docs: <https://burla.dev>
- Live examples gallery: <https://burla-cloud.github.io/examples/>
- Burla GitHub: <https://github.com/Burla-Cloud>
