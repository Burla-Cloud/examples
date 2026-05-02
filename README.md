# Burla Examples

Real workloads that show what it feels like to run ordinary Python across a lot
of computers.

This repo is a curated set of Burla examples: full-corpus data stories,
GPU embedding jobs, production ETL patterns, geospatial pipelines,
bioinformatics jobs, API backfills, web scraping, Monte Carlo simulations, and
more. The examples are intentionally concrete. They use real datasets, real
images, real model/runtime requirements, real rate limits, and artifacts you can
open.

<p align="center">
  <a href="https://burla-cloud.github.io/airbnb-burla/">
    <img src="assets/readme/airbnb-burla.png" alt="Multimodal Airbnb analysis" width="49%">
  </a>
  <a href="https://burla-cloud.github.io/examples/world-photo-index/">
    <img src="assets/readme/world-photo-index.png" alt="World Photo Index" width="49%">
  </a>
</p>

<p align="center">
  <a href="https://burla-cloud.github.io/examples/">Live gallery</a>
  &middot;
  <a href="https://burla.dev">Burla docs</a>
  &middot;
  <a href="#featured-examples">Featured examples</a>
  &middot;
  <a href="#copyable-patterns">Copyable patterns</a>
</p>

<table>
  <tr>
    <td align="center"><strong>19 examples</strong><br>from simple fan-out to multi-stage pipelines</td>
    <td align="center"><strong>7 live sites</strong><br>with generated artifacts and explorable findings</td>
    <td align="center"><strong>CPUs, GPUs, containers</strong><br>picked per function call</td>
    <td align="center"><strong>One Python API</strong><br><code>remote_parallel_map</code></td>
  </tr>
</table>

## Featured examples

These are the ones to open first. They are big, visual, and opinionated enough
to make the point quickly: Burla lets a Python project grow into a serious
distributed workload without turning into a separate platform project.

<table>
  <tr>
    <td width="50%" valign="top">
      <a href="https://burla-cloud.github.io/airbnb-burla/"><img src="assets/readme/airbnb-burla.png" alt="Multimodal Airbnb analysis"></a>
      <h3><a href="https://burla-cloud.github.io/airbnb-burla/">Multimodal Airbnb analysis</a></h3>
      <p><strong>1.1M listings, 1.4M photos, 50.7M reviews.</strong></p>
      <p>Listings, photos, CLIP scoring, A100 YOLOv8 detection, review funnels, bootstrap confidence intervals, and a polished public site from one Python project.</p>
      <p><a href="https://burla-cloud.github.io/airbnb-burla/">Live site</a> &middot; <a href="airbnb-burla/">Source</a></p>
    </td>
    <td width="50%" valign="top">
      <a href="https://burla-cloud.github.io/examples/world-photo-index/"><img src="assets/readme/world-photo-index.png" alt="World Photo Index"></a>
      <h3><a href="https://burla-cloud.github.io/examples/world-photo-index/">World Photo Index</a></h3>
      <p><strong>9.49M geotagged Flickr photos, 967 workers, about 8 minutes.</strong></p>
      <p>Reverse-geocode public photos, tokenize user-written tags, and find what every country photographs more than anywhere else.</p>
      <p><a href="https://burla-cloud.github.io/examples/world-photo-index/">Live site</a> &middot; <a href="world-photo-index/">Source</a></p>
    </td>
  </tr>
  <tr>
    <td width="50%" valign="top">
      <a href="https://burla-cloud.github.io/examples/met-weirdest-art/"><img src="assets/readme/met-weirdest-art.png" alt="The Met hidden twins"></a>
      <h3><a href="https://burla-cloud.github.io/examples/met-weirdest-art/">The Met's Hidden Twins</a></h3>
      <p><strong>192K public-domain artwork images.</strong></p>
      <p>Fetch Open Access museum images, embed them with CLIP, search with FAISS, and surface visual near-duplicates across centuries and mediums.</p>
      <p><a href="https://burla-cloud.github.io/examples/met-weirdest-art/">Live site</a> &middot; <a href="met-weirdest-art/">Source</a></p>
    </td>
    <td width="50%" valign="top">
      <a href="https://burla-cloud.github.io/examples/nyc-ghost-neighborhoods/"><img src="assets/readme/nyc-ghost-neighborhoods.png" alt="NYC Ghost Neighborhoods"></a>
      <h3><a href="https://burla-cloud.github.io/examples/nyc-ghost-neighborhoods/">NYC Ghost Neighborhoods</a></h3>
      <p><strong>2.76B taxi and FHV trips, about 15 seconds on Burla.</strong></p>
      <p>Scan every monthly public trip file to find neighborhoods that faded, recovered, or became newly important after years of city-scale change.</p>
      <p><a href="https://burla-cloud.github.io/examples/nyc-ghost-neighborhoods/">Live site</a> &middot; <a href="nyc-ghost-neighborhoods/">Source</a></p>
    </td>
  </tr>
  <tr>
    <td width="50%" valign="top">
      <a href="https://burla-cloud.github.io/examples/amazon-review-distiller/"><img src="assets/readme/amazon-review-distiller.png" alt="Amazon Review Distiller"></a>
      <h3><a href="https://burla-cloud.github.io/examples/amazon-review-distiller/">Amazon Review Distiller</a></h3>
      <p><strong>571M reviews, 275GB JSONL, 500+ parallel CPUs.</strong></p>
      <p>Read the entire public review corpus, score every review deterministically, keep tiny heaps per shard, and reduce them into searchable findings.</p>
      <p><a href="https://burla-cloud.github.io/examples/amazon-review-distiller/">Live site</a> &middot; <a href="amazon-review-distiller/">Source</a></p>
    </td>
    <td width="50%" valign="top">
      <a href="https://burla-cloud.github.io/examples/arxiv-fossils/"><img src="assets/readme/arxiv-fossils.png" alt="Fossils of the arXiv"></a>
      <h3><a href="https://burla-cloud.github.io/examples/arxiv-fossils/">Fossils of the arXiv</a></h3>
      <p><strong>2.71M abstracts embedded and clustered.</strong></p>
      <p>Find extinct research topics, emerging clusters, and the loneliest papers by embedding the full arXiv metadata corpus instead of sampling it.</p>
      <p><a href="https://burla-cloud.github.io/examples/arxiv-fossils/">Live site</a> &middot; <a href="arxiv-fossils/">Source</a></p>
    </td>
  </tr>
  <tr>
    <td width="50%" valign="top">
      <a href="https://burla-cloud.github.io/examples/github-repo-summarizer/"><img src="assets/readme/github-repo-summarizer.png" alt="One million GitHub READMEs"></a>
      <h3><a href="https://burla-cloud.github.io/examples/github-repo-summarizer/">One Million GitHub READMEs</a></h3>
      <p><strong>1.2M READMEs, 2.3B file rows scanned upstream.</strong></p>
      <p>Export README Parquet, shard deterministic summarizers, write per-shard JSON to shared storage, and reduce category stats without calling an LLM.</p>
      <p><a href="https://burla-cloud.github.io/examples/github-repo-summarizer/">Live site</a> &middot; <a href="github-repo-summarizer/">Source</a></p>
    </td>
    <td width="50%" valign="top">
      <a href="ghcn-rainiest-day/agents/ghcn-rainiest-day/"><img src="assets/readme/ghcn-rainiest-day.png" alt="Global rainiest day ever"></a>
      <h3><a href="ghcn-rainiest-day/agents/ghcn-rainiest-day/">Global Rainiest Day Ever</a></h3>
      <p><strong>3.18B NOAA weather rows, 245 year-file workers, about 2 minutes.</strong></p>
      <p>Stream every yearly GHCN-Daily file, keep station-level top heaps, reduce country-decade statistics, and render a map of extreme rainfall.</p>
      <p><a href="ghcn-rainiest-day/agents/ghcn-rainiest-day/">Source</a></p>
    </td>
  </tr>
</table>

## What these examples show

The examples are not just "run a map over a list." They show the parts that
usually make distributed work awkward:

```python
from burla import remote_parallel_map

results = remote_parallel_map(
    process_one_shard,
    shards,
    func_cpu=4,                 # choose CPU per function call
    func_ram=16,                # choose RAM per function call
    func_gpu="A100",            # ask for GPU workers when the stage needs them
    image="my-worker:latest",   # bring your own Docker runtime
    max_parallelism=1000,       # protect APIs, databases, and websites
    generator=True,             # stream results as workers finish
    grow=True,                  # grow the cluster from code
)
```

The big pattern across the repo is simple: keep the Python function visible,
then change the hardware, runtime, concurrency, or artifact handoff around it.
That is why the examples stay readable even when the datasets are very large.

## Copyable patterns

These are the examples you reach for when a script is correct locally and needs
to become fast, reliable, or production-shaped.

<table>
  <tr>
    <td width="33%" valign="top">
      <a href="gpu-embedding-demo/"><img src="assets/readme/gpu-embedding-demo.png" alt="GPU embeddings"></a>
      <h3><a href="gpu-embedding-demo/">GPU embeddings on A100s</a></h3>
      <p>CPU download stage, custom CUDA image, A100 embedding stage, shared vector shards, local search.</p>
    </td>
    <td width="33%" valign="top">
      <a href="ml-inference-batch/"><img src="assets/readme/ml-inference-batch.png" alt="Batch ML inference"></a>
      <h3><a href="ml-inference-batch/">Batch inference without serving</a></h3>
      <p>Run Hugging Face models over 10M rows without building an endpoint, manifest, or serving layer.</p>
    </td>
    <td width="33%" valign="top">
      <a href="image-dataset-resize/"><img src="assets/readme/image-dataset-resize.png" alt="Image dataset resize"></a>
      <h3><a href="image-dataset-resize/">Millions of image resizes</a></h3>
      <p>Chunk S3 image keys, resize with Pillow, write outputs back to S3, and stream progress.</p>
    </td>
  </tr>
  <tr>
    <td width="33%" valign="top">
      <a href="bioinformatics-alignment/"><img src="assets/readme/bioinformatics-alignment.png" alt="Bioinformatics alignment"></a>
      <h3><a href="bioinformatics-alignment/">Genome alignment</a></h3>
      <p>Run BWA-MEM and samtools in a custom image with one paired-end FASTQ sample per worker.</p>
    </td>
    <td width="33%" valign="top">
      <a href="gdal-raster-processing/"><img src="assets/readme/gdal-raster-processing.png" alt="GDAL raster processing"></a>
      <h3><a href="gdal-raster-processing/">GDAL raster processing</a></h3>
      <p>Compute NDVI, clip, or reproject one Sentinel tile per worker with geospatial dependencies ready.</p>
    </td>
    <td width="33%" valign="top">
      <a href="monte-carlo-simulation/"><img src="assets/readme/monte-carlo-simulation.png" alt="Monte Carlo simulation"></a>
      <h3><a href="monte-carlo-simulation/">Billion-path Monte Carlo</a></h3>
      <p>Run independent simulations across thousands of workers and return tiny aggregate summaries.</p>
    </td>
  </tr>
  <tr>
    <td width="33%" valign="top">
      <a href="parquet-parallel/"><img src="assets/readme/parquet-parallel.png" alt="Parquet fan-out"></a>
      <h3><a href="parquet-parallel/">One Parquet file per worker</a></h3>
      <p>Compute QA stats across thousands of files without starting Spark for a file-parallel job.</p>
    </td>
    <td width="33%" valign="top">
      <a href="pandas-apply-parallel/"><img src="assets/readme/pandas-apply-parallel.png" alt="Pandas apply parallel"></a>
      <h3><a href="pandas-apply-parallel/">Pandas apply in parallel</a></h3>
      <p>Keep the row-wise Python function and scale the partitioned dataset around it.</p>
    </td>
    <td width="33%" valign="top">
      <a href="python-etl-no-airflow/"><img src="assets/readme/python-etl-no-airflow.png" alt="Python ETL no Airflow"></a>
      <h3><a href="python-etl-no-airflow/">ETL without Airflow</a></h3>
      <p>Transform 10,000 gzipped JSON drops while protecting Postgres with <code>max_parallelism</code>.</p>
    </td>
  </tr>
  <tr>
    <td width="33%" valign="top">
      <a href="rate-limited-api-requests/"><img src="assets/readme/rate-limited-api-requests.png" alt="Rate limited API requests"></a>
      <h3><a href="rate-limited-api-requests/">Rate-limited API jobs</a></h3>
      <p>Run millions of requests while keeping the provider limit explicit in chunking, sleeps, and concurrency.</p>
    </td>
    <td width="33%" valign="top">
      <a href="parallel-web-scraping/"><img src="assets/readme/parallel-web-scraping.png" alt="Parallel web scraping"></a>
      <h3><a href="parallel-web-scraping/">Parallel web scraping</a></h3>
      <p>Scrape large static archives with retries, error rows, connection reuse, and a global cap.</p>
    </td>
    <td width="33%" valign="top">
      <a href="arxiv-fossils/"><img src="assets/readme/arxiv-fossils.png" alt="Corpus embedding pattern"></a>
      <h3><a href="arxiv-fossils/">Corpus-scale embedding</a></h3>
      <p>Use the arXiv pipeline as a template for embedding and reducing any text corpus you do not want to sample.</p>
    </td>
  </tr>
</table>

## How to choose an example

| If you want to learn... | Start here |
| --- | --- |
| The smallest possible Burla shape | [parquet-parallel](parquet-parallel/) |
| GPU workers and custom CUDA runtimes | [gpu-embedding-demo](gpu-embedding-demo/) |
| Custom containers with native binaries | [bioinformatics-alignment](bioinformatics-alignment/) |
| Multi-stage artifacts on `/workspace/shared` | [airbnb-burla](airbnb-burla/) or [github-repo-summarizer](github-repo-summarizer/) |
| Rate limits and controlled fan-out | [rate-limited-api-requests](rate-limited-api-requests/) or [parallel-web-scraping](parallel-web-scraping/) |
| A beautiful full-corpus data story | [World Photo Index](world-photo-index/) or [The Met's Hidden Twins](met-weirdest-art/) |

## Why this repo is useful

Most parallel examples stop at "hello world, but on more cores." These examples
keep going into the parts that matter in real work:

- choosing CPU, RAM, and GPU per stage
- using custom Docker images for CUDA, GDAL, BWA, samtools, or other native tools
- writing intermediate Parquet, JSON, and vector shards to shared storage
- reducing large remote artifacts without pulling everything through your laptop
- streaming results, progress, and failures back to the client
- controlling concurrency for APIs, databases, websites, and public data sources

That is the point of Burla: normal Python that can become serious infrastructure
only at the places where the workload actually needs it.

## Links

- Burla docs: <https://burla.dev>
- Live examples gallery: <https://burla-cloud.github.io/examples/>
- Burla GitHub: <https://github.com/Burla-Cloud>
