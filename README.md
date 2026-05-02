# Burla Examples

**Copyable Python examples for large-scale data work.**
Run real workloads across remote CPUs, GPUs, custom containers, and controlled
concurrency without rewriting your project around a distributed framework.

<table>
  <tr>
    <td align="center"><strong>19 examples</strong><br>from one-file fan-out to multi-stage pipelines</td>
    <td align="center"><strong>7 live demos</strong><br>with generated findings and explorable artifacts</td>
    <td align="center"><strong>CPU, GPU, Docker</strong><br>chosen per function call</td>
    <td align="center"><strong>One Python API</strong><br><code>remote_parallel_map</code></td>
  </tr>
</table>

<p align="center">
  <a href="https://burla-cloud.github.io/examples/">Live gallery</a>
  &middot;
  <a href="https://burla.dev">Burla docs</a>
  &middot;
  <a href="#choose-a-collection">Choose a collection</a>
  &middot;
  <a href="#what-these-examples-demonstrate">What they demonstrate</a>
</p>

## Choose a collection

| Collection | Good for | Examples |
| --- | --- | --- |
| [Full-corpus data stories](#full-corpus-data-stories) | Scanning huge public datasets and turning them into polished findings | Airbnb, Amazon Reviews, NYC Taxi, World Photo Index, GitHub READMEs, NOAA rain |
| [ML, embeddings, and vision](#ml-embeddings-and-vision) | GPU runtimes, custom CUDA images, model loading, vector search, visual search | A100 embeddings, batch inference, arXiv, The Met |
| [Production data jobs](#production-data-jobs) | ETL, Parquet, pandas, image processing, API limits, web scraping | S3 image resize, pandas apply, Postgres ETL, rate-limited APIs |
| [Scientific and geospatial](#scientific-and-geospatial) | Native tools, raster processing, bioinformatics, simulations | BWA-MEM, GDAL, Monte Carlo |

## Full-corpus data stories

These are the showpieces: real corpora, credible scale, and finished outputs you
can open.

<table>
  <tr>
    <td width="50%" valign="top">
      <a href="https://burla-cloud.github.io/airbnb-burla/"><img src="assets/readme/airbnb-burla.png" alt="Multimodal Airbnb analysis"></a>
      <h3><a href="https://burla-cloud.github.io/airbnb-burla/">Multimodal Airbnb analysis</a></h3>
      <p><strong>1.1M listings, 1.4M photos, 50.7M reviews.</strong></p>
      <p>Run listings, photos, CLIP scoring, A100 YOLOv8 detection, review funnels, and confidence intervals across the public corpus.</p>
      <p><a href="https://burla-cloud.github.io/airbnb-burla/">Live demo</a> &middot; <a href="airbnb-burla/">Source</a></p>
    </td>
    <td width="50%" valign="top">
      <a href="https://burla-cloud.github.io/examples/amazon-review-distiller/"><img src="assets/readme/amazon-review-distiller.png" alt="Amazon Review Distiller"></a>
      <h3><a href="https://burla-cloud.github.io/examples/amazon-review-distiller/">Amazon Review Distiller</a></h3>
      <p><strong>571M reviews, 275GB JSONL, 500+ parallel CPUs.</strong></p>
      <p>Score every public Amazon review deterministically, keep tiny heaps per shard, and reduce them into searchable findings.</p>
      <p><a href="https://burla-cloud.github.io/examples/amazon-review-distiller/">Live demo</a> &middot; <a href="amazon-review-distiller/">Source</a></p>
    </td>
  </tr>
  <tr>
    <td width="50%" valign="top">
      <a href="https://burla-cloud.github.io/examples/nyc-ghost-neighborhoods/"><img src="assets/readme/nyc-ghost-neighborhoods.png" alt="NYC Ghost Neighborhoods"></a>
      <h3><a href="https://burla-cloud.github.io/examples/nyc-ghost-neighborhoods/">NYC Ghost Neighborhoods</a></h3>
      <p><strong>2.76B taxi and FHV trips in about 15 seconds.</strong></p>
      <p>Scan every monthly public trip file to find zones that faded, recovered, or became newly important.</p>
      <p><a href="https://burla-cloud.github.io/examples/nyc-ghost-neighborhoods/">Live demo</a> &middot; <a href="nyc-ghost-neighborhoods/">Source</a></p>
    </td>
    <td width="50%" valign="top">
      <a href="https://burla-cloud.github.io/examples/world-photo-index/"><img src="assets/readme/world-photo-index.png" alt="World Photo Index"></a>
      <h3><a href="https://burla-cloud.github.io/examples/world-photo-index/">World Photo Index</a></h3>
      <p><strong>9.49M geotagged Flickr photos, 967 workers, about 8 minutes.</strong></p>
      <p>Reverse-geocode public photos and build country-level signatures from user-written tags.</p>
      <p><a href="https://burla-cloud.github.io/examples/world-photo-index/">Live demo</a> &middot; <a href="world-photo-index/">Source</a></p>
    </td>
  </tr>
  <tr>
    <td width="50%" valign="top">
      <a href="https://burla-cloud.github.io/examples/github-repo-summarizer/"><img src="assets/readme/github-repo-summarizer.png" alt="One million GitHub READMEs"></a>
      <h3><a href="https://burla-cloud.github.io/examples/github-repo-summarizer/">One Million GitHub READMEs</a></h3>
      <p><strong>1.2M READMEs, 2.3B upstream file rows.</strong></p>
      <p>Shard deterministic summarizers, write per-shard JSON to shared storage, and reduce category stats without calling an LLM.</p>
      <p><a href="https://burla-cloud.github.io/examples/github-repo-summarizer/">Live demo</a> &middot; <a href="github-repo-summarizer/">Source</a></p>
    </td>
    <td width="50%" valign="top">
      <a href="ghcn-rainiest-day/agents/ghcn-rainiest-day/"><img src="assets/readme/ghcn-rainiest-day.png" alt="Global rainiest day ever"></a>
      <h3><a href="ghcn-rainiest-day/agents/ghcn-rainiest-day/">Global Rainiest Day Ever</a></h3>
      <p><strong>3.18B NOAA weather rows, 245 year-file workers, about 2 minutes.</strong></p>
      <p>Stream every yearly GHCN-Daily file, keep station-level top heaps, reduce country-decade stats, and render a map.</p>
      <p><a href="ghcn-rainiest-day/agents/ghcn-rainiest-day/">Source</a></p>
    </td>
  </tr>
</table>

## ML, embeddings, and vision

Examples for model-heavy jobs where the runtime matters: CUDA, GPUs, model
weights, vector artifacts, and visual search.

<table>
  <tr>
    <td width="50%" valign="top">
      <a href="gpu-embedding-demo/"><img src="assets/readme/gpu-embedding-demo.png" alt="GPU embeddings on A100s"></a>
      <h3><a href="gpu-embedding-demo/">GPU embeddings on A100s</a></h3>
      <p><strong>50K Wikipedia articles across CPU and A100 stages.</strong></p>
      <p>Download text on CPU workers, embed with a custom CUDA image, write vector shards, and search locally.</p>
    </td>
    <td width="50%" valign="top">
      <a href="ml-inference-batch/"><img src="assets/readme/ml-inference-batch.png" alt="Batch inference without serving"></a>
      <h3><a href="ml-inference-batch/">Batch inference without serving</a></h3>
      <p><strong>10M text rows scored as a batch job.</strong></p>
      <p>Load a Hugging Face model once per worker and score Parquet batches without building an endpoint.</p>
    </td>
  </tr>
  <tr>
    <td width="50%" valign="top">
      <a href="https://burla-cloud.github.io/examples/arxiv-fossils/"><img src="assets/readme/arxiv-fossils.png" alt="Fossils of the arXiv"></a>
      <h3><a href="https://burla-cloud.github.io/examples/arxiv-fossils/">Fossils of the arXiv</a></h3>
      <p><strong>2.71M abstracts embedded and clustered.</strong></p>
      <p>Find extinct research topics, emerging clusters, and isolated papers by embedding the full metadata corpus.</p>
      <p><a href="https://burla-cloud.github.io/examples/arxiv-fossils/">Live demo</a> &middot; <a href="arxiv-fossils/">Source</a></p>
    </td>
    <td width="50%" valign="top">
      <a href="https://burla-cloud.github.io/examples/met-weirdest-art/"><img src="assets/readme/met-weirdest-art.png" alt="The Met hidden twins"></a>
      <h3><a href="https://burla-cloud.github.io/examples/met-weirdest-art/">The Met's Hidden Twins</a></h3>
      <p><strong>192K public-domain artwork images.</strong></p>
      <p>Fetch Open Access museum images, embed with CLIP, search with FAISS, and surface visual near-duplicates.</p>
      <p><a href="https://burla-cloud.github.io/examples/met-weirdest-art/">Live demo</a> &middot; <a href="met-weirdest-art/">Source</a></p>
    </td>
  </tr>
</table>

## Production data jobs

Practical patterns for work that starts as a script and then needs throughput,
progress, and failure handling.

<table>
  <tr>
    <td width="33%" valign="top">
      <a href="image-dataset-resize/"><img src="assets/readme/image-dataset-resize.png" alt="Image dataset resize"></a>
      <h3><a href="image-dataset-resize/">Millions of image resizes</a></h3>
      <p>Chunk S3 image keys, resize with Pillow, write outputs back to S3, and stream progress.</p>
    </td>
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
  </tr>
  <tr>
    <td width="33%" valign="top">
      <a href="python-etl-no-airflow/"><img src="assets/readme/python-etl-no-airflow.png" alt="Python ETL no Airflow"></a>
      <h3><a href="python-etl-no-airflow/">ETL without Airflow</a></h3>
      <p>Transform 10,000 gzipped JSON drops while protecting Postgres with <code>max_parallelism</code>.</p>
    </td>
    <td width="33%" valign="top">
      <a href="rate-limited-api-requests/"><img src="assets/readme/rate-limited-api-requests.png" alt="Rate limited API requests"></a>
      <h3><a href="rate-limited-api-requests/">Rate-limited API jobs</a></h3>
      <p>Run millions of requests while keeping provider limits explicit in chunking, sleeps, and concurrency.</p>
    </td>
    <td width="33%" valign="top">
      <a href="parallel-web-scraping/"><img src="assets/readme/parallel-web-scraping.png" alt="Parallel web scraping"></a>
      <h3><a href="parallel-web-scraping/">Parallel web scraping</a></h3>
      <p>Scrape large static archives with retries, error rows, connection reuse, and a global cap.</p>
    </td>
  </tr>
</table>

## Scientific and geospatial

Examples for native binaries, geospatial dependencies, and simulations that do
not fit cleanly into dataframe systems.

<table>
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
</table>

## What these examples demonstrate

```python
from burla import remote_parallel_map

results = remote_parallel_map(
    process_one_shard,
    shards,
    func_cpu=4,
    func_ram=16,
    func_gpu="A100",
    image="my-worker:latest",
    max_parallelism=1000,
    generator=True,
    grow=True,
)
```

Across the repo you will see:

- CPU, RAM, GPU, and Docker runtime chosen per stage
- shared files under `/workspace/shared` for Parquet, JSON, and vector handoffs
- streaming progress and failures back to the client with `generator=True`
- concurrency caps for APIs, databases, websites, and public data sources
- reducers that run on different hardware than the wide map stage

## Links

- Burla docs: <https://burla.dev>
- Live examples gallery: <https://burla-cloud.github.io/examples/>
- Burla GitHub: <https://github.com/Burla-Cloud>
