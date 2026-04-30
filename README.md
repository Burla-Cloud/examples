# Burla Examples

Examples, demos, and use cases for running Python at cluster scale with Burla.

Start with the gallery site: https://burla-cloud.github.io/examples/

Burla's core API is small: write normal Python, then use `remote_parallel_map()` to run it across many CPUs or GPUs. This repo collects the demos, data stories, and practical patterns that show what that looks like in real workloads.

## Live Demos

These examples already had GitHub Pages sites, so they are published under this repo:

| Demo | What it shows |
| --- | --- |
| [Airbnb at continental scale](https://burla-cloud.github.io/examples/airbnb-burla-demo/) | 1.7M photos and 50.7M reviews across 119 cities processed with CLIP, Claude Haiku Vision, and bootstrap CIs. |
| [Amazon Review Distiller](https://burla-cloud.github.io/examples/amazon-review-distiller/) | 571M reviews ranked and searched with parallel deterministic text analysis. |
| [The Met's Hidden Twins](https://burla-cloud.github.io/examples/met-weirdest-art/) | 192K museum artworks embedded to find visual near-duplicates across centuries. |
| [NYC Ghost Neighborhoods](https://burla-cloud.github.io/examples/nyc-ghost-neighborhoods/) | 2.76B taxi trips processed to find neighborhoods that changed after the pandemic. |
| [Fossils of the arXiv](https://burla-cloud.github.io/examples/arxiv-fossils/) | 2.71M abstracts embedded and clustered to find extinct and emerging research topics. |
| [World Photo Index](https://burla-cloud.github.io/examples/world-photo-index/) | 9.49M geotagged Flickr photos analyzed to find what every country photographs. |
| [One Million GitHub READMEs](https://burla-cloud.github.io/examples/github-repo-summarizer/) | 1.2M READMEs classified, summarized, and searched without an LLM. |

## Examples

### Heavy Workloads

| Example | Focus |
| --- | --- |
| [gpu-embedding-demo](gpu-embedding-demo/) | GPU embeddings on A100s. |
| [image-dataset-resize](image-dataset-resize/) | Resizing millions of images in parallel. |
| [bioinformatics-alignment](bioinformatics-alignment/) | BWA-MEM alignment over many FASTQ files. |
| [gdal-raster-processing](gdal-raster-processing/) | GDAL raster jobs across many workers. |
| [ml-inference-batch](ml-inference-batch/) | Batch inference without a serving layer. |
| [ghcn-rainiest-day](ghcn-rainiest-day/) | Scanning billions of weather rows. |

### Everyday Patterns

| Example | Focus |
| --- | --- |
| [parallel-web-scraping](parallel-web-scraping/) | Scraping thousands of pages concurrently. |
| [python-etl-no-airflow](python-etl-no-airflow/) | Simple Python ETL without Airflow. |
| [rate-limited-api-requests](rate-limited-api-requests/) | Large API jobs with explicit rate limits. |
| [pandas-apply-parallel](pandas-apply-parallel/) | Scaling slow `pandas.apply()` functions. |
| [parquet-parallel](parquet-parallel/) | Processing many Parquet files in parallel. |
| [monte-carlo-simulation](monte-carlo-simulation/) | Independent Monte Carlo simulations across many cores. |

GitHub Pages only deploys examples that already had Pages sites before this repo was created.
