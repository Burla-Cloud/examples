# Worker image for the bioinformatics-alignment demo

Base: `python:3.12`. Adds `bwa 0.7.18`, `samtools 1.21`, and `awscli 2.23` via apt.

## Pre-built public image

A pre-built image is hosted publicly; no auth needed to pull:

```
us-docker.pkg.dev/test-burla/burla-demos/burla-bio-worker:latest
```

The demo's `main.py` uses this image by default via `remote_parallel_map(..., image=IMAGE, ...)`.

## Build your own

```bash
cd worker-image
docker build --platform linux/amd64 -t <your-registry>/burla-bio-worker:latest .
docker push <your-registry>/burla-bio-worker:latest
```

Then set `IMAGE = "<your-registry>/burla-bio-worker:latest"` at the top of `../main.py`.

## Use with Burla (1.5.6+)

```python
from burla import remote_parallel_map

import subprocess

IMAGE = "us-docker.pkg.dev/test-burla/burla-demos/burla-bio-worker:latest"

def align(sample_id, fq1, fq2):
    # bwa, samtools, aws are all on PATH
    ...

remote_parallel_map(align, samples, image=IMAGE, grow=True)
```
