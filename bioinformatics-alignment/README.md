# Run BWA-MEM Alignment on Thousands of FASTQ Files in Parallel

Align thousands of paired-end FASTQ samples to a reference genome using BWA-MEM and samtools, one sample per worker, across 2,500 machines at the same time.

## The Problem

You have 2,500 paired-end FASTQ samples on S3 and you need to align each to GRCh38, sort, mark duplicates, and upload the BAM. On one 32-core node, each sample takes 1-4 hours. Sequential = months.

Nextflow and Snakemake can run on AWS Batch, but they need a compute environment, a job queue, a container image with `bwa`, `samtools`, and `picard`, and IAM wiring. Running 2,500 EC2 instances yourself means an AMI, user-data scripts, a queue, and a retry layer.

You want every sample to start processing within seconds and come back as a BAM.

## The Solution (Burla)

Write one function that takes a sample ID, streams the FASTQ pair from S3, runs `bwa mem | samtools sort`, and uploads the BAM. Hand Burla a list of sample IDs with a custom container image that has `bwa`, `samtools`, and `awscli` pre-installed. It runs 2,500 workers at the same time.

No Nextflow, no Batch compute environment, no queue, no AMI.

## The Container Image

The worker needs `bwa`, `samtools`, and `awscli`. Burla's default worker image (`python:3.12`) doesn't have them, so we ship a tiny image on top of it - see [`worker-image/`](worker-image/) in this repo for the `Dockerfile` and build instructions.

A pre-built public image is available (no auth needed to pull):

```
us-docker.pkg.dev/test-burla/burla-demos/burla-bio-worker:latest
```

Pass it to Burla via the `image=` argument on `remote_parallel_map`.

## Example

```python
import boto3
from burla import remote_parallel_map

IMAGE = "us-docker.pkg.dev/test-burla/burla-demos/burla-bio-worker:latest"
S3_IN = "s3://my-fastq-bucket"
S3_OUT = "s3://my-bam-bucket"

with open("manifest.tsv") as f:
    samples = [line.strip().split("\t") for line in f if line.strip()]

sample_jobs = [
    {"sample_id": s[0], "fq1": s[1], "fq2": s[2]}
    for s in samples
]
print(f"aligning {len(sample_jobs)} samples")


def align_sample(job: dict) -> dict:
    import os
    import subprocess
    import time

    sid, fq1, fq2 = job["sample_id"], job["fq1"], job["fq2"]
    work = f"/tmp/{sid}"
    os.makedirs(work, exist_ok=True)

    def run(cmd: str):
        subprocess.run(cmd, shell=True, check=True, executable="/bin/bash")

    t0 = time.time()
    run(f"aws s3 cp s3://my-refs/GRCh38.fa     {work}/ref.fa")
    run(f"aws s3 cp s3://my-refs/GRCh38.fa.fai {work}/ref.fa.fai")
    run(f"aws s3 cp s3://my-refs/GRCh38.fa.bwt {work}/ref.fa.bwt")
    run(f"aws s3 cp s3://my-refs/GRCh38.fa.pac {work}/ref.fa.pac")
    run(f"aws s3 cp s3://my-refs/GRCh38.fa.ann {work}/ref.fa.ann")
    run(f"aws s3 cp s3://my-refs/GRCh38.fa.amb {work}/ref.fa.amb")
    run(f"aws s3 cp s3://my-refs/GRCh38.fa.sa  {work}/ref.fa.sa")
    run(f"aws s3 cp {fq1} {work}/R1.fastq.gz")
    run(f"aws s3 cp {fq2} {work}/R2.fastq.gz")

    run(
        f"bwa mem -t 4 -R '@RG\\tID:{sid}\\tSM:{sid}\\tLB:{sid}\\tPL:ILLUMINA' "
        f"{work}/ref.fa {work}/R1.fastq.gz {work}/R2.fastq.gz "
        f"| samtools sort -@ 4 -o {work}/{sid}.bam -"
    )
    run(f"samtools index {work}/{sid}.bam")
    run(f"aws s3 cp {work}/{sid}.bam     {S3_OUT}/bams/{sid}.bam")
    run(f"aws s3 cp {work}/{sid}.bam.bai {S3_OUT}/bams/{sid}.bam.bai")

    size = os.path.getsize(f"{work}/{sid}.bam")
    return {"sample_id": sid, "bam_bytes": size, "elapsed_s": round(time.time() - t0, 1)}


reports = remote_parallel_map(
    align_sample,
    sample_jobs,
    func_cpu=4,
    func_ram=16,
    image=IMAGE,
    grow=True,
)

import pandas as pd
pd.DataFrame(reports).to_csv("alignment_report.csv", index=False)
```

## Validated End-To-End

Smoke-tested on Burla with one public E. coli paired-end sample (ENA `ERR022075`, 1.9 GB + 2.0 GB FASTQs) against the E. coli K-12 MG1655 reference (NCBI `U00096.3`).

One worker aligned 45,447,728 reads in ~25 minutes with a 99.57% mapping rate. Scale this to 2,500 workers by handing Burla 2,500 sample dicts.

## Why This Is Better

**vs Nextflow on AWS Batch** - Nextflow + Batch requires a compute environment, a job queue, a container image, an S3 work dir, and the Nextflow DSL. Burla is a Python function, a list, and `image=`.

**vs Snakemake** - Snakemake is great for complex DAGs but overkill for "run the same command per sample." You don't need a rule graph here.

**vs Ray** - Ray wasn't built for shelling out to `bwa` on 2,500 independent machines. You still need the binaries on every worker.

**vs AWS Batch directly** - no job definition, no job queue, no compute env, no custom AMI. Burla workers run your container, and your function shells out.

## How It Works

You pass a list of sample descriptors and a container image. Burla boots workers running that image, runs `align_sample(job)` on each worker, and streams the results back. Each worker downloads the reference and the FASTQ pair, runs `bwa mem | samtools sort`, indexes the BAM, and uploads it. You get a small metadata record per sample.

## When To Use This

- Per-sample alignment (BWA, Minimap2, STAR, Bowtie2) for thousands of samples.
- Nanopore basecalling with Guppy/Dorado on a sample-per-worker basis.
- Variant calling per sample (DeepVariant, GATK HaplotypeCaller).
- QC and trimming pipelines (fastp, Trim Galore) over a cohort.

