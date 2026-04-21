import boto3
from burla import remote_parallel_map

S3_IN = "s3://my-fastq-bucket"
S3_OUT = "s3://my-bam-bucket"
REF = "s3://my-refs/GRCh38.fa"

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
    run(f"aws s3 cp s3://my-refs/GRCh38.fa {work}/ref.fa")
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


# 2,500 samples -> 2,500 workers each running bwa+samtools in parallel
reports = remote_parallel_map(align_sample, sample_jobs, func_cpu=4, func_ram=16, grow=True)

import pandas as pd
pd.DataFrame(reports).to_csv("alignment_report.csv", index=False)
