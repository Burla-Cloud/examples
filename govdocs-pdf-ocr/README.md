# GovDocs1 PDF text extraction benchmark

This reproducible benchmark processes every PDF in the official GovDocs1 ZIP
corpus. It extracts embedded text directly, uses OCRmyPDF when a document has
image-only pages, and writes one plain-text object per successful PDF.

## Workload

`stage_corpus.py`:

1. Discovers the exact published versions of all 1,000 GovDocs1 archives.
2. Copies each archive into a private S3 bucket and verifies its published MD5
   and SHA-1 checksums.
3. Range-reads each ZIP central directory and creates a 231,231-document PDF
   manifest without inflating every archive.

`run_burla.py` gives each PDF to one Burla call. The worker range-fetches and
decompresses only that ZIP member, extracts existing text with `pdftotext`, runs
OCRmyPDF when needed, and uploads only the resulting text and structured run
metadata. Workers use presigned S3 requests and receive no AWS credentials.

The immutable worker image is:

```text
public.ecr.aws/e2g5l2y4/burla-pdf-ocr-benchmark@sha256:ccd4d8d959e922da7017cfd12e93a0d6ec47a738aea45301aaeeea691ab8d74a
```

Its source is the included `Dockerfile`.

## Requirements

- Python 3.12 and [uv](https://docs.astral.sh/uv/)
- an authenticated Burla cluster
- AWS credentials that can call STS and create and write an S3 bucket

```bash
cd govdocs-pdf-ocr
uv sync --locked
```

Both scripts use the default AWS credential chain. Add `--profile my-profile`
to each command when using a named profile. By default, data is stored in a
private `burla-govdocs1-corpus-<aws-account-id>` bucket; use `--bucket` to
choose another name.

## Run a pilot

Copy and index one archive:

```bash
uv run python stage_corpus.py \
  --limit-archives 1 \
  --run-id govdocs1-pilot
```

Process ten PDFs with at most eight concurrent calls:

```bash
uv run python run_burla.py \
  --corpus-run-id govdocs1-pilot \
  --run-id govdocs1-text-pilot \
  --limit 10 \
  --max-parallelism 8
```

## Run the full corpus

```bash
uv run python stage_corpus.py \
  --run-id govdocs1-v1

uv run python run_burla.py \
  --corpus-run-id govdocs1-v1 \
  --run-id govdocs1-text-v1
```

Without an explicit parallelism cap, Burla can launch up to its 2,560-vCPU grow
limit. Run the pilot first and confirm your cloud quota and budget.

Interrupted extraction runs save `results.partial.jsonl` and resume only missing
document IDs. Final records are `results.jsonl` and `summary.json`. Text objects,
manifests, and run metadata remain in the private S3 bucket.

## August 16, 2026 full run

The unfiltered corpus contained 1,000 archives, 231,231 PDFs, and 137.31 GB of
uncompressed PDF members. The run produced:

- 221,106 successful text outputs and 10,125 structured document failures
- 6,094,903 pages, including 196,424 OCR pages
- 13,781,512,383 text bytes
- complete one-to-one manifest coverage with no missing or extra document IDs

The main invocation, job `extract_text-Sun-nhN2TdW_`, returned 231,225 results
from 03:39:45 to 03:59:47 UTC before a pressure-retirement cleanup race failed
the job. Job `extract_text-ofKWlFo1Txa-` resumed the six missing documents and
completed 6/6 after the race fix. The original artifacts remain in the private
Burla test-account bucket `s3://burla-govdocs1-corpus-002645521087`.

The main invocation launched 45 EC2 instances totaling 2,572 vCPUs despite the
2,000-worker ceiling. During the three steady five-minute CloudWatch windows,
vCPU-weighted CPU utilization was 71.59%, 72.10%, and 67.75%. Only 19 to 22 of
45 nodes exceeded 90% CPU while 19 to 20 nodes remained below 50%. Network
traffic was 142.74 GiB inbound and 17.42 GiB outbound.

This run does not validate the intended all-but-one-node saturation goal or a
compute-efficiency advantage over Ray or Dask. Replacement and worker
re-add/retire oscillation must be fixed before running those baselines.
