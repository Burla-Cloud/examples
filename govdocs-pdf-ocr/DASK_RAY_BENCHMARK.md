# Dask and Ray GovDocs1 benchmark

Run date: August 19, 2026

## Workload and hardware

Both frameworks processed the same 231,231-document GovDocs1 manifest in the
BurlaTest AWS account. Each full run used:

- 32 `m7i.16xlarge` workers
- 2,048 worker vCPUs
- One `m7i.2xlarge` scheduler or head node
- The same PDF extraction and OCR function, corpus manifest, S3 bucket, base
  container, availability zone, and maximum of 4,000 in-flight tasks

Dask used version 2026.7.1. Ray used version 2.57.0.

## Results

| Measurement | Dask | Ray |
| --- | ---: | ---: |
| Engineering setup time to a working pilot | 1h 15m 33s | 48m 17s |
| Repeatable fresh pilot deployment | 1m 19s | 2m 43s |
| Full 32-worker cluster bootstrap | 17m 7s | 2m 58s |
| Workload wall time | 30m 20.25s | 24m 22.85s |
| Documents per second | 127.03 | 158.07 |
| Documents succeeded | 221,106 | 221,106 |
| Documents failed | 10,125 | 10,125 |
| Pages processed | 6,094,903 | 6,094,903 |
| Average CPU across all 33 nodes | 72.40% | 91.52% |
| Peak cluster-wide CPU | 99.67% | 99.68% |
| Average memory across all 33 nodes | 5.78% | 11.70% |
| Peak cluster-wide memory | 9.75% | 15.69% |

Ray completed the workload 19.6% sooner and delivered 24.4% more documents per
second. Its higher throughput corresponded to substantially higher sustained
CPU utilization.

The document counts, page counts, and failure types matched exactly. Aggregate
text output differed by 0.12%, consistent with nondeterministic OCR output.

Ray setup began after the Dask run. Its 48m 17s engineering setup measurement
starts when Ray-specific work began; Ray became operational 3h 54m 16s after
the original benchmark request.

## Utilization methodology

The CloudWatch agent sampled host CPU and memory every 10 seconds. Reported
averages are capacity-weighted across the scheduler or head and all 32 workers,
limited to the actual workload interval. Peaks are the highest capacity-weighted
cluster-wide 10-second samples, not the highest reading from one node.

Mean worker CPU ranged from 69.62% to 78.86% for Dask and 87.76% to 95.27% for
Ray. Mean worker memory ranged from 4.96% to 6.62% for Dask and 10.09% to
13.33% for Ray.

## Dashboards and persisted artifacts

Dask exposes its scheduler dashboard on port 8787. Ray exposes its dashboard
on port 8265. These dashboards are live cluster views and were not preserved
after the benchmark instances were terminated.

The full per-node time series remain available in CloudWatch and S3:

- Dask CloudWatch namespace:
  `GovDocsBenchmark/Dask/dask-govdocs1-32x-m7i16-v4-20260819`
- Dask summary:
  `s3://burla-govdocs1-corpus-002645521087/runs/dask-govdocs1-32x-m7i16-v4-20260819/summary.json`
- Dask per-node metrics:
  `s3://burla-govdocs1-corpus-002645521087/runs/dask-govdocs1-32x-m7i16-v4-20260819/node-metrics.json`
- Ray CloudWatch namespace:
  `GovDocsBenchmark/Ray/ray-full-20260819T0820Z`
- Ray summary:
  `s3://burla-govdocs1-corpus-002645521087/runs/ray-full-20260819T0820Z/summary.json`
- Ray per-node metrics:
  `s3://burla-govdocs1-corpus-002645521087/runs/ray-full-20260819T0820Z/node-metrics.json`
- Ray cluster bootstrap timing:
  `s3://burla-govdocs1-corpus-002645521087/runs/ray-full-20260819T0820Z/setup.json`

The Dask full-cluster bootstrap timing is embedded in its `summary.json`.
