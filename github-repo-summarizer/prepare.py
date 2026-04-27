"""GRS data prep — export 1M+ GitHub READMEs from BigQuery to a local parquet
that Burla workers can read from the shared filesystem.

Strategy:
  The `sample_files`/`sample_contents` path only yields ~16k matched READMEs
  (the sample_contents table is only 2.9M rows and is uncorrelated with
  README files in sample_files). To hit the "1M+ repos" bar, we must scan
  the full `github_repos.contents` table. Cost: ~2.96 TB on-demand ≈ $15
  beyond the 1 TB free tier. Accepted for the demo.

Pipeline:
  1. JOIN full `files` + `contents` on `id` where path is a README.
  2. Pick the largest README per repo (many repos have multiple, e.g.
     README.md and readme.md in forks).
  3. Attach primary language via `languages` table (LEFT JOIN — null OK).
  4. Export as parquet to samples/readmes.parquet.

Output row schema:
  repo_name STRING
  path      STRING
  lang      STRING | NULL
  size      INT
  content   STRING  (README text, utf-8)
"""
from __future__ import annotations

import argparse
import os
import time
from pathlib import Path


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--project", default=os.getenv("GRS_BQ_PROJECT", "burla-testing"))
    ap.add_argument("--limit", type=int, default=1_200_000)
    ap.add_argument("--out", default="samples/readmes.parquet")
    ap.add_argument("--cost-cap-tb", type=float, default=4.0,
                    help="Abort if dry-run scan exceeds this many TB")
    ap.add_argument("--force", action="store_true",
                    help="Skip confirmation prompt and execute")
    args = ap.parse_args()

    try:
        from google.cloud import bigquery
    except ImportError:
        raise SystemExit("pip install google-cloud-bigquery[bqstorage,pandas] pyarrow db-dtypes")

    client = bigquery.Client(project=args.project)
    print(f"BigQuery client ready (project={client.project})")

    t0 = time.time()

    # Full-table JOIN: files (2.3B rows) + contents (281M rows). We accept the
    # ~3 TB scan cost to get well over 1M real READMEs.
    #
    # Memory-safe pattern: first pick ONE readme id per repo (no content column
    # in the aggregation — that's the OOM trap), then JOIN to contents. No final
    # ORDER BY on content-carrying rows.
    q = f"""
    WITH readme_ids AS (
      SELECT
        f.repo_name AS repo_name,
        f.path AS path,
        f.id AS id,
        ROW_NUMBER() OVER (
          PARTITION BY f.repo_name
          ORDER BY
            CASE LOWER(f.path)
              WHEN 'readme.md' THEN 0
              WHEN 'readme.markdown' THEN 1
              WHEN 'readme' THEN 2
              WHEN 'readme.rst' THEN 3
              WHEN 'readme.txt' THEN 4
              ELSE 5
            END
        ) AS rn
      FROM `bigquery-public-data.github_repos.files` AS f
      WHERE LOWER(f.path) IN ('readme.md', 'readme.rst', 'readme.txt', 'readme', 'readme.markdown')
    ),
    langs AS (
      SELECT
        repo_name,
        ARRAY_AGG(name ORDER BY bytes DESC LIMIT 1)[OFFSET(0)] AS lang
      FROM `bigquery-public-data.github_repos.languages`,
      UNNEST(language)
      GROUP BY repo_name
    )
    SELECT
      r.repo_name,
      l.lang,
      r.path,
      LENGTH(c.content) AS size,
      c.content AS content
    FROM readme_ids r
    JOIN `bigquery-public-data.github_repos.contents` c
      ON r.id = c.id
    LEFT JOIN langs l ON r.repo_name = l.repo_name
    WHERE r.rn = 1
      AND c.binary = FALSE
      AND c.content IS NOT NULL
      AND LENGTH(c.content) BETWEEN 120 AND 32000
    LIMIT {args.limit}
    """

    print(f"submitting query (limit={args.limit:,})...")
    print("  dry-run to estimate cost...")
    dry_cfg = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
    job_dry = client.query(q, job_config=dry_cfg)
    tb_scanned = job_dry.total_bytes_processed / 1e12
    est_cost = max(0, (tb_scanned - 0.0)) * 5.0  # $5 / TB on-demand
    print(f"  estimated scan: {tb_scanned:.2f} TB  (≈ ${est_cost:.2f} on-demand)")
    if tb_scanned > args.cost_cap_tb:
        raise SystemExit(f"too expensive ({tb_scanned:.1f} TB > cap {args.cost_cap_tb}); aborting")
    if not args.force:
        print(f"  (pass --force to skip this 3-s pause)")
        time.sleep(3)

    print("  executing query (this will take 2-5 minutes)...")
    job = client.query(q)
    rows_iter = job.result(page_size=2000)

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)

    # Stream via arrow batches → parquet, to avoid loading all rows into
    # a single DataFrame (content column can blow past RAM at 1M rows).
    import pyarrow as pa
    import pyarrow.parquet as pq

    try:
        from google.cloud.bigquery_storage_v1 import BigQueryReadClient  # noqa: F401
        bqs = client._create_bqstorage_client() if hasattr(client, "_create_bqstorage_client") else None
    except Exception:
        bqs = None
    try:
        from google.cloud import bigquery_storage
        bqs = bqs or bigquery_storage.BigQueryReadClient()
    except Exception:
        pass

    batches = rows_iter.to_arrow_iterable(bqstorage_client=bqs)

    writer = None
    total_rows = 0
    total_size = 0
    lang_counts: dict = {}
    try:
        for batch in batches:
            if writer is None:
                writer = pq.ParquetWriter(out, batch.schema, compression="zstd")
            writer.write_table(pa.Table.from_batches([batch]))
            n = batch.num_rows
            total_rows += n
            try:
                sizes = batch.column("size").to_pylist()
                total_size += sum(int(s or 0) for s in sizes)
            except Exception:
                pass
            try:
                langs = batch.column("lang").to_pylist()
                for l in langs:
                    lang_counts[l or "_unknown"] = lang_counts.get(l or "_unknown", 0) + 1
            except Exception:
                pass
            if total_rows % 50000 < 2000:
                print(f"  wrote {total_rows:,} rows so far ({time.time() - t0:.0f}s)")
    finally:
        if writer is not None:
            writer.close()

    elapsed = time.time() - t0
    size_mb = out.stat().st_size / 1e6
    print(f"wrote {out} ({size_mb:.1f} MB, {total_rows:,} rows)")
    print(f"  bytes_billed={(job.total_bytes_billed or 0)/1e9:.2f} GB")
    print(f"  elapsed: {elapsed:.1f}s")
    if total_rows:
        avg_chars = total_size / total_rows
        print(f"  avg README size: {avg_chars:.0f} chars")
    if lang_counts:
        print("  langs (top 10):")
        for l, n in sorted(lang_counts.items(), key=lambda kv: -kv[1])[:10]:
            print(f"    {l:<24} {n:>7,}")


if __name__ == "__main__":
    main()
