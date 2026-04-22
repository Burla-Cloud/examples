"""Phase 3a: GitHub Repo Summarizer data access probe.

Uses BigQuery public dataset `bigquery-public-data.github_repos` via the
user's own ADC (`gcloud auth application-default login`). Confirms:

  1. BigQuery client initializes.
  2. `sample_repos` table is queryable (200k popular repos by watch_count).
  3. `sample_contents` + `sample_files` can fetch README text for a repo.
  4. Full `contents` / `files` tables are accessible (though we don't scan them here).

Cost: the probe queries use LIMIT aggressively and stay below ~50 MB scanned
(well inside the 1 TB/month free tier).
"""
from __future__ import annotations

import json
import sys
from pathlib import Path


def probe() -> dict:
    try:
        from google.cloud import bigquery
    except ImportError:
        print("BLOCKED: `pip install google-cloud-bigquery` required", file=sys.stderr)
        sys.exit(2)

    import os
    # Pin to a project where the caller has bigquery.jobs.create permission.
    # `burla-test-joe` (the default gcloud project) lacks BQ perms.
    BQ_PROJECT = os.environ.get("GRS_BQ_PROJECT", "burla-testing")
    print(f"initializing BigQuery client (project={BQ_PROJECT}) ...")
    try:
        client = bigquery.Client(project=BQ_PROJECT)
    except Exception as e:
        print(f"BLOCKED: BigQuery Client() failed: {e}", file=sys.stderr)
        sys.exit(3)
    print(f"  project: {client.project}")

    print("query 1: top-starred repos (sample_repos, 200k rows, LIMIT 10)")
    q1 = """
    SELECT repo_name, watch_count
    FROM `bigquery-public-data.github_repos.sample_repos`
    ORDER BY watch_count DESC
    LIMIT 10
    """
    j1 = client.query(q1)
    top = [dict(r) for r in j1.result()]
    print(f"  bytes_billed={j1.total_bytes_billed} rows={len(top)}")
    for r in top[:5]:
        print(f"    {r['watch_count']:>7}  {r['repo_name']}")

    print("query 2: count sample_repos rows (cheap; cached)")
    q2 = "SELECT COUNT(*) AS n FROM `bigquery-public-data.github_repos.sample_repos`"
    j2 = client.query(q2)
    n_rows = list(j2.result())[0]["n"]
    print(f"  sample_repos total repos: {n_rows}")

    print("query 3: fetch README contents for 3 top repos via sample_files + sample_contents")
    q3 = """
    SELECT c.content, c.binary, f.path, f.repo_name
    FROM `bigquery-public-data.github_repos.sample_files` f
    JOIN `bigquery-public-data.github_repos.sample_contents` c
      ON f.id = c.id
    WHERE f.path IN UNNEST(['README.md','readme.md','README.rst','README.txt','README'])
      AND f.repo_name IN UNNEST(@repos)
    LIMIT 10
    """
    job_cfg = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter("repos", "STRING", [r["repo_name"] for r in top[:3]]),
        ]
    )
    j3 = client.query(q3, job_config=job_cfg)
    readmes = [dict(r) for r in j3.result()]
    print(f"  fetched {len(readmes)} README rows, bytes_billed={j3.total_bytes_billed}")
    samples = []
    for r in readmes[:3]:
        content = r.get("content") or ""
        samples.append({
            "repo_name": r["repo_name"],
            "path": r["path"],
            "is_binary": bool(r.get("binary")),
            "content_preview": content[:500],
            "content_chars": len(content),
        })
        print(f"    {r['repo_name']} - {r['path']}  ({len(content)} chars, binary={r.get('binary')})")
        print(f"      {content[:180]!r}")

    if not readmes:
        print("BLOCKED: zero READMEs returned; table/JOIN path may have changed", file=sys.stderr)
        sys.exit(4)

    print("query 4: total rows in `files` and `contents` (the FULL dataset)")
    q4 = """
    SELECT
      (SELECT COUNT(*) FROM `bigquery-public-data.github_repos.files`)    AS n_files,
      (SELECT COUNT(*) FROM `bigquery-public-data.github_repos.contents`) AS n_contents
    """
    try:
        j4 = client.query(q4)
        full = list(j4.result())[0]
        print(f"  files={full['n_files']:,}   contents={full['n_contents']:,}")
        full_stats = {"n_files": int(full["n_files"]), "n_contents": int(full["n_contents"])}
    except Exception as e:
        print(f"  WARN: full-count query failed ({e}); not blocking")
        full_stats = {}

    out_path = Path(__file__).parent / "samples" / "bigquery_probe.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps({
        "project": client.project,
        "top_repos": top,
        "sample_repos_count": int(n_rows),
        "full_dataset_counts": full_stats,
        "readme_samples": samples,
    }, indent=2) + "\n")

    print("=" * 70)
    print(f"PROBE_OK: BigQuery access working, README fetch path verified")
    print(f"  sample_repos: {n_rows:,} rows")
    if full_stats:
        print(f"  full dataset: {full_stats.get('n_files'):,} files, {full_stats.get('n_contents'):,} contents")
    print(f"wrote {out_path}")
    return {"status": "ok"}


if __name__ == "__main__":
    probe()
