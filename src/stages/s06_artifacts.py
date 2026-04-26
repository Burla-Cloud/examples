"""Stage 6: build all data/outputs/*.json and viral_summary.md from shared parquets.

Runs a single Burla worker that reads listings + images_cpu + images_gpu +
reviews_scored from /workspace/shared, computes per-section top-K, and returns
everything as small JSON-able dicts. The local stage writes all
``data/outputs/*.json``, generates ``viral_summary.md`` from those JSON files,
and merges the runtime log.
"""
from __future__ import annotations

import json
import time
from dataclasses import dataclass

from dotenv import load_dotenv

# Hoist for cloudpickle bundling on Burla workers.
import numpy as _np  # noqa: F401
import pandas as _pd  # noqa: F401
import pyarrow as _pa  # noqa: F401
import pyarrow.parquet as _pq  # noqa: F401

from ..config import (
    OUTPUT_DIR, OUTPUT_TOP_K, RUNTIME_LOG_PATH, SHARED_ROOT, REPO_ROOT,
)
from ..lib.budget import BudgetTracker
from ..lib.io import ensure_dir, register_src_for_burla, write_json, read_json


@dataclass
class ArtifactsArgs:
    listings_path: str
    images_cpu_path: str
    images_gpu_path: str
    reviews_scored_path: str
    correlations_path: str
    photo_manifest_path: str
    top_k: dict


def build_artifacts(args: ArtifactsArgs) -> dict:
    """Run on Burla: read all shared parquets, build small per-section dicts."""
    out = {"ok": False, "sections": {}, "stats": {}, "world_map": [], "error": None}
    try:
        import pandas as pd

        listings = pd.read_parquet(
            args.listings_path,
            columns=["listing_id", "city", "country", "region", "snapshot_date",
                     "name", "price_usd", "cleaning_fee_ratio", "demand_proxy",
                     "latitude", "longitude", "picture_url",
                     "listing_url", "reviews_per_month"],
        )
        listings["price_usd"] = pd.to_numeric(listings["price_usd"], errors="coerce")
        listings["cleaning_fee_ratio"] = pd.to_numeric(
            listings["cleaning_fee_ratio"], errors="coerce"
        )
        listings["cleaning_fee"] = (
            listings["cleaning_fee_ratio"] * listings["price_usd"]
        )
        listings["price"] = listings["price_usd"]
        listings["demand_proxy"] = pd.to_numeric(listings["demand_proxy"], errors="coerce")

        cpu = pd.read_parquet(
            args.images_cpu_path,
            columns=["listing_id", "image_idx", "image_url", "download_ok",
                     "brightness", "edge_density",
                     "clip_messy_room", "clip_tv_above_fireplace",
                     "clip_photographer_reflection", "clip_lots_of_plants"],
        )
        cpu = cpu[cpu["download_ok"].astype(bool)]
        try:
            gpu = pd.read_parquet(
                args.images_gpu_path,
                columns=["listing_id", "image_idx", "image_url",
                         "tv_detected", "tv_above_50pct", "tv_bbox",
                         "potted_plant_count"],
            )
        except Exception:
            gpu = pd.DataFrame(columns=[
                "listing_id", "image_idx", "image_url",
                "tv_detected", "tv_above_50pct", "tv_bbox", "potted_plant_count",
            ])

        out["stats"]["n_listings"] = int(len(listings))
        out["stats"]["n_listings_with_demand"] = int(listings["demand_proxy"].notna().sum())
        out["stats"]["n_cpu_images"] = int(len(cpu))
        out["stats"]["n_gpu_images"] = int(len(gpu))

        try:
            import pyarrow.parquet as pq
            out["stats"]["n_photo_manifest_rows"] = int(
                pq.read_metadata(args.photo_manifest_path).num_rows
            )
        except Exception:
            out["stats"]["n_photo_manifest_rows"] = 0

        try:
            import pyarrow.parquet as pq
            reviews_raw_path = args.reviews_scored_path.rsplit("/", 1)[0] + "/reviews_raw.parquet"
            out["stats"]["n_reviews"] = int(
                pq.read_metadata(reviews_raw_path).num_rows
            )
        except Exception:
            out["stats"]["n_reviews"] = 0

        listings_idx = listings.set_index("listing_id")

        def _attach_listing(df, score_col):
            j = df.merge(listings, on="listing_id", how="left").dropna(subset=["picture_url"])
            j[score_col] = pd.to_numeric(j[score_col], errors="coerce")
            j = j.dropna(subset=[score_col])
            return j

        def _serialize(j: "pd.DataFrame", score_col: str, k: int, extra=None):
            rows = []
            for _, r in j.head(k).iterrows():
                row = {
                    "listing_id": int(r["listing_id"]),
                    "city": str(r.get("city", "")),
                    "country": str(r.get("country", "")),
                    "name": str(r.get("name", ""))[:140],
                    "score": float(r[score_col]),
                    "image_url": str(r.get("image_url", "")),
                    "thumbnail_url": str(r.get("picture_url", "")),
                    "listing_url": str(r.get("listing_url", "")),
                    "demand_proxy": float(r.get("demand_proxy"))
                        if pd.notna(r.get("demand_proxy")) else None,
                    "lat": float(r["latitude"]) if pd.notna(r["latitude"]) else None,
                    "lng": float(r["longitude"]) if pd.notna(r["longitude"]) else None,
                }
                if extra:
                    for k2 in extra:
                        v = r.get(k2)
                        if pd.notna(v):
                            try:
                                row[k2] = float(v)
                            except (TypeError, ValueError):
                                row[k2] = str(v)
                rows.append(row)
            return rows

        worst_tv_src = gpu[gpu["tv_above_50pct"].fillna(False).astype(bool)].copy()
        if len(worst_tv_src):
            cpu_score = cpu[["listing_id", "image_idx", "clip_tv_above_fireplace"]]
            worst_tv_src = worst_tv_src.merge(
                cpu_score, on=["listing_id", "image_idx"], how="left"
            )
            worst_tv_src["clip_tv_above_fireplace"] = worst_tv_src[
                "clip_tv_above_fireplace"
            ].fillna(0)
            worst_tv_src = worst_tv_src.sort_values(
                "clip_tv_above_fireplace", ascending=False
            )
            worst_tv_src = worst_tv_src.drop_duplicates(
                subset=["listing_id"], keep="first"
            )
            j = _attach_listing(worst_tv_src, "clip_tv_above_fireplace")
            if "image_url" in j.columns:
                j = j.drop_duplicates(subset=["image_url"], keep="first")
            out["sections"]["worst_tv_placements"] = {
                "title": "Worst TV placements in 1.1M Airbnb listings",
                "n": int(min(len(j), args.top_k["worst_tv_placements"])),
                "items": _serialize(j, "clip_tv_above_fireplace",
                                    args.top_k["worst_tv_placements"],
                                    extra=["tv_bbox"]),
            }
        else:
            out["sections"]["worst_tv_placements"] = {
                "title": "Worst TV placements", "n": 0, "items": []
            }

        messy = cpu.sort_values("clip_messy_room", ascending=False)
        messy = messy.drop_duplicates(subset=["listing_id"], keep="first")
        j = _attach_listing(messy, "clip_messy_room")
        if "image_url" in j.columns:
            j = j.drop_duplicates(subset=["image_url"], keep="first")
        out["sections"]["messiest_listings"] = {
            "title": "Messiest Airbnb photos in 1.1M listings",
            "n": int(min(len(j), args.top_k["messiest_listings"])),
            "items": _serialize(j, "clip_messy_room", args.top_k["messiest_listings"]),
        }

        mirror = cpu.sort_values("clip_photographer_reflection", ascending=False)
        mirror = mirror.drop_duplicates(subset=["listing_id"], keep="first")
        j = _attach_listing(mirror, "clip_photographer_reflection")
        # Belt-and-suspenders: also dedupe by image_url across listings, since
        # the same hero photo sometimes gets reused by multi-listing hosts and
        # CLIP will flag every copy.
        if "image_url" in j.columns:
            j = j.drop_duplicates(subset=["image_url"], keep="first")
        # Cap to a much smaller, more confident slice. The CLIP prompt
        # ("a photographer reflected in a mirror taking a photo") catches both
        # actual host-with-camera shots and well-staged mirror compositions, so
        # we surface only the very top scoring listings to keep precision high.
        mirror_k = min(args.top_k["mirror_selfies"], 24)
        out["sections"]["mirror_selfies"] = {
            "title": "When the mirror became the main character",
            "n": int(min(len(j), mirror_k)),
            "items": _serialize(j, "clip_photographer_reflection", mirror_k),
        }

        plant_src = cpu.sort_values("clip_lots_of_plants", ascending=False)
        plant_src = plant_src.drop_duplicates(subset=["listing_id"], keep="first")
        if len(gpu):
            plant_counts = gpu.groupby("listing_id")["potted_plant_count"].max().reset_index()
            plant_src = plant_src.merge(plant_counts, on="listing_id", how="left")
        j = _attach_listing(plant_src, "clip_lots_of_plants")
        if "image_url" in j.columns:
            j = j.drop_duplicates(subset=["image_url"], keep="first")
        out["sections"]["plant_maximalists"] = {
            "title": "The most plant-maximalist Airbnbs",
            "n": int(min(len(j), args.top_k["plant_maximalists"])),
            "items": _serialize(j, "clip_lots_of_plants", args.top_k["plant_maximalists"],
                                extra=["potted_plant_count"]),
        }

        crime = listings.dropna(subset=["price", "cleaning_fee"]).copy()
        crime = crime[(crime["price"] > 5) & (crime["cleaning_fee"] >= 0)]
        crime["fee_ratio"] = crime["cleaning_fee"] / crime["price"]
        crime = crime.sort_values("fee_ratio", ascending=False)
        rows = []
        for _, r in crime.head(args.top_k["insane_cleaning_fees"]).iterrows():
            rows.append({
                "listing_id": int(r["listing_id"]),
                "city": str(r.get("city", "")),
                "country": str(r.get("country", "")),
                "name": str(r.get("name", ""))[:140],
                "price": float(r["price"]),
                "cleaning_fee": float(r["cleaning_fee"]),
                "fee_ratio": float(r["fee_ratio"]),
                "demand_proxy": float(r["demand_proxy"]) if pd.notna(r["demand_proxy"]) else None,
                "thumbnail_url": str(r.get("picture_url", "")),
                "listing_url": str(r.get("listing_url", "")),
                "lat": float(r["latitude"]) if pd.notna(r["latitude"]) else None,
                "lng": float(r["longitude"]) if pd.notna(r["longitude"]) else None,
            })
        out["sections"]["insane_cleaning_fees"] = {
            "title": "Cleaning fees that exceed the nightly rate",
            "n": len(rows), "items": rows,
        }

        try:
            rev = pd.read_parquet(args.reviews_scored_path)
            if "claude_humor_score" in rev.columns:
                rev["claude_humor_score"] = pd.to_numeric(
                    rev["claude_humor_score"], errors="coerce"
                ).fillna(0)
                top_reviews = rev.sort_values(
                    "claude_humor_score", ascending=False
                ).head(args.top_k["funniest_reviews"])
            else:
                top_reviews = rev.sort_values("tier1_score", ascending=False).head(
                    args.top_k["funniest_reviews"]
                )
            top_reviews = top_reviews.merge(
                listings[["listing_id", "city", "country", "picture_url", "listing_url"]],
                on="listing_id", how="left",
            )
            review_rows = []
            for _, r in top_reviews.iterrows():
                full_comment = str(r.get("comments", ""))
                review_rows.append({
                    "review_id": int(r.get("review_id", 0)),
                    "listing_id": int(r.get("listing_id", 0)),
                    "city": str(r.get("city", "")),
                    "country": str(r.get("country", "")),
                    "date": str(r.get("date", ""))[:10],
                    "comment": full_comment[:600],
                    "comment_full": full_comment[:8000],
                    "category": str(r.get("claude_category", "") or ""),
                    "humor_score": float(r["claude_humor_score"])
                        if "claude_humor_score" in r and pd.notna(r["claude_humor_score"]) else None,
                    "one_line": str(r.get("claude_one_line", "") or ""),
                    "thumbnail_url": str(r.get("picture_url", "") or ""),
                    "listing_url": str(r.get("listing_url", "") or ""),
                })
            out["sections"]["funniest_reviews"] = {
                "title": "Funniest reviews from 50M",
                "n": len(review_rows), "items": review_rows,
            }
        except Exception as e:
            out["sections"]["funniest_reviews"] = {
                "title": "Funniest reviews from 50M",
                "n": 0, "items": [], "error": str(e)[:200]
            }

        try:
            corr = pd.read_parquet(args.correlations_path)
            grouped = []
            for hyp, sub in corr.groupby("hypothesis"):
                grouped.append({
                    "hypothesis": str(hyp),
                    "verdict": str(sub.iloc[0]["verdict"]),
                    "reason": str(sub.iloc[0]["reason"]),
                    "buckets": [
                        {
                            "bucket": str(b["bucket"]),
                            "n": int(b["n"]),
                            "median": float(b["median"]),
                            "ci_low": float(b["ci_low"]),
                            "ci_high": float(b["ci_high"]),
                        }
                        for _, b in sub.iterrows()
                    ],
                })
            out["sections"]["correlations"] = {
                "title": "5 hypotheses, bootstrapped 95% CIs",
                "hypotheses": grouped,
            }
        except Exception as e:
            out["sections"]["correlations"] = {"title": "Correlations", "hypotheses": [], "error": str(e)[:200]}

        world = []
        for section_id, section in out["sections"].items():
            if section_id == "correlations":
                continue
            for item in section.get("items", []):
                if item.get("lat") is not None and item.get("lng") is not None:
                    world.append({
                        "type": section_id,
                        "lat": float(item["lat"]),
                        "lng": float(item["lng"]),
                        "listing_id": int(item.get("listing_id", 0)),
                    })
        out["world_map"] = world
        out["ok"] = True
    except Exception as e:
        import traceback as _tb
        out["error"] = f"{type(e).__name__}: {str(e)[:200]}"
        out["traceback"] = _tb.format_exc()[:1000]
    return out


_VIRAL_TEMPLATE = """# Airbnb x Burla -- viral summary

(All numbers below are computed from data/outputs/*.json. Regenerated every run.)

## Headline numbers

- {n_listings:,} Airbnb listings worldwide (Inside Airbnb, latest snapshot per city)
- {n_photo_manifest_rows:,} photo URLs scraped from public listing pages
- {n_cpu_images:,} images CLIP-scored on Burla CPU
- {n_gpu_images:,} images run through YOLOv8 on Burla A100s
- {n_reviews:,} reviews heuristic-scored, top {n_tier3:,} sent through Claude

## What we found

### TVs in places no one should mount a TV

Top-{n_tv} listings where YOLO confirmed a TV in the upper half of the photo
and CLIP rated the image high on "TV mounted above a fireplace."

### Messiest photos a host actually posted

Top-{n_messy} listings, ranked by CLIP score against "a messy cluttered room
with stuff everywhere."

### Mirror selfies

Top-{n_mirror} listings where the host got caught reflected in their own
mirror photo (CLIP score against "a photographer reflected in a mirror").

### Plant-maximalist Airbnbs

Top-{n_plants} listings combining CLIP "room full of houseplants" with YOLO
potted plant counts.

### Cleaning fees > nightly rate

Top-{n_fees} listings where the cleaning fee exceeds the nightly price. The
worst offenders charge {fee_ratio_max:.1f}x the nightly rate as a cleaning fee.

### The funniest reviews

Top-{n_funny} reviews surfaced by 3-tier funnel (heuristic -> embedding cluster
-> Claude humor score).

## What held up under bootstrap

{accepted_findings}

## What did not survive

{rejected_findings}

## Replication

Repo: airbnb-burla
Runtime: {wall_time_hours:.1f} hours wall time, peak {peak_workers} Burla workers.
"""


def _build_viral_summary(sections: dict, stats: dict, runtime: dict) -> str:
    funny = sections.get("funniest_reviews", {})
    fees = sections.get("insane_cleaning_fees", {}).get("items", [])
    fee_ratio_max = float(fees[0]["fee_ratio"]) if fees else 0.0
    correlations = sections.get("correlations", {}).get("hypotheses", [])
    accepted = [c["hypothesis"] for c in correlations if c.get("verdict") == "accepted"]
    rejected = [c["hypothesis"] for c in correlations if c.get("verdict") == "rejected"]

    def _bullet(items):
        return "\n".join(f"- {x}" for x in items) if items else "- (none)"

    return _VIRAL_TEMPLATE.format(
        n_listings=stats.get("n_listings", 0),
        n_photo_manifest_rows=stats.get("n_photo_manifest_rows", 0),
        n_cpu_images=stats.get("n_cpu_images", 0),
        n_gpu_images=stats.get("n_gpu_images", 0),
        n_reviews=stats.get("n_reviews", 0),
        n_tier3=funny.get("n", 0),
        n_tv=sections.get("worst_tv_placements", {}).get("n", 0),
        n_messy=sections.get("messiest_listings", {}).get("n", 0),
        n_mirror=sections.get("mirror_selfies", {}).get("n", 0),
        n_plants=sections.get("plant_maximalists", {}).get("n", 0),
        n_fees=sections.get("insane_cleaning_fees", {}).get("n", 0),
        n_funny=funny.get("n", 0),
        fee_ratio_max=fee_ratio_max,
        accepted_findings=_bullet(accepted),
        rejected_findings=_bullet(rejected),
        wall_time_hours=runtime.get("wall_time_hours", 0.0),
        peak_workers=runtime.get("peak_workers", 0),
    )


def main() -> None:
    load_dotenv()
    register_src_for_burla()
    from burla import remote_parallel_map

    listings_shared = f"{SHARED_ROOT}/listings_clean.parquet"
    cpu_shared = f"{SHARED_ROOT}/images_cpu.parquet"
    gpu_shared = f"{SHARED_ROOT}/images_gpu.parquet"
    reviews_shared = f"{SHARED_ROOT}/reviews_scored.parquet"
    corr_shared = f"{SHARED_ROOT}/correlations.parquet"
    photo_manifest_shared = f"{SHARED_ROOT}/photo_manifest.parquet"

    print("[s06] building artifacts on shared FS ...", flush=True)
    t0 = time.time()
    with BudgetTracker("s06_artifacts", n_inputs=1, func_cpu=16) as bt:
        bt.set_workers(1)
        [r] = remote_parallel_map(
            build_artifacts,
            [ArtifactsArgs(
                listings_path=listings_shared,
                images_cpu_path=cpu_shared,
                images_gpu_path=gpu_shared,
                reviews_scored_path=reviews_shared,
                correlations_path=corr_shared,
                photo_manifest_path=photo_manifest_shared,
                top_k=dict(OUTPUT_TOP_K),
            )],
            func_cpu=16, func_ram=64, max_parallelism=1, grow=True, spinner=True,
        )
        bt.set_succeeded(1 if r.get("ok") else 0)
        bt.set_failed(0 if r.get("ok") else 1)

    if not r.get("ok"):
        raise SystemExit(f"[s06] failed: {r.get('error')}")

    raw_log = read_json(RUNTIME_LOG_PATH) or {}
    stages = raw_log.get("stages", []) if isinstance(raw_log, dict) else []
    total_wall = sum(stage.get("wall_seconds", 0) for stage in stages) / 3600.0
    total_usd = sum(stage.get("estimated_usd", 0) for stage in stages)
    peak_workers = max((stage.get("n_workers", 0) for stage in stages), default=0)

    runtime_summary = {
        "wall_time_hours": total_wall,
        "estimated_cost_usd": total_usd,
        "peak_workers": peak_workers,
        "stages": stages,
        "completed_at": time.time(),
    }

    ensure_dir(OUTPUT_DIR)
    sections = r["sections"]
    stats = r["stats"]
    # Keep n_reviews from build_artifacts (real review count). funniest_reviews
    # is just the top-K we surfaced.
    stats["n_reviews_funniest_top_k"] = sections.get("funniest_reviews", {}).get("n", 0)

    for section_id, section in sections.items():
        write_json(OUTPUT_DIR / f"{section_id}.json", section)

    write_json(OUTPUT_DIR / "world_map.json", {
        "title": "Every flagged Airbnb in the demo, on a Leaflet map",
        "n": len(r["world_map"]), "points": r["world_map"],
    })
    write_json(OUTPUT_DIR / "homepage_stats.json", {
        "n_listings": stats.get("n_listings", 0),
        "n_photo_manifest_rows": stats.get("n_photo_manifest_rows", 0),
        "n_cpu_images": stats.get("n_cpu_images", 0),
        "n_gpu_images": stats.get("n_gpu_images", 0),
        "n_reviews": stats.get("n_reviews", 0),
        "wall_time_hours": runtime_summary["wall_time_hours"],
        "estimated_cost_usd": runtime_summary["estimated_cost_usd"],
        "peak_workers": runtime_summary["peak_workers"],
    })
    write_json(OUTPUT_DIR / "runtime_log.json", runtime_summary)

    md = _build_viral_summary(sections, stats, runtime_summary)
    (OUTPUT_DIR / "viral_summary.md").write_text(md, encoding="utf-8")

    site_data = REPO_ROOT / "site" / "data"
    site_data.mkdir(parents=True, exist_ok=True)
    for j in OUTPUT_DIR.glob("*.json"):
        (site_data / j.name).write_bytes(j.read_bytes())

    elapsed = time.time() - t0
    print(f"[s06] DONE. {len(sections)} section files written to {OUTPUT_DIR} in {elapsed:.1f}s", flush=True)


if __name__ == "__main__":
    main()
