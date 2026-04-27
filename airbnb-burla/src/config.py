"""All magic numbers, paths, prompts, and policy live here.

Stages and tasks import from this module so we never reach into another file
to discover what budget we're holding to or which CLIP prompt drives Stage 3.
"""
from __future__ import annotations

import os
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = REPO_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
INTERIM_DIR = DATA_DIR / "interim"
OUTPUT_DIR = DATA_DIR / "outputs"

INSIDE_AIRBNB_INDEX_URL = "https://insideairbnb.com/get-the-data/"

VALIDATION_REPORT_PATH = OUTPUT_DIR / "validation_report.json"
LISTINGS_CLEAN_PATH = INTERIM_DIR / "listings_clean.parquet"
PHOTO_MANIFEST_PATH = INTERIM_DIR / "photo_manifest.parquet"
IMAGES_CPU_PATH = INTERIM_DIR / "images_cpu.parquet"
IMAGES_GPU_PATH = INTERIM_DIR / "images_gpu.parquet"
REVIEWS_SCORED_PATH = INTERIM_DIR / "reviews_scored.parquet"
CORRELATIONS_PATH = INTERIM_DIR / "correlations.parquet"
RUNTIME_LOG_PATH = OUTPUT_DIR / "runtime_log.json"

SHARED_ROOT = "/workspace/shared/airbnb"
SHARED_LISTINGS = f"{SHARED_ROOT}/listings"
SHARED_PHOTOS = f"{SHARED_ROOT}/photos"
SHARED_REVIEWS = f"{SHARED_ROOT}/reviews"
SHARED_IMAGES_CPU = f"{SHARED_ROOT}/images_cpu"
SHARED_IMAGES_GPU = f"{SHARED_ROOT}/images_gpu"
SHARED_REVIEWS_TIER1 = f"{SHARED_ROOT}/reviews_tier1_v2"
SHARED_REVIEWS_TIER2 = f"{SHARED_ROOT}/reviews_tier2_v2"
SHARED_REVIEWS_TIER3 = f"{SHARED_ROOT}/reviews_tier3"

MIN_LISTINGS_PER_CITY = 5_000
MAX_SAMPLE_IMAGE_FAIL_RATIO = 0.20
SAMPLE_IMAGES_PER_CITY = 5

LISTINGS_BATCH_SIZE = 1000
SCRAPE_REQ_PER_SEC_PER_WORKER = 0.5
SCRAPE_MAX_PARALLELISM = 1000
SCRAPE_MIN_SUCCESS_RATE = 0.50
SCRAPE_RETRY_LIMIT = 2

CPU_IMAGE_BATCH_SIZE = 700
CPU_IMAGE_MAX_PARALLELISM = 500

GPU_BATCH_SIZE = 32
GPU_MAX_PARALLELISM = 12  # GCP A100 quota in us-central1 is 16, leave headroom

CLIP_MODEL = "ViT-B-32"
CLIP_PRETRAINED = "openai"
CLIP_PROMPTS = {
    "messy_room": "a messy cluttered room with stuff everywhere",
    "tv_above_fireplace": "a television mounted above a fireplace",
    "photographer_reflection": "a photographer reflected in a mirror taking a photo",
    "lots_of_plants": "a room full of houseplants",
    "bright_natural_light": "a bright sunny room with large windows and natural light",
    "bathroom": "a bathroom with toilet shower and sink",
    "kitchen": "a kitchen with stove counters and appliances",
    "bedroom": "a bedroom with a bed and pillows",
    "living_room": "a living room with a couch and tv",
    "minimalist": "a minimalist clean empty room",
}

TOP_N_PER_AXIS = {
    "clip_tv_above_fireplace": 20_000,
    "clip_messy_room": 15_000,
    "clip_photographer_reflection": 10_000,
    "clip_lots_of_plants": 5_000,
}

YOLO_MODEL = "yolov8x.pt"
YOLO_DEVICE = "cuda"
YOLO_TARGET_CLASSES = {
    "tv": 62,
    "person": 0,
    "potted plant": 58,
    "couch": 57,
    "bed": 59,
    "toilet": 61,
}

REVIEW_TIER1_BATCH_SIZE = 5000
REVIEW_TIER1_MAX_PARALLELISM = 1000
REVIEW_TIER2_TOP_K = 200_000
REVIEW_TIER2_NUM_CLUSTERS = 30
REVIEW_TIER3_TOP_K = 10_000
REVIEW_TIER3_BATCH_SIZE = 50
REVIEW_TIER3_MAX_PARALLELISM = 200

ANTHROPIC_MODEL = "claude-haiku-4-5-20251001"
ANTHROPIC_MAX_TOKENS = 200

REVIEW_HEURISTIC_KEYWORDS = [
    "cockroach", "roach", "bedbug", "rat", "mouse", "mice",
    "ghost", "haunted", "scary", "terrifying",
    "police", "cops", "arrest", "fight", "blood",
    "toilet", "plumbing", "leak", "flood", "smell",
    "fire", "smoke alarm", "alarm",
    "neighbor", "loud", "argue",
    "dog", "cat", "snake", "spider",
    "weed", "drugs", "needles",
    "host showed up", "knocked on the door", "midnight",
    "however", " but ", "BUT ",
]

REVIEW_HUMOR_CATEGORIES = [
    "This escalated quickly",
    "Five stars but terrifying",
    "Passive aggressive poetry",
    "Host said what now",
    "Animal incident",
    "Plumbing lore",
    "Noise complaint hall of fame",
    "Not funny",
]

HYPOTHESES = [
    ("brightness_quartile", "demand_proxy"),
    ("plant_count_bucket", "demand_proxy"),
    ("messiness_quartile", "demand_proxy"),
    ("cleaning_fee_ratio_bucket", "demand_proxy"),
    ("tv_too_high", "demand_proxy"),
]
BOOTSTRAP_RESAMPLES = 1000
MIN_BUCKET_N = 100

OUTPUT_TOP_K = {
    "worst_tv_placements": 50,
    "messiest_listings": 50,
    "mirror_selfies": 50,
    "plant_maximalists": 30,
    "insane_cleaning_fees": 100,
    "funniest_reviews": 100,
}

PIPELINE_HARD_CAP_USD = float(os.environ.get("PIPELINE_HARD_CAP_USD", 500.0))
PIPELINE_HARD_CAP_HOURS = float(os.environ.get("PIPELINE_HARD_CAP_HOURS", 24.0))

STAGE_BUDGETS = {
    "s00_validate": {"hours": 0.1, "usd": 2.0},
    "s01_listings": {"hours": 0.1, "usd": 2.0},
    "s02a_scrape": {"hours": 6.0, "usd": 250.0},
    "s02b_images_cpu": {"hours": 2.0, "usd": 100.0},
    "s03_images_gpu": {"hours": 3.0, "usd": 60.0},
    "s04_reviews": {"hours": 4.0, "usd": 80.0},
    "s05_correlate": {"hours": 0.5, "usd": 5.0},
    "s06_artifacts": {"hours": 0.5, "usd": 5.0},
}
