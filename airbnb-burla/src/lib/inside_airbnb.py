"""Discover and download Inside Airbnb city dumps.

The index page at https://insideairbnb.com/get-the-data/ links every city's
listings.csv.gz, reviews.csv.gz, and calendar.csv.gz. Snapshot dates change
quarterly, so we scrape the index every run instead of hard-coding URLs.
"""
from __future__ import annotations

import gzip
import re
from dataclasses import dataclass
from typing import Iterable, List, Optional

import requests

from ..config import INSIDE_AIRBNB_INDEX_URL


_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36"
)
_LISTINGS_FILE = "listings.csv.gz"
_REVIEWS_FILE = "reviews.csv.gz"

_DATA_URL_RE = re.compile(
    r"https?://data\.insideairbnb\.com/"
    r"([^/\s\"']+)/"        # country
    r"([^/\s\"']+)/"        # region
    r"([^/\s\"']+)/"        # city slug
    r"(\d{4}-\d{2}-\d{2})/" # snapshot
    r"data/(listings|reviews)\.csv\.gz",
    re.IGNORECASE,
)


@dataclass
class City:
    """One row in Inside Airbnb's data table."""
    city: str
    country: str
    region: str
    snapshot_date: str
    listings_url: str
    reviews_url: str

    def slug(self) -> str:
        s = re.sub(r"[^A-Za-z0-9]+", "-", self.city.lower()).strip("-")
        return f"{self.country.lower()}_{s}_{self.snapshot_date}"


def _http_get(url: str, timeout: int = 60) -> requests.Response:
    r = requests.get(url, timeout=timeout, headers={"User-Agent": _USER_AGENT})
    r.raise_for_status()
    if r.encoding is None or r.encoding.lower() == "iso-8859-1":
        r.encoding = "utf-8"
    return r


def discover_all_cities() -> List[City]:
    """Scrape the Inside Airbnb index page; return the latest snapshot per city.

    The HTML contains current + archived snapshots plus per-city summary CSV
    links we don't want. We regex out the structured Detailed csv.gz URLs and
    keep the latest snapshot per (country, region, city).
    """
    r = _http_get(INSIDE_AIRBNB_INDEX_URL, timeout=60)
    listings: dict[tuple[str, str, str], tuple[str, str]] = {}
    reviews: dict[tuple[str, str, str, str], str] = {}

    for m in _DATA_URL_RE.finditer(r.text):
        country, region, city_slug, snapshot, kind = m.group(1, 2, 3, 4, 5)
        url = m.group(0)
        if kind.lower() == "listings":
            key = (country, region, city_slug)
            prev = listings.get(key)
            if prev is None or snapshot > prev[0]:
                listings[key] = (snapshot, url)
        else:
            reviews[(country, region, city_slug, snapshot)] = url

    cities: List[City] = []
    for (country, region, city_slug), (snapshot, listings_url) in listings.items():
        cities.append(City(
            city=_humanize(city_slug),
            country=country,
            region=region,
            snapshot_date=snapshot,
            listings_url=listings_url,
            reviews_url=reviews.get((country, region, city_slug, snapshot), ""),
        ))
    cities.sort(key=lambda c: (c.country, c.region, c.city))
    return cities


def _humanize(slug: str) -> str:
    return " ".join(part.capitalize() for part in slug.replace("_", "-").split("-"))


def fetch_csv_gz_bytes(url: str, timeout: int = 600) -> bytes:
    """Download a csv.gz from Inside Airbnb. Returns the *uncompressed* bytes."""
    r = _http_get(url, timeout=timeout)
    return gzip.decompress(r.content)


def parse_price(value: object) -> Optional[float]:
    """Parse Inside Airbnb price strings into floats. Returns None on garbage.

    Inside Airbnb normalizes prices to USD, but the actual string varies:
        '$1,250.00', '$95.00', '$1,250', '1250', '$0.00', '', 'null', NaN, ...
    """
    if value is None:
        return None
    if isinstance(value, (int, float)):
        if isinstance(value, float) and (value != value):
            return None
        return float(value)
    s = str(value).strip()
    if not s or s.lower() in ("nan", "null", "none"):
        return None
    s = s.lstrip("$\u20ac\u00a3\u00a5").strip()
    s = s.replace(",", "")
    s = s.replace(" ", "")
    if not s:
        return None
    try:
        return float(s)
    except ValueError:
        return None


def head_check(url: str, timeout: int = 30) -> tuple[bool, int]:
    """HEAD-check a URL, return (ok, content_length)."""
    try:
        r = requests.head(url, timeout=timeout, headers={"User-Agent": _USER_AGENT}, allow_redirects=True)
        return (r.status_code == 200, int(r.headers.get("Content-Length", 0)))
    except Exception:
        return (False, 0)


def sample_image_urls_ok(image_urls: Iterable[str], timeout: int = 10) -> tuple[int, int]:
    """Return (n_ok, n_total) for a small sample of listing image URLs."""
    n_ok = 0
    urls = list(image_urls)
    for u in urls:
        if not u:
            continue
        try:
            r = requests.head(u, timeout=timeout, allow_redirects=True, headers={"User-Agent": _USER_AGENT})
            if r.status_code == 200:
                n_ok += 1
        except Exception:
            pass
    return (n_ok, len(urls))
