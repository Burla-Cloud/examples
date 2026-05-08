"""Hospital index loading from real CMS-registered hospital MRF URLs."""
from __future__ import annotations

import csv
import hashlib
import json
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent
DATA_DIR = REPO_ROOT / "data"
DATA_SOURCES = REPO_ROOT / "data_sources"

# Valid USPS state codes (50 states + DC + 5 inhabited territories). Several
# upstream MRF feeds carry junk values pulled from address parsing failures
# (``PO`` from "PO Box", ``ST`` from "ST Petersburg", ``EL`` from "EL Paso",
# ``FT`` from "FT Walton Beach", ``SE`` and ``NW`` from abbreviated city names).
# Anything outside this set gets normalized to ``None``.
VALID_US_STATES: frozenset[str] = frozenset({
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA", "HI", "ID",
    "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS",
    "MO", "MT", "NE", "NV", "NH", "NJ", "NM", "NY", "NC", "ND", "OH", "OK",
    "OR", "PA", "RI", "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV",
    "WI", "WY",
    "DC", "PR", "VI", "GU", "AS", "MP",
})


def _normalize_state(raw: str | None) -> str | None:
    """Return a USPS state code or None for any invalid/junk value."""
    if not raw:
        return None
    s = str(raw).strip().upper()
    return s if s in VALID_US_STATES else None


# State centroids for map plots (capital lat/lon, used only when MRF row carries state).
STATE_CENTROIDS: dict[str, tuple[float, float]] = {
    "AL": (32.806671, -86.79113), "AK": (61.370716, -152.404419), "AZ": (33.729759, -111.431221),
    "AR": (34.969704, -92.373123), "CA": (36.116203, -119.681564), "CO": (39.059811, -105.311104),
    "CT": (41.597782, -72.755371), "DE": (39.318523, -75.507141), "FL": (27.766279, -81.686783),
    "GA": (33.040619, -83.643074), "HI": (21.094318, -157.498337), "ID": (44.240459, -114.478828),
    "IL": (40.349457, -88.986137), "IN": (39.849426, -86.258278), "IA": (42.011539, -93.210526),
    "KS": (38.5266, -96.726486), "KY": (37.66814, -84.670067), "LA": (31.169546, -91.867805),
    "ME": (44.693947, -69.381927), "MD": (39.063946, -76.802101), "MA": (42.230171, -71.530106),
    "MI": (43.326618, -84.536095), "MN": (45.694454, -93.900192), "MS": (32.741646, -89.678696),
    "MO": (38.456085, -92.288368), "MT": (46.921925, -110.454353), "NE": (41.12537, -98.268082),
    "NV": (38.313515, -117.055374), "NH": (43.452492, -71.563896), "NJ": (40.298904, -74.521011),
    "NM": (34.840515, -106.248482), "NY": (42.165726, -74.948051), "NC": (35.630066, -79.806419),
    "ND": (47.528912, -99.784012), "OH": (40.388783, -82.764915), "OK": (35.565342, -96.928917),
    "OR": (44.572021, -122.070938), "PA": (40.590752, -77.209755), "RI": (41.680893, -71.51178),
    "SC": (33.856892, -80.945007), "SD": (44.299782, -99.438828), "TN": (35.747845, -86.692345),
    "TX": (31.054487, -97.563461), "UT": (40.150032, -111.862434), "VT": (44.045876, -72.710686),
    "VA": (37.769337, -78.169968), "WA": (47.400902, -121.490494), "WV": (38.491226, -80.954456),
    "WI": (44.268543, -89.616508), "WY": (42.755966, -107.30249), "DC": (38.897438, -77.026817),
    "PR": (18.220833, -66.590149), "VI": (18.335765, -64.896335), "GU": (13.444304, 144.793732),
    "AS": (-14.270972, -170.132217), "MP": (15.0979, 145.6739),
}

TPAFS_CSV = DATA_SOURCES / "tpafs_machine_readable_links.csv"
CURATED_JSON = DATA_SOURCES / "curated_v3_mrfs.json"
DOLTHUB_JSON = DATA_SOURCES / "dolthub_v4_mrfs.json"
ORIA_JSON = DATA_SOURCES / "oria_v3_mrfs.json"


def _slug(s: str) -> str:
    return "".join(c for c in s.lower().replace(" ", "-") if c.isalnum() or c == "-")[:64]


def _stable_id_suffix(seed: str) -> str:
    """Deterministic across processes (Python's `hash()` is per-process random)."""
    return hashlib.sha256(seed.encode("utf-8")).hexdigest()[:6]


def load_curated() -> list[dict]:
    """Load the hand-curated list of real, current (v3.0-era) hospital MRF URLs."""
    if not CURATED_JSON.is_file():
        return []
    payload = json.loads(CURATED_JSON.read_text(encoding="utf-8"))
    out: list[dict] = []
    seen: set[str] = set()
    for h in payload.get("hospitals", []):
        url = (h.get("mrf_url") or "").strip()
        if not url:
            continue
        ccn = (h.get("ccn") or "").strip()
        name = (h.get("name") or "").strip()
        if not name:
            continue
        hospital_id = f"{_slug(name)[:40]}-{_stable_id_suffix(ccn or url)}"
        if hospital_id in seen:
            continue
        seen.add(hospital_id)
        state = _normalize_state(h.get("state"))
        lat, lon = STATE_CENTROIDS.get(state or "", (None, None))
        out.append(
            {
                "hospital_id": hospital_id,
                "ccn": ccn or None,
                "name": name,
                "state": state,
                "city": h.get("city"),
                "latitude": lat,
                "longitude": lon,
                "mrf_url": url,
                "file_format": (h.get("file_format") or "").lower() or None,
                "system": h.get("system"),
                "source": "curated",
            }
        )
    return out


def load_real_hospitals_from_tpafs(
    csv_path: Path = TPAFS_CSV,
    only_status_up: bool = True,
    formats: tuple[str, ...] = ("csv", "json", "xlsx", "xls", "xml"),
) -> list[dict]:
    """Read the TPAFS public hospital MRF index and return normalized hospital dicts.

    Source: https://github.com/TPAFS/transparency-data
    Each row gives a real hospital (CCN, legal name, state, MRF URL). Many URLs may be
    stale; the pipeline gracefully skips fetch failures and we surface that in the run summary.
    """
    if not csv_path.is_file():
        return []
    out: list[dict] = []
    seen: set[str] = set()
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            url = (row.get("machine_readable_url") or "").strip()
            if not url or not url.startswith(("http://", "https://")):
                continue
            if only_status_up and (row.get("machine_readable_url_status") or "").strip().lower() not in ("up", ""):
                continue
            fmt = (row.get("file_format") or "").strip().lower()
            if formats and fmt and fmt not in formats:
                continue
            ccn = (row.get("ccn") or "").strip()
            name = (
                row.get("reporting_entity_name_common")
                or row.get("reporting_entity_name_legal")
                or ""
            ).strip()
            state = _normalize_state(row.get("state_or_region"))
            if not name:
                continue
            hid_seed = ccn or url
            hospital_id = f"{_slug(name)[:32]}-{_stable_id_suffix(hid_seed)}"
            if hospital_id in seen:
                continue
            seen.add(hospital_id)
            lat, lon = STATE_CENTROIDS.get(state or "", (None, None))
            out.append(
                {
                    "hospital_id": hospital_id,
                    "ccn": ccn or None,
                    "name": name,
                    "state": state,
                    "city": None,
                    "latitude": lat,
                    "longitude": lon,
                    "mrf_url": url,
                    "file_format": fmt or None,
                    "last_updated_date": (row.get("last_updated_date") or "").strip() or None,
                    "system": (row.get("reporting_entity_name_legal") or "").strip() or None,
                }
            )
    return out


def load_dolthub(json_path: Path = DOLTHUB_JSON) -> list[dict]:
    """Load the dolthub transparency-in-pricing snapshot of US hospital MRF URLs."""
    if not json_path.is_file():
        return []
    payload = json.loads(json_path.read_text(encoding="utf-8"))
    rows = payload.get("hospitals") or payload
    out: list[dict] = []
    seen: set[str] = set()
    for h in rows:
        url = (h.get("mrf_url") or "").strip()
        if not url or not url.startswith(("http://", "https://")):
            continue
        ccn = (h.get("ccn") or "").strip()
        name = (h.get("name") or "").strip()
        if not name:
            continue
        hid_seed = ccn or url
        hospital_id = f"{_slug(name)[:32]}-{_stable_id_suffix(hid_seed)}"
        if hospital_id in seen:
            continue
        seen.add(hospital_id)
        state = _normalize_state(h.get("state"))
        lat, lon = STATE_CENTROIDS.get(state or "", (None, None))
        fmt = url.lower().rsplit(".", 1)[-1].split("?")[0] if "." in url else None
        if fmt and len(fmt) > 5:
            fmt = None
        out.append(
            {
                "hospital_id": hospital_id,
                "ccn": ccn or None,
                "name": name,
                "state": state,
                "city": h.get("city"),
                "latitude": lat,
                "longitude": lon,
                "mrf_url": url,
                "file_format": fmt,
                "last_updated_date": h.get("last_updated_date"),
                "system": h.get("system"),
                "source": "dolthub",
            }
        )
    return out


def load_oria(json_path: Path = ORIA_JSON) -> list[dict]:
    """Load the Trilliant Health Oria hospital MRF directory snapshot."""
    if not json_path.is_file():
        return []
    payload = json.loads(json_path.read_text(encoding="utf-8"))
    rows = payload.get("hospitals") or payload
    out: list[dict] = []
    seen: set[str] = set()
    for h in rows:
        url = (h.get("mrf_url") or "").strip()
        if not url or not url.startswith(("http://", "https://")):
            continue
        name = (h.get("name") or "").strip()
        if not name:
            continue
        oria_id = (h.get("id") or "").strip()
        hid_seed = oria_id or url
        hospital_id = f"{_slug(name)[:32]}-{_stable_id_suffix(hid_seed)}"
        if hospital_id in seen:
            continue
        seen.add(hospital_id)
        state = _normalize_state(h.get("state"))
        lat, lon = STATE_CENTROIDS.get(state or "", (None, None))
        fmt = (h.get("file_format") or "").lower() or None
        out.append(
            {
                "hospital_id": hospital_id,
                "ccn": None,
                "name": name,
                "state": state,
                "city": h.get("city"),
                "latitude": lat,
                "longitude": lon,
                "mrf_url": url,
                "file_format": fmt,
                "last_updated_date": None,
                "system": None,
                "source": "oria",
                "oria_id": oria_id or None,
                "source_page_url": h.get("source_page_url"),
            }
        )
    return out


def load_hospitals(
    idx_path: Path | None = None,
    include_tpafs: bool = False,
    include_dolthub: bool = False,
    include_oria: bool = False,
) -> list[dict]:
    """Return the list of hospitals to process.

    Priority:
      1. data/hospital_index.json (if hand-curated/pre-filtered overrides exist)
      2. data_sources/curated_v3_mrfs.json (current v3.0 real hospital MRFs)
      3. data_sources/oria_v3_mrfs.json (Trilliant Health Oria snapshot, optional)
      4. data_sources/dolthub_v4_mrfs.json (dolthub snapshot, optional)
      5. data_sources/tpafs_machine_readable_links.csv (older TPAFS index, optional)
    """
    if idx_path and idx_path.is_file():
        return json.loads(idx_path.read_text(encoding="utf-8"))
    rows = load_curated()
    seen_urls = {r.get("mrf_url") for r in rows}
    if include_oria and ORIA_JSON.is_file():
        for r in load_oria():
            if r.get("mrf_url") not in seen_urls:
                rows.append(r)
                seen_urls.add(r.get("mrf_url"))
    if include_dolthub and DOLTHUB_JSON.is_file():
        for r in load_dolthub():
            if r.get("mrf_url") not in seen_urls:
                rows.append(r)
                seen_urls.add(r.get("mrf_url"))
    if include_tpafs and TPAFS_CSV.is_file():
        for r in load_real_hospitals_from_tpafs(TPAFS_CSV):
            if r.get("mrf_url") not in seen_urls:
                rows.append(r)
                seen_urls.add(r.get("mrf_url"))
    return rows


if __name__ == "__main__":
    hospitals = load_hospitals()
    print(f"loaded {len(hospitals)} hospitals from {'data/hospital_index.json' if (DATA_DIR / 'hospital_index.json').is_file() else 'TPAFS CSV'}")
    by_state: dict[str, int] = {}
    by_format: dict[str, int] = {}
    for h in hospitals:
        s = h.get("state") or "??"
        by_state[s] = by_state.get(s, 0) + 1
        f = h.get("file_format") or "?"
        by_format[f] = by_format.get(f, 0) + 1
    print("by format:", dict(sorted(by_format.items(), key=lambda kv: -kv[1])))
    print("top 10 states:", dict(sorted(by_state.items(), key=lambda kv: -kv[1])[:10]))
