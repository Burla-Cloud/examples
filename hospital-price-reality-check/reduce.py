#!/usr/bin/env python3
"""Aggregate observation jsonl files into one reduced summary (run locally or via Burla)."""
from __future__ import annotations

import argparse
import json
import re
from collections import defaultdict
from pathlib import Path

from pipeline import shared_hpt_root
from dosage_extractor import extract_dose_from_description

REPO_ROOT = Path(__file__).resolve().parent
SAMPLES = REPO_ROOT / "samples"


def percentile(sorted_vals: list[float], p: float) -> float | None:
    if not sorted_vals:
        return None
    k = (len(sorted_vals) - 1) * p / 100.0
    f = int(k)
    c = min(f + 1, len(sorted_vals) - 1)
    if f == c:
        return sorted_vals[f]
    return sorted_vals[f] + (sorted_vals[c] - sorted_vals[f]) * (k - f)


_MIN_DRUG_PRICE = 0.05
_MIN_PROCEDURE_PRICE = 5.0
_MAX_DRUG_PRICE = 25_000.0
_MAX_PROCEDURE_PRICE = 100_000.0
_MAX_DRG_PRICE = 500_000.0


# Unit normalization: when a row carries an explicit `drug_unit` string (CMS v3
# JSON `drug_information`), we scale the row price to the HCPCS unit defined in
# the code's display name (e.g., "per 10mg"). Rows whose unit type does not
# match the HCPCS unit type at all (per-vial vs per-mg, per-mL vs per-unit) are
# dropped because the comparison is meaningless. CSV/XLSX MRFs rarely publish
# drug_information so unit normalization only fires on the ~30% of v3 JSON
# files; for the rest we rely on the ratio guard in analysis.py.

_HCPCS_UNIT_RE = re.compile(
    r"per\s+(\d+(?:\.\d+)?)\s*(mg|mcg|ug|g|ml|l|unit|units|iu)\b",
    re.IGNORECASE,
)

_DRUG_UNIT_RE = re.compile(r"^\s*(\d+(?:\.\d+)?)\s*([a-zA-Z]+)?\s*$")


def _normalize_unit_label(qty: float, unit: str) -> tuple[float, str] | None:
    u = (unit or "").lower()
    if u in ("mg",):
        return qty, "mg"
    if u in ("g",):
        return qty * 1000.0, "mg"
    if u in ("mcg", "ug"):
        return qty / 1000.0, "mg"
    if u in ("ml",):
        return qty, "ml"
    if u in ("l",):
        return qty * 1000.0, "ml"
    if u in ("unit", "units", "iu", "international units"):
        return qty, "unit"
    if u in ("each", "ea"):
        return qty, "each"
    return None


def _parse_hcpcs_unit(display_name: str) -> tuple[float, str] | None:
    if not display_name:
        return None
    m = _HCPCS_UNIT_RE.search(display_name)
    if not m:
        return None
    return _normalize_unit_label(float(m.group(1)), m.group(2))


def _parse_drug_unit(drug_unit: str | None) -> tuple[float, str] | None:
    if not drug_unit:
        return None
    m = _DRUG_UNIT_RE.match(str(drug_unit))
    if not m:
        return None
    qty_str, unit_str = m.group(1), (m.group(2) or "")
    return _normalize_unit_label(float(qty_str), unit_str)


def _resolve_dose(
    drug_unit: str | None, description: str | None
) -> tuple[float, str] | None:
    """Pick the most reliable dose signal for a row.

    1. The structured ``drug_unit`` field (CMS v3 JSON ``drug_information``).
    2. The text dose extracted from the line item description, e.g.
       "DOXORUBICIN 50 MG INJ" -> ``(50.0, "mg")``. Most CSV/XLSX MRFs
       publish the dose only in the description, so this fallback is what
       lets us actually normalize chargemaster rows.
    """
    parsed = _parse_drug_unit(drug_unit)
    if parsed is not None:
        return parsed
    return extract_dose_from_description(description)


def normalize_to_hcpcs_unit(
    price: float,
    drug_unit: str | None,
    display_name: str,
    description: str | None = None,
) -> float | None:
    """Scale ``price`` so it represents one HCPCS unit (e.g., per 10 mg).

    Returns ``None`` to signal the row should be dropped from the
    comparison. The dosage filter is intentionally strict for codes that
    bill per-X-unit because mixing un-scaled vial prices with per-mg
    prices destroys any honest cheapest/priciest comparison.

    Cases:

    * Code's display name has no parseable HCPCS unit (e.g. CPT 27447
      knee replacement) -> return ``price`` unchanged.
    * Code has a per-X HCPCS unit AND a dose can be resolved (from
      ``drug_unit`` or by parsing the description) AND the unit family
      matches -> scale: ``price * (hcpcs_qty / dose_qty)``.
    * Code has a per-X HCPCS unit but no dose can be resolved at all
      (description is just the bare drug name like "DOXORUBICIN HCL") ->
      return ``None`` so the row is dropped.
    * Code has a per-X HCPCS unit and the dose parses to a different
      unit family (per-mL when HCPCS expects per-mg) -> ``None``.
    """
    hcpcs = _parse_hcpcs_unit(display_name)
    if hcpcs is None:
        return price
    parsed = _resolve_dose(drug_unit, description)
    if parsed is None:
        return None
    if parsed[1] != hcpcs[1]:
        return None
    if parsed[0] <= 0:
        return None
    return price * (hcpcs[0] / parsed[0])


VALID_US_STATES = frozenset({
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA", "HI", "ID",
    "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS",
    "MO", "MT", "NE", "NV", "NH", "NJ", "NM", "NY", "NC", "ND", "OH", "OK",
    "OR", "PA", "RI", "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV",
    "WI", "WY",
    "DC", "PR", "VI", "GU", "AS", "MP",
})


def normalize_state(raw: str | None) -> str | None:
    """Return a USPS state code or None.

    Several upstream MRF directories ship junk state values pulled from address
    parsing failures (``PO`` from "PO Box", ``ST`` from "ST Petersburg", ``EL``
    from "EL Paso", ``FT`` from "FT Walton Beach", ``SE`` and ``NW`` from
    abbreviated city names). Anything outside the 50 states + DC + 5 territories
    becomes ``None`` so it never lands in a state bucket."""
    if not raw:
        return None
    s = str(raw).strip().upper()
    if s in VALID_US_STATES:
        return s
    return None

CATEGORY_CEILING: dict[str, float] = {
    "lab": 2_500.0,
    "vaccine": 2_000.0,
    "pediatric": 5_000.0,
    "mental_health": 5_000.0,
    "cancer_screening": 10_000.0,
    "cardiovascular": 10_000.0,
    "imaging": 20_000.0,
    "er": 30_000.0,
    "gi_endoscopy": 20_000.0,
    "maternity": 50_000.0,
    "surgical": 100_000.0,
    "inpatient_drg": 500_000.0,
    "infused_drug": 25_000.0,
    "hospital_line_item": 50_000.0,
}


def canonical_code_system(code_system: str | None, code: str | None) -> str:
    """Normalize HCPCS Level I (numeric 5-digit) codes to CPT.

    Real-world MRFs use CPT and HCPCS interchangeably for the same numeric
    5-digit codes. This collapses them under one bucket so spreads, state
    summaries, and code lookups line up.
    """
    sysu = (code_system or "").upper().strip()
    c = (code or "").strip()
    if sysu == "HCPCS" and c.isdigit() and len(c) == 5:
        return "CPT"
    return sysu


def effective_price(
    row: dict,
    category: str | None = None,
    display_name: str | None = None,
) -> float | None:
    """Return the consumer-facing PRE-INSURANCE price for this row, or None to skip.

    Pre-insurance means: what the hospital bills before any insurer-negotiated
    discount is applied. CMS Hospital Price Transparency MRFs publish four price
    types per service. Only two of these are pre-insurance:

      gross_charge      = chargemaster list price (the "first number on the bill")
      discounted_cash   = self-pay cash price for an uninsured patient

    The other two (payer_negotiated_min, minimum_reported) are post-insurance
    rates negotiated with specific insurers; they are explicitly excluded so the
    medians and "cheapest hospital" lists are not biased toward whoever has the
    cheapest insurance contract.

    We take min(gross_charge, discounted_cash) per row so the patient-favorable
    pre-insurance number wins. If a hospital published only post-insurance rates
    (no gross, no cash), the row is dropped (returns None).

    Filters:
      * unit normalization scales row prices to the HCPCS unit defined in the
        code's display name when the row carries a `drug_unit` (CMS v3 JSON);
        rows whose unit family is incompatible (per-vial vs per-mg) are dropped
      * floors per category drop $0 / $0.01 placeholder rows
      * ceilings per category drop chargemaster outliers and parser noise
      * `_looks_like_chargemaster_id` drops values that are clearly internal IDs
    """
    sysu = canonical_code_system(row.get("code_system"), row.get("code"))
    if sysu == "NDC":
        floor, ceiling = _MIN_DRUG_PRICE, _MAX_DRUG_PRICE
    elif sysu == "MS-DRG":
        floor, ceiling = _MIN_PROCEDURE_PRICE, _MAX_DRG_PRICE
    else:
        floor, ceiling = _MIN_PROCEDURE_PRICE, _MAX_PROCEDURE_PRICE
    if category and category in CATEGORY_CEILING:
        ceiling = min(ceiling, CATEGORY_CEILING[category])
    code_str = (row.get("code") or "").strip()
    drug_unit = row.get("drug_unit")
    description = row.get("description")
    candidates = []
    for k in ("gross_charge", "discounted_cash"):
        v = row.get(k)
        if v is None or not isinstance(v, (int, float)):
            continue
        if display_name:
            scaled = normalize_to_hcpcs_unit(
                float(v), drug_unit, display_name, description
            )
            if scaled is None:
                continue
            v = scaled
        if v < floor or v > ceiling:
            continue
        if _looks_like_chargemaster_id(v, code_str):
            continue
        candidates.append(float(v))
    if not candidates:
        return None
    return min(candidates)


def _looks_like_chargemaster_id(value: float, code: str) -> bool:
    """Reject values that are clearly the hospital's internal item ID, not a price.

    Pre-v3.0 MRFs sometimes published files where the "price" column actually
    contains a chargemaster identifier that embeds the CPT/HCPCS code. We have
    seen e.g. value=4310061 for CPT 80061, value=200820 for CPT 83036, where
    the 5-digit suffix or substring is the procedure code itself.
    """
    if value < 5_000:
        return False
    if value != float(int(value)):
        return False
    if not code.isdigit() or len(code) < 4:
        return False
    code_int = int(code)
    vi = int(value)
    if vi == code_int:
        return True
    s = str(vi)
    if s.endswith(code) or s.startswith(code):
        return True
    if abs(vi - code_int) <= 100 and vi >= 10_000:
        return True
    return False


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--obs-dir", type=Path, default=None, help="Override observations directory")
    args = ap.parse_args()

    obs_dir = args.obs_dir or (shared_hpt_root() / "observations")
    if not obs_dir.is_dir():
        print(f"No observations dir: {obs_dir}")
        return

    from codes import CODES

    code_category: dict[str, str] = {}
    code_display_name: dict[str, str] = {}
    for c in CODES:
        sysu = canonical_code_system(c.get("code_system"), c.get("code"))
        cat = c.get("category") or ""
        key = f"{sysu}:{c.get('code')}"
        code_category[key] = cat
        code_display_name[key] = c.get("display_name") or ""

    by_code: dict[str, list[float]] = defaultdict(list)
    by_code_state: dict[str, dict[str, list[float]]] = defaultdict(lambda: defaultdict(list))
    by_hospital_code: dict[str, dict[str, list[float]]] = defaultdict(lambda: defaultdict(list))
    by_code_hospital: dict[str, dict[str, list[float]]] = defaultdict(lambda: defaultdict(list))
    by_code_state_hospital: dict[str, dict[str, dict[str, list[float]]]] = defaultdict(
        lambda: defaultdict(lambda: defaultdict(list))
    )
    # (hid, key) -> list of (price, description, drug_unit, gross_charge,
    # discounted_cash). After we know the hospital's median for this code we
    # pick the observation closest to that median and surface its description /
    # unit on the cheapest/priciest podium.
    obs_by_hospital_code: dict[tuple[str, str], list[dict]] = defaultdict(list)
    honesty_num: dict[str, int] = defaultdict(int)
    honesty_den: dict[str, int] = defaultdict(int)
    hospital_states: dict[str, str] = {}
    hospital_names: dict[str, str] = {}
    hospital_cities: dict[str, str] = {}

    files = list(obs_dir.glob("*.jsonl"))

    pass1: dict[str, list[float]] = defaultdict(list)
    for fp in files:
        for line in fp.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            row = json.loads(line)
            sys = canonical_code_system(row.get("code_system"), row.get("code"))
            key = f"{sys}:{row.get('code') or ''}"
            price = effective_price(row, category=code_category.get(key), display_name=code_display_name.get(key))
            if price is None:
                continue
            pass1[key].append(price)

    # Per-code soft ceiling: 50x median is a generous guard against parser noise
    # (chargemaster IDs that landed in price columns) without losing legitimate spread.
    per_code_ceiling: dict[str, float] = {}
    for key, vals in pass1.items():
        if len(vals) < 5:
            continue
        vs = sorted(vals)
        median = percentile(vs, 50) or 0.0
        if median <= 0:
            continue
        per_code_ceiling[key] = max(50.0 * median, 1_000.0)

    for fp in files:
        for line in fp.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            row = json.loads(line)
            hid = row.get("hospital_id") or ""
            st = normalize_state(row.get("state"))
            if st is None:
                hospital_states.setdefault(hid, "")
            else:
                hospital_states[hid] = st
            if row.get("hospital_name"):
                hospital_names[hid] = row["hospital_name"]
            if row.get("city"):
                hospital_cities[hid] = row["city"]
            sys = canonical_code_system(row.get("code_system"), row.get("code"))
            code = row.get("code") or ""
            key = f"{sys}:{code}"
            price = effective_price(row, category=code_category.get(key), display_name=code_display_name.get(key))
            if price is None:
                continue
            soft = per_code_ceiling.get(key)
            if soft is not None and price > soft:
                continue
            by_code[key].append(price)
            by_hospital_code[hid][key].append(price)
            by_code_hospital[key][hid].append(price)
            if st is not None:
                by_code_state[key][st].append(price)
                by_code_state_hospital[key][st][hid].append(price)

            display_name = code_display_name.get(key) or ""
            hcpcs = _parse_hcpcs_unit(display_name)
            dose = _resolve_dose(row.get("drug_unit"), row.get("description"))
            scaled_gross = None
            scaled_cash = None
            if hcpcs is not None and dose is not None and dose[0] > 0 and dose[1] == hcpcs[1]:
                ratio = hcpcs[0] / dose[0]
                gv = row.get("gross_charge")
                if isinstance(gv, (int, float)):
                    scaled_gross = float(gv) * ratio
                cv = row.get("discounted_cash")
                if isinstance(cv, (int, float)):
                    scaled_cash = float(cv) * ratio
            obs_by_hospital_code[(hid, key)].append(
                {
                    "price": price,
                    "description": (row.get("description") or "").strip() or None,
                    "drug_unit": row.get("drug_unit"),
                    "gross_charge": row.get("gross_charge"),
                    "discounted_cash": row.get("discounted_cash"),
                    "gross_charge_per_unit": scaled_gross,
                    "discounted_cash_per_unit": scaled_cash,
                    "dose_qty": dose[0] if dose else None,
                    "dose_unit": dose[1] if dose else None,
                    "hcpcs_qty": hcpcs[0] if hcpcs else None,
                    "hcpcs_unit": hcpcs[1] if hcpcs else None,
                    "setting": row.get("setting") or None,
                }
            )

            cash = row.get("discounted_cash")
            neg_min = row.get("payer_negotiated_min")
            if cash is not None and neg_min is not None and cash > 0 and neg_min > 0:
                honesty_den[hid] += 1
                if float(cash) < float(neg_min):
                    honesty_num[hid] += 1

    reduced: dict = {
        "observation_files": len(files),
        "codes": {},
        "states_by_code": {},
        "hospitals_by_code": {},
        "hospitals_by_code_by_state": {},
        "hospitals": {},
    }

    for key, vals in by_code.items():
        vals.sort()
        n = len(vals)
        reduced["codes"][key] = {
            "count": n,
            "min": vals[0],
            "max": vals[-1],
            "mean": sum(vals) / n if n else None,
            "p10": percentile(vals, 10),
            "p25": percentile(vals, 25),
            "median": percentile(vals, 50),
            "p75": percentile(vals, 75),
            "p90": percentile(vals, 90),
        }

    for key, stmap in by_code_state.items():
        reduced["states_by_code"][key] = {}
        for st, vals in stmap.items():
            vals.sort()
            m = percentile(vals, 50)
            if m is None:
                continue
            reduced["states_by_code"][key][st] = {
                "median": m,
                "mean": sum(vals) / len(vals),
                "min": vals[0],
                "max": vals[-1],
                "count": len(vals),
                "p10": percentile(vals, 10),
                "p25": percentile(vals, 25),
                "p75": percentile(vals, 75),
                "p90": percentile(vals, 90),
            }

    def _hospital_meta(hid: str) -> dict:
        return {
            "hospital_id": hid,
            "name": hospital_names.get(hid, hid),
            "state": hospital_states.get(hid, ""),
            "city": hospital_cities.get(hid),
        }

    def _representative(hid: str, key: str, target_price: float) -> dict | None:
        """Return the captured observation closest to `target_price` for this
        (hospital, code). Used to surface the actual MRF line item description
        and unit on the cheapest/priciest podium so readers can verify the
        comparison is apples-to-apples (same drug, same dosage).

        For drug codes with a per-X HCPCS unit, the representative also
        carries:

        * ``dose_qty`` / ``dose_unit`` -- the dose extracted from the line
          item description (e.g. 50 mg vial)
        * ``hcpcs_qty`` / ``hcpcs_unit`` -- the HCPCS billing unit pulled
          from the code's display name (e.g. 10 mg)
        * ``gross_charge_per_unit`` / ``discounted_cash_per_unit`` --
          the raw chargemaster numbers scaled to that HCPCS billing unit
          so cards can show "Gross $1,106 per 10 mg" instead of the raw
          per-vial number, which is what was confusing readers.
        """
        obs = obs_by_hospital_code.get((hid, key)) or []
        if not obs:
            return None
        # Pick the observation whose effective price is closest to the
        # hospital's median for this code. For an even number of rows the
        # median is the average of the two middle values, so two
        # observations bracket the median equidistantly. We tiebreak by
        # preferring the LOWER-priced row: it's the more patient-friendly
        # number, and it keeps the rep's price <= median, which avoids
        # the "the displayed price is less than this row's cash discount"
        # confusion when the rep's gross/cash sits above the median.
        chosen = min(
            obs,
            key=lambda o: (
                abs(float(o.get("price") or 0) - float(target_price)),
                float(o.get("price") or 0),
            ),
        )
        out: dict = {}
        scalar_fields = (
            "description",
            "drug_unit",
            "gross_charge",
            "discounted_cash",
            "gross_charge_per_unit",
            "discounted_cash_per_unit",
            "dose_qty",
            "dose_unit",
            "hcpcs_qty",
            "hcpcs_unit",
            "setting",
        )
        for k in scalar_fields:
            v = chosen.get(k)
            if v is None or (isinstance(v, str) and not v.strip()):
                continue
            out[k] = v
        out["price"] = chosen.get("price")
        return out or None

    for key, hmap in by_code_hospital.items():
        rows = []
        for hid, vals in hmap.items():
            if not vals:
                continue
            med = percentile(sorted(vals), 50) or 0.0
            rep = _representative(hid, key, med)
            row = {
                **_hospital_meta(hid),
                "median": med if med else None,
                "count": len(vals),
            }
            if rep:
                row["representative"] = rep
            rows.append(row)
        rows.sort(key=lambda r: (r.get("median") is None, r.get("median") or 0))
        reduced["hospitals_by_code"][key] = rows

    for key, st_map in by_code_state_hospital.items():
        reduced["hospitals_by_code_by_state"][key] = {}
        for st, hmap in st_map.items():
            rows = []
            for hid, vals in hmap.items():
                if not vals:
                    continue
                med = percentile(sorted(vals), 50) or 0.0
                rep = _representative(hid, key, med)
                row = {
                    **_hospital_meta(hid),
                    "median": med if med else None,
                    "count": len(vals),
                }
                if rep:
                    row["representative"] = rep
                rows.append(row)
            rows.sort(key=lambda r: (r.get("median") is None, r.get("median") or 0))
            reduced["hospitals_by_code_by_state"][key][st] = rows

    for hid, cmap in by_hospital_code.items():
        reduced["hospitals"][hid] = {
            "state": hospital_states.get(hid, ""),
            "per_code": {k: {"median": percentile(sorted(v), 50), "count": len(v)} for k, v in cmap.items()},
            "honesty_score": (
                round(honesty_num[hid] / honesty_den[hid], 4) if honesty_den[hid] else None
            ),
            "honesty_pairs": honesty_den[hid],
        }

    SAMPLES.mkdir(parents=True, exist_ok=True)
    out = SAMPLES / "hpt_reduced.json"
    out.write_text(json.dumps(reduced, indent=2), encoding="utf-8")
    print(f"wrote {out} ({len(reduced['codes'])} code keys)")


if __name__ == "__main__":
    main()
