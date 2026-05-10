#!/usr/bin/env python3
"""Cross-check our drug medians against CMS Part B ASP payment limits.

CMS publishes the per-HCPCS-unit payment allowance every quarter (it's
the ASP plus 6 percent for most drugs). Hospital chargemaster prices
should be a multiple of that allowance, never a fraction. If our median
per-unit price is below the ASP allowance or more than 100x above it,
that's a red flag for a decimal-place or unit-encoding bug we missed.

Reads ``data/cms_asp/section 5208 version of April 2026 Medicare
Part B Payment Limit File 033026.csv`` (downloadable from CMS) and
compares against ``frontend/public/data/code_summary.json``.

Output: ``samples/cms_asp_check.json`` with a row per drug code
showing the CMS allowance, our median, the ratio, and a verdict.
"""
from __future__ import annotations

import csv
import json
import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent
ASP_CSV = REPO_ROOT / "data" / "cms_asp" / (
    "section 5208 version of April 2026 Medicare Part B Payment Limit "
    "File 033026.csv"
)
SUMMARY_JSON = REPO_ROOT / "frontend" / "public" / "data" / "code_summary.json"
OUTPUT = REPO_ROOT / "samples" / "cms_asp_check.json"

# Hospital chargemaster prices are typically 2x to 50x the CMS payment
# allowance for the same drug (gross charges are intentionally inflated
# relative to what CMS pays). Below 0.5x is suspicious -- you'd be
# charging less than Medicare pays. Above 100x is also suspicious for
# drugs where CMS pays a meaningful amount, since 100x is already a
# very generous chargemaster markup. We skip the high-side check when
# the CMS allowance is tiny (very off-patent generics), because the
# ratio loses signal when the denominator is fractions of a cent --
# any markup looks 1000x and chargemasters notoriously lag generic
# erosion by years. Those cases get a "low_cms_basis" verdict instead
# of being flagged as bugs.
EXPECTED_LOW_MULTIPLE = 0.5
EXPECTED_HIGH_MULTIPLE = 100.0
LOW_CMS_BASIS = 0.50  # USD per HCPCS unit -- below this, ratio is unreliable


def _parse_dose(s: str) -> tuple[float, str] | None:
    """Parse "10 MG" / "1 ML" / "100 MCG" / "0.025 mg" into (qty, unit)."""
    if not s:
        return None
    m = re.match(
        r"\s*([0-9]*\.?[0-9]+)\s*([A-Za-z]+)",
        s.strip(),
    )
    if not m:
        return None
    qty = float(m.group(1))
    unit = m.group(2).strip().lower()
    if unit in ("mg", "ml", "mcg", "iu", "g", "gm"):
        return qty, unit
    if unit in ("unit", "units", "u"):
        return qty, "unit"
    return None


def _load_asp() -> dict[str, dict]:
    text = ASP_CSV.read_text(encoding="latin-1")
    out: dict[str, dict] = {}
    reader = csv.reader(text.splitlines())
    in_data = False
    for row in reader:
        if not row:
            continue
        if not in_data:
            if row and row[0].strip() == "HCPCS Code":
                in_data = True
            continue
        if len(row) < 4 or not row[0].strip():
            continue
        code = row[0].strip()
        short = row[1].strip()
        dose_text = row[2].strip()
        try:
            limit = float(row[3].strip())
        except ValueError:
            continue
        dose = _parse_dose(dose_text)
        out[code] = {
            "short_description": short,
            "dose_text": dose_text,
            "dose_qty": dose[0] if dose else None,
            "dose_unit": dose[1] if dose else None,
            "payment_limit": limit,
        }
    return out


def _our_dose(billing_unit: str) -> tuple[float, str] | None:
    return _parse_dose(billing_unit) if billing_unit else None


def _verdict(ratio: float | None, cms_per_unit: float | None) -> str:
    if ratio is None:
        return "unknown"
    if cms_per_unit is not None and cms_per_unit < LOW_CMS_BASIS:
        # Off-patent generics where CMS allowance is fractions of a
        # cent: ratio is mathematically extreme but isn't diagnostic.
        # We treat these as a separate "low CMS basis" cohort.
        if ratio < EXPECTED_LOW_MULTIPLE:
            return "below_cms_allowance"
        return "low_cms_basis"
    if ratio < EXPECTED_LOW_MULTIPLE:
        return "below_cms_allowance"
    if ratio > EXPECTED_HIGH_MULTIPLE:
        return "implausibly_high"
    return "ok"


def main() -> None:
    if not ASP_CSV.exists():
        sys.exit(f"CMS ASP CSV not found at {ASP_CSV}. Did you download it?")
    if not SUMMARY_JSON.exists():
        sys.exit(f"code_summary.json not found at {SUMMARY_JSON}")

    asp = _load_asp()
    summary = json.loads(SUMMARY_JSON.read_text())

    rows: list[dict] = []
    flagged: list[dict] = []
    for entry in summary:
        if entry.get("code_system") != "HCPCS":
            continue
        code = entry.get("code")
        if not code:
            continue
        billing_unit = entry.get("billing_unit")
        if not billing_unit:
            continue
        stats = entry.get("stats") or {}
        median = stats.get("median")
        if median is None:
            continue

        cms = asp.get(code)
        if not cms:
            row = {
                "code": code,
                "display_name": entry.get("display_name"),
                "verdict": "no_cms_record",
            }
            rows.append(row)
            continue

        our_dose = _our_dose(billing_unit)
        cms_dose = (cms.get("dose_qty"), cms.get("dose_unit"))
        cms_limit = cms.get("payment_limit")

        # Convert CMS payment limit to "per our billing unit" so the
        # ratio is apples-to-apples. CMS dose and our billing unit
        # should normally already match (both come from the same HCPCS
        # definition), but we scale defensively.
        cms_per_our_unit: float | None = None
        if (
            our_dose
            and cms_dose
            and cms_dose[0] is not None
            and our_dose[1] == cms_dose[1]
        ):
            cms_per_our_unit = cms_limit * (our_dose[0] / cms_dose[0])
        elif (
            our_dose
            and cms_dose
            and cms_dose[0] is not None
            and our_dose[1] != cms_dose[1]
        ):
            # Unit mismatch (mg vs mcg vs ml). Try a 1000x conversion
            # for mg<->mcg, leave alone otherwise.
            mg_mcg = {("mg", "mcg"): 1000.0, ("mcg", "mg"): 1 / 1000.0}
            factor = mg_mcg.get((cms_dose[1], our_dose[1]))
            if factor is not None:
                cms_per_our_unit = cms_limit * (our_dose[0] / cms_dose[0]) * factor

        ratio = (
            float(median) / cms_per_our_unit
            if cms_per_our_unit and cms_per_our_unit > 0
            else None
        )

        row = {
            "code": code,
            "display_name": entry.get("display_name"),
            "billing_unit": billing_unit,
            "cms_dose": cms.get("dose_text"),
            "cms_payment_limit": cms_limit,
            "cms_per_our_unit": (
                round(cms_per_our_unit, 4) if cms_per_our_unit else None
            ),
            "our_median": median,
            "ratio_chargemaster_to_cms": (
                round(ratio, 2) if ratio is not None else None
            ),
            "verdict": _verdict(ratio, cms_per_our_unit),
            "hospitals_in_summary": stats.get("count"),
        }
        rows.append(row)
        if row["verdict"] in ("below_cms_allowance", "implausibly_high"):
            flagged.append(row)

    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT.write_text(json.dumps(rows, indent=2))

    print(f"== CMS ASP cross-check ({len(rows)} drug codes) ==")
    ok = sum(1 for r in rows if r["verdict"] == "ok")
    no_cms = sum(1 for r in rows if r["verdict"] == "no_cms_record")
    print(f"  ok: {ok}")
    print(f"  no CMS record: {no_cms}")
    print(f"  flagged: {len(flagged)}")
    print()
    print("Per-code ratio (chargemaster median / CMS allowance):")
    for r in rows:
        if r["verdict"] == "no_cms_record":
            continue
        marker = ""
        if r["verdict"] != "ok":
            marker = f"  <-- {r['verdict'].upper()}"
        print(
            f"  {r['code']:<8} ratio={r['ratio_chargemaster_to_cms']!s:<8} "
            f"median=${r['our_median']!s:<10} cms=${r['cms_per_our_unit']!s:<10} "
            f"({r['display_name']}){marker}"
        )

    if flagged:
        print(f"\n== flagged ==")
        for r in flagged:
            print(json.dumps(r, indent=2))

    print(f"\nWrote {OUTPUT}")


if __name__ == "__main__":
    main()
