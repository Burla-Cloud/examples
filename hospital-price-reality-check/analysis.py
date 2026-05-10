#!/usr/bin/env python3
"""Build site-ready JSON bundles from hpt_reduced.json + codes + index.

Reads:
  samples/hpt_reduced.json     (output of reduce.py)
  samples/hpt_scale_summary.json (output of scale.py)

Writes (frontend/public/data + samples mirror):
  code_summary.json            list of CodeEntry with stats + per-state list of cheapest/priciest
  state_summary.json           per-state median/min/max/mean per code
  hospital_summary.json        per-hospital roll-up (state, codes covered, honesty)
  hospital_index.json          full index used by the Hospitals page
  spread_leaderboard.json      sorted by max/min spread ratio (the "biggest gaps")
  run_metadata.json            real-run only (no synthetic metadata)
"""
from __future__ import annotations

import json
from pathlib import Path

from codes import CODES
from description_filter import description_matches_code
from hospital_index import load_hospitals

REPO_ROOT = Path(__file__).resolve().parent
SAMPLES = REPO_ROOT / "samples"
FRONT_DATA = REPO_ROOT / "frontend" / "public" / "data"


def _round_money(v):
    if v is None:
        return None
    try:
        return round(float(v), 2)
    except (TypeError, ValueError):
        return None


_EIN_PREFIX_RE = None


def _clean_hospital_name(name: str | None) -> str | None:
    """Strip the federal EIN prefix that some MRFs prepend to hospital names.

    Real MRFs from the Baptist Memorial chain and others publish names like
    ``64-0682111 BMH DeSoto`` where the leading nine digits are the federal
    employer identification number. Drop it for display.
    """
    global _EIN_PREFIX_RE
    if not name:
        return name
    if _EIN_PREFIX_RE is None:
        import re

        _EIN_PREFIX_RE = re.compile(r"^\s*\d{2}-\d{7}\s+")
    return _EIN_PREFIX_RE.sub("", name).strip() or name


def _clean_city(city: str | None) -> str | None:
    """Drop garbage city values that came from misparsed address columns.

    A ZIP code, PO Box, pipe-separated multi-line address, or bare numeric
    string is not useful as a city display.
    """
    if not city:
        return None
    s = str(city).strip()
    if not s:
        return None
    if "|" in s:
        return None
    upper = s.upper()
    if "PO BOX" in upper or "P.O. BOX" in upper or upper.startswith("BOX "):
        return None
    if any(ch.isdigit() for ch in s):
        return None
    return s


def _format_dose(qty: float | int | None, unit: str | None) -> str | None:
    """Render a dose in chargemaster-style ("50 mg", "100 mcg", "0.25 mg")."""
    if qty is None or unit is None:
        return None
    try:
        q = float(qty)
    except (TypeError, ValueError):
        return None
    if q <= 0:
        return None
    if q == int(q):
        qstr = str(int(q))
    else:
        qstr = f"{q:.3f}".rstrip("0").rstrip(".") or "0"
    return f"{qstr} {unit}"


def _build_line_item(rep: dict) -> dict:
    """Render a per-hospital representative line item card.

    The frontend uses this to show readers exactly which row from the
    hospital's MRF was compared. We surface both the raw chargemaster
    numbers (`gross_charge`, `discounted_cash`) and the same numbers
    scaled to the HCPCS billing unit (`gross_charge_per_unit`,
    `discounted_cash_per_unit`) so a 50 mg vial doesn't look more
    expensive than a 10 mg vial just because it ships in bigger
    increments.
    """
    line: dict = {
        "description": rep.get("description") or None,
        "unit": rep.get("drug_unit"),
        "gross_charge": _round_money(rep.get("gross_charge")),
        "discounted_cash": _round_money(rep.get("discounted_cash")),
        "gross_charge_per_unit": _round_money(rep.get("gross_charge_per_unit")),
        "discounted_cash_per_unit": _round_money(rep.get("discounted_cash_per_unit")),
        "dose": _format_dose(rep.get("dose_qty"), rep.get("dose_unit")),
        "hcpcs_billing_unit": _format_dose(rep.get("hcpcs_qty"), rep.get("hcpcs_unit")),
        "setting": rep.get("setting"),
    }
    return line


# Per-category ratio bounds: a hospital's median must lie within
# [low_ratio * national_median, high_ratio * national_median] to be eligible.
# These catch unit-encoding glitches (per-mg vs per-vial, per-dose vs per-cycle)
# without throwing away genuinely cheap or genuinely expensive hospitals.
# Drugs are tighter because per-mg vs per-vial errors are an order of
# magnitude apart and dominate the chargemaster noise we see in the data.
# Surgical and DRG codes are TWO things in the wild: a surgeon-only
# professional fee (~$1.5k-3k) and an all-in surgical package (~$20k-80k).
# Mixing them on one page makes the cheapest hospital look like a chargemaster
# bug. We pull the floor up to half the post-filter median so the headline
# pool is whichever band of reporting dominates this code (almost always the
# all-in package for major joint replacement, hip, etc.).
_RATIO_BOUNDS: dict[str, tuple[float, float]] = {
    "infused_drug":      (0.30, 50.0),
    "cancer_treatment":  (0.30, 50.0),
    "vaccine":           (0.30, 50.0),
    "surgical":          (0.60, 25.0),
    "inpatient_drg":     (0.55, 20.0),
    "maternity":         (0.40, 25.0),
    "gi_endoscopy":      (0.30, 50.0),
    "cardiovascular":    (0.40, 30.0),
    "imaging":           (0.25, 80.0),
    "er":                (0.20, 100.0),
    "lab":               (0.25, 100.0),
    "cancer_screening":  (0.25, 80.0),
    "mental_health":     (0.20, 100.0),
    "pediatric":         (0.20, 100.0),
    "hospital_line_item": (0.20, 200.0),
    "misc":              (0.20, 100.0),
}


def _ranking_floor(
    code_system: str, category: str, code: str = "",
    national_median: float | None = None,
) -> float:
    """Minimum price for a hospital row to be eligible.

    Real MRFs publish $5 hip replacements, $0.17 chemo agents, and $1 cancer
    admissions as chargemaster placeholders. Those should never headline the
    "cheapest hospital in America for X" list.

    For each category we enforce both a static floor (kills $0/$1/$5
    placeholders) and a ratio-based floor (kills unit encoding errors where a
    hospital priced 10mg as 1mg and reported 1/10th the real number).
    """
    sysu = (code_system or "").upper().replace("-", "")
    if sysu == "MSDRG":
        static = 1_000.0
    elif sysu == "NDC":
        static = 0.50
    elif category == "infused_drug":
        static = 10.0
    elif category == "cancer_treatment":
        static = 100.0
    elif category in ("inpatient_drg", "surgical", "maternity"):
        static = 250.0
    elif category in ("imaging", "er", "gi_endoscopy", "cardiovascular"):
        static = 50.0
    else:
        static = 20.0
    low_ratio, _ = _RATIO_BOUNDS.get(category or "", (0.20, 100.0))
    if national_median and national_median > 0:
        return max(static, low_ratio * float(national_median))
    return static


def _ranking_ceiling(
    code_system: str, category: str, all_median: float | None,
) -> float:
    """Maximum price for a hospital row to be eligible.

    A hospital median > N x the national median almost always means a unit
    error (per-vial reported instead of per-dose) or a chargemaster ID encoded
    as a price. The per-category ratio cap is the primary guard; the per-code-
    system absolute backstop is a sanity ceiling for the no-national-median
    edge case.
    """
    sysu = (code_system or "").upper().replace("-", "")
    if sysu == "MSDRG":
        backstop = 5_000_000.0
    elif sysu == "NDC":
        backstop = 200_000.0
    else:
        backstop = 1_000_000.0
    _, high_ratio = _RATIO_BOUNDS.get(category or "", (0.20, 100.0))
    if all_median and all_median > 0:
        return min(backstop, max(high_ratio * float(all_median), 50_000.0))
    return backstop


def _hospital_url(hid: str, hospitals_full: list[dict]) -> str | None:
    return None


def _percentile(sorted_vals: list[float], pct: float) -> float | None:
    """Linear-interpolated percentile over a pre-sorted list of values."""
    n = len(sorted_vals)
    if n == 0:
        return None
    if n == 1:
        return float(sorted_vals[0])
    if pct <= 0:
        return float(sorted_vals[0])
    if pct >= 100:
        return float(sorted_vals[-1])
    k = (n - 1) * pct / 100.0
    f = int(k)
    c = min(f + 1, n - 1)
    if f == c:
        return float(sorted_vals[f])
    d = k - f
    return float(sorted_vals[f]) * (1.0 - d) + float(sorted_vals[c]) * d


def _stats_from_hospital_medians(hospitals: list[dict]) -> dict | None:
    """Build headline stats from a pre-filtered pool of hospital medians.

    We deliberately compute percentiles over per-hospital medians (one weight
    per hospital) rather than over raw line items. This guarantees the chart,
    the price grid, and the cheapest/priciest podium tell the same story:

      "10% of hospitals charge less than $X for this code"

    Without this, a hospital that publishes the same code at a $7 placeholder
    once and a $400 real price five times would push the raw P10 well below
    the cheapest-hospital row, which surfaced as a confusing discrepancy in
    earlier builds.
    """
    medians: list[float] = []
    for h in hospitals:
        m = h.get("median")
        if m is None:
            continue
        try:
            mv = float(m)
        except (TypeError, ValueError):
            continue
        if mv > 0:
            medians.append(mv)
    if not medians:
        return None
    medians.sort()
    n = len(medians)
    return {
        "count": n,
        "min": medians[0],
        "max": medians[-1],
        "mean": sum(medians) / n,
        "p10": _percentile(medians, 10),
        "p25": _percentile(medians, 25),
        "median": _percentile(medians, 50),
        "p75": _percentile(medians, 75),
        "p90": _percentile(medians, 90),
    }


def _filter_eligible(
    hospitals: list[dict],
    code_system: str,
    category: str,
    code: str,
    national_median: float | None,
    code_meta: dict | None = None,
) -> list[dict]:
    """Apply the per-category floor and ceiling that the cheapest/priciest
    podium uses, including a ratio guard against the national median to
    eliminate per-mg-vs-per-vial unit encoding glitches.

    When ``code_meta`` is provided we also enforce a description match: the
    hospital's representative line item must plausibly describe the code,
    which catches MRFs that publish a surgical plate or bone screw under a
    flu vaccine code. See ``description_filter.description_matches_code``.

    Returning the same set everywhere keeps the headline stats, the percentile
    chart, the state breakdowns, and the leaderboard consistent.
    """
    floor = _ranking_floor(code_system or "", category or "", code or "", national_median)
    ceiling = _ranking_ceiling(code_system or "", category or "", national_median)
    out: list[dict] = []
    for h in hospitals:
        m = h.get("median")
        if m is None:
            continue
        try:
            mv = float(m)
        except (TypeError, ValueError):
            continue
        if mv < floor or mv > ceiling:
            continue
        if code_meta is not None:
            rep = h.get("representative") or {}
            desc = rep.get("description")
            if not description_matches_code(code_meta, desc):
                continue
        out.append(h)
    return out


def _load_cms_asp() -> dict[str, dict]:
    """Load the CMS Part B ASP payment limit table if it's been
    downloaded into ``data/cms_asp/``. Returns ``{HCPCS code: {limit,
    dose_qty, dose_unit, dose_text}}`` or an empty dict if the file is
    missing. Pre-existing scripts already produce ``samples/cms_asp_check.json``
    -- we re-parse the source CSV here to keep ``analysis.py`` self
    contained and to avoid a stale samples file silently disagreeing
    with the published CMS file.
    """
    import csv as _csv
    import re as _re

    csv_path = REPO_ROOT / "data" / "cms_asp" / (
        "section 5208 version of April 2026 Medicare Part B Payment Limit "
        "File 033026.csv"
    )
    if not csv_path.exists():
        return {}
    text = csv_path.read_text(encoding="latin-1")
    out: dict[str, dict] = {}
    in_data = False
    for row in _csv.reader(text.splitlines()):
        if not row:
            continue
        if not in_data:
            if row and row[0].strip() == "HCPCS Code":
                in_data = True
            continue
        if len(row) < 4 or not row[0].strip():
            continue
        code = row[0].strip()
        try:
            limit = float(row[3].strip())
        except ValueError:
            continue
        dose_text = row[2].strip()
        m = _re.match(r"\s*([0-9]*\.?[0-9]+)\s*([A-Za-z]+)", dose_text)
        dose_qty = float(m.group(1)) if m else None
        dose_unit = m.group(2).lower() if m else None
        out[code] = {
            "payment_limit": limit,
            "dose_qty": dose_qty,
            "dose_unit": dose_unit,
            "dose_text": dose_text,
            "short_description": row[1].strip(),
        }
    return out


def main() -> None:
    reduced_path = SAMPLES / "hpt_reduced.json"
    if not reduced_path.is_file():
        print("Run reduce.py first")
        return
    reduced = json.loads(reduced_path.read_text(encoding="utf-8"))
    cms_asp = _load_cms_asp()

    # Load hospital index up front so we can attach the source MRF URL and
    # cleaner city/state values to every hospital card we render. This is the
    # "show me where this came from" link on the website.
    hospitals_full = load_hospitals(
        include_tpafs=True, include_dolthub=True, include_oria=True
    )
    mrf_by_id: dict[str, str] = {}
    city_by_id: dict[str, str] = {}
    state_by_id: dict[str, str] = {}
    for h in hospitals_full:
        hid = h.get("hospital_id")
        if not hid:
            continue
        url = h.get("mrf_url")
        if url:
            mrf_by_id[hid] = url
        c = _clean_city(h.get("city"))
        if c:
            city_by_id[hid] = c
        s = h.get("state")
        if s:
            state_by_id[hid] = s

    code_summary = []
    eligible_state_stats: dict[str, dict[str, dict]] = {}
    # Per-hospital detail rollup: { hospital_id -> [ { code_system, code,
    # display_name, category, billing_unit, median, line_item } ] }.
    # We build this in the same loop where we iterate codes so we don't
    # have to walk the reduced file twice. The output is written as one
    # JSON file per hospital under frontend/public/data/hospitals/ so
    # the React app can fetch a single hospital's full coverage on
    # demand without loading every hospital at once.
    hospital_codes: dict[str, list[dict]] = {}
    for meta in CODES:
        key = f"{meta['code_system']}:{meta['code']}"
        raw_stats = reduced["codes"].get(key, {}) or {}

        # One source of truth for everything on the page: the same eligible
        # hospital pool that the cheapest/priciest podium uses. Headline stats,
        # the percentile chart, state breakdowns, and the spread leaderboard
        # all derive from this so a reader never sees the chart's P10 sit
        # below the cheapest hospital on the same page.
        all_hospitals_for_code: list[dict] = list(
            reduced.get("hospitals_by_code", {}).get(key, []) or []
        )

        # Anchor the ratio guard on the median of per-hospital medians, which
        # is robust to a handful of $1/$5 chargemaster placeholders. Two
        # passes: rough filter against the seed median, then a final filter
        # against the post-filter median so the ratio guard converges around
        # the real distribution rather than chasing the noise.
        hosp_medians = sorted(
            float(h["median"]) for h in all_hospitals_for_code
            if h.get("median") and float(h["median"]) > 0
        )
        seed_median = (
            hosp_medians[len(hosp_medians) // 2] if hosp_medians else None
        ) or raw_stats.get("median")
        rough = _filter_eligible(
            all_hospitals_for_code,
            meta.get("code_system") or "",
            meta.get("category") or "",
            meta.get("code") or "",
            seed_median,
            code_meta=meta,
        )
        rough_medians = sorted(
            float(h["median"]) for h in rough
            if h.get("median") and float(h["median"]) > 0
        )
        anchor_median = (
            rough_medians[len(rough_medians) // 2] if rough_medians else seed_median
        )
        eligible = _filter_eligible(
            all_hospitals_for_code,
            meta.get("code_system") or "",
            meta.get("category") or "",
            meta.get("code") or "",
            anchor_median,
            code_meta=meta,
        )
        eligible.sort(key=lambda h: float(h.get("median") or 0))

        eligible_stats = _stats_from_hospital_medians(eligible)
        if eligible_stats is None:
            stats: dict = {}
        else:
            stats = {
                "count": eligible_stats["count"],
                "min": _round_money(eligible_stats["min"]),
                "max": _round_money(eligible_stats["max"]),
                "mean": _round_money(eligible_stats["mean"]),
                "median": _round_money(eligible_stats["median"]),
                "p10": _round_money(eligible_stats["p10"]),
                "p25": _round_money(eligible_stats["p25"]),
                "p75": _round_money(eligible_stats["p75"]),
                "p90": _round_money(eligible_stats["p90"]),
                "abs_min": _round_money(raw_stats.get("min")),
                "abs_max": _round_money(raw_stats.get("max")),
                "raw_observations": raw_stats.get("count"),
            }

        cheapest_in_state: dict[str, dict] = {}
        priciest_in_state: dict[str, dict] = {}
        per_state_eligible: dict[str, list[dict]] = {}
        for st, hospitals in (reduced.get("hospitals_by_code_by_state") or {}).get(key, {}).items():
            # Use the national anchor for the ratio guard so a single-hospital
            # state cannot bend the floor around its own median.
            st_eligible = _filter_eligible(
                hospitals or [],
                meta.get("code_system") or "",
                meta.get("category") or "",
                meta.get("code") or "",
                anchor_median,
                code_meta=meta,
            )
            if not st_eligible:
                continue
            st_eligible.sort(key=lambda h: float(h.get("median") or 0))
            per_state_eligible[st] = st_eligible

            cheapest = st_eligible[0]
            priciest = st_eligible[-1]
            cheapest_hid = cheapest.get("hospital_id")
            priciest_hid = priciest.get("hospital_id")

            def _state_card(h: dict) -> dict:
                hid = h.get("hospital_id")
                card: dict = {
                    "name": _clean_hospital_name(h.get("name")),
                    "city": _clean_city(h.get("city")) or city_by_id.get(hid),
                    "median": _round_money(h.get("median")),
                    "hospital_id": hid,
                    "mrf_url": mrf_by_id.get(hid),
                }
                rep = h.get("representative") or {}
                if rep:
                    card["line_item"] = _build_line_item(rep)
                return card

            cheapest_in_state[st] = _state_card(cheapest)
            priciest_in_state[st] = _state_card(priciest)

        def _hospital_card(h: dict) -> dict:
            hid = h.get("hospital_id")
            card: dict = {
                "hospital_id": hid,
                "name": _clean_hospital_name(h.get("name")),
                "city": _clean_city(h.get("city")) or city_by_id.get(hid),
                "state": h.get("state") or state_by_id.get(hid),
                "median": _round_money(h.get("median")),
                "count": h.get("count") or 0,
                "mrf_url": mrf_by_id.get(hid),
            }
            rep = h.get("representative") or {}
            if rep:
                card["line_item"] = _build_line_item(rep)
            return card

        # Dedupe priciest from cheapest so the same hospital never appears in
        # both lists when there are only a handful of eligible hospitals.
        top_cheapest = [_hospital_card(h) for h in eligible[:3]]
        cheapest_ids = {h["hospital_id"] for h in top_cheapest}
        priciest_pool = [
            h for h in reversed(eligible) if h.get("hospital_id") not in cheapest_ids
        ]
        top_priciest = [_hospital_card(h) for h in priciest_pool[:3]]

        for st, st_eligible in per_state_eligible.items():
            st_stats = _stats_from_hospital_medians(st_eligible)
            if st_stats is None:
                continue
            eligible_state_stats.setdefault(st, {})[key] = {
                "median": _round_money(st_stats["median"]),
                "min": _round_money(st_stats["min"]),
                "max": _round_money(st_stats["max"]),
                "mean": _round_money(st_stats["mean"]),
                "count": st_stats["count"],
                "p10": _round_money(st_stats["p10"]),
                "p25": _round_money(st_stats["p25"]),
                "p75": _round_money(st_stats["p75"]),
                "p90": _round_money(st_stats["p90"]),
            }

        # Pull the HCPCS billing unit out of the code display name so the
        # frontend can stamp every price (headline grid, cheapest/priciest
        # cards, percentile chart) with the unit those numbers were
        # standardized to. We only set it when the display name actually
        # encodes a "per X" expression -- regular CPT/MS-DRG codes don't
        # carry a per-unit semantic.
        from reduce import _parse_hcpcs_unit
        hcpcs = _parse_hcpcs_unit(meta.get("display_name") or "")
        billing_unit = (
            _format_dose(hcpcs[0], hcpcs[1]) if hcpcs is not None else None
        )

        # Cross-reference against the CMS Part B ASP payment limit so the
        # frontend can show readers "Medicare pays $X per unit, this
        # hospital lists $Y" side by side. Only meaningful for HCPCS
        # codes that have an ASP entry (drug J-codes do, surgical CPT
        # codes don't).
        cms_ref = None
        if billing_unit and meta.get("code_system") == "HCPCS":
            cms_entry = cms_asp.get(meta.get("code") or "")
            if cms_entry and hcpcs is not None:
                our_qty, our_unit = hcpcs
                cms_qty = cms_entry.get("dose_qty")
                cms_unit = cms_entry.get("dose_unit")
                cms_limit = cms_entry.get("payment_limit") or 0.0
                cms_per_our_unit = None
                if cms_qty and cms_unit == our_unit:
                    cms_per_our_unit = cms_limit * (our_qty / cms_qty)
                elif cms_qty and {cms_unit, our_unit} == {"mg", "mcg"}:
                    factor = 1000.0 if cms_unit == "mg" else 1 / 1000.0
                    cms_per_our_unit = cms_limit * (our_qty / cms_qty) * factor
                if cms_per_our_unit is not None:
                    cms_ref = {
                        "source": "CMS Medicare Part B ASP, April 2026",
                        "payment_limit": _round_money(cms_limit),
                        "dose_text": cms_entry.get("dose_text"),
                        "per_billing_unit": _round_money(cms_per_our_unit),
                    }
                    median_val = stats.get("median") if stats else None
                    if median_val and cms_per_our_unit > 0:
                        cms_ref["chargemaster_to_cms_ratio"] = round(
                            float(median_val) / cms_per_our_unit, 2
                        )

        code_summary.append(
            {
                **meta,
                "billing_unit": billing_unit,
                "cms_reference": cms_ref,
                "stats": stats,
                "cheapest_in_state": cheapest_in_state,
                "priciest_in_state": priciest_in_state,
                "top_cheapest": top_cheapest,
                "top_priciest": top_priciest,
                "ranking_eligible_count": len(eligible),
            }
        )

        # Per-hospital rollup: every hospital that priced this code gets
        # an entry pointing back at it. We use the same per-state lists
        # the cheapest/priciest podiums were drawn from so the prices a
        # reader sees on the per-hospital profile match exactly what
        # they'd see on the per-code page (same eligibility filter,
        # same "drop placeholders" rules, same per-unit normalization).
        for st, hosps in (reduced.get("hospitals_by_code_by_state") or {}).get(key, {}).items():
            for h in (hosps or []):
                hid = h.get("hospital_id")
                if not hid:
                    continue
                rep = h.get("representative") or {}
                # We deliberately do NOT include cms_reference here --
                # it's identical across hospitals for the same code and
                # already lives on code_summary.json, so the frontend
                # joins on (code_system, code) at render time. Keeping
                # the per-hospital file slim matters: 3.3K hospitals x
                # ~50KB each is the difference between a 60MB and 250MB
                # static site.
                hospital_codes.setdefault(hid, []).append(
                    {
                        "code_system": meta["code_system"],
                        "code": meta["code"],
                        "display_name": meta["display_name"],
                        "category": meta["category"],
                        "setting": meta.get("setting"),
                        "billing_unit": billing_unit,
                        "median": _round_money(h.get("median")),
                        "count": h.get("count"),
                        "line_item": _build_line_item(rep) if rep else None,
                    }
                )

    # Spread leaderboard. We use P10..P90 for the "real" spread (rejects rogue
    # chargemaster placeholders and per-mg vs per-vial pharma encoding bugs that
    # otherwise dominate the top of the list with $5 minimums or $0.17 NDC
    # outliers). Fall back to min/max only for codes where percentiles aren't
    # reported. Skip codes with too few price points to be meaningful.
    spread = []
    for c in code_summary:
        st = c.get("stats") or {}
        if not st or st.get("median") in (None, 0):
            continue
        n = int(st.get("count") or 0)
        if n < 20:
            continue
        med = float(st["median"])
        p10 = float(st.get("p10") or 0) or None
        p90 = float(st.get("p90") or 0) or None
        if p10 and p90 and p10 > 0 and p90 > p10:
            lo = p10
            hi = p90
        else:
            lo = float(st.get("min") or med)
            hi = float(st.get("max") or med)
        # Floors per code system to avoid clearly-broken minimums
        if c["code_system"] == "MS-DRG":
            lo = max(lo, 1000.0)
        elif c["code_system"] in ("CPT", "HCPCS"):
            lo = max(lo, 20.0)
        elif c["code_system"] == "NDC":
            lo = max(lo, 0.50)
        spread_ratio = (hi / lo) if lo > 0 else None
        if spread_ratio is None or spread_ratio < 1.5:
            continue
        # Cap displayed ratio so a single bad data point doesn't headline the page
        if spread_ratio > 200:
            continue
        spread.append(
            {
                "key": f"{c['code_system']}:{c['code']}",
                "display_name": c["display_name"],
                "spread_ratio": spread_ratio,
                "dollar_spread": hi - lo,
                "lowest": _round_money(lo),
                "highest": _round_money(hi),
                "median": _round_money(med),
                "mean": st.get("mean"),
                "category": c["category"],
                "count": n,
            }
        )
    spread.sort(key=lambda x: (x["spread_ratio"] is None, -(x["spread_ratio"] or 0)))
    spread_leaderboard = spread[:50]

    state_summary: dict = {}
    for st, code_map in eligible_state_stats.items():
        if not code_map:
            continue
        state_summary[st] = {"codes": code_map}

    hosp_in = reduced.get("hospitals", {})
    hospital_summary = []
    for hid, payload in hosp_in.items():
        hospital_summary.append(
            {
                "hospital_id": hid,
                "state": payload.get("state"),
                "honesty_score": payload.get("honesty_score"),
                "codes_with_prices": len(payload.get("per_code") or {}),
            }
        )

    hospital_index = []
    for h in hospitals_full:
        hid = h["hospital_id"]
        reduced_h = hosp_in.get(hid, {})
        # Two possible counts here. The raw count from the reduce step
        # (``per_code`` dict) is "every code this hospital priced",
        # including rows that get dropped later by the description
        # filter or chargemaster placeholder rejection. The per-hospital
        # detail file we ship to the frontend only contains
        # post-filter rows. Show the post-filter count on the index so
        # the number on the list matches the number on the profile
        # (otherwise you get an off-by-N where a hospital says "296
        # codes priced" on the list but only 295 rows render on the
        # profile).
        codes_covered = len(hospital_codes.get(hid) or [])
        if codes_covered <= 0:
            continue
        hospital_index.append(
            {
                **{
                    k: h.get(k)
                    for k in (
                        "hospital_id",
                        "name",
                        "state",
                        "city",
                        "ccn",
                        "system",
                        "latitude",
                        "longitude",
                        "mrf_url",
                    )
                },
                "codes_covered": codes_covered,
                "honesty_score": reduced_h.get("honesty_score"),
            }
        )

    scale_path = SAMPLES / "hpt_scale_summary.json"
    scale = json.loads(scale_path.read_text(encoding="utf-8")) if scale_path.is_file() else {}

    obs_dir = REPO_ROOT / "scratch" / "hpt" / "observations"
    real_obs_rows = 0
    if obs_dir.is_dir():
        for fp in obs_dir.glob("*.jsonl"):
            with open(fp, "r", encoding="utf-8") as f:
                for _ in f:
                    real_obs_rows += 1
    if real_obs_rows:
        scale = {**scale, "observation_rows_reported": real_obs_rows}

    run_metadata = {
        "pipeline": "hospital-price-reality-check",
        "code_list_version": str(len(CODES)),
        "hospitals_indexed": len(hospitals_full),
        "hospitals_attempted": len(hospitals_full),
        "hospitals_with_data": sum(1 for h in hospital_index if (h.get("codes_covered") or 0) > 0),
        "observation_files": reduced.get("observation_files"),
        "unique_code_keys_in_reduce": len(reduced.get("codes") or {}),
        "scale_summary": scale,
        "data_source_note": (
            "Data is parsed directly from hospitals' federal Hospital Price Transparency "
            "machine readable files. No synthetic prices, no backfill, no scatter. "
            "If a hospital does not publish a code, that code simply does not appear "
            "in their column."
        ),
    }

    FRONT_DATA.mkdir(parents=True, exist_ok=True)
    SAMPLES.mkdir(parents=True, exist_ok=True)
    payloads = {
        "code_summary.json": code_summary,
        "state_summary.json": state_summary,
        "hospital_summary.json": hospital_summary,
        "hospital_index.json": hospital_index,
        "spread_leaderboard.json": spread_leaderboard,
        "run_metadata.json": run_metadata,
    }
    for fname, payload in payloads.items():
        (FRONT_DATA / fname).write_text(json.dumps(payload, indent=2), encoding="utf-8")
        (SAMPLES / fname).write_text(json.dumps(payload, indent=2), encoding="utf-8")

    # Per-hospital detail files. One JSON per hospital, written under
    # frontend/public/data/hospitals/{hospital_id}.json. The hospital
    # profile page fetches a single file at click time instead of
    # loading every hospital's data into the index. Hospitals with no
    # priced codes are skipped (matches the codes_covered>0 filter on
    # the index).
    hospitals_dir = FRONT_DATA / "hospitals"
    hospitals_dir.mkdir(parents=True, exist_ok=True)
    # Wipe stale files from prior runs so a hospital that disappears
    # from the dataset doesn't keep serving an old detail file.
    for stale in hospitals_dir.glob("*.json"):
        stale.unlink()

    hospital_meta_by_id: dict[str, dict] = {h.get("hospital_id"): h for h in hospital_index}
    written = 0
    for hid, codes in hospital_codes.items():
        if not codes:
            continue
        meta = hospital_meta_by_id.get(hid) or {}
        # Sort each hospital's codes by category, then display name, so
        # the profile page can group them naturally.
        codes_sorted = sorted(
            codes,
            key=lambda c: (
                c.get("category") or "zzz",
                (c.get("display_name") or "").lower(),
            ),
        )
        # Per-category coverage tally for the hospital header.
        cat_counts: dict[str, int] = {}
        for c in codes_sorted:
            cat = c.get("category") or "other"
            cat_counts[cat] = cat_counts.get(cat, 0) + 1

        detail = {
            "hospital_id": hid,
            "name": _clean_hospital_name(meta.get("name")),
            "system": meta.get("system"),
            "city": _clean_city(meta.get("city")),
            "state": meta.get("state"),
            "ccn": meta.get("ccn"),
            "mrf_url": meta.get("mrf_url"),
            "codes_covered": len(codes_sorted),
            "category_counts": cat_counts,
            "honesty_score": meta.get("honesty_score"),
            "codes": codes_sorted,
        }
        # Compact JSON (no indent) -- the per-hospital files are not
        # meant to be read by humans, only by the frontend, and the
        # 50%+ size reduction matters for shipping ~3.3K of them.
        (hospitals_dir / f"{hid}.json").write_text(
            json.dumps(detail, separators=(",", ":")), encoding="utf-8"
        )
        written += 1

    print(
        f"wrote frontend/public/data/*.json and samples/*.json "
        f"({len(code_summary)} codes, {len(hospital_index)} hospitals, "
        f"{written} per-hospital detail files)"
    )


if __name__ == "__main__":
    main()
