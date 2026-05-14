#!/usr/bin/env python3
"""Compact per-hospital chargemaster JSONLs into gzipped JSON files the
frontend can lazy-load for the per-hospital 'Full chargemaster' search view.

Inputs (one of):
  • /workspace/shared/hpt/chargemaster/{hospital_id}.jsonl  (on a Burla worker)
  • samples/chargemaster_raw/{hospital_id}.jsonl           (locally synced copy)

Outputs:
  • frontend/public/data/chargemaster/{hospital_id}.json.gz
      one compact JSON per hospital. Field keys are short to keep wire size down.
      The browser decompresses with DecompressionStream('gzip') on demand.
  • frontend/public/data/chargemaster_index.json
      { "count": N, "hospitals": [ { "hospital_id", "rows", "kb" }, ... ] }
      Used to drive the index/search affordance.

Field keys (all optional except cs+c, all numbers rounded to 2dp):
  d  description
  cs code_system  (CPT / HCPCS / MS-DRG / NDC / CDT / RC / ...)
  c  code
  ds dose         (e.g. "10 mg", "100 mL")
  se setting      (inpatient / outpatient / both / ...)
  u  billing_unit (per-unit label like "10 mg" or "each")
  g  gross_charge
  ca discounted_cash
  mn min_allowed
  mx max_allowed
  p  price_per_unit (normalized: charge / dose_units_in_per_unit)
"""
from __future__ import annotations

import gzip
import json
import os
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent
SAMPLES = REPO_ROOT / "samples"
FRONT_DATA = REPO_ROOT / "frontend" / "public" / "data"

try:
    from dosage_extractor import extract_dose_from_description  # type: ignore
except Exception:
    def extract_dose_from_description(_desc):  # type: ignore[no-redef]
        return None


def _format_dose(parsed) -> str | None:
    """Render the (qty, unit) tuple from dosage_extractor as 'X mg' / '100 mL' /
    etc. for display. Mirrors what the curated table shows so users don't see
    two different formats."""
    if not parsed:
        return None
    qty, unit = parsed
    if qty is None or unit is None:
        return None
    try:
        if float(qty).is_integer():
            qty_str = str(int(qty))
        else:
            qty_str = f"{qty:g}"
    except Exception:
        qty_str = str(qty)
    unit_disp = {
        "mg": "mg",
        "mcg": "mcg",
        "ml": "mL",
        "g": "g",
        "unit": "unit",
        "each": "each",
    }.get(str(unit).lower(), str(unit))
    return f"{qty_str} {unit_disp}"


def _round_money(v):
    if v is None:
        return None
    try:
        n = float(v)
    except (ValueError, TypeError):
        return None
    if n != n or n in (float("inf"), float("-inf")):
        return None
    return round(n, 2)


def _compact_row(r: dict) -> dict:
    description = (r.get("description") or "").strip() or None
    # Parsers don't extract dose themselves (that lives in reduce.py for the
    # curated path) so we fall back to dosage_extractor here so drug rows are
    # legible in the search results.
    dose_display = r.get("dose")
    if not dose_display and description:
        dose_display = _format_dose(extract_dose_from_description(description))
    out = {
        "d": description,
        "cs": r.get("code_system"),
        "c": r.get("code"),
        "ds": dose_display,
        "se": r.get("setting"),
        "u": r.get("billing_unit"),
        "g": _round_money(r.get("gross_charge")),
        "ca": _round_money(r.get("discounted_cash")),
        "mn": _round_money(r.get("min_allowed")),
        "mx": _round_money(r.get("max_allowed")),
        "p": _round_money(
            r.get("price_per_unit")
            or r.get("gross_charge_per_unit")
            or r.get("discounted_cash_per_unit")
        ),
    }
    return {k: v for k, v in out.items() if v not in (None, "")}


def _load_hospital_meta() -> dict[str, dict]:
    """Best-effort: return { hospital_id: {name, state, mrf_url} } from the
    same hospital index the parser used. Tolerates a missing index file
    (e.g. local-only build where the index lives in shared FS only)."""
    try:
        from hospital_index import load_hospitals  # type: ignore

        return {h["hospital_id"]: h for h in load_hospitals()}
    except Exception as e:
        print(f"WARN: could not load hospital index ({e}); names will be blank")
        return {}


def _resolve_input_dir() -> Path:
    """Prefer the shared FS path when present (we're on a Burla worker); fall
    back to a local samples/chargemaster_raw copy synced down from the cluster.
    """
    shared_full = Path("/workspace/shared/hpt/chargemaster_full")
    if shared_full.is_dir():
        return shared_full
    shared_legacy = Path("/workspace/shared/hpt/chargemaster")
    if shared_legacy.is_dir():
        return shared_legacy
    return SAMPLES / "chargemaster_raw"


def _resolve_output_dir() -> Path:
    """Allow CHARGEMASTER_OUT_DIR to redirect the output (Burla worker case)."""
    override = os.environ.get("CHARGEMASTER_OUT_DIR")
    if override:
        return Path(override)
    return FRONT_DATA / "chargemaster"


def main() -> None:
    in_dir = _resolve_input_dir()
    if not in_dir.is_dir():
        print(f"no chargemaster input dir at {in_dir}; nothing to do")
        return
    out_dir = _resolve_output_dir()
    out_dir.mkdir(parents=True, exist_ok=True)
    hospitals = _load_hospital_meta()

    index: list[dict] = []
    total_in = 0
    total_out_bytes = 0
    for jsonl_path in sorted(in_dir.glob("*.jsonl")):
        hospital_id = jsonl_path.stem
        if jsonl_path.stat().st_size == 0:
            continue
        h = hospitals.get(hospital_id, {})
        out_path = out_dir / f"{hospital_id}.json.gz"
        n_rows = 0
        try:
            with gzip.open(out_path, "wt", encoding="utf-8") as fout:
                fout.write('{"hospital_id":')
                fout.write(json.dumps(hospital_id))
                fout.write(',"name":')
                fout.write(json.dumps(h.get("name")))
                fout.write(',"state":')
                fout.write(json.dumps(h.get("state")))
                fout.write(',"mrf_url":')
                fout.write(json.dumps(h.get("mrf_url")))
                fout.write(',"truncated":false,"rows":[')
                first = True
                with open(jsonl_path, "r", encoding="utf-8") as fin:
                    for line in fin:
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            compacted = _compact_row(json.loads(line))
                        except json.JSONDecodeError:
                            continue
                        if not first:
                            fout.write(",")
                        json.dump(compacted, fout, separators=(",", ":"))
                        first = False
                        n_rows += 1
                fout.write('],"total":')
                fout.write(str(n_rows))
                fout.write("}")
        except OSError as e:
            print(f"{hospital_id}: write error {e}", file=sys.stderr)
            try:
                out_path.unlink(missing_ok=True)
            except OSError:
                pass
            continue
        if n_rows == 0:
            try:
                out_path.unlink(missing_ok=True)
            except OSError:
                pass
            continue
        total_in += n_rows
        sz_kb = out_path.stat().st_size / 1024
        total_out_bytes += out_path.stat().st_size
        index.append(
            {"hospital_id": hospital_id, "rows": n_rows, "kb": round(sz_kb, 1)}
        )
        if len(index) % 200 == 0:
            print(f"  built {len(index)} hospitals…", flush=True)

    index_path = out_dir.parent / "chargemaster_index.json"
    index_path.write_text(
        json.dumps(
            {
                "count": len(index),
                "total_rows": total_in,
                "total_bytes": total_out_bytes,
                "hospitals": index,
            },
            separators=(",", ":"),
        ),
        encoding="utf-8",
    )
    print(
        f"done: {len(index)} hospitals, {total_in:,} rows, "
        f"{total_out_bytes / 1024 / 1024:.1f} MB compressed -> {out_dir}"
    )


if __name__ == "__main__":
    main()
