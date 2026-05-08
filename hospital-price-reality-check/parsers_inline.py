"""Single-file inlined parsers (CMS Hospital Price Transparency MRFs).

This is the same code as ``parsers/__init__.py`` plus the four submodules,
flattened into one Python module so Burla's auto-import detection always ships
the parser code with ``parse_hospital_mrf``. The ``parsers/`` subpackage in this
repo is kept for local development / testability; production runs go through
this module.
"""
from __future__ import annotations

import csv
import io
import json
import zipfile
from pathlib import Path
from typing import Iterator, TextIO

csv.field_size_limit(2**27)

try:
    import ijson  # type: ignore
except Exception:  # pragma: no cover
    ijson = None


KNOWN_ZIP_SUFFIXES = (".json", ".csv", ".txt", ".tsv", ".xlsx", ".xls", ".xlsm")


def _norm_sys(s: str) -> str:
    u = (s or "").upper().replace(" ", "").replace("_", "")
    if u in ("MSDRG", "MS-DRG"):
        return "MS-DRG"
    return u


def _norm_code(c: str) -> str:
    return str(c or "").replace(".", "").replace(" ", "").replace("-", "").strip()


def _in_targets(system: str, code: str, targets: set[tuple[str, str]]) -> tuple[str, str] | None:
    sysn = _norm_sys(system)
    coden = _norm_code(code)
    if not coden:
        return None
    for p in (
        (sysn, coden),
        ("CPT", coden),
        ("HCPCS", coden),
        ("MS-DRG", coden),
        ("NDC", coden),
        ("CDT", coden),
    ):
        if p in targets:
            return p
    return None


def _to_float(s) -> float | None:
    if s is None:
        return None
    if isinstance(s, (int, float)):
        return float(s) if s > 0 else None
    t = str(s).strip().replace("$", "").replace(",", "")
    if not t or t.upper() in ("N/A", "NA", "NULL", "NONE", "-", "TBD"):
        return None
    try:
        v = float(t)
    except ValueError:
        return None
    return v if v > 0 else None


def _norm_header(s: str) -> str:
    s = (s or "").strip().lower()
    while " | " in s:
        s = s.replace(" | ", "|")
    while " |" in s:
        s = s.replace(" |", "|")
    while "| " in s:
        s = s.replace("| ", "|")
    return " ".join(s.split())


def _detect_header(rows: list[list[str]]) -> int:
    for i, row in enumerate(rows[:60]):
        cells = [_norm_header(c) for c in row if isinstance(c, str)]
        has_desc = any(c == "description" for c in cells)
        has_code = any(c == "code" or c.startswith("code|") for c in cells)
        if has_desc and has_code:
            return i
    for i, row in enumerate(rows[:60]):
        cells = [_norm_header(c) for c in row if isinstance(c, str)]
        if any(c in ("description", "service description", "procedure description") for c in cells):
            return i
    return -1


def _find_header_index(hmap: dict[str, int], *needles: str) -> int | None:
    keys = list(hmap.keys())
    lowered = [(k, _norm_header(k)) for k in keys]
    for n in needles:
        nl = _norm_header(n)
        for k, kl in lowered:
            if kl == nl:
                return hmap[k]
    for n in needles:
        nl = _norm_header(n)
        for k, kl in lowered:
            if nl in kl:
                return hmap[k]
    return None


def _classify_payer_columns(header: list[str]) -> list[int]:
    out: list[int] = []
    for i, h in enumerate(header):
        if not h:
            continue
        kl = _norm_header(h)
        if kl.startswith("standard_charge|") and kl.endswith("|negotiated_dollar"):
            mid = kl[len("standard_charge|") : -len("|negotiated_dollar")]
            if mid not in ("min", "max", "gross", "discounted_cash"):
                out.append(i)
    if out:
        return out
    for i, h in enumerate(header):
        kl = (h or "").lower()
        if any(t in kl for t in ("aetna", "cigna", "blue cross", "anthem", "humana", "united", "kaiser", "tricare")):
            if "min" not in kl and "max" not in kl and "discount" not in kl:
                out.append(i)
    return out


def _find_code_columns(header: list[str]) -> list[tuple[int, int]]:
    code_indices: dict[str, int] = {}
    type_indices: dict[str, int] = {}
    for i, h in enumerate(header):
        if not h:
            continue
        kl = _norm_header(h)
        if kl == "code":
            code_indices.setdefault("0", i)
        elif kl == "code|1" or (kl.startswith("code|") and not kl.endswith("|type") and kl[5:].isdigit()):
            n = kl.split("|")[1] if "|" in kl else "0"
            code_indices.setdefault(n, i)
        elif kl.startswith("code|") and kl.endswith("|type") and len(kl.split("|")) == 3:
            n = kl.split("|")[1]
            type_indices.setdefault(n, i)
        elif kl in ("cpt code", "hcpcs code", "procedure code", "procedure"):
            code_indices.setdefault("0", i)
    pairs: list[tuple[int, int]] = []
    for n, ci in code_indices.items():
        ti = type_indices.get(n)
        pairs.append((ci, ti if ti is not None else -1))
    return pairs


def _peek_preamble(path: Path, max_bytes: int = 256_000) -> tuple[str, list[list[str]]]:
    with open(path, "rb") as fb:
        head = fb.read(max_bytes).decode("utf-8", errors="replace")
    delimiter = "\t" if "\t" in head[:2000] and head.count("\t") > 4 * head.count(",") else ","
    rows = list(csv.reader(io.StringIO(head), delimiter=delimiter))
    return delimiter, rows[:60]


def _open_skipping_lines(path: Path, skip_lines: int) -> TextIO:
    f = open(path, "r", encoding="utf-8", errors="replace", newline="")
    for _ in range(skip_lines):
        f.readline()
    return f


def _csv_iter_priced_items(
    path: Path,
    target_codes: set[tuple[str, str]],
    max_rows: int = 20_000_000,
) -> Iterator[dict]:
    delimiter, preamble = _peek_preamble(path)
    if not preamble:
        return
    hi = _detect_header(preamble)
    if hi < 0:
        return
    header = preamble[hi]
    hmap = {h.strip(): i for i, h in enumerate(header) if h and h.strip()}

    idx_desc = _find_header_index(hmap, "description", "service description", "procedure description", "item")
    if idx_desc is None:
        return
    code_pairs = _find_code_columns(header)
    if not code_pairs:
        return

    idx_setting = _find_header_index(hmap, "setting", "billing_class", "billing class")
    idx_gross = _find_header_index(hmap, "standard_charge|gross", "gross charge", "charge", "standard charge")
    idx_cash = _find_header_index(hmap, "standard_charge|discounted_cash", "discounted cash", "self pay", "cash price", "cash discount")
    idx_min = _find_header_index(hmap, "standard_charge|min", "deidentified_min_allowed", "deidentified minimum", "de-identified minimum", "min")
    idx_max = _find_header_index(hmap, "standard_charge|max", "deidentified_max_allowed", "deidentified maximum", "de-identified maximum", "max")

    idx_payer_name = _find_header_index(hmap, "payer_name")
    idx_neg_dollar_tall = _find_header_index(hmap, "standard_charge|negotiated_dollar")

    payer_indices_wide = _classify_payer_columns(header)

    def cell(row: list[str], i: int | None) -> float | None:
        if i is None or i < 0 or i >= len(row):
            return None
        return _to_float(row[i])

    def match_row(row: list[str]) -> tuple[str, str] | None:
        for ci, ti in code_pairs:
            if ci >= len(row):
                continue
            code = row[ci].strip()
            ctype = row[ti].strip() if 0 <= ti < len(row) else ""
            m = _in_targets(ctype, code, target_codes)
            if m:
                return m
        return None

    fh = _open_skipping_lines(path, hi + 1)

    def _safe_iter(rdr):
        while True:
            try:
                row = next(rdr)
            except StopIteration:
                return
            except csv.Error:
                continue
            yield row

    try:
        raw_reader = csv.reader(fh, delimiter=delimiter)
        reader = _safe_iter(raw_reader)

        if idx_payer_name is not None and idx_neg_dollar_tall is not None:
            agg: dict[tuple[str, str, str], dict] = {}
            count = 0
            for row in reader:
                if max_rows and count >= max_rows:
                    break
                count += 1
                if not row or len(row) <= idx_desc:
                    continue
                m = match_row(row)
                if not m:
                    continue
                desc = row[idx_desc].strip() if idx_desc < len(row) else ""
                setting = row[idx_setting].strip().lower() if (idx_setting is not None and idx_setting < len(row)) else ""
                key = (m[0], m[1], setting)
                entry = agg.get(key)
                if entry is None:
                    entry = {
                        "code_system": m[0],
                        "code": m[1],
                        "description": desc,
                        "setting": setting,
                        "gross": None,
                        "cash": None,
                        "min": None,
                        "max": None,
                        "neg_min": None,
                        "neg_max": None,
                        "neg_count": 0,
                    }
                    agg[key] = entry
                else:
                    if not entry["description"] and desc:
                        entry["description"] = desc
                g = cell(row, idx_gross)
                c = cell(row, idx_cash)
                mi = cell(row, idx_min)
                ma = cell(row, idx_max)
                if g is not None:
                    entry["gross"] = g if entry["gross"] is None else min(entry["gross"], g)
                if c is not None:
                    entry["cash"] = c if entry["cash"] is None else min(entry["cash"], c)
                if mi is not None:
                    entry["min"] = mi if entry["min"] is None else min(entry["min"], mi)
                if ma is not None:
                    entry["max"] = ma if entry["max"] is None else max(entry["max"], ma)
                v = cell(row, idx_neg_dollar_tall)
                if v is not None:
                    entry["neg_min"] = v if entry["neg_min"] is None else min(entry["neg_min"], v)
                    entry["neg_max"] = v if entry["neg_max"] is None else max(entry["neg_max"], v)
                    entry["neg_count"] += 1

            for entry in agg.values():
                if not any(
                    (
                        entry["gross"],
                        entry["cash"],
                        entry["min"],
                        entry["max"],
                        entry["neg_min"],
                        entry["neg_max"],
                    )
                ):
                    continue
                yield {
                    "code_system": entry["code_system"],
                    "code": entry["code"],
                    "code_modifier": None,
                    "description": entry["description"],
                    "gross_charge": entry["gross"],
                    "discounted_cash": entry["cash"],
                    "payer_negotiated_min": entry["neg_min"],
                    "payer_negotiated_max": entry["neg_max"],
                    "minimum_reported": entry["min"],
                    "maximum_reported": entry["max"],
                    "billing_class": None,
                    "setting": entry["setting"],
                }
            return

        count = 0
        for row in reader:
            if max_rows and count >= max_rows:
                break
            count += 1
            if not row or len(row) <= idx_desc:
                continue
            m = match_row(row)
            if not m:
                continue
            desc = row[idx_desc].strip() if idx_desc < len(row) else ""
            gross = cell(row, idx_gross)
            cash = cell(row, idx_cash)
            min_rep = cell(row, idx_min)
            max_rep = cell(row, idx_max)
            neg_vals: list[float] = []
            for pi in payer_indices_wide:
                v = cell(row, pi)
                if v is not None:
                    neg_vals.append(v)
            neg_min = min(neg_vals) if neg_vals else None
            neg_max = max(neg_vals) if neg_vals else None
            if not any((gross, cash, min_rep, max_rep, neg_min, neg_max)):
                continue

            yield {
                "code_system": m[0],
                "code": m[1],
                "code_modifier": None,
                "description": desc,
                "gross_charge": gross,
                "discounted_cash": cash,
                "payer_negotiated_min": neg_min,
                "payer_negotiated_max": neg_max,
                "minimum_reported": min_rep,
                "maximum_reported": max_rep,
                "billing_class": None,
                "setting": row[idx_setting].strip() if (idx_setting is not None and idx_setting < len(row)) else "",
            }
    finally:
        fh.close()


def _xlsx_iter_priced_items(
    path: Path,
    target_codes: set[tuple[str, str]],
    max_rows: int = 5_000_000,
) -> Iterator[dict]:
    try:
        import pandas as pd  # heavy; only imported on the xlsx path

        df = pd.read_excel(path, header=None, dtype=str)
    except Exception:
        return
    rows = df.fillna("").values.tolist()
    reader = [[str(c) for c in r] for r in rows]
    if not reader:
        return
    hi = _detect_header(reader)
    if hi < 0:
        return
    header = reader[hi]
    hmap = {h.strip(): i for i, h in enumerate(header) if str(h).strip()}

    idx_desc = _find_header_index(hmap, "description", "service description", "procedure description", "item")
    if idx_desc is None:
        return
    code_pairs = _find_code_columns(header)
    if not code_pairs:
        return

    idx_setting = _find_header_index(hmap, "setting", "billing_class", "billing class")
    idx_gross = _find_header_index(hmap, "standard_charge|gross", "gross charge", "charge", "standard charge")
    idx_cash = _find_header_index(hmap, "standard_charge|discounted_cash", "discounted cash", "self pay", "cash price", "cash discount")
    idx_min = _find_header_index(hmap, "standard_charge|min", "deidentified_min_allowed", "deidentified minimum", "de-identified minimum", "min")
    idx_max = _find_header_index(hmap, "standard_charge|max", "deidentified_max_allowed", "deidentified maximum", "de-identified maximum", "max")

    idx_payer_name = _find_header_index(hmap, "payer_name")
    idx_neg_dollar_tall = _find_header_index(hmap, "standard_charge|negotiated_dollar")
    payer_indices_wide = _classify_payer_columns(header)

    def cell(row: list[str], i: int | None) -> float | None:
        if i is None or i < 0 or i >= len(row):
            return None
        return _to_float(row[i])

    def match_row(row: list[str]) -> tuple[str, str] | None:
        for ci, ti in code_pairs:
            if ci >= len(row):
                continue
            code = str(row[ci]).strip()
            ctype = str(row[ti]).strip() if 0 <= ti < len(row) else ""
            m = _in_targets(ctype, code, target_codes)
            if m:
                return m
        return None

    if idx_payer_name is not None and idx_neg_dollar_tall is not None:
        agg: dict[tuple[str, str, str, str], dict] = {}
        count = 0
        for row in reader[hi + 1 :]:
            if max_rows and count >= max_rows:
                break
            if not row or len(row) <= idx_desc:
                continue
            m = match_row(row)
            if not m:
                continue
            desc = str(row[idx_desc]).strip() if idx_desc < len(row) else ""
            setting = str(row[idx_setting]).strip() if (idx_setting is not None and idx_setting < len(row)) else ""
            key = (m[0], m[1], desc, setting)
            entry = agg.setdefault(
                key,
                {
                    "code_system": m[0],
                    "code": m[1],
                    "description": desc,
                    "setting": setting,
                    "gross": None,
                    "cash": None,
                    "min": None,
                    "max": None,
                    "neg": [],
                },
            )
            entry["gross"] = entry["gross"] or cell(row, idx_gross)
            entry["cash"] = entry["cash"] or cell(row, idx_cash)
            entry["min"] = entry["min"] or cell(row, idx_min)
            entry["max"] = entry["max"] or cell(row, idx_max)
            v = cell(row, idx_neg_dollar_tall)
            if v is not None:
                entry["neg"].append(v)
            count += 1

        for entry in agg.values():
            neg = entry["neg"]
            if not any((entry["gross"], entry["cash"], entry["min"], entry["max"], neg)):
                continue
            yield {
                "code_system": entry["code_system"],
                "code": entry["code"],
                "code_modifier": None,
                "description": entry["description"],
                "gross_charge": entry["gross"],
                "discounted_cash": entry["cash"],
                "payer_negotiated_min": min(neg) if neg else None,
                "payer_negotiated_max": max(neg) if neg else None,
                "minimum_reported": entry["min"],
                "maximum_reported": entry["max"],
                "billing_class": None,
                "setting": entry["setting"],
            }
        return

    count = 0
    for row in reader[hi + 1 :]:
        if max_rows and count >= max_rows:
            break
        if len(row) <= idx_desc:
            continue
        m = match_row(row)
        if not m:
            continue
        desc = str(row[idx_desc]).strip()
        gross = cell(row, idx_gross)
        cash = cell(row, idx_cash)
        min_rep = cell(row, idx_min)
        max_rep = cell(row, idx_max)
        neg_vals: list[float] = []
        for pi in payer_indices_wide:
            v = cell(row, pi)
            if v is not None:
                neg_vals.append(v)
        neg_min = min(neg_vals) if neg_vals else None
        neg_max = max(neg_vals) if neg_vals else None
        if not any((gross, cash, min_rep, max_rep, neg_min, neg_max)):
            continue

        yield {
            "code_system": m[0],
            "code": m[1],
            "code_modifier": None,
            "description": desc,
            "gross_charge": gross,
            "discounted_cash": cash,
            "payer_negotiated_min": neg_min,
            "payer_negotiated_max": neg_max,
            "minimum_reported": min_rep,
            "maximum_reported": max_rep,
            "billing_class": None,
            "setting": str(row[idx_setting]).strip() if (idx_setting is not None and idx_setting < len(row)) else "",
        }
        count += 1


def _json_normalize_system(s: str | None) -> str:
    return _norm_sys(s or "")


def _json_normalize_code(c: str | None) -> str:
    return str(c or "").replace(".", "").replace(" ", "").strip()


def _json_code_in_targets(system: str, code: str, targets: set[tuple[str, str]]) -> tuple[str, str] | None:
    sysn = _json_normalize_system(system)
    coden = _json_normalize_code(code)
    if not coden:
        return None
    pairs = [
        (sysn, coden),
        ("CPT", coden),
        ("HCPCS", coden),
        ("MS-DRG", coden),
        ("NDC", coden),
        ("CDT", coden),
    ]
    for p in pairs:
        if p in targets:
            return p
    return None


_DRUG_TYPE_LABEL = {
    "GR": "g",
    "ME": "mg",
    "ML": "mL",
    "UN": "units",
    "F2": "international units",
    "EA": "each",
    "GM": "g",
}


def _format_drug_unit(drug_info: dict | None) -> str | None:
    """Render CMS v3 drug_information into a human-readable unit string.

    The CMS schema encodes the unit as a numeric magnitude plus a NCPDP type
    code (``GR`` grams, ``ME`` milligrams, ``ML`` milliliters, ``UN`` units,
    ``F2`` international units, ``EA`` each). Surfacing this lets readers see
    whether a $5,000 chemo line is actually per-vial vs per-mg, which is the
    most common cause of apparent price discrepancies between hospitals.
    """
    if not isinstance(drug_info, dict):
        return None
    raw_unit = drug_info.get("unit")
    raw_type = (drug_info.get("type") or "").strip().upper()
    if raw_unit is None and not raw_type:
        return None
    unit_str = ""
    if raw_unit is not None:
        try:
            num = float(raw_unit)
            if num == int(num):
                unit_str = str(int(num))
            else:
                unit_str = f"{num:g}"
        except (TypeError, ValueError):
            unit_str = str(raw_unit).strip()
    label = _DRUG_TYPE_LABEL.get(raw_type, raw_type.lower() if raw_type else "")
    parts = [p for p in (unit_str, label) if p]
    if not parts:
        return None
    return " ".join(parts)


def _json_flatten_v3_charges(
    item: dict, matched: tuple[str, str], desc: str
) -> Iterator[dict]:
    matched_sys, matched_code = matched
    drug_unit = _format_drug_unit(item.get("drug_information"))
    standard_charges = item.get("standard_charges") or []
    if not isinstance(standard_charges, list):
        return
    for sc in standard_charges:
        if not isinstance(sc, dict):
            continue
        setting = sc.get("setting") or sc.get("billing_class") or ""
        gross = _to_float(sc.get("gross_charge"))
        cash = _to_float(sc.get("discounted_cash"))
        neg_vals: list[float] = []
        for p in sc.get("payers_information") or []:
            if not isinstance(p, dict):
                continue
            v = _to_float(p.get("standard_charge_dollar"))
            if v is not None:
                neg_vals.append(v)
        neg_min = min(neg_vals) if neg_vals else None
        neg_max = max(neg_vals) if neg_vals else None
        minimum = _to_float(sc.get("minimum"))
        maximum = _to_float(sc.get("maximum"))
        if not any((gross, cash, neg_min, neg_max, minimum, maximum)):
            continue
        yield {
            "code_system": matched_sys,
            "code": matched_code,
            "code_modifier": None,
            "description": desc,
            "drug_unit": drug_unit,
            "gross_charge": gross,
            "discounted_cash": cash,
            "payer_negotiated_min": neg_min,
            "payer_negotiated_max": neg_max,
            "minimum_reported": minimum,
            "maximum_reported": maximum,
            "billing_class": sc.get("billing_class"),
            "setting": setting if isinstance(setting, str) else "",
        }


def _json_v3_item_iterator(path: Path) -> Iterator[dict]:
    if ijson is not None:
        # CMS-published JSON MRFs occasionally start with a UTF-8 BOM (Mount Sinai,
        # for instance, ships ``\ufeff`` as the first three bytes). ijson refuses
        # them as "invalid char in json text". Skip a BOM if we see one.
        def _open_no_bom(p: Path):
            f = open(p, "rb")
            head = f.read(3)
            if head != b"\xef\xbb\xbf":
                f.seek(0)
            return f

        f = _open_no_bom(path)
        try:
            for item in ijson.items(f, "standard_charge_information.item"):
                if isinstance(item, dict):
                    yield item
            return
        except Exception:
            pass
        finally:
            f.close()
        f = _open_no_bom(path)
        try:
            for item in ijson.items(f, "item"):
                if isinstance(item, dict):
                    yield item
            return
        except Exception:
            pass
        finally:
            f.close()
    try:
        data = json.loads(path.read_text(encoding="utf-8-sig", errors="replace"))
    except Exception:
        return
    if isinstance(data, dict):
        items = data.get("standard_charge_information") or []
        if isinstance(items, list):
            for it in items:
                if isinstance(it, dict):
                    yield it
        return
    if isinstance(data, list):
        for entry in data:
            if isinstance(entry, dict):
                inner = entry.get("standard_charge_information") or entry.get("item")
                if isinstance(inner, list):
                    for it in inner:
                        if isinstance(it, dict):
                            yield it
                else:
                    yield entry


def _json_iter_priced_items(path: Path, target_codes: set[tuple[str, str]]) -> Iterator[dict]:
    for item in _json_v3_item_iterator(path):
        desc = item.get("description") or item.get("Description") or ""
        codes = (
            item.get("code_information")
            or item.get("codes")
            or []
        )
        matched_pair: tuple[str, str] | None = None
        if isinstance(codes, list):
            for ci in codes:
                if not isinstance(ci, dict):
                    continue
                sysn = _json_normalize_system(ci.get("type") or ci.get("code_type"))
                coden = _json_normalize_code(ci.get("code"))
                if sysn == "RC":
                    continue
                m = _json_code_in_targets(sysn, coden, target_codes)
                if m:
                    matched_pair = m
                    break
        if not matched_pair:
            continue
        yield from _json_flatten_v3_charges(item, matched_pair, desc if isinstance(desc, str) else "")


def _zip_pick_member(zf: zipfile.ZipFile) -> zipfile.ZipInfo | None:
    candidates = [
        zi for zi in zf.infolist()
        if not zi.is_dir() and Path(zi.filename).suffix.lower() in KNOWN_ZIP_SUFFIXES
    ]
    if not candidates:
        return None
    candidates.sort(key=lambda zi: zi.file_size, reverse=True)
    return candidates[0]


def _zip_iter_priced_items(path: Path, target_codes: set[tuple[str, str]]) -> Iterator[dict]:
    if not path.is_file():
        return
    try:
        with zipfile.ZipFile(path) as zf:
            zi = _zip_pick_member(zf)
            if zi is None:
                return
            extract_to = path.with_suffix("")
            extract_to.mkdir(parents=True, exist_ok=True)
            target = extract_to / Path(zi.filename).name
            if not target.is_file() or target.stat().st_size != zi.file_size:
                with zf.open(zi) as src, open(target, "wb") as dst:
                    while True:
                        chunk = src.read(1 << 20)
                        if not chunk:
                            break
                        dst.write(chunk)
    except zipfile.BadZipFile:
        return

    suffix = target.suffix.lower()
    if suffix == ".json":
        yield from _json_iter_priced_items(target, target_codes)
    elif suffix in (".xlsx", ".xls", ".xlsm"):
        yield from _xlsx_iter_priced_items(target, target_codes)
    else:
        yield from _csv_iter_priced_items(target, target_codes)


class _ParserShim:
    """Looks like the old ``parsers.csv_parser`` etc. to ``pipeline.parse_hospital_mrf``."""

    def __init__(self, fn):
        self._fn = fn

    def iter_priced_items(self, path: Path, target_codes: set[tuple[str, str]]) -> Iterator[dict]:
        yield from self._fn(path, target_codes)


_CSV = _ParserShim(_csv_iter_priced_items)
_JSON = _ParserShim(_json_iter_priced_items)
_XLSX = _ParserShim(_xlsx_iter_priced_items)
_ZIP = _ParserShim(_zip_iter_priced_items)


def pick_parser(path: Path):
    s = path.suffix.lower()
    if s == ".json":
        return _JSON
    if s in (".csv", ".txt", ".tsv"):
        return _CSV
    if s in (".xlsx", ".xls", ".xlsm"):
        return _XLSX
    if s == ".zip":
        return _ZIP
    return _CSV
