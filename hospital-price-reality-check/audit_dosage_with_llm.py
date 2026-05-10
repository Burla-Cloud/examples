#!/usr/bin/env python3
"""LLM-backed audit of the cheapest/priciest podium for drug codes.

For each ranked hospital on a code that bills per-X HCPCS unit (J9000,
J9305, J2469, J9271, J1626, J1745, ...), this script asks an LLM to:

  1. Extract the actual dose from the line item description
     ("DOXORUBICIN 50 MG INJ" -> 50 mg vial).
  2. Compare to the dose our regex pulled out.
  3. Flag any disagreements so we can tune ``dosage_extractor.py``.
  4. Optionally flag descriptions that look like they describe a
     different drug than the HCPCS code claims (mis-coded chargemaster
     entries; the pre-existing ``description_filter.py`` is a regex
     defense, this is an LLM second opinion).

Run with either ``OPENAI_API_KEY`` or ``ANTHROPIC_API_KEY`` set in the
environment. The script writes a JSON report to
``samples/dosage_audit.json`` and prints a per-code summary.

Usage:
  python3 audit_dosage_with_llm.py             # audit top drug codes
  python3 audit_dosage_with_llm.py --code J9000  # one code only
  python3 audit_dosage_with_llm.py --provider openai
  python3 audit_dosage_with_llm.py --provider anthropic
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path
from typing import Iterable

REPO_ROOT = Path(__file__).resolve().parent
SAMPLES = REPO_ROOT / "samples"
DEFAULT_REPORT = SAMPLES / "dosage_audit.json"

# Default fallback if --all isn't passed and we can't infer from the
# code summary. Keeps backwards compatibility with earlier runs.
DEFAULT_DRUG_CODES = [
    ("HCPCS", "J9000"),
    ("HCPCS", "J9305"),
    ("HCPCS", "J9271"),
    ("HCPCS", "J2469"),
    ("HCPCS", "J1626"),
    ("HCPCS", "J1745"),
]

PROMPT_TEMPLATE = """You are auditing a medical chargemaster line item.

The hospital published this row under HCPCS code {code} ({display_name}).
HCPCS billing unit: {hcpcs_unit}.

Line item description from the hospital's machine-readable file:
  {description!r}
Hospital reported gross charge: {gross_charge}
Hospital reported discounted cash: {discounted_cash}
Our regex extracted dose: {extracted_dose}
We then scaled the price to the HCPCS billing unit and got:
  per {hcpcs_unit}: gross={gross_per_unit}, cash={cash_per_unit}

Answer four questions in JSON:

1. dose_qty (number) and dose_unit (string, one of "mg", "mcg", "ml",
   "unit", or null) -- the total dose described by this line item, or
   null if the description doesn't say.
2. matches_code (bool) -- does the description plausibly describe the
   drug named by the HCPCS code? Something like "ONDANSETRON" under a
   doxorubicin code is mismatch.
3. normalization_correct (bool) -- given the dose you extracted, is our
   per-unit price correct?
4. notes (string, <=120 chars) -- anything notable.

Reply with ONLY valid JSON, no prose. Example:
{{"dose_qty": 50, "dose_unit": "mg", "matches_code": true, "normalization_correct": true, "notes": ""}}
"""


def _load_code_summary() -> list[dict]:
    p = REPO_ROOT / "frontend" / "public" / "data" / "code_summary.json"
    if not p.exists():
        p = SAMPLES / "code_summary.json"
    if not p.exists():
        sys.exit(f"code_summary.json not found in {p.parent}")
    return json.loads(p.read_text())


def _drug_entries(
    only_code: str | None, scope: str = "default"
) -> Iterable[dict]:
    """Yield code summary entries to audit.

    ``scope`` controls which codes are included:
      - ``default``: the hard-coded short list of marquee drug codes.
      - ``all-drugs``: every code in the summary that has a ``billing_unit``
        set (i.e., every drug code we normalized to per-HCPCS-unit).
    """
    summary = _load_code_summary()
    if scope == "all-drugs":
        targets = {
            (e.get("code_system"), e.get("code"))
            for e in summary
            if e.get("billing_unit")
        }
    else:
        targets = set(DEFAULT_DRUG_CODES)
    for entry in summary:
        cs = entry.get("code_system")
        code = entry.get("code")
        if (cs, code) not in targets:
            continue
        if only_code and code != only_code:
            continue
        yield entry


def _ask_openai(prompt: str, model: str) -> dict:
    import requests

    api_key = os.environ["OPENAI_API_KEY"]
    r = requests.post(
        "https://api.openai.com/v1/chat/completions",
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        json={
            "model": model,
            "messages": [{"role": "user", "content": prompt}],
            "temperature": 0,
            "response_format": {"type": "json_object"},
        },
        timeout=60,
    )
    r.raise_for_status()
    body = r.json()
    return json.loads(body["choices"][0]["message"]["content"])


def _ask_anthropic(prompt: str, model: str) -> dict:
    import requests

    api_key = os.environ["ANTHROPIC_API_KEY"]
    r = requests.post(
        "https://api.anthropic.com/v1/messages",
        headers={
            "x-api-key": api_key,
            "anthropic-version": "2023-06-01",
            "Content-Type": "application/json",
        },
        json={
            "model": model,
            "max_tokens": 200,
            "temperature": 0,
            "messages": [{"role": "user", "content": prompt}],
        },
        timeout=60,
    )
    r.raise_for_status()
    body = r.json()
    text = "".join(b["text"] for b in body["content"] if b["type"] == "text").strip()
    if text.startswith("```"):
        text = text.strip("`")
        if text.lower().startswith("json"):
            text = text[4:].strip()
    return json.loads(text)


def _audit_card(card: dict, code_entry: dict, ask) -> dict:
    li = card.get("line_item") or {}
    prompt = PROMPT_TEMPLATE.format(
        code=code_entry["code"],
        display_name=code_entry["display_name"],
        hcpcs_unit=li.get("hcpcs_billing_unit") or "?",
        description=li.get("description") or "(no description)",
        gross_charge=li.get("gross_charge"),
        discounted_cash=li.get("discounted_cash"),
        extracted_dose=li.get("dose"),
        gross_per_unit=li.get("gross_charge_per_unit"),
        cash_per_unit=li.get("discounted_cash_per_unit"),
    )
    try:
        result = ask(prompt)
    except Exception as exc:
        return {
            "hospital": card.get("name"),
            "median": card.get("median"),
            "error": str(exc),
        }
    return {
        "hospital": card.get("name"),
        "city": card.get("city"),
        "median": card.get("median"),
        "description": li.get("description"),
        "extracted_dose": li.get("dose"),
        "per_unit_gross": li.get("gross_charge_per_unit"),
        "per_unit_cash": li.get("discounted_cash_per_unit"),
        "llm": result,
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--provider",
        choices=("openai", "anthropic", "auto"),
        default="auto",
        help="Which LLM backend to call. auto picks whichever key is set.",
    )
    ap.add_argument("--openai-model", default="gpt-4o-mini")
    ap.add_argument("--anthropic-model", default="claude-3-5-sonnet-20241022")
    ap.add_argument("--code", default=None, help="Audit only this HCPCS code")
    ap.add_argument("--top-n", type=int, default=3, help="Cards per direction")
    ap.add_argument("--output", default=str(DEFAULT_REPORT))
    ap.add_argument(
        "--scope",
        choices=("default", "all-drugs"),
        default="default",
        help="Which codes to audit. all-drugs = every code with a billing_unit.",
    )
    args = ap.parse_args()

    provider = args.provider
    if provider == "auto":
        if os.environ.get("OPENAI_API_KEY"):
            provider = "openai"
        elif os.environ.get("ANTHROPIC_API_KEY"):
            provider = "anthropic"
        else:
            sys.exit("No OPENAI_API_KEY or ANTHROPIC_API_KEY set in environment.")

    if provider == "openai":
        if not os.environ.get("OPENAI_API_KEY"):
            sys.exit("OPENAI_API_KEY not set.")
        ask = lambda p: _ask_openai(p, args.openai_model)  # noqa: E731
    else:
        if not os.environ.get("ANTHROPIC_API_KEY"):
            sys.exit("ANTHROPIC_API_KEY not set.")
        ask = lambda p: _ask_anthropic(p, args.anthropic_model)  # noqa: E731

    report: list[dict] = []
    for entry in _drug_entries(args.code, args.scope):
        code_label = f"{entry['code_system']}:{entry['code']}"
        print(f"== {code_label} {entry['display_name']} ==")
        for label, key in (("cheapest", "top_cheapest"), ("priciest", "top_priciest")):
            cards = (entry.get(key) or [])[: args.top_n]
            for c in cards:
                row = _audit_card(c, entry, ask)
                row["code"] = code_label
                row["bucket"] = label
                report.append(row)
                print(
                    f"  [{label}] {row.get('hospital','?')[:35]:35} "
                    f"median=${row.get('median')} llm={row.get('llm')}"
                )
                time.sleep(0.3)  # be polite to the rate limiter

    Path(args.output).write_text(json.dumps(report, indent=2))
    print(f"\nWrote {len(report)} rows to {args.output}")

    flagged = [
        r
        for r in report
        if r.get("llm")
        and (
            r["llm"].get("matches_code") is False
            or r["llm"].get("normalization_correct") is False
        )
    ]
    print(f"\n== flagged: {len(flagged)} ==")
    for r in flagged:
        print(
            f"  {r['code']} {r['bucket']:8} {r.get('hospital','?')[:30]:30} "
            f"-> {r['llm'].get('notes')}"
        )


if __name__ == "__main__":
    main()
