#!/usr/bin/env python3
"""LLM-backed audit of procedure-code podiums.

Drug codes are checked by ``audit_dosage_with_llm.py`` which verifies
dose extraction and per-unit normalization. Procedure codes (CPT, MS-DRG)
don't have a dosage to extract, but they do depend on
``description_filter.py`` keeping the right rows. This script asks an LLM
whether each podium hospital's line item description plausibly describes
the same procedure the HCPCS/CPT code names.

Run with ``ANTHROPIC_API_KEY`` (or ``OPENAI_API_KEY``) set and pass
``--top-codes N`` to audit the N most-trafficked non-drug codes.

Usage:
  python3 audit_procedures_with_llm.py --top-codes 50
  python3 audit_procedures_with_llm.py --code 27447   # single code
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
DEFAULT_REPORT = SAMPLES / "procedure_audit.json"

PROMPT_TEMPLATE = """You are auditing a medical chargemaster line item.

The hospital published this row under {code_system} code {code}
({display_name}). This is a {category} code. Setting: {setting}.

What this code clinically describes:
  {what_it_is}

Line item description from the hospital's machine-readable file:
  {description!r}
Hospital reported gross charge: {gross_charge}
Hospital reported discounted cash: {discounted_cash}

Answer in JSON with these fields:
1. matches_code (bool) -- does the description plausibly describe the
   procedure named by this code? Be generous with abbreviations,
   chargemaster shorthand, and revenue-code modifiers, but reject
   obviously different procedures (e.g., a cataract surgery row filed
   under a knee replacement code).
2. is_a_variant (bool) -- if matches_code is true, is this a clinical
   variant that has its own different code? Examples: bilateral when
   the code is unilateral, revision when the code is primary, partial
   when the code is total, open when the code is laparoscopic.
3. variant_kind (string or null) -- if is_a_variant is true, name it
   ("bilateral", "revision", "partial", "open", "diagnostic-only",
   "with-biopsy", etc.).
4. price_plausible (bool) -- given the procedure, is the gross/cash
   charge in a believable range for a US hospital chargemaster (use
   broad bounds; placeholder $5 surgery rows are not plausible).
5. notes (string, <=120 chars).

Reply with ONLY valid JSON, no prose. Example:
{{"matches_code": true, "is_a_variant": false, "variant_kind": null, "price_plausible": true, "notes": ""}}
"""


def _load_code_summary() -> list[dict]:
    p = REPO_ROOT / "frontend" / "public" / "data" / "code_summary.json"
    if not p.exists():
        p = SAMPLES / "code_summary.json"
    if not p.exists():
        sys.exit(f"code_summary.json not found in {p.parent}")
    return json.loads(p.read_text())


def _candidate_codes(top_n: int, only_code: str | None) -> Iterable[dict]:
    """Yield non-drug codes ranked by hospital coverage (descending).

    "Most trafficked" = the codes the largest number of hospitals
    publish. That's also where a wrong filter has the largest impact on
    the headlines.
    """
    summary = _load_code_summary()
    drug_codes = {
        (e.get("code_system"), e.get("code"))
        for e in summary
        if e.get("billing_unit")
    }

    candidates = []
    for entry in summary:
        cs = entry.get("code_system")
        code = entry.get("code")
        if (cs, code) in drug_codes:
            continue
        stats = entry.get("stats") or {}
        count = int(stats.get("count") or 0)
        candidates.append((count, entry))

    candidates.sort(key=lambda x: x[0], reverse=True)

    yielded = 0
    for _count, entry in candidates:
        if only_code and entry.get("code") != only_code:
            continue
        yield entry
        yielded += 1
        if not only_code and yielded >= top_n:
            return


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
            "max_tokens": 250,
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


def _audit_card(card: dict, code_entry: dict, ask) -> dict:
    li = card.get("line_item") or {}
    prompt = PROMPT_TEMPLATE.format(
        code_system=code_entry.get("code_system") or "",
        code=code_entry.get("code") or "",
        display_name=code_entry.get("display_name") or "",
        category=code_entry.get("category") or "",
        setting=code_entry.get("setting") or "",
        what_it_is=code_entry.get("what_it_is") or "(no description)",
        description=li.get("description") or "(no description)",
        gross_charge=li.get("gross_charge"),
        discounted_cash=li.get("discounted_cash"),
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
        "gross_charge": li.get("gross_charge"),
        "discounted_cash": li.get("discounted_cash"),
        "llm": result,
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--provider",
        choices=("openai", "anthropic", "auto"),
        default="auto",
    )
    ap.add_argument("--openai-model", default="gpt-4o-mini")
    ap.add_argument("--anthropic-model", default="claude-sonnet-4-5")
    ap.add_argument("--code", default=None)
    ap.add_argument("--top-codes", type=int, default=50)
    ap.add_argument("--top-n", type=int, default=3, help="Cards per direction")
    ap.add_argument("--output", default=str(DEFAULT_REPORT))
    args = ap.parse_args()

    provider = args.provider
    if provider == "auto":
        if os.environ.get("ANTHROPIC_API_KEY"):
            provider = "anthropic"
        elif os.environ.get("OPENAI_API_KEY"):
            provider = "openai"
        else:
            sys.exit("No ANTHROPIC_API_KEY or OPENAI_API_KEY set.")

    if provider == "anthropic":
        if not os.environ.get("ANTHROPIC_API_KEY"):
            sys.exit("ANTHROPIC_API_KEY not set.")
        ask = lambda p: _ask_anthropic(p, args.anthropic_model)  # noqa: E731
    else:
        if not os.environ.get("OPENAI_API_KEY"):
            sys.exit("OPENAI_API_KEY not set.")
        ask = lambda p: _ask_openai(p, args.openai_model)  # noqa: E731

    report: list[dict] = []
    for entry in _candidate_codes(args.top_codes, args.code):
        code_label = f"{entry['code_system']}:{entry['code']}"
        print(f"== {code_label} {entry['display_name']} ==")
        for label, key in (("cheapest", "top_cheapest"), ("priciest", "top_priciest")):
            cards = (entry.get(key) or [])[: args.top_n]
            for c in cards:
                row = _audit_card(c, entry, ask)
                row["code"] = code_label
                row["bucket"] = label
                report.append(row)
                llm = row.get("llm") or {}
                marker = ""
                if llm.get("matches_code") is False:
                    marker = " [MISMATCH]"
                elif llm.get("is_a_variant"):
                    marker = f" [VARIANT:{llm.get('variant_kind')}]"
                elif llm.get("price_plausible") is False:
                    marker = " [IMPLAUSIBLE]"
                hosp = (row.get("hospital") or "?")[:35]
                print(
                    f"  [{label}] {hosp:35} median=${row.get('median')}{marker} "
                    f"notes={llm.get('notes','')[:60]}"
                )
                time.sleep(0.25)

    Path(args.output).write_text(json.dumps(report, indent=2))

    flagged = [
        r for r in report
        if r.get("llm")
        and (
            r["llm"].get("matches_code") is False
            or r["llm"].get("is_a_variant") is True
            or r["llm"].get("price_plausible") is False
        )
    ]
    print(f"\nWrote {len(report)} rows to {args.output}")
    print(f"== flagged: {len(flagged)} ==")
    for r in flagged:
        llm = r.get("llm") or {}
        kind = (
            "MISMATCH" if llm.get("matches_code") is False
            else ("VARIANT:" + str(llm.get("variant_kind"))) if llm.get("is_a_variant")
            else "IMPLAUSIBLE"
        )
        print(
            f"  {r['code']} {r['bucket']:8} {kind:25} "
            f"{(r.get('hospital') or '?')[:30]:30} -> {llm.get('notes')}"
        )


if __name__ == "__main__":
    main()
