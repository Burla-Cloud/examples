"""250 target codes with plain-English patient-facing fields."""
from __future__ import annotations

import csv
import json
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent
SEEDS = REPO_ROOT / "data" / "code_seeds.csv"

CATEGORY_DEFAULTS: dict[str, dict] = {
    "surgical": {
        "setting": "surgical_center",
        "bundled_with": "Often includes facility and OR time; implants may be bundled or on a separate line.",
        "not_bundled": "Surgeon and anesthesia are commonly billed separately from the facility.",
        "tips": [
            "Ask for itemized estimate in writing before elective surgery.",
            "Confirm whether implants or hardware are included in the facility quote.",
            "Cash or self-pay discounts are sometimes lower than the lowest insurer-negotiated line.",
        ],
    },
    "imaging": {
        "setting": "outpatient",
        "bundled_with": "Relying on one test price may include only the technical component at some sites.",
        "not_bundled": "Radiologist interpretation can be billed separately as professional fee.",
        "tips": [
            "Ask if contrast, drugs, and supplies are included in the quoted imaging price.",
            "Compare outpatient imaging center vs hospital-based prices for the same CPT.",
        ],
    },
    "er": {
        "setting": "ER",
        "bundled_with": "Facility fee often bundles nursing, triage, and room for a visit level.",
        "not_bundled": "Physician services, imaging, labs, and procedures are often separate charges.",
        "tips": [
            "ER prices vary widely; if not emergent, urgent care may be dramatically cheaper.",
            "Request an itemized bill and coding summary after discharge.",
        ],
    },
    "maternity": {
        "setting": "inpatient",
        "bundled_with": "Global OB packages sometimes bundle routine prenatal visits and delivery.",
        "not_bundled": "Anesthesia, NICU, and complications often add separate charges.",
        "tips": [
            "Ask the hospital for a maternity package estimate early in pregnancy.",
            "Clarify pediatrician and anesthesia as separate or global fees.",
        ],
    },
    "pediatric": {
        "setting": "outpatient",
        "bundled_with": "Well-child visits may bundle screening instruments at some clinics.",
        "not_bundled": "Vaccine product and administration can appear as separate lines.",
        "tips": [
            "Compare pediatric hospital clinic vs community clinic for the same vaccine CPT.",
            "Ask about facility fees for hospital-owned outpatient offices.",
        ],
    },
    "cancer_screening": {
        "setting": "outpatient",
        "bundled_with": "Colonoscopy may bundle moderate sedation at some facilities.",
        "not_bundled": "Pathology, anesthesia, and follow-up procedures are often extra.",
        "tips": [
            "Screening vs diagnostic coding changes what you owe; confirm which applies.",
            "Facility vs office location can dominate price for the same screening test.",
        ],
    },
    "cardiovascular": {
        "setting": "both",
        "bundled_with": "Facility and device components may be split across lines in the MRF.",
        "not_bundled": "Surgeon fees, implants, and imaging often bill separately.",
        "tips": [
            "High-cost cardiac procedures show huge hospital-to-hospital spread; get written estimates.",
            "Ask whether quoted price includes devices such as stents or valves.",
        ],
    },
    "gi_endoscopy": {
        "setting": "outpatient",
        "bundled_with": "Endoscopy suites may bundle base facility time.",
        "not_bundled": "Pathology, anesthesia, and therapeutic maneuvers may add costs.",
        "tips": [
            "ASC endoscopy is often cheaper than hospital outpatient for the same CPT.",
            "Ask if moderate sedation is included in the scope price.",
        ],
    },
    "inpatient_drg": {
        "setting": "inpatient",
        "bundled_with": "DRG-style bundles roll many services into one hospital stay payment context.",
        "not_bundled": "Physicians, certain implants, and carve-outs can still appear separately on real bills.",
        "tips": [
            "MRF DRG numbers are not the same as your personal out-of-pocket.",
            "Use DRG rows as relative hospital-to-hospital signals, not final patient quotes.",
        ],
    },
    "infused_drug": {
        "setting": "inpatient",
        "bundled_with": "Drug administration hours may bundle with infusion chair time at some sites.",
        "not_bundled": "Drug product, lab monitoring, and facility fees often separate.",
        "tips": [
            "Infused biologics show extreme price spread; compare hospital vs outpatient infusion center.",
            "Ask for NDC-level pricing when comparing the same vial size.",
        ],
    },
    "hospital_line_item": {
        "setting": "inpatient",
        "bundled_with": "Per-item charges sometimes reflect chargemaster list prices, not what you ultimately pay.",
        "not_bundled": "These items often appear alongside separate facility and physician charges.",
        "tips": [
            "Line items like IV fluids or supplies can look absurd in MRFs but bundle differently on real bills.",
            "Request charge master line items tied to dates of service when auditing.",
        ],
    },
    "mental_health": {
        "setting": "outpatient",
        "bundled_with": "Some systems bundle intake paperwork with first visit.",
        "not_bundled": "After-hours crisis services and medications are often separate.",
        "tips": [
            "Confirm telehealth vs in-person pricing for identical CPT.",
            "Ask about sliding scale or charity care for behavioral health.",
        ],
    },
    "vaccine": {
        "setting": "outpatient",
        "bundled_with": "Pharmacy clinics may bundle product and administration on one charge.",
        "not_bundled": "Hospital outpatient offices often split product from administration fee.",
        "tips": [
            "Retail clinics often beat hospital outpatient for the same vaccine code.",
            "Ask if visit fee applies on top of immunization admin.",
        ],
    },
    "lab": {
        "setting": "outpatient",
        "bundled_with": "Some labs bundle reflex testing in practice but show separate MRF lines.",
        "not_bundled": "Draw fee, courier, and pathologist interpretation can be separate.",
        "tips": [
            "Standalone lab patient portals sometimes post lower cash prices than hospital labs.",
            "Ask for a bundled lab panel price instead of sum of a la carte if offered.",
        ],
    },
    "cancer_treatment": {
        "setting": "outpatient",
        "bundled_with": "Radiation and chemo administration codes often bundle facility time and dosimetry.",
        "not_bundled": "Drug product, anti-nausea meds, IV access, and physician oversight may be billed separately.",
        "tips": [
            "Ask for the all-in cost of a full radiation course or full chemo cycle, not just one fraction.",
            "Compare hospital infusion centers to free-standing oncology centers, the spread can be enormous.",
            "Confirm whether the price includes the drug, the administration, and any monitoring labs.",
        ],
    },
    "misc": {
        "setting": "outpatient",
        "bundled_with": "Varies by procedure and site.",
        "not_bundled": "Supplies, drugs, and facility fees may be separate.",
        "tips": [
            "Always ask for the all-in cash estimate in writing.",
            "Outpatient office vs hospital outpatient department pricing can diverge sharply.",
        ],
    },
}


def load_codes() -> list[dict]:
    if not SEEDS.is_file():
        raise FileNotFoundError(f"Missing {SEEDS}; run python scripts/generate_code_seeds.py")
    codes = []
    with SEEDS.open(encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            cat = row["category"]
            defaults = CATEGORY_DEFAULTS.get(cat, CATEGORY_DEFAULTS["misc"])
            entry = {
                "category": cat,
                "code_system": row["code_system"],
                "code": row["code"].strip(),
                "display_name": row["display_name"],
                "what_it_is": row["what_it_is"],
                "when_youd_need_it": row["when_youd_need_it"],
                "setting": defaults["setting"],
                "bundled_with": defaults["bundled_with"],
                "not_bundled": defaults["not_bundled"],
                "tips": list(defaults["tips"]),
            }
            codes.append(entry)
    return codes


CODES = load_codes()

# Normalized keys for parser matching: (system_upper, code_no_dots)
TARGET_LOOKUP: dict[tuple[str, str], dict] = {}
for c in CODES:
    sysu = c["code_system"].upper().replace("-", "")
    code_norm = c["code"].replace(".", "").replace(" ", "").strip()
    TARGET_LOOKUP[(sysu, code_norm)] = c
    if sysu == "CPT":
        TARGET_LOOKUP[("HCPCS", code_norm)] = c
        TARGET_LOOKUP[("HCPCS/CPT", code_norm)] = c
    if sysu == "HCPCS":
        TARGET_LOOKUP[("CPT", code_norm)] = c


def target_codes_normalized() -> set[tuple[str, str]]:
    return set(TARGET_LOOKUP.keys())


def meta_for_code(system: str, code: str) -> dict | None:
    sysu = system.upper().replace("-", "")
    code_norm = code.replace(".", "").replace(" ", "").strip()
    return TARGET_LOOKUP.get((sysu, code_norm)) or TARGET_LOOKUP.get(("CPT", code_norm))
