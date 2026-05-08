const CATEGORY_LABELS: Record<string, string> = {
  surgical: "Surgery",
  imaging: "Imaging",
  er: "Emergency room",
  maternity: "Maternity",
  pediatric: "Pediatrics",
  cancer_screening: "Cancer screening",
  cancer_treatment: "Chemo & radiation",
  cardiovascular: "Cardiovascular",
  gi_endoscopy: "GI endoscopy",
  inpatient_drg: "Inpatient (DRG)",
  infused_drug: "Infused drugs",
  hospital_line_item: "Hospital line item",
  mental_health: "Mental health",
  vaccine: "Vaccines",
  lab: "Lab tests",
  preventive: "Preventive care",
  injection: "Injections",
  outpatient_visit: "Outpatient visit",
  rehab: "Rehab",
  ophthalmology: "Eye care",
  dermatology: "Dermatology",
  dental: "Dental",
  primary_care: "Primary care",
  obstetric: "Obstetric",
  women_health: "Women's health",
  men_health: "Men's health",
  geriatric: "Geriatric",
  procedure: "Procedure",
};

export function categoryLabel(slug: string | undefined | null): string {
  if (!slug) return "";
  if (CATEGORY_LABELS[slug]) return CATEGORY_LABELS[slug];
  return slug
    .split(/[_\s]+/)
    .map((s) => (s.length ? s[0].toUpperCase() + s.slice(1) : s))
    .join(" ");
}

export function fmtMoney(n: number | undefined | null): string {
  if (n == null || Number.isNaN(n)) return "n/a";
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    maximumFractionDigits: 0,
  }).format(n);
}

export function fmtNum(n: number | undefined | null): string {
  if (n == null || Number.isNaN(n)) return "n/a";
  return new Intl.NumberFormat("en-US").format(Math.round(n));
}

export const STATE_NAMES: Record<string, string> = {
  AL: "Alabama", AK: "Alaska", AZ: "Arizona", AR: "Arkansas", CA: "California",
  CO: "Colorado", CT: "Connecticut", DE: "Delaware", FL: "Florida", GA: "Georgia",
  HI: "Hawaii", ID: "Idaho", IL: "Illinois", IN: "Indiana", IA: "Iowa",
  KS: "Kansas", KY: "Kentucky", LA: "Louisiana", ME: "Maine", MD: "Maryland",
  MA: "Massachusetts", MI: "Michigan", MN: "Minnesota", MS: "Mississippi", MO: "Missouri",
  MT: "Montana", NE: "Nebraska", NV: "Nevada", NH: "New Hampshire", NJ: "New Jersey",
  NM: "New Mexico", NY: "New York", NC: "North Carolina", ND: "North Dakota",
  OH: "Ohio", OK: "Oklahoma", OR: "Oregon", PA: "Pennsylvania", RI: "Rhode Island",
  SC: "South Carolina", SD: "South Dakota", TN: "Tennessee", TX: "Texas",
  UT: "Utah", VT: "Vermont", VA: "Virginia", WA: "Washington", WV: "West Virginia",
  WI: "Wisconsin", WY: "Wyoming", DC: "District of Columbia",
  PR: "Puerto Rico", VI: "Virgin Islands", GU: "Guam",
};

export function stateName(abbr: string | undefined | null): string {
  if (!abbr) return "";
  return STATE_NAMES[abbr] || abbr;
}
