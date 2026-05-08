import { useEffect, useMemo, useState } from "react";
import { Link, useParams, useSearchParams } from "react-router-dom";
import {
  Area,
  AreaChart,
  CartesianGrid,
  LabelList,
  ReferenceLine,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import { loadAll } from "../api";
import type { CodeEntry, RankedHospital, StateCodeStat } from "../types";
import { CaveatBanner } from "../components/CaveatBanner";
import { StateFilter } from "../components/StateFilter";
import { RateBadge } from "../components/RateBadge";
import { categoryLabel, fmtMoney, stateName } from "../format";

// Compact dollar formatter for chart tick labels and dot annotations.
// Examples: 11 -> "$11", 1234 -> "$1.2k", 12340 -> "$12k", 1234567 -> "$1.2M".
function fmtPriceShort(v: number | null | undefined): string {
  if (v == null || !isFinite(v)) return "";
  const n = Number(v);
  if (n >= 1_000_000) return `$${(n / 1_000_000).toFixed(1)}M`;
  if (n >= 10_000) return `$${Math.round(n / 1000)}k`;
  if (n >= 1000) return `$${(n / 1000).toFixed(1)}k`;
  if (n >= 10) return `$${Math.round(n)}`;
  return `$${n.toFixed(2)}`;
}

function SpreadExplainer({ entry }: { entry: CodeEntry }) {
  const sysu = entry.code_system.toUpperCase();
  const cat = entry.category;
  const isHcpcsJ =
    sysu === "HCPCS" && /^J\d/i.test(entry.code);
  const isDrug = isHcpcsJ || sysu === "NDC" || cat === "infused_drug";
  const isDrg = sysu === "MS-DRG" || cat === "inpatient_drg";

  // Headline answer: yes, the underlying thing is the same. The price isn't.
  const headline = isDrug
    ? "Yes, it's the same molecule. The price still isn't."
    : isDrg
      ? "Yes, the diagnosis category is the same. The price still isn't."
      : "Yes, the procedure code is the same. The price still isn't.";

  const lede = isDrug
    ? `Every hospital here is billing the exact same drug under the same HCPCS unit (${entry.code} = ${entry.display_name.toLowerCase()}). What changes is the chargemaster, not the chemistry.`
    : isDrg
      ? `Every hospital here is billing the same MS-DRG (${entry.code}). The diagnosis-related group defines a single bundled stay. What changes is the hospital's standard charge for that bundle, not the underlying admission category.`
      : `Every hospital here is billing the same procedure code (${sysu} ${entry.code}). What changes is the standard charge each hospital sets, plus what they bundle into the facility fee.`;

  const reasons: { title: string; body: string }[] = isDrug
    ? [
        {
          title: "Chargemaster is fiction",
          body:
            "Every hospital sets its own list price. Insurance almost never pays it. The list price is mostly used to anchor negotiations, calculate self-pay discounts, and bill the rare uninsured patient. There is no requirement that it relate to acquisition cost.",
        },
        {
          title: "Some hospitals report the cash price, others report gross charges",
          body:
            "The Hospital Price Transparency rule lets hospitals publish multiple prices: gross charge, discounted cash, payer-negotiated rates, and de-identified min/max. Cash prices for the same drug are often a fraction of the gross charge.",
        },
        {
          title: "340B and acquisition cost vary",
          body:
            "Safety-net hospitals can buy this drug at deeply discounted 340B rates. Wealthy academic centers pay closer to wholesale and mark up more. The drug bottle is identical; the supply chain isn't.",
        },
        {
          title: "Per-mg vs per-vial reporting glitches",
          body:
            "Some MRFs publish a per-vial price even when the HCPCS unit is per-mg. We try to filter those, but the floor at the cheap end and the ceiling at the pricey end can still leak the occasional encoding mistake. Click View source MRF to verify.",
        },
      ]
    : isDrg
      ? [
          {
            title: "DRG bundles services, not items",
            body:
              "An MS-DRG is one inpatient stay with a defined diagnosis and complication tier. The bundle includes room, nursing, drugs, supplies, and most ancillary services. Different hospitals stuff different things into the bundle even though the DRG number is identical.",
          },
          {
            title: "Chargemaster is the wholesale list price",
            body:
              "MRFs publish the hospital's gross charge for the bundle. Insurers negotiate that number down by 50-80% behind the scenes. Two hospitals charging $1k and $28k for the same DRG often get paid roughly similar amounts after those negotiations.",
          },
          {
            title: "Teaching hospitals carry overhead",
            body:
              "Academic medical centers add residency and research costs into chargemaster lines. Critical access and rural hospitals do not. The DRG code is identical; the cost basis isn't.",
          },
        ]
      : [
          {
            title: "What is bundled into the facility fee differs",
            body:
              "Two hospitals can bill the same CPT for the same hour of work, but one charges separately for the room, supplies, recovery, and nursing while the other bundles them. The line item in the MRF only captures part of the bill.",
          },
          {
            title: "Chargemaster is set by accountants, not biology",
            body:
              "Every hospital sets its own list price and updates it on its own schedule. There is no national price for a CPT code. Identical work product, very different number on the bill.",
          },
          {
            title: "Rural critical access vs urban academic",
            body:
              "A small county hospital and a major teaching center pay totally different overheads. Both publish the same procedure code, but their cost basis is different by an order of magnitude.",
          },
          {
            title: "Insurance pays a different number than the chargemaster",
            body:
              "Whatever the chargemaster says, what an insured patient actually owes after negotiation, deductible, and copay is usually a fraction of the highest line in this list.",
          },
        ];

  return (
    <section>
      <div className="mb-7 max-w-3xl">
        <p className="eyebrow">Why such a huge gap</p>
        <h2 className="display-2 mt-3 text-balance">{headline}</h2>
        <p className="text-base text-inkMuted mt-4 leading-relaxed">{lede}</p>
      </div>
      <ul className="grid gap-px overflow-hidden rounded-2xl border border-line bg-line md:grid-cols-2">
        {reasons.map((r, i) => (
          <li key={r.title} className="bg-surface p-7 md:p-8">
            <span className="font-display text-2xl font-medium text-accent tracking-[-0.02em] tabular-nums">
              {String(i + 1).padStart(2, "0")}
            </span>
            <h3 className="font-display text-xl font-semibold text-ink mt-3">
              {r.title}
            </h3>
            <p className="mt-3 text-sm text-inkMuted leading-relaxed">
              {r.body}
            </p>
          </li>
        ))}
      </ul>
      <p className="mt-5 text-xs text-inkSubtle max-w-3xl">
        Bottom line: the line on the chargemaster is what the hospital says it
        charges. What you actually pay depends on insurance, network, plan
        deductible, and any cash-pay discount the hospital offers. Always ask
        for a written estimate that lists facility, physician, drugs, and
        supplies separately before a non-emergency visit.
      </p>
    </section>
  );
}

function LineItemBlock({
  item,
}: {
  item: NonNullable<RankedHospital["line_item"]>;
}) {
  const billingUnit = item.hcpcs_billing_unit || item.unit || null;
  const dose = item.dose || null;
  const meta = [
    dose ? `Vial / dose ${dose}` : null,
    billingUnit ? `billed per ${billingUnit}` : null,
    item.setting || null,
  ].filter(Boolean);

  // Per-HCPCS-unit numbers are what we ranked the hospitals by, so they go
  // first. The raw chargemaster numbers go beneath as "What the MRF says"
  // so a reader can verify the math against the source file.
  const perUnit = [
    item.gross_charge_per_unit != null
      ? `Gross ${fmtMoney(item.gross_charge_per_unit)}`
      : null,
    item.discounted_cash_per_unit != null
      ? `Cash ${fmtMoney(item.discounted_cash_per_unit)}`
      : null,
  ].filter(Boolean);

  const raw = [
    item.gross_charge != null ? `Gross ${fmtMoney(item.gross_charge)}` : null,
    item.discounted_cash != null ? `Cash ${fmtMoney(item.discounted_cash)}` : null,
  ].filter(Boolean);

  // If the hospital's row was already in per-HCPCS-unit form (no
  // normalization needed), perUnit and raw match; collapse to one line.
  const perUnitMatchesRaw =
    item.gross_charge_per_unit === item.gross_charge &&
    item.discounted_cash_per_unit === item.discounted_cash;

  return (
    <div className="mt-3 rounded-md border border-line bg-section/40 px-3 py-2">
      <p className="text-[10px] uppercase tracking-[0.18em] text-inkMuted">
        Exact line in this hospital's file
      </p>
      {item.description ? (
        <p className="text-xs text-ink mt-1 leading-snug">{item.description}</p>
      ) : null}
      {meta.length > 0 ? (
        <p className="text-[11px] text-inkSubtle mt-1 leading-snug">
          {meta.join(" \u00b7 ")}
        </p>
      ) : null}
      {perUnit.length > 0 && billingUnit ? (
        <p className="text-[11px] text-ink mt-1 leading-snug">
          <span className="font-medium">Per {billingUnit}:</span>{" "}
          {perUnit.join(" \u00b7 ")}
        </p>
      ) : null}
      {raw.length > 0 && (perUnit.length === 0 || !perUnitMatchesRaw) ? (
        <p className="text-[11px] text-inkSubtle mt-1 leading-snug">
          <span className="font-medium">MRF lists:</span>{" "}
          {raw.join(" \u00b7 ")}
          {dose ? ` (full ${dose})` : ""}
        </p>
      ) : null}
    </div>
  );
}

function RankedHospitalList({
  title,
  tone,
  hospitals,
}: {
  title: string;
  tone: "mint" | "rose";
  hospitals: RankedHospital[];
}) {
  const dotClass = tone === "mint" ? "bg-mint" : "bg-rose";
  const eyebrowClass = tone === "mint" ? "text-mint" : "text-rose";
  return (
    <div className="bg-surface p-7 md:p-8">
      <div className="flex items-center gap-2.5 mb-6">
        <span className={`h-2 w-2 rounded-full ${dotClass}`} aria-hidden />
        <p className={`eyebrow ${eyebrowClass}`}>{title}</p>
      </div>
      <ol className="space-y-px overflow-hidden rounded-xl border border-line bg-line">
        {hospitals.map((h, i) => (
          <li
            key={`${tone}-${h.hospital_id || i}`}
            className="bg-surface p-5 flex items-start gap-4"
          >
            <span className="font-display text-2xl font-medium text-inkSubtle leading-none mt-1 tabular-nums">
              {String(i + 1).padStart(2, "0")}
            </span>
            <div className="flex-1 min-w-0">
              <p className="font-display text-lg font-semibold text-ink leading-snug">
                {h.name || h.hospital_id}
              </p>
              <p className="text-xs text-inkMuted mt-1">
                {[h.city, h.state ? stateName(h.state) : null]
                  .filter(Boolean)
                  .join(", ") || "Location unknown"}
                {h.count
                  ? ` \u00b7 ${h.count} list price${h.count === 1 ? "" : "s"}`
                  : ""}
              </p>
              {h.line_item ? <LineItemBlock item={h.line_item} /> : null}
              {h.mrf_url ? (
                <a
                  href={h.mrf_url}
                  target="_blank"
                  rel="noreferrer"
                  className="mt-2 inline-flex items-center gap-1 text-[11px] font-medium text-inkMuted underline-offset-4 hover:text-ink hover:underline"
                  aria-label={`Open the source machine-readable file for ${
                    h.name || h.hospital_id
                  }`}
                >
                  View source MRF
                  <svg viewBox="0 0 24 24" className="h-3 w-3" fill="none">
                    <path
                      d="M14 4h6m0 0v6m0-6L10 14M5 9v11h11"
                      stroke="currentColor"
                      strokeWidth="1.6"
                      strokeLinecap="round"
                      strokeLinejoin="round"
                    />
                  </svg>
                </a>
              ) : null}
            </div>
            <span className="font-display text-2xl font-medium text-ink whitespace-nowrap tracking-[-0.02em]">
              {fmtMoney(h.median)}
            </span>
          </li>
        ))}
      </ol>
    </div>
  );
}

export function CodeDetail() {
  const { system, code } = useParams();
  const [searchParams, setSearchParams] = useSearchParams();
  const initialState = searchParams.get("state") || "";

  const [entry, setEntry] = useState<CodeEntry | null>(null);
  const [statePrices, setStatePrices] = useState<
    Record<string, StateCodeStat>
  >({});
  const [stateAbbr, setStateAbbr] = useState<string>(initialState);
  const [showRange, setShowRange] = useState<"core" | "full">("full");
  const [err, setErr] = useState<string | null>(null);

  useEffect(() => {
    loadAll()
      .then((d) => {
        const e = d.codes.find((c) => c.code_system === system && c.code === code);
        setEntry(e ?? null);
        const key = `${system}:${code}`;
        const sp: Record<string, StateCodeStat> = {};
        for (const [st, v] of Object.entries(d.stateSummary)) {
          if (v?.codes?.[key]) sp[st] = v.codes[key];
        }
        setStatePrices(sp);
      })
      .catch((e) => setErr(String(e)));
  }, [system, code]);

  const onStateChange = (v: string) => {
    setStateAbbr(v);
    if (v) {
      searchParams.set("state", v);
    } else {
      searchParams.delete("state");
    }
    setSearchParams(searchParams, { replace: true });
  };

  const states = useMemo(
    () =>
      Object.entries(statePrices)
        .sort((a, b) => (b[1].count ?? 0) - (a[1].count ?? 0))
        .map(([st]) => st),
    [statePrices]
  );

  const stateRanking = useMemo(
    () =>
      Object.entries(statePrices)
        .filter(([, v]) => v?.median && (v.count ?? 0) >= 2)
        .map(([st, v]) => ({
          state: st,
          median: v.median ?? 0,
          min: v.min ?? 0,
          max: v.max ?? 0,
          mean: v.mean ?? 0,
          count: v.count ?? 0,
        }))
        .sort((a, b) => a.median - b.median),
    [statePrices]
  );

  const cur = useMemo(() => {
    if (!entry) return null;
    const base = stateAbbr
      ? statePrices[stateAbbr]
      : (entry.stats as typeof entry.stats);
    if (!base) return null;
    return {
      min: base.min ?? null,
      max: base.max ?? null,
      median: base.median ?? null,
      mean: base.mean ?? null,
      p10: base.p10 ?? null,
      p25: base.p25 ?? null,
      p75: base.p75 ?? null,
      p90: base.p90 ?? null,
      count: base.count ?? 0,
      scope: stateAbbr ? stateName(stateAbbr) : "the United States",
      scopeAbbr: stateAbbr || "US",
    };
  }, [entry, stateAbbr, statePrices]);

  // Headline "Lowest / Highest" use the eligible pool's actual min/max so the
  // four price cards line up exactly with the Cheapest 3 / Priciest 3 podium
  // below. Chargemaster placeholders are filtered out upstream in analysis.py.
  const headline = useMemo(() => {
    if (!cur) return null;
    const lo = cur.min && cur.min > 0 ? cur.min : null;
    const hi = cur.max && cur.max > 0 ? cur.max : null;
    return { lo, hi };
  }, [cur]);

  const chartData = useMemo(() => {
    if (!cur) return [];
    const points: Array<{ name: string; v: number }> = [];
    if (showRange === "full" && cur.min) points.push({ name: "Lowest", v: cur.min });
    if (cur.p10) points.push({ name: "P10", v: cur.p10 });
    if (cur.p25) points.push({ name: "P25", v: cur.p25 });
    if (cur.median) points.push({ name: "Median", v: cur.median });
    if (cur.p75) points.push({ name: "P75", v: cur.p75 });
    if (cur.p90) points.push({ name: "P90", v: cur.p90 });
    if (showRange === "full" && cur.max) points.push({ name: "Highest", v: cur.max });
    return points.filter((p) => p.v && p.v > 0);
  }, [cur, showRange]);

  const cheapest = useMemo(() => {
    if (!entry || !stateAbbr) return null;
    return entry.cheapest_in_state?.[stateAbbr] || null;
  }, [entry, stateAbbr]);

  const priciest = useMemo(() => {
    if (!entry || !stateAbbr) return null;
    return entry.priciest_in_state?.[stateAbbr] || null;
  }, [entry, stateAbbr]);

  const topCheapest = useMemo(() => {
    const list = entry?.top_cheapest || [];
    return list.slice(0, 3).filter((h) => h.median != null);
  }, [entry]);

  const topPriciest = useMemo(() => {
    const list = entry?.top_priciest || [];
    return list.slice(0, 3).filter((h) => h.median != null);
  }, [entry]);

  if (err) return <p className="text-rose">{err}</p>;
  if (!entry || !cur) {
    return (
      <div className="space-y-6 animate-pulse">
        <div className="h-72 rounded-2xl bg-section" />
        <div className="grid gap-5 md:grid-cols-4">
          <div className="h-28 rounded-2xl bg-section" />
          <div className="h-28 rounded-2xl bg-section" />
          <div className="h-28 rounded-2xl bg-section" />
          <div className="h-28 rounded-2xl bg-section" />
        </div>
        <div className="h-80 rounded-2xl bg-section" />
      </div>
    );
  }

  const spreadX =
    headline?.lo && headline?.hi && headline.lo > 0
      ? headline.hi / headline.lo
      : null;
  const dollarSpread =
    headline?.lo && headline?.hi ? headline.hi - headline.lo : null;

  return (
    <div className="space-y-16 animate-floatIn">
      {/* HEADER */}
      <div>
        <Link
          to="/explore"
          className="inline-flex items-center gap-2 text-xs font-medium text-inkMuted hover:text-ink tracking-eyebrowTight uppercase"
        >
          <svg viewBox="0 0 24 24" className="h-3.5 w-3.5" fill="none">
            <path
              d="M19 12H5m0 0l5-5m-5 5l5 5"
              stroke="currentColor"
              strokeWidth="2"
              strokeLinecap="round"
              strokeLinejoin="round"
            />
          </svg>
          Back to look up a price
        </Link>

        <div className="mt-8 flex flex-wrap items-center gap-3">
          <span className="pill-soft">{categoryLabel(entry.category)}</span>
          <span className="pill font-mono">
            {entry.code_system} {entry.code}
          </span>
          {entry.setting && <span className="pill">{categoryLabel(entry.setting)}</span>}
        </div>
        <h1 className="display-1 mt-6 max-w-4xl text-balance">
          {entry.display_name}
        </h1>
        <p className="body-lead mt-6 max-w-3xl text-pretty">{entry.what_it_is}</p>
        {entry.when_youd_need_it && (
          <p className="mt-4 text-base text-inkMuted max-w-3xl leading-relaxed">
            <span className="font-semibold text-ink">When you would need it.</span>{" "}
            {entry.when_youd_need_it}
          </p>
        )}
      </div>

      {/* SCOPE & PRICE GRID */}
      <div>
        <RateBadge variant="banner" className="mb-8" />

        <div className="mb-7 flex flex-wrap items-end justify-between gap-4">
          <div>
            <p className="eyebrow">Showing pre-insurance list prices in</p>
            <p className="font-display text-3xl font-medium text-ink mt-2 tracking-[-0.01em]">
              {cur.scope}
            </p>
            <p className="text-sm text-inkMuted mt-1">
              Across {cur.count.toLocaleString()} hospital
              {cur.count === 1 ? "" : "s"} that publish this code
            </p>
          </div>
          {states.length > 0 && (
            <StateFilter
              states={states}
              value={stateAbbr}
              onChange={onStateChange}
              label="Filter"
            />
          )}
        </div>

        <p className="mb-3 text-[10px] font-semibold uppercase tracking-eyebrowTight text-inkSubtle">
          Pre-insurance list price <span className="text-inkSubtle/70">·</span>{" "}
          <span className="font-medium normal-case tracking-normal text-inkMuted">
            gross charge or cash-pay rate the hospital publishes, before any insurance
          </span>
        </p>
        <div className="price-grid">
          <div className="price-cell">
            <p className="price-cell-key">
              <span className="text-mint">●</span> Lowest hospital
            </p>
            <p className="price-cell-num">{fmtMoney(headline?.lo)}</p>
            <p className="price-cell-sub">cheapest list price we found</p>
          </div>
          <div className="price-cell bg-section/40 border-ink/15">
            <p className="price-cell-key">Typical (median)</p>
            <p className="price-cell-num">{fmtMoney(cur.median)}</p>
            <p className="price-cell-sub">half of hospitals list less, half list more</p>
          </div>
          <div className="price-cell">
            <p className="price-cell-key">Average (mean)</p>
            <p className="price-cell-num">{fmtMoney(cur.mean)}</p>
            <p className="price-cell-sub">arithmetic average of hospital list prices</p>
          </div>
          <div className="price-cell">
            <p className="price-cell-key">
              <span className="text-rose">●</span> Highest hospital
            </p>
            <p className="price-cell-num">{fmtMoney(headline?.hi)}</p>
            <p className="price-cell-sub">priciest list price we found</p>
          </div>
        </div>

        <div className="mt-3 text-xs text-inkSubtle">
          One row per hospital, one number per row (each hospital's own median
          for this code). We drop hospitals whose published list price is
          obviously a chargemaster placeholder (a $5 ACL repair, a $0.17 drug)
          before counting, ranking, or charting. Below: the same {cur.count.toLocaleString()} hospitals as a percentile curve.
        </div>

        <div className="mt-5 grid gap-3 md:grid-cols-2 text-sm">
          <div className="surface-quiet px-6 py-5">
            <p className="eyebrow">Highest vs lowest</p>
            <p className="font-display text-3xl font-medium text-ink mt-2 tracking-[-0.01em]">
              {spreadX ? `${spreadX < 10 ? spreadX.toFixed(1) : Math.round(spreadX)}×` : "n/a"}
            </p>
            <p className="text-xs text-inkMuted mt-1">
              {spreadX
                ? `the priciest hospital charges ${
                    spreadX < 10 ? spreadX.toFixed(1) : Math.round(spreadX)
                  } times what the cheapest one does`
                : "not enough data to compare"}
            </p>
          </div>
          <div className="surface-quiet px-6 py-5">
            <p className="eyebrow">Dollar gap</p>
            <p className="font-display text-3xl font-medium text-ink mt-2 tracking-[-0.01em]">
              {dollarSpread ? fmtMoney(dollarSpread) : "n/a"}
            </p>
            <p className="text-xs text-inkMuted mt-1">
              difference between cheapest and priciest in {cur.scope}
            </p>
          </div>
        </div>

        {stateAbbr && (cheapest || priciest) && (
          <div className="mt-5 grid gap-3 md:grid-cols-2">
            {cheapest && (
              <div className="surface-edge px-6 py-6">
                <p className="eyebrow text-mint">
                  Lowest list price in {stateName(stateAbbr)}
                </p>
                <p className="font-display text-xl font-semibold text-ink mt-2">
                  {cheapest.name}
                </p>
                {cheapest.city && (
                  <p className="text-sm text-inkMuted">{cheapest.city}</p>
                )}
                <p className="font-display text-3xl font-medium text-ink mt-3 tracking-[-0.01em]">
                  {fmtMoney(cheapest.median)}
                </p>
                <p className="mt-1 text-xs text-inkSubtle">
                  hospital's pre-insurance list price
                </p>
                {cheapest.line_item ? (
                  <LineItemBlock item={cheapest.line_item} />
                ) : null}
              </div>
            )}
            {priciest && (
              <div className="surface-edge px-6 py-6">
                <p className="eyebrow text-rose">
                  Highest list price in {stateName(stateAbbr)}
                </p>
                <p className="font-display text-xl font-semibold text-ink mt-2">
                  {priciest.name}
                </p>
                {priciest.city && (
                  <p className="text-sm text-inkMuted">{priciest.city}</p>
                )}
                <p className="font-display text-3xl font-medium text-ink mt-3 tracking-[-0.01em]">
                  {fmtMoney(priciest.median)}
                </p>
                <p className="mt-1 text-xs text-inkSubtle">
                  hospital's pre-insurance list price
                </p>
                {priciest.line_item ? (
                  <LineItemBlock item={priciest.line_item} />
                ) : null}
              </div>
            )}
          </div>
        )}
      </div>

      {/* TOP 3 CHEAPEST + TOP 3 PRICIEST HOSPITALS NATIONWIDE */}
      {(topCheapest.length > 0 || topPriciest.length > 0) && (
        <section>
          <div className="mb-7 flex flex-wrap items-end justify-between gap-4">
            <div>
              <p className="eyebrow">By hospital</p>
              <h2 className="display-2 mt-3">
                Where it's cheapest. Where it's not.
              </h2>
              <p className="text-sm text-inkMuted mt-2 max-w-2xl">
                Top three hospitals in the country by their median price for
                this code, with the three priciest for contrast. Pulled from
                the same MRFs.
              </p>
            </div>
          </div>
          <div
            className={`grid gap-px overflow-hidden rounded-2xl border border-line bg-line ${
              topPriciest.length === 0 ? "md:grid-cols-1" : "md:grid-cols-2"
            }`}
          >
            {topCheapest.length > 0 && (
              <RankedHospitalList
                title="Cheapest 3 in the country"
                tone="mint"
                hospitals={topCheapest}
              />
            )}
            {topPriciest.length > 0 && (
              <RankedHospitalList
                title="Priciest 3 in the country"
                tone="rose"
                hospitals={topPriciest}
              />
            )}
          </div>
          <p className="mt-4 text-xs text-inkSubtle max-w-3xl">
            We rank by each hospital's own median price for this code so a
            single $5 chargemaster placeholder doesn't take the podium.
            Hospitals appearing here published this code in their own
            machine-readable file. Click "View source MRF" on any card to open
            the original file straight from the hospital's website and verify
            the price.
          </p>
        </section>
      )}

      {/* WHY THE SPREAD IS SO LARGE */}
      {(topCheapest.length > 0 || topPriciest.length > 0) && (
        <SpreadExplainer entry={entry} />
      )}

      <CaveatBanner />

      {/* PRICE SHAPE CHART */}
      {chartData.length > 0 && (
        <section>
          <div className="mb-7 flex flex-wrap items-end justify-between gap-4">
            <div>
              <p className="eyebrow">Price shape</p>
              <h2 className="display-2 mt-3">From cheapest to most expensive</h2>
              <p className="text-sm text-inkMuted mt-2 max-w-2xl">
                Each point is a percentile of all the prices we found in{" "}
                {cur.scope}. Toggle the full range to include the absolute lowest
                and highest hospitals.
              </p>
            </div>
            <div className="flex items-center rounded-full border border-line bg-surface p-1 text-xs font-medium">
              <button
                type="button"
                onClick={() => setShowRange("core")}
                className={`rounded-full px-4 py-1.5 transition-all ${
                  showRange === "core"
                    ? "bg-ink text-bg"
                    : "text-inkMuted hover:text-ink"
                }`}
              >
                Typical range
              </button>
              <button
                type="button"
                onClick={() => setShowRange("full")}
                className={`rounded-full px-4 py-1.5 transition-all ${
                  showRange === "full"
                    ? "bg-ink text-bg"
                    : "text-inkMuted hover:text-ink"
                }`}
              >
                Full range
              </button>
            </div>
          </div>
          <div className="surface-card p-6 md:p-8">
            <div className="h-[22rem]">
              <ResponsiveContainer width="100%" height="100%">
                <AreaChart
                  data={chartData}
                  margin={{ top: 36, right: 64, bottom: 12, left: 8 }}
                >
                  <defs>
                    <linearGradient id="priceFill" x1="0" y1="0" x2="0" y2="1">
                      <stop offset="0%" stopColor="#0A0F1A" stopOpacity={0.18} />
                      <stop offset="100%" stopColor="#0A0F1A" stopOpacity={0.02} />
                    </linearGradient>
                  </defs>
                  <CartesianGrid
                    strokeDasharray="2 4"
                    stroke="#EFEBDF"
                    vertical={false}
                  />
                  <XAxis
                    dataKey="name"
                    stroke="#8A93A1"
                    tickLine={false}
                    axisLine={false}
                    tick={{ fontSize: 12 }}
                  />
                  <YAxis
                    stroke="#8A93A1"
                    tickLine={false}
                    axisLine={false}
                    tick={{ fontSize: 12 }}
                    tickFormatter={fmtPriceShort}
                  />
                  <Tooltip
                    cursor={{ stroke: "#0A0F1A", strokeOpacity: 0.15 }}
                    contentStyle={{
                      borderRadius: 12,
                      border: "1px solid #E5E1D5",
                      boxShadow: "0 8px 28px rgba(10, 15, 26, 0.08)",
                      fontSize: 13,
                    }}
                    formatter={(v: number) => [
                      new Intl.NumberFormat("en-US", {
                        style: "currency",
                        currency: "USD",
                        maximumFractionDigits: 0,
                      }).format(v),
                      "price",
                    ]}
                  />
                  {cur?.mean && cur.mean > 0 ? (
                    <ReferenceLine
                      y={cur.mean}
                      stroke="#E5784E"
                      strokeDasharray="4 4"
                      strokeWidth={1.5}
                      ifOverflow="extendDomain"
                      label={{
                        value: `Avg ${fmtPriceShort(cur.mean)}`,
                        position: "right",
                        fill: "#E5784E",
                        fontSize: 11,
                        fontWeight: 600,
                        offset: 8,
                      }}
                    />
                  ) : null}
                  <Area
                    type="monotone"
                    dataKey="v"
                    stroke="#0A0F1A"
                    strokeWidth={2}
                    fill="url(#priceFill)"
                    dot={{
                      r: 4,
                      fill: "#0A0F1A",
                      stroke: "#FBF7EC",
                      strokeWidth: 2,
                    }}
                    activeDot={{
                      r: 6,
                      fill: "#0A0F1A",
                      stroke: "#FBF7EC",
                      strokeWidth: 2,
                    }}
                  >
                    <LabelList
                      dataKey="v"
                      position="top"
                      offset={12}
                      formatter={fmtPriceShort}
                      className="font-display"
                      style={{
                        fill: "#0A0F1A",
                        fontSize: 12,
                        fontWeight: 600,
                      }}
                    />
                  </Area>
                </AreaChart>
              </ResponsiveContainer>
            </div>
            <p className="mt-5 border-t border-line pt-4 text-xs text-inkSubtle">
              Dashed line is the simple average. The percentile curve usually
              flattens near the median and steepens toward P90 because a small
              number of hospitals publish very high chargemaster prices.
            </p>
          </div>
        </section>
      )}

      {/* WHAT'S INCLUDED */}
      <section className="grid gap-5 md:grid-cols-2">
        <div className="surface-card p-7">
          <div className="flex items-center gap-3">
            <div className="grid h-9 w-9 place-items-center rounded-full bg-mintSoft text-mint">
              <svg viewBox="0 0 24 24" className="h-4 w-4" fill="none">
                <path
                  d="M5 12l4 4L19 6"
                  stroke="currentColor"
                  strokeWidth="2.4"
                  strokeLinecap="round"
                  strokeLinejoin="round"
                />
              </svg>
            </div>
            <h2 className="font-display text-xl font-semibold text-ink">
              Usually included
            </h2>
          </div>
          <p className="mt-4 text-base text-inkMuted leading-relaxed">
            {entry.bundled_with}
          </p>
        </div>
        <div className="surface-card p-7">
          <div className="flex items-center gap-3">
            <div className="grid h-9 w-9 place-items-center rounded-full bg-roseSoft text-rose">
              <svg viewBox="0 0 24 24" className="h-4 w-4" fill="none">
                <path
                  d="M12 9v4m0 3.5h.01"
                  stroke="currentColor"
                  strokeWidth="2.2"
                  strokeLinecap="round"
                />
                <circle
                  cx="12"
                  cy="12"
                  r="9"
                  stroke="currentColor"
                  strokeWidth="1.8"
                />
              </svg>
            </div>
            <h2 className="font-display text-xl font-semibold text-ink">
              Often a separate bill
            </h2>
          </div>
          <p className="mt-4 text-base text-inkMuted leading-relaxed">
            {entry.not_bundled}
          </p>
        </div>
      </section>

      {entry.tips?.length > 0 && (
        <section>
          <div className="mb-6">
            <p className="eyebrow">Before you go</p>
            <h2 className="display-2 mt-3">Three things to ask.</h2>
          </div>
          <ul className="grid gap-px overflow-hidden rounded-2xl border border-line bg-line md:grid-cols-3">
            {entry.tips.map((t, i) => (
              <li
                key={t}
                className="bg-surface px-7 py-7 flex flex-col gap-3"
              >
                <span className="font-display text-3xl font-medium text-accent tracking-[-0.02em]">
                  {String(i + 1).padStart(2, "0")}
                </span>
                <span className="text-base text-ink leading-relaxed">{t}</span>
              </li>
            ))}
          </ul>
        </section>
      )}

      {stateRanking.length >= 2 && !stateAbbr && (
        <section>
          <div className="mb-6">
            <p className="eyebrow">By state</p>
            <h2 className="display-2 mt-3">How it varies across the country.</h2>
          </div>
          <div className="grid gap-3 md:grid-cols-2">
            <div className="surface-card p-7">
              <p className="eyebrow text-mint">Cheapest state median</p>
              <p className="font-display text-2xl font-semibold text-ink mt-2">
                {stateName(stateRanking[0].state)}
              </p>
              <p className="font-display text-4xl font-medium text-ink mt-3 tracking-[-0.02em]">
                {fmtMoney(stateRanking[0].median)}
              </p>
              <p className="text-xs text-inkMuted mt-2">
                across {stateRanking[0].count} hospital
                {stateRanking[0].count === 1 ? "" : "s"}
              </p>
            </div>
            <div className="surface-card p-7">
              <p className="eyebrow text-rose">Priciest state median</p>
              <p className="font-display text-2xl font-semibold text-ink mt-2">
                {stateName(stateRanking[stateRanking.length - 1].state)}
              </p>
              <p className="font-display text-4xl font-medium text-ink mt-3 tracking-[-0.02em]">
                {fmtMoney(stateRanking[stateRanking.length - 1].median)}
              </p>
              <p className="text-xs text-inkMuted mt-2">
                across {stateRanking[stateRanking.length - 1].count} hospital
                {stateRanking[stateRanking.length - 1].count === 1 ? "" : "s"}
              </p>
            </div>
          </div>
          <p className="mt-4 text-xs text-inkSubtle">
            Pick a state above to see the cheapest and most expensive hospital
            in that state.
          </p>
        </section>
      )}
    </div>
  );
}
