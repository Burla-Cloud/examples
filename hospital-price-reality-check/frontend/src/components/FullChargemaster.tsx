import { Fragment, useEffect, useMemo, useState } from "react";
import { Link } from "react-router-dom";
import { loadFullChargemaster } from "../api";
import { fmtMoney, fmtNum } from "../format";
import type {
  ChargemasterDoc,
  ChargemasterRow,
  CodeEntry,
  HospitalCodeRow,
  StateCodeStat,
} from "../types";

type Status = "loading" | "ready" | "error";

const PAGE_SIZE = 50;

const SEMANTIC_BUCKETS: Array<{
  key: string;
  label: string;
}> = [
  { key: "all", label: "All items" },
  { key: "procedure", label: "Procedures" },
  { key: "drug", label: "Drugs" },
  { key: "lab", label: "Labs" },
  { key: "imaging", label: "Imaging" },
  { key: "supply", label: "Supplies" },
  { key: "other", label: "Other" },
];

/**
 * Categorize a chargemaster row into a user-facing bucket by code range.
 * This is intentionally simple (no LLM, no lookup table) because we run it on
 * tens of thousands of rows in the browser as the user types.
 */
function categorize(cs?: string, code?: string): string {
  const sys = (cs || "").toUpperCase();
  const c = (code || "").toUpperCase();
  if (sys === "NDC") return "drug";
  if (sys === "MS-DRG" || sys === "DRG") return "procedure";
  if (sys === "RC" || sys === "REVCODE") return "supply";
  if (sys === "HCPCS") {
    if (c.startsWith("J") || c.startsWith("Q")) return "drug";
    if (c.startsWith("P")) return "lab";
    if (c.startsWith("A")) return "supply";
    if (c.startsWith("E")) return "supply";
    if (c.startsWith("G")) return "procedure";
    return "other";
  }
  if (sys === "CPT") {
    const num = parseInt(c.replace(/\D+/g, ""), 10);
    if (Number.isFinite(num) && num > 0) {
      if (num >= 70000 && num < 80000) return "imaging";
      if (num >= 80000 && num < 90000) return "lab";
      return "procedure";
    }
    return "procedure";
  }
  if (sys === "CDT") return "procedure";
  return "other";
}

export type FullChargemasterProps = {
  hospitalId: string;
  hospitalName?: string | null;
  state?: string | null;
  /**
   * National stats for the curated 360 codes, keyed by "{cs}:{code}". When a
   * chargemaster row matches a curated entry, we surface the national median,
   * percentile placement, and link to the full comparison page.
   */
  codesByKey?: Map<string, CodeEntry>;
  /**
   * Statewide stats per code, keyed by "{cs}:{code}". Same coverage as
   * codesByKey (curated 360 only).
   */
  stateStatsByKey?: Map<string, StateCodeStat>;
  /**
   * The hospital's own per-code median (from the per-hospital JSON, curated
   * 360 only). Lets us show "Memorial's median for J9271 across all 5 rows
   * is $X" rather than just the clicked row's price.
   */
  hospitalCodesByKey?: Map<string, HospitalCodeRow>;
  /**
   * Fallback rows from the per-hospital JSON (curated subset, ~80-130 rows).
   * Used when the full chargemaster bundle is still being built so the page
   * never looks empty.
   */
  fallbackRows?: ChargemasterRow[];
};

export function FullChargemaster({
  hospitalId,
  hospitalName,
  state,
  codesByKey,
  stateStatsByKey,
  hospitalCodesByKey,
  fallbackRows,
}: FullChargemasterProps) {
  const [status, setStatus] = useState<Status>("loading");
  const [doc, setDoc] = useState<ChargemasterDoc | null>(null);
  const [errorMsg, setErrorMsg] = useState<string | null>(null);
  const [q, setQ] = useState("");
  const [bucket, setBucket] = useState("all");
  const [page, setPage] = useState(0);
  const [expanded, setExpanded] = useState<number | null>(null);

  useEffect(() => {
    let cancelled = false;
    setStatus("loading");
    setErrorMsg(null);
    setDoc(null);
    setExpanded(null);
    loadFullChargemaster(hospitalId)
      .then((d) => {
        if (!cancelled) {
          setDoc(d);
          setStatus("ready");
        }
      })
      .catch((e) => {
        if (cancelled) return;
        setErrorMsg(e instanceof Error ? e.message : String(e));
        setStatus("error");
      });
    return () => {
      cancelled = true;
    };
  }, [hospitalId]);

  // When the full chargemaster bundle hasn't been built yet (or fetch fails)
  // we degrade gracefully to the curated rows that live in the hospital JSON
  // we already have on disk. Same UI, smaller dataset, with a banner so the
  // user knows the bigger data is coming.
  const usingFallback = status !== "ready" && (fallbackRows?.length ?? 0) > 0;
  const effectiveDoc: ChargemasterDoc | null = useMemo(() => {
    const filterPriced = (rows: ChargemasterRow[]): ChargemasterRow[] =>
      rows.filter((r) => {
        // Older bundles include rows with no price (a description + code but
        // no gross/cash/min/max/per-unit). Drop them client-side so the UI
        // only shows rows the hospital actually priced.
        const vals: Array<number | null | undefined> = [r.g, r.ca, r.mn, r.mx, r.p];
        return vals.some((v) => typeof v === "number" && v > 0);
      });
    if (status === "ready" && doc) {
      const rows = filterPriced(doc.rows);
      return {
        ...doc,
        rows,
        total: rows.length,
      };
    }
    if (fallbackRows && fallbackRows.length > 0) {
      const rows = filterPriced(fallbackRows);
      return {
        hospital_id: hospitalId,
        name: hospitalName,
        state: state ?? null,
        mrf_url: null,
        total: rows.length,
        truncated: false,
        rows,
      };
    }
    return null;
  }, [status, doc, fallbackRows, hospitalId, hospitalName, state]);

  const categorized = useMemo(() => {
    if (!effectiveDoc) return null;
    const counts: Record<string, number> = { all: effectiveDoc.rows.length };
    const tagged = effectiveDoc.rows.map((r) => {
      const cat = categorize(r.cs, r.c);
      counts[cat] = (counts[cat] || 0) + 1;
      return { row: r, cat };
    });
    return { tagged, counts };
  }, [effectiveDoc]);

  const filtered = useMemo(() => {
    if (!categorized) return [];
    const ql = q.trim().toLowerCase();
    return categorized.tagged.filter(({ row, cat }) => {
      if (bucket !== "all" && cat !== bucket) return false;
      if (!ql) return true;
      return (
        (row.d || "").toLowerCase().includes(ql) ||
        (row.c || "").toLowerCase().includes(ql) ||
        (row.ds || "").toLowerCase().includes(ql)
      );
    });
  }, [categorized, q, bucket]);

  const pageCount = Math.max(1, Math.ceil(filtered.length / PAGE_SIZE));
  const safePage = Math.min(page, pageCount - 1);
  const start = safePage * PAGE_SIZE;
  const slice = filtered.slice(start, start + PAGE_SIZE);

  const onChangeQ = (v: string) => {
    setQ(v);
    setPage(0);
    setExpanded(null);
  };
  const onChangeBucket = (v: string) => {
    setBucket(v);
    setPage(0);
    setExpanded(null);
  };

  return (
    <section className="space-y-6">
      <div className="max-w-3xl">
        <p className="eyebrow">Search this hospital's prices</p>
        <h2 className="display-3 mt-3">
          Find any procedure, drug, or lab
          {hospitalName ? (
            <>
              {" "}
              <span className="text-inkMuted font-light">
                at {hospitalName}
              </span>
            </>
          ) : null}
          .
        </h2>
        <p className="body-lead mt-4 text-pretty">
          Every priced row this hospital lists in their federal price
          transparency file, searchable. Click a row to see how their list
          price compares to other hospitals in {state ? state : "your state"}{" "}
          and nationwide.
        </p>
      </div>

      <div className="space-y-3">
        <div className="relative">
          <svg
            viewBox="0 0 24 24"
            className="absolute left-7 top-1/2 -translate-y-1/2 h-5 w-5 text-inkSubtle"
            fill="none"
          >
            <circle cx="11" cy="11" r="7" stroke="currentColor" strokeWidth="1.8" />
            <path d="M16.5 16.5L21 21" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" />
          </svg>
          <input
            className="input-search pl-16"
            placeholder={
              status === "ready"
                ? "Search by procedure name, drug, code, or dose..."
                : "Loading prices..."
            }
            value={q}
            onChange={(e) => onChangeQ(e.target.value)}
            disabled={status !== "ready"}
          />
          {q && (
            <button
              type="button"
              onClick={() => onChangeQ("")}
              aria-label="Clear search"
              className="absolute right-6 top-1/2 -translate-y-1/2 grid h-7 w-7 place-items-center rounded-full bg-section hover:bg-line text-inkMuted"
            >
              <svg viewBox="0 0 24 24" className="h-3.5 w-3.5" fill="none">
                <path
                  d="M6 6l12 12M18 6L6 18"
                  stroke="currentColor"
                  strokeWidth="2"
                  strokeLinecap="round"
                />
              </svg>
            </button>
          )}
        </div>

        <div className="flex flex-wrap items-center gap-2">
          {SEMANTIC_BUCKETS.map((b) => {
            const count = categorized?.counts[b.key] ?? 0;
            return (
              <button
                type="button"
                key={b.key}
                onClick={() => onChangeBucket(b.key)}
                disabled={status !== "ready"}
                className={`rounded-full border px-4 py-2 text-xs font-medium transition-all duration-200 disabled:opacity-50 ${
                  bucket === b.key
                    ? "border-ink bg-ink text-bg"
                    : "border-line bg-surface text-inkMuted hover:border-ink hover:text-ink"
                }`}
              >
                {b.label}
                {status === "ready" ? (
                  <span
                    className={`ml-2 text-[10px] ${
                      bucket === b.key ? "text-bg/70" : "text-inkSubtle"
                    }`}
                  >
                    {fmtNum(count)}
                  </span>
                ) : null}
              </button>
            );
          })}
        </div>
      </div>

      {status === "loading" && !usingFallback && <ChargemasterSkeleton />}

      {status === "error" && !usingFallback && (
        <div className="surface-card p-6 md:p-7">
          <p className="text-sm text-inkMuted">
            {errorMsg || "Couldn't load this hospital's prices."}
          </p>
          <button
            type="button"
            onClick={() => {
              setStatus("loading");
              setErrorMsg(null);
              loadFullChargemaster(hospitalId)
                .then((d) => {
                  setDoc(d);
                  setStatus("ready");
                })
                .catch((e) => {
                  setErrorMsg(e instanceof Error ? e.message : String(e));
                  setStatus("error");
                });
            }}
            className="mt-4 inline-flex items-center gap-1.5 text-sm font-medium text-ink underline-offset-4 hover:underline"
          >
            Try again
          </button>
        </div>
      )}

      {usingFallback && (
        <div className="surface-card px-5 py-4 md:px-6 border-line bg-section/40">
          <p className="text-sm text-inkMuted leading-relaxed">
            <span className="font-medium text-ink">
              Showing the {fmtNum(fallbackRows?.length ?? 0)} most-common
              procedures, drugs, and labs this hospital lists.
            </span>{" "}
            The full chargemaster (every priced row in this hospital's federal
            MRF, often 10,000-50,000 items) is still being processed and will
            replace this view once it's ready.
          </p>
        </div>
      )}

      {effectiveDoc && categorized && (
        <>
          <div className="flex flex-wrap items-baseline gap-x-6 gap-y-2 text-sm text-inkMuted">
            <span>
              <span className="text-ink font-medium">
                {fmtNum(effectiveDoc.total)}
              </span>{" "}
              priced rows
              {effectiveDoc.truncated ? " (first 50,000 shown)" : ""}.
            </span>
            <span>
              <span className="text-ink font-medium">
                {fmtNum(filtered.length)}
              </span>{" "}
              match.
            </span>
          </div>

          <div className="surface-card overflow-hidden">
            <div className="overflow-x-auto">
              <table className="editorial-table">
                <thead>
                  <tr>
                    <th className="px-7 py-4 text-left">Description</th>
                    <th className="px-7 py-4 text-left">Code</th>
                    <th className="px-7 py-4 text-left">Dose / setting</th>
                    <th className="px-7 py-4 text-right">Gross</th>
                    <th className="px-7 py-4 text-right">Cash</th>
                    <th className="px-7 py-4 text-right">Per unit</th>
                  </tr>
                </thead>
                <tbody>
                  {slice.map(({ row: r }, i) => {
                    const idx = start + i;
                    const isOpen = expanded === idx;
                    const key = `${r.cs || ""}:${r.c || ""}`;
                    const curated = codesByKey?.get(key);
                    const stateStat = stateStatsByKey?.get(key);
                    const hospCode = hospitalCodesByKey?.get(key);
                    return (
                      <Fragment key={idx}>
                        <tr
                          className={`cursor-pointer transition-colors hover:bg-section/40 ${
                            isOpen ? "bg-section/40" : ""
                          }`}
                          onClick={() => setExpanded(isOpen ? null : idx)}
                        >
                          <td className="px-7 py-4 align-top text-sm text-ink">
                            {r.d || (
                              <span className="text-inkSubtle">
                                (no description)
                              </span>
                            )}
                            {curated ? (
                              <p className="text-[10px] uppercase tracking-[0.16em] text-accent mt-1.5 font-medium">
                                Comparison available
                              </p>
                            ) : null}
                          </td>
                          <td className="px-7 py-4 align-top whitespace-nowrap">
                            <p className="text-[11px] uppercase tracking-[0.16em] text-inkSubtle">
                              {r.cs || "—"}
                            </p>
                            <p className="text-sm text-inkMuted mt-0.5">
                              {r.c || "—"}
                            </p>
                          </td>
                          <td className="px-7 py-4 align-top text-sm text-inkMuted whitespace-nowrap">
                            {r.ds ? <p>{r.ds}</p> : null}
                            {r.se ? (
                              <p className="text-[11px] text-inkSubtle mt-0.5 uppercase tracking-wider">
                                {r.se}
                              </p>
                            ) : null}
                            {!r.ds && !r.se ? (
                              <span className="text-inkSubtle">—</span>
                            ) : null}
                          </td>
                          <td className="px-7 py-4 text-right align-top whitespace-nowrap text-sm text-ink">
                            {r.g != null ? (
                              fmtMoney(r.g)
                            ) : (
                              <span className="text-inkSubtle">—</span>
                            )}
                          </td>
                          <td className="px-7 py-4 text-right align-top whitespace-nowrap text-sm text-ink">
                            {r.ca != null ? (
                              fmtMoney(r.ca)
                            ) : (
                              <span className="text-inkSubtle">—</span>
                            )}
                          </td>
                          <td className="px-7 py-4 text-right align-top whitespace-nowrap text-sm font-medium text-ink">
                            {r.p != null ? (
                              <>
                                {fmtMoney(r.p)}
                                {r.u ? (
                                  <span className="text-inkSubtle ml-1 font-normal">
                                    / {r.u}
                                  </span>
                                ) : null}
                              </>
                            ) : (
                              <span className="text-inkSubtle">—</span>
                            )}
                          </td>
                        </tr>
                        {isOpen ? (
                          <tr>
                            <td
                              colSpan={6}
                              className="px-7 py-6 bg-section/30 border-t border-line/50"
                            >
                              <ComparisonPanel
                                row={r}
                                hospitalName={hospitalName}
                                state={state}
                                curated={curated}
                                stateStat={stateStat}
                                hospCode={hospCode}
                              />
                            </td>
                          </tr>
                        ) : null}
                      </Fragment>
                    );
                  })}
                </tbody>
              </table>
            </div>
            {filtered.length === 0 ? (
              <p className="px-6 py-12 text-center text-sm text-inkSubtle">
                No rows match those filters.
              </p>
            ) : (
              <div className="flex items-center justify-between px-6 py-4 text-xs text-inkMuted border-t border-line/70">
                <span>
                  Showing {fmtNum(start + 1)}–{fmtNum(start + slice.length)} of{" "}
                  {fmtNum(filtered.length)}
                </span>
                <div className="flex items-center gap-2">
                  <button
                    type="button"
                    onClick={() => {
                      setPage((p) => Math.max(0, p - 1));
                      setExpanded(null);
                    }}
                    disabled={safePage <= 0}
                    className="rounded-full border border-line px-3 py-1.5 hover:border-ink hover:text-ink disabled:opacity-40 disabled:cursor-not-allowed transition-colors"
                  >
                    ← Prev
                  </button>
                  <span>
                    Page {safePage + 1} / {pageCount}
                  </span>
                  <button
                    type="button"
                    onClick={() => {
                      setPage((p) => Math.min(pageCount - 1, p + 1));
                      setExpanded(null);
                    }}
                    disabled={safePage >= pageCount - 1}
                    className="rounded-full border border-line px-3 py-1.5 hover:border-ink hover:text-ink disabled:opacity-40 disabled:cursor-not-allowed transition-colors"
                  >
                    Next →
                  </button>
                </div>
              </div>
            )}
          </div>

          <p className="text-xs text-inkSubtle leading-snug">
            Source: this hospital's federal machine-readable file. Rows that
            published a normalized HCPCS billing unit are shown per that unit;
            the rest fall back to the raw gross charge. Cash columns reflect
            the hospital's discounted cash-pay rate, where published. None of
            this is what insured patients pay.
          </p>
        </>
      )}
    </section>
  );
}

function ChargemasterSkeleton() {
  return (
    <div className="surface-card overflow-hidden">
      <div className="px-7 py-5 border-b border-line/40">
        <div className="h-3 w-2/3 rounded bg-section animate-pulse" />
      </div>
      {Array.from({ length: 8 }).map((_, i) => (
        <div
          key={i}
          className="px-7 py-5 border-b border-line/30 flex items-center gap-6"
        >
          <div className="h-3 w-1/3 rounded bg-section animate-pulse" />
          <div className="h-3 w-16 rounded bg-section animate-pulse" />
          <div className="h-3 w-20 rounded bg-section animate-pulse ml-auto" />
          <div className="h-3 w-20 rounded bg-section animate-pulse" />
        </div>
      ))}
    </div>
  );
}

/**
 * Inline panel shown when a chargemaster row is expanded. Pulls together
 * this hospital's price details + (if the code is in our curated 360) the
 * statewide and national medians for comparison.
 */
function ComparisonPanel({
  row,
  hospitalName,
  state,
  curated,
  stateStat,
  hospCode,
}: {
  row: ChargemasterRow;
  hospitalName?: string | null;
  state?: string | null;
  curated?: CodeEntry;
  stateStat?: StateCodeStat;
  hospCode?: HospitalCodeRow;
}) {
  const hospLabel = hospitalName || "This hospital";
  const unitLabel = row.u || curated?.billing_unit || "unit";
  const ratio = (() => {
    if (curated?.stats?.median == null || !row.p) return null;
    return row.p / curated.stats.median;
  })();

  return (
    <div className="grid gap-8 md:grid-cols-2">
      <div>
        <p className="eyebrow mb-3">{hospLabel}'s list price</p>
        <div className="space-y-3 text-sm">
          {row.p != null ? (
            <div>
              <p className="text-[10px] uppercase tracking-[0.16em] text-inkSubtle">
                Per {unitLabel}
              </p>
              <p className="font-display text-2xl font-medium text-ink tracking-[-0.02em] mt-1">
                {fmtMoney(row.p)}
              </p>
            </div>
          ) : null}
          {row.g != null ? (
            <p className="text-inkMuted">
              <span className="text-inkSubtle">Gross charge:</span>{" "}
              {fmtMoney(row.g)}
            </p>
          ) : null}
          {row.ca != null ? (
            <p className="text-inkMuted">
              <span className="text-inkSubtle">Cash-pay rate:</span>{" "}
              {fmtMoney(row.ca)}
            </p>
          ) : null}
          {row.mn != null || row.mx != null ? (
            <p className="text-inkMuted">
              <span className="text-inkSubtle">
                De-identified negotiated range:
              </span>{" "}
              {row.mn != null ? fmtMoney(row.mn) : "—"} —{" "}
              {row.mx != null ? fmtMoney(row.mx) : "—"}
            </p>
          ) : null}
          {row.ds ? (
            <p className="text-inkMuted">
              <span className="text-inkSubtle">Dose / package:</span> {row.ds}
            </p>
          ) : null}
          {row.se ? (
            <p className="text-inkMuted capitalize">
              <span className="text-inkSubtle normal-case">Setting:</span>{" "}
              {row.se}
            </p>
          ) : null}
          {hospCode?.median != null && (hospCode.count ?? 0) > 1 ? (
            <p className="text-inkSubtle text-xs leading-snug pt-2 border-t border-line/40">
              {hospLabel} lists {fmtNum(hospCode.count)} different rows for
              this code. The median across all of them is{" "}
              <span className="font-medium text-ink">
                {fmtMoney(hospCode.median)}
              </span>{" "}
              per {unitLabel}.
            </p>
          ) : null}
        </div>
      </div>

      <div>
        <p className="eyebrow mb-3">How that compares</p>
        {curated ? (
          <div className="space-y-3 text-sm">
            {stateStat?.median != null ? (
              <div className="border border-line/60 bg-surface px-4 py-3 rounded-lg">
                <p className="text-[10px] uppercase tracking-[0.16em] text-inkSubtle">
                  {state || "State"} median
                </p>
                <p className="font-display text-lg font-medium text-ink tracking-[-0.02em] mt-1">
                  {fmtMoney(stateStat.median)}
                </p>
                {stateStat.p25 != null && stateStat.p75 != null ? (
                  <p className="text-xs text-inkMuted mt-1">
                    middle 50% ranges {fmtMoney(stateStat.p25)} —{" "}
                    {fmtMoney(stateStat.p75)} ({fmtNum(stateStat.count || 0)}{" "}
                    hospitals)
                  </p>
                ) : null}
              </div>
            ) : null}
            {curated.stats?.median != null ? (
              <div className="border border-line/60 bg-surface px-4 py-3 rounded-lg">
                <p className="text-[10px] uppercase tracking-[0.16em] text-inkSubtle">
                  Nationwide median
                </p>
                <p className="font-display text-lg font-medium text-ink tracking-[-0.02em] mt-1">
                  {fmtMoney(curated.stats.median)}
                </p>
                {curated.stats.p25 != null && curated.stats.p75 != null ? (
                  <p className="text-xs text-inkMuted mt-1">
                    middle 50% ranges {fmtMoney(curated.stats.p25)} —{" "}
                    {fmtMoney(curated.stats.p75)} ({fmtNum(curated.stats.count || 0)}{" "}
                    hospitals)
                  </p>
                ) : null}
              </div>
            ) : null}
            {ratio != null && Number.isFinite(ratio) ? (
              <p className="text-xs text-inkMuted leading-snug">
                {hospLabel}'s {fmtMoney(row.p ?? null)} per {unitLabel} is{" "}
                <span className="font-medium text-ink">
                  {ratio >= 1
                    ? `${ratio.toFixed(1)}x the national median`
                    : `${(1 / ratio).toFixed(1)}x cheaper than the national median`}
                </span>
                .
              </p>
            ) : null}
            <Link
              to={`/explore/${encodeURIComponent(
                row.cs || curated.code_system,
              )}/${encodeURIComponent(row.c || curated.code)}`}
              className="inline-flex items-center gap-1.5 mt-2 text-sm font-medium text-ink underline-offset-4 hover:underline"
            >
              See every hospital that lists this code
              <svg viewBox="0 0 24 24" className="h-3 w-3" fill="none">
                <path
                  d="M5 12h14m0 0l-6-6m6 6l-6 6"
                  stroke="currentColor"
                  strokeWidth="1.8"
                  strokeLinecap="round"
                  strokeLinejoin="round"
                />
              </svg>
            </Link>
          </div>
        ) : (
          <div className="text-sm text-inkMuted space-y-2">
            <p>
              We track nationwide stats for about 360 of the most common
              procedures, drugs, and labs. This particular code isn't in
              that comparison set yet.
            </p>
            <p className="text-xs text-inkSubtle leading-snug">
              You can still see the published price above. For the wider
              comparison list, head over to{" "}
              <Link to="/explore" className="underline-offset-4 hover:underline text-ink">
                Search by procedure
              </Link>
              .
            </p>
          </div>
        )}
      </div>
    </div>
  );
}
