import { useEffect, useMemo, useState } from "react";
import { loadAll } from "../api";
import { fmtNum, stateName } from "../format";
import { StateFilter } from "../components/StateFilter";
import { RateBadge } from "../components/RateBadge";
import type { HospitalIndexRow } from "../types";

type SortKey = "name" | "state" | "codes_covered" | "honesty_score";

export function Hospitals() {
  const [rows, setRows] = useState<HospitalIndexRow[]>([]);
  const [totalCodes, setTotalCodes] = useState(250);
  const [q, setQ] = useState("");
  const [stateFilter, setStateFilter] = useState("");
  const [sortKey, setSortKey] = useState<SortKey>("codes_covered");
  const [asc, setAsc] = useState(false);

  useEffect(() => {
    loadAll().then((d) => {
      setRows(d.hospitalIndex);
      setTotalCodes(d.codes.length);
    });
  }, []);

  const states = useMemo(
    () =>
      Array.from(new Set(rows.map((r) => r.state).filter(Boolean) as string[])).sort(),
    [rows]
  );

  const filtered = useMemo(() => {
    const list = rows.filter((h) => {
      const matchesQ =
        !q ||
        String(h.name || "").toLowerCase().includes(q.toLowerCase()) ||
        String(h.system || "").toLowerCase().includes(q.toLowerCase()) ||
        String(h.hospital_id || "").toLowerCase().includes(q.toLowerCase());
      const matchesState = !stateFilter || h.state === stateFilter;
      return matchesQ && matchesState;
    });
    return list.sort((a, b) => {
      let av: string | number | null = (a as Record<string, unknown>)[sortKey] as
        | string
        | number
        | null;
      let bv: string | number | null = (b as Record<string, unknown>)[sortKey] as
        | string
        | number
        | null;
      if (av == null) av = sortKey === "name" || sortKey === "state" ? "" : -1;
      if (bv == null) bv = sortKey === "name" || sortKey === "state" ? "" : -1;
      if (typeof av === "number" && typeof bv === "number")
        return asc ? av - bv : bv - av;
      return asc
        ? String(av).localeCompare(String(bv))
        : String(bv).localeCompare(String(av));
    });
  }, [rows, q, stateFilter, sortKey, asc]);

  const summary = useMemo(() => {
    const withData = rows.filter((r) => (r.codes_covered ?? 0) > 0).length;
    const honestyVals = rows
      .map((r) => r.honesty_score)
      .filter((s): s is number => typeof s === "number");
    const avgHonesty =
      honestyVals.length > 0
        ? honestyVals.reduce((a, b) => a + b, 0) / honestyVals.length
        : null;
    return {
      total: rows.length,
      withData,
      avgHonesty,
      states: states.length,
    };
  }, [rows, states]);

  function toggleSort(key: SortKey) {
    if (sortKey === key) setAsc(!asc);
    else {
      setSortKey(key);
      setAsc(false);
    }
  }

  if (rows.length === 0) {
    return (
      <div className="space-y-6 animate-pulse">
        <div className="h-12 w-1/2 rounded-2xl bg-section" />
        <div className="grid gap-4 md:grid-cols-3">
          {Array.from({ length: 3 }).map((_, i) => (
            <div key={i} className="h-24 rounded-2xl bg-section" />
          ))}
        </div>
        <div className="h-80 rounded-2xl bg-section" />
      </div>
    );
  }

  return (
    <div className="space-y-14 animate-floatIn">
      <div className="max-w-3xl">
        <p className="eyebrow">The hospitals in this run</p>
        <h1 className="display-2 mt-3 text-balance">
          {fmtNum(summary.total)} real US hospitals.
        </h1>
        <p className="body-lead mt-5 text-pretty">
          These are the hospitals whose federal price transparency files we
          read for this dataset. Click any row to open their official MRF.
        </p>
        <RateBadge variant="inline" plural className="mt-5" />
      </div>

      {/* Stat row */}
      <div className="grid gap-12 md:grid-cols-3 md:gap-8 border-t border-b border-line py-12">
        <Stat
          label="Hospitals included"
          value={fmtNum(summary.total)}
          hint={`${fmtNum(summary.withData)} had readable prices`}
        />
        <Stat
          label="States covered"
          value={String(summary.states)}
          hint="50 states plus DC and Puerto Rico"
        />
        <Stat
          label="Cash beats insurance"
          value={
            summary.avgHonesty != null
              ? `${(summary.avgHonesty * 100).toFixed(0)}%`
              : "n/a"
          }
          hint="how often the cash list price is lower than the hospital's lowest negotiated insurance rate"
        />
      </div>

      {/* Search + filters */}
      <div className="space-y-5">
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
            placeholder="Search hospital or health system..."
            value={q}
            onChange={(e) => setQ(e.target.value)}
          />
          {q && (
            <button
              type="button"
              onClick={() => setQ("")}
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
        <div className="flex justify-end">
          <StateFilter
            states={states}
            value={stateFilter}
            onChange={setStateFilter}
            label="Filter by state"
          />
        </div>
      </div>

      {/* Table */}
      <div className="surface-card overflow-hidden">
        <div className="overflow-x-auto">
          <table className="editorial-table">
            <caption className="bg-section/40 px-7 py-3 text-left text-[10px] font-semibold uppercase tracking-eyebrowTight text-inkSubtle">
              "Codes priced" counts how many of our tracked procedures and drugs the hospital actually publishes a pre-insurance list price for.
            </caption>
            <thead>
              <tr>
                <SortableTh
                  active={sortKey === "name"}
                  asc={asc}
                  onClick={() => toggleSort("name")}
                  className="px-7 py-4"
                >
                  Hospital
                </SortableTh>
                <SortableTh
                  active={sortKey === "state"}
                  asc={asc}
                  onClick={() => toggleSort("state")}
                  className="px-7 py-4"
                >
                  State
                </SortableTh>
                <SortableTh
                  active={sortKey === "codes_covered"}
                  asc={asc}
                  onClick={() => toggleSort("codes_covered")}
                  className="px-7 py-4 text-right"
                >
                  Codes priced
                </SortableTh>
                <th className="px-7 py-4 text-right">Source</th>
              </tr>
            </thead>
            <tbody>
              {filtered.slice(0, 500).map((h) => (
                <tr key={h.hospital_id}>
                  <td className="px-7">
                    <p className="font-medium text-ink">{h.name}</p>
                    {(h.system || h.city) && (
                      <p className="text-xs text-inkSubtle mt-0.5">
                        {h.system}
                        {h.city ? ` · ${h.city}` : ""}
                      </p>
                    )}
                  </td>
                  <td className="px-7">
                    {h.state ? (
                      <span className="text-inkMuted">{stateName(h.state)}</span>
                    ) : (
                      <span className="text-inkSubtle">n/a</span>
                    )}
                  </td>
                  <td className="px-7 text-right">
                    <span className="font-display text-lg font-medium text-ink tracking-[-0.01em]">
                      {h.codes_covered ?? 0}
                    </span>
                    <span className="text-xs font-normal text-inkSubtle">
                      {" "}/ {totalCodes}
                    </span>
                  </td>
                  <td className="px-7 text-right">
                    {h.mrf_url ? (
                      <a
                        href={h.mrf_url}
                        target="_blank"
                        rel="noreferrer"
                        className="inline-flex items-center gap-1 text-xs font-medium text-ink underline-offset-4 hover:underline"
                      >
                        Open MRF
                        <svg
                          viewBox="0 0 24 24"
                          className="h-3 w-3"
                          fill="none"
                        >
                          <path
                            d="M7 17L17 7M17 7H8m9 0v9"
                            stroke="currentColor"
                            strokeWidth="2"
                            strokeLinecap="round"
                            strokeLinejoin="round"
                          />
                        </svg>
                      </a>
                    ) : (
                      <span className="text-xs text-inkSubtle">n/a</span>
                    )}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
        {filtered.length === 0 && (
          <p className="px-6 py-12 text-center text-sm text-inkSubtle">
            No hospitals match those filters.
          </p>
        )}
        {filtered.length > 500 && (
          <p className="px-6 py-3 text-center text-xs text-inkSubtle border-t border-line/70">
            Showing the first 500 of {filtered.length.toLocaleString()} matches.
          </p>
        )}
      </div>
    </div>
  );
}

function Stat({
  label,
  value,
  hint,
}: {
  label: string;
  value: string;
  hint?: string;
}) {
  return (
    <div>
      <p className="eyebrow">{label}</p>
      <p className="stat-num mt-3 text-ink">{value}</p>
      {hint && <p className="mt-2 text-sm text-inkMuted">{hint}</p>}
    </div>
  );
}

function SortableTh({
  active,
  asc,
  onClick,
  className,
  children,
}: {
  active: boolean;
  asc: boolean;
  onClick: () => void;
  className: string;
  children: React.ReactNode;
}) {
  return (
    <th className={className}>
      <button
        type="button"
        onClick={onClick}
        className={`inline-flex items-center gap-1.5 ${
          active ? "text-ink" : "hover:text-ink"
        }`}
      >
        {children}
        <span
          className={`transition-transform text-[10px] ${
            active ? (asc ? "rotate-180" : "") : "opacity-30"
          }`}
        >
          ▾
        </span>
      </button>
    </th>
  );
}
