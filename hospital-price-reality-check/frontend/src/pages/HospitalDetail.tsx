import { useEffect, useMemo, useState } from "react";
import { Link, useParams } from "react-router-dom";
import { loadAll, loadHospitalDetail } from "../api";
import { fmtMoney, fmtNum, stateName } from "../format";
import type {
  CmsReference,
  HospitalCodeRow,
  HospitalDetail as HospitalDetailType,
} from "../types";

const CATEGORY_LABELS: Record<string, string> = {
  cancer_screening: "Cancer screening",
  cancer_treatment: "Cancer treatment",
  cardiovascular: "Cardiovascular",
  er: "Emergency",
  gi_endoscopy: "GI endoscopy",
  hospital_line_item: "Hospital line items",
  imaging: "Imaging",
  infused_drug: "Infused drugs",
  inpatient_drg: "Inpatient stays",
  lab: "Labs",
  maternity: "Maternity",
  mental_health: "Mental health",
  pediatric: "Pediatric",
  surgical: "Surgery",
  vaccine: "Vaccines",
};

function categoryLabel(cat: string | null | undefined): string {
  if (!cat) return "Other";
  return CATEGORY_LABELS[cat] || cat;
}

type SortKey = "name" | "median" | "category";

export function HospitalDetail() {
  const { hospitalId = "" } = useParams<{ hospitalId: string }>();
  const [detail, setDetail] = useState<HospitalDetailType | null>(null);
  const [cmsByKey, setCmsByKey] = useState<Map<string, CmsReference>>(new Map());
  const [error, setError] = useState<string | null>(null);
  const [q, setQ] = useState("");
  const [activeCat, setActiveCat] = useState<string>("all");
  const [sortKey, setSortKey] = useState<SortKey>("category");
  const [asc, setAsc] = useState(true);

  useEffect(() => {
    let cancelled = false;
    setError(null);
    setDetail(null);
    loadHospitalDetail(hospitalId)
      .then((d) => {
        if (!cancelled) setDetail(d);
      })
      .catch((e) => {
        if (!cancelled) setError(String(e?.message || e));
      });
    loadAll()
      .then((all) => {
        if (cancelled) return;
        const m = new Map<string, CmsReference>();
        for (const c of all.codes) {
          if (c.cms_reference) {
            m.set(`${c.code_system}:${c.code}`, c.cms_reference);
          }
        }
        setCmsByKey(m);
      })
      .catch(() => undefined);
    return () => {
      cancelled = true;
    };
  }, [hospitalId]);

  const categories = useMemo(() => {
    if (!detail) return [] as string[];
    const keys = new Set<string>();
    for (const c of detail.codes) keys.add(c.category || "other");
    return Array.from(keys).sort();
  }, [detail]);

  const visible = useMemo(() => {
    if (!detail) return [] as HospitalCodeRow[];
    const ql = q.trim().toLowerCase();
    let rows = detail.codes.filter((c) => {
      if (activeCat !== "all" && (c.category || "other") !== activeCat) return false;
      if (!ql) return true;
      return (
        (c.display_name || "").toLowerCase().includes(ql) ||
        (c.code || "").toLowerCase().includes(ql) ||
        (c.line_item?.description || "").toLowerCase().includes(ql)
      );
    });
    rows = rows.slice().sort((a, b) => {
      let av: string | number | null;
      let bv: string | number | null;
      if (sortKey === "median") {
        av = a.median ?? -1;
        bv = b.median ?? -1;
      } else if (sortKey === "category") {
        av = `${a.category || "zzz"}|${a.display_name || ""}`;
        bv = `${b.category || "zzz"}|${b.display_name || ""}`;
      } else {
        av = a.display_name || "";
        bv = b.display_name || "";
      }
      if (typeof av === "number" && typeof bv === "number")
        return asc ? av - bv : bv - av;
      return asc
        ? String(av).localeCompare(String(bv))
        : String(bv).localeCompare(String(av));
    });
    return rows;
  }, [detail, q, activeCat, sortKey, asc]);

  if (error) {
    return (
      <div className="space-y-6">
        <p className="eyebrow">Hospital profile</p>
        <h1 className="display-2">We couldn't load that hospital.</h1>
        <p className="body-lead">
          {error}. The hospital may not exist in this dataset, or the page may
          have been opened from a stale link.
        </p>
        <Link to="/hospitals" className="text-ink underline-offset-4 hover:underline">
          Back to all hospitals
        </Link>
      </div>
    );
  }

  if (!detail) {
    return (
      <div className="space-y-6 animate-pulse">
        <div className="h-12 w-2/3 rounded-2xl bg-section" />
        <div className="grid gap-4 md:grid-cols-3">
          {Array.from({ length: 3 }).map((_, i) => (
            <div key={i} className="h-24 rounded-2xl bg-section" />
          ))}
        </div>
        <div className="h-64 rounded-2xl bg-section" />
      </div>
    );
  }

  const header = [
    detail.city,
    detail.state ? stateName(detail.state) : null,
  ]
    .filter(Boolean)
    .join(", ");

  return (
    <div className="space-y-14 animate-floatIn">
      {/* HEADER */}
      <div className="max-w-3xl">
        <p className="eyebrow">
          <Link to="/hospitals" className="hover:text-ink underline-offset-4 hover:underline">
            Hospitals
          </Link>{" "}
          / Profile
        </p>
        <h1 className="display-2 mt-3 text-balance">{detail.name || detail.hospital_id}</h1>
        {(header || detail.system) && (
          <p className="text-base text-inkMuted mt-3">
            {detail.system ? <span>{detail.system}</span> : null}
            {detail.system && header ? <span className="mx-2">·</span> : null}
            {header ? <span>{header}</span> : null}
          </p>
        )}
        <p className="body-lead mt-5 text-pretty">
          Every priced code this hospital published in its federal price
          transparency file. Search by procedure or drug, filter by category,
          or click into a code to see how this price compares to other
          hospitals nationwide.
        </p>
        {detail.mrf_url ? (
          <a
            href={detail.mrf_url}
            target="_blank"
            rel="noreferrer"
            className="mt-5 inline-flex items-center gap-1.5 text-sm font-medium text-ink underline-offset-4 hover:underline"
          >
            Open the source MRF
            <svg viewBox="0 0 24 24" className="h-3.5 w-3.5" fill="none">
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

      {/* STATS */}
      <div className="grid gap-12 md:grid-cols-3 md:gap-8 border-t border-b border-line py-12">
        <div>
          <p className="eyebrow">Codes priced</p>
          <p className="stat-num mt-3 text-ink">{fmtNum(detail.codes_covered)}</p>
          <p className="mt-2 text-sm text-inkMuted">
            distinct procedures, drugs, and stays this hospital lists a
            pre-insurance list price for
          </p>
        </div>
        <div>
          <p className="eyebrow">Cash beats insurance</p>
          <p className="stat-num mt-3 text-ink">
            {detail.honesty_score != null
              ? `${(detail.honesty_score * 100).toFixed(0)}%`
              : "n/a"}
          </p>
          <p className="mt-2 text-sm text-inkMuted">
            of the time this hospital's cash list price is below their lowest
            negotiated insurance rate
          </p>
        </div>
        <div>
          <p className="eyebrow">Categories covered</p>
          <p className="stat-num mt-3 text-ink">{categories.length}</p>
          <p className="mt-2 text-sm text-inkMuted">
            different clinical categories represented in this hospital's file
          </p>
        </div>
      </div>

      {/* SEARCH + FILTERS */}
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
            placeholder="Search procedures, drugs, codes, or line item text..."
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

        <div className="flex flex-wrap items-center gap-2">
          <CategoryPill
            active={activeCat === "all"}
            onClick={() => setActiveCat("all")}
          >
            All ({fmtNum(detail.codes.length)})
          </CategoryPill>
          {categories.map((cat) => {
            const n = (detail.category_counts || {})[cat] ?? 0;
            return (
              <CategoryPill
                key={cat}
                active={activeCat === cat}
                onClick={() => setActiveCat(cat)}
              >
                {categoryLabel(cat)} ({fmtNum(n)})
              </CategoryPill>
            );
          })}
        </div>
      </div>

      {/* TABLE */}
      <div className="surface-card overflow-hidden">
        <div className="overflow-x-auto">
          <table className="editorial-table">
            <caption className="bg-section/40 px-7 py-3 text-left text-[10px] font-semibold uppercase tracking-eyebrowTight text-inkSubtle">
              Pre-insurance list prices this hospital publishes. Drug rows are
              normalized to the HCPCS billing unit (per 1 mg, per 10 mg, etc.)
              so a 100 mg vial sits on the same scale as a 1 mg vial. The
              hospital's exact line item description is shown beneath each
              row so you can verify against their MRF.
            </caption>
            <thead>
              <tr>
                <SortableTh
                  active={sortKey === "category"}
                  asc={asc}
                  onClick={() => {
                    if (sortKey === "category") setAsc(!asc);
                    else {
                      setSortKey("category");
                      setAsc(true);
                    }
                  }}
                  className="px-7 py-4"
                >
                  Procedure / drug
                </SortableTh>
                <SortableTh
                  active={sortKey === "median"}
                  asc={asc}
                  onClick={() => {
                    if (sortKey === "median") setAsc(!asc);
                    else {
                      setSortKey("median");
                      setAsc(false);
                    }
                  }}
                  className="px-7 py-4 text-right"
                >
                  This hospital's price
                </SortableTh>
                <th className="px-7 py-4 text-right">Compare nationwide</th>
              </tr>
            </thead>
            <tbody>
              {visible.slice(0, 800).map((row) => {
                const cms = cmsByKey.get(`${row.code_system}:${row.code}`);
                return (
                  <tr key={`${row.code_system}:${row.code}`}>
                    <td className="px-7 py-5 align-top">
                      <p className="text-[10px] uppercase tracking-[0.18em] text-inkSubtle">
                        {row.code_system} {row.code}
                        {row.category ? (
                          <>
                            {" · "}
                            <span className="text-inkMuted">{categoryLabel(row.category)}</span>
                          </>
                        ) : null}
                      </p>
                      <p className="font-display text-base font-medium text-ink mt-1.5 leading-snug">
                        {row.display_name}
                      </p>
                      {row.line_item?.description ? (
                        <p className="text-xs text-inkSubtle mt-1.5 leading-snug">
                          MRF row: {row.line_item.description}
                        </p>
                      ) : null}
                      {row.line_item?.dose && row.billing_unit ? (
                        <p className="text-xs text-inkSubtle mt-1 leading-snug">
                          Vial {row.line_item.dose} · billed per {row.billing_unit}
                        </p>
                      ) : null}
                      {cms?.per_billing_unit != null ? (
                        <p className="text-[11px] text-inkSubtle mt-1 leading-snug">
                          Medicare allowance: {fmtMoney(cms.per_billing_unit)}
                          {row.billing_unit ? ` / ${row.billing_unit}` : ""}
                        </p>
                      ) : null}
                    </td>
                    <td className="px-7 py-5 text-right align-top">
                      <p className="font-display text-xl font-medium text-ink whitespace-nowrap tracking-[-0.02em]">
                        {fmtMoney(row.median)}
                        {row.billing_unit ? (
                          <span className="ml-1 text-sm font-medium text-inkMuted">
                            / {row.billing_unit}
                          </span>
                        ) : null}
                      </p>
                      {row.line_item?.gross_charge != null &&
                      row.line_item?.gross_charge !==
                        row.line_item?.gross_charge_per_unit ? (
                        <p className="text-[11px] text-inkSubtle mt-1 whitespace-nowrap leading-snug">
                          Raw: {fmtMoney(row.line_item.gross_charge)} gross
                          {row.line_item.discounted_cash != null
                            ? ` · ${fmtMoney(row.line_item.discounted_cash)} cash`
                            : ""}
                        </p>
                      ) : null}
                    </td>
                    <td className="px-7 py-5 text-right align-top">
                      <Link
                        to={`/explore/${row.code_system}/${row.code}`}
                        className="inline-flex items-center gap-1 text-xs font-medium text-ink underline-offset-4 hover:underline"
                      >
                        See all hospitals
                        <svg
                          viewBox="0 0 24 24"
                          className="h-3 w-3"
                          fill="none"
                        >
                          <path
                            d="M5 12h14m0 0l-6-6m6 6l-6 6"
                            stroke="currentColor"
                            strokeWidth="1.8"
                            strokeLinecap="round"
                            strokeLinejoin="round"
                          />
                        </svg>
                      </Link>
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
        {visible.length === 0 && (
          <p className="px-6 py-12 text-center text-sm text-inkSubtle">
            No procedures or drugs match those filters.
          </p>
        )}
        {visible.length > 800 && (
          <p className="px-6 py-3 text-center text-xs text-inkSubtle border-t border-line/70">
            Showing the first 800 of {visible.length.toLocaleString()} matches.
          </p>
        )}
      </div>
    </div>
  );
}

function CategoryPill({
  active,
  onClick,
  children,
}: {
  active: boolean;
  onClick: () => void;
  children: React.ReactNode;
}) {
  return (
    <button
      type="button"
      onClick={onClick}
      className={`rounded-full border px-4 py-2 text-xs font-medium transition-all duration-200 ${
        active
          ? "border-ink bg-ink text-bg"
          : "border-line bg-surface text-inkMuted hover:border-ink hover:text-ink"
      }`}
    >
      {children}
    </button>
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
