import { useEffect, useMemo, useState } from "react";
import { Link, useParams } from "react-router-dom";
import { loadAll, loadHospitalDetail } from "../api";
import { FullChargemaster } from "../components/FullChargemaster";
import { fmtNum, stateName } from "../format";
import type {
  ChargemasterRow,
  CodeEntry,
  HospitalCodeRow,
  HospitalDetail as HospitalDetailType,
  StateCodeStat,
} from "../types";

/**
 * Flatten the curated hospital JSON into ChargemasterRow shape so the
 * FullChargemaster panel can render something useful even when the full
 * chargemaster bundle hasn't been built yet. Expands line_items[] when
 * available; falls back to the sample line_item; degrades to display_name +
 * median when neither exists.
 */
function hospitalRowsToChargemasterRows(
  codes: HospitalCodeRow[],
): ChargemasterRow[] {
  const out: ChargemasterRow[] = [];
  for (const c of codes) {
    if (c.line_items && c.line_items.length > 0) {
      for (const li of c.line_items) {
        out.push({
          d: li.description || c.display_name || undefined,
          cs: c.code_system,
          c: c.code,
          ds: li.dose || undefined,
          se: li.setting || c.setting || undefined,
          u: c.billing_unit || undefined,
          g: li.gross_charge ?? undefined,
          ca: li.discounted_cash ?? undefined,
          p:
            li.price ??
            li.gross_charge_per_unit ??
            li.discounted_cash_per_unit ??
            undefined,
        });
      }
      continue;
    }
    const li = c.line_item;
    out.push({
      d: li?.description || c.display_name || undefined,
      cs: c.code_system,
      c: c.code,
      ds: li?.dose || undefined,
      se: li?.setting || c.setting || undefined,
      u: c.billing_unit || undefined,
      g: li?.gross_charge ?? undefined,
      ca: li?.discounted_cash ?? undefined,
      p:
        c.median ??
        li?.gross_charge_per_unit ??
        li?.discounted_cash_per_unit ??
        undefined,
    });
  }
  return out;
}

export function HospitalDetail() {
  const { hospitalId = "" } = useParams<{ hospitalId: string }>();
  const [detail, setDetail] = useState<HospitalDetailType | null>(null);
  const [codesByKey, setCodesByKey] = useState<Map<string, CodeEntry>>(
    new Map(),
  );
  const [stateStatsByKey, setStateStatsByKey] = useState<
    Map<string, StateCodeStat>
  >(new Map());
  const [error, setError] = useState<string | null>(null);

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
    return () => {
      cancelled = true;
    };
  }, [hospitalId]);

  useEffect(() => {
    let cancelled = false;
    loadAll()
      .then((all) => {
        if (cancelled) return;
        const cmap = new Map<string, CodeEntry>();
        for (const c of all.codes) {
          cmap.set(`${c.code_system}:${c.code}`, c);
        }
        setCodesByKey(cmap);
        const stateAbbr = detail?.state || null;
        if (stateAbbr && all.stateSummary[stateAbbr]) {
          const smap = new Map<string, StateCodeStat>();
          const codes = all.stateSummary[stateAbbr].codes || {};
          for (const [k, v] of Object.entries(codes)) {
            smap.set(k, v as StateCodeStat);
          }
          setStateStatsByKey(smap);
        } else {
          setStateStatsByKey(new Map());
        }
      })
      .catch(() => undefined);
    return () => {
      cancelled = true;
    };
  }, [detail?.state]);

  const hospitalCodesByKey = useMemo(() => {
    if (!detail) return new Map<string, HospitalCodeRow>();
    const m = new Map<string, HospitalCodeRow>();
    for (const c of detail.codes) {
      m.set(`${c.code_system}:${c.code}`, c);
    }
    return m;
  }, [detail]);

  const fallbackRows = useMemo<ChargemasterRow[]>(() => {
    if (!detail) return [];
    return hospitalRowsToChargemasterRows(detail.codes);
  }, [detail]);

  const categories = useMemo(() => {
    if (!detail) return [] as string[];
    const keys = new Set<string>();
    for (const c of detail.codes) keys.add(c.category || "other");
    return Array.from(keys).sort();
  }, [detail]);

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

      {/* FULL CHARGEMASTER SEARCH (primary experience) */}
      <FullChargemaster
        hospitalId={detail.hospital_id}
        hospitalName={detail.name}
        state={detail.state ? stateName(detail.state) : null}
        codesByKey={codesByKey}
        stateStatsByKey={stateStatsByKey}
        hospitalCodesByKey={hospitalCodesByKey}
        fallbackRows={fallbackRows}
      />
    </div>
  );
}
