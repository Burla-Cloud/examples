import { useEffect, useMemo, useState } from "react";
import { Link } from "react-router-dom";
import { loadAll } from "../api";
import type { CodeEntry, StateCodeStat } from "../types";
import { categoryLabel, fmtMoney, stateName } from "../format";
import { StateFilter } from "../components/StateFilter";
import { RateBadge } from "../components/RateBadge";

export function Explore() {
  const [codes, setCodes] = useState<CodeEntry[]>([]);
  const [stateSummary, setStateSummary] = useState<
    Record<string, { codes: Record<string, StateCodeStat> }>
  >({});
  const [q, setQ] = useState("");
  const [cat, setCat] = useState("");
  const [stateAbbr, setStateAbbr] = useState("");
  const [err, setErr] = useState<string | null>(null);

  useEffect(() => {
    loadAll()
      .then((d) => {
        setCodes(d.codes);
        setStateSummary(d.stateSummary);
      })
      .catch((e) => setErr(String(e)));
  }, []);

  const cats = useMemo(
    () => Array.from(new Set(codes.map((c) => c.category))).sort(),
    [codes]
  );
  const states = useMemo(
    () => Object.keys(stateSummary).sort(),
    [stateSummary]
  );

  const filtered = useMemo(() => {
    return codes
      .filter((c) => {
        const m =
          !q ||
          c.display_name.toLowerCase().includes(q.toLowerCase()) ||
          c.code.toLowerCase().includes(q.toLowerCase()) ||
          c.what_it_is.toLowerCase().includes(q.toLowerCase());
        const k = !cat || c.category === cat;
        const hasGlobal = (c.stats?.count ?? 0) > 0;
        if (!m || !k || !hasGlobal) return false;
        if (stateAbbr) {
          const key = `${c.code_system}:${c.code}`;
          const s = stateSummary[stateAbbr]?.codes?.[key];
          if (!s || !(s.count ?? 0)) return false;
        }
        return true;
      })
      .map((c) => {
        const key = `${c.code_system}:${c.code}`;
        const stStat = stateAbbr ? stateSummary[stateAbbr]?.codes?.[key] : undefined;
        return { c, stStat };
      });
  }, [codes, q, cat, stateAbbr, stateSummary]);

  if (err) return <p className="text-rose">{err}</p>;
  if (!codes.length) {
    return (
      <div className="space-y-6 animate-pulse">
        <div className="h-12 w-1/2 rounded-2xl bg-section" />
        <div className="h-32 rounded-2xl bg-section" />
        <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-3">
          {Array.from({ length: 6 }).map((_, i) => (
            <div key={i} className="h-44 rounded-2xl bg-section" />
          ))}
        </div>
      </div>
    );
  }

  const totalWithData = codes.filter((c) => (c.stats?.count ?? 0) > 0).length;

  return (
    <div className="space-y-12 animate-floatIn">
      <div className="max-w-3xl">
        <p className="eyebrow">Look up a price</p>
        <h1 className="display-2 mt-3 text-balance">
          Search procedures, drugs, and labs.
        </h1>
        <p className="body-lead mt-5 text-pretty">
          Type something. We will show you what real hospitals charge, the
          typical price, and the range from cheapest to most expensive.
        </p>
        <RateBadge variant="inline" plural className="mt-5" />
      </div>

      {/* Search bar — large and obvious */}
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
            placeholder="MRI, chemotherapy, Keytruda, knee replacement, colonoscopy..."
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

        <div className="flex flex-wrap items-center gap-3">
          <CategoryChip active={!cat} onClick={() => setCat("")}>
            All <span className="text-inkSubtle font-normal">·</span>{" "}
            {stateAbbr ? filtered.length : totalWithData}
          </CategoryChip>
          {cats.map((c) => {
            const inThisCat = codes.filter((x) => {
              if (x.category !== c) return false;
              if ((x.stats?.count ?? 0) === 0) return false;
              if (stateAbbr) {
                const k = `${x.code_system}:${x.code}`;
                const s = stateSummary[stateAbbr]?.codes?.[k];
                if (!s || !(s.count ?? 0)) return false;
              }
              return true;
            }).length;
            if (inThisCat === 0) return null;
            return (
              <CategoryChip
                key={c}
                active={cat === c}
                onClick={() => setCat(cat === c ? "" : c)}
              >
                {categoryLabel(c)}{" "}
                <span className="text-inkSubtle font-normal">·</span> {inThisCat}
              </CategoryChip>
            );
          })}
          {states.length > 0 && (
            <div className="ml-auto">
              <StateFilter
                states={states}
                value={stateAbbr}
                onChange={setStateAbbr}
                label="Filter by state"
              />
            </div>
          )}
        </div>
      </div>

      {stateAbbr && (
        <div className="surface-edge bg-mintSoft/40 px-6 py-4 text-sm text-ink">
          <span className="font-semibold">{stateName(stateAbbr)}:</span>{" "}
          <span className="text-inkMuted">
            showing prices from hospitals in {stateName(stateAbbr)} only. Switch
            to all states for the national picture.
          </span>
        </div>
      )}

      <div className="grid gap-5 md:grid-cols-2 lg:grid-cols-3">
        {filtered.slice(0, 300).map(({ c, stStat }) => (
          <Link
            key={`${c.code_system}:${c.code}`}
            to={`/explore/${encodeURIComponent(c.code_system)}/${encodeURIComponent(c.code)}${stateAbbr ? `?state=${stateAbbr}` : ""}`}
            className="surface-card surface-card-hover p-7 group flex flex-col"
          >
            <div className="flex items-center justify-between">
              <span className="pill">{categoryLabel(c.category)}</span>
              <span className="font-mono text-[11px] text-inkSubtle">
                {c.code_system} {c.code}
              </span>
            </div>
            <h3 className="font-display text-xl font-semibold text-ink leading-snug mt-5 group-hover:text-accent transition-colors">
              {c.display_name}
            </h3>
            <p className="mt-3 text-sm text-inkMuted line-clamp-2 leading-relaxed">
              {c.what_it_is}
            </p>
            <p className="mt-auto pt-6 text-[10px] font-semibold uppercase tracking-eyebrowTight text-inkSubtle">
              Pre-insurance list price
            </p>
            <div className="mt-2 grid grid-cols-3 gap-3 border-t border-lineSoft pt-3 text-sm">
              <div>
                <p className="eyebrow text-mint">Low</p>
                <p className="font-display font-semibold text-ink mt-1">
                  {fmtMoney(
                    stStat?.p10 ?? c.stats?.p10 ?? stStat?.min ?? c.stats?.min
                  )}
                </p>
              </div>
              <div>
                <p className="eyebrow">Typical</p>
                <p className="font-display font-semibold text-ink mt-1">
                  {fmtMoney(stStat?.median ?? c.stats?.median)}
                </p>
              </div>
              <div>
                <p className="eyebrow text-rose">High</p>
                <p className="font-display font-semibold text-ink mt-1">
                  {fmtMoney(
                    stStat?.p90 ?? c.stats?.p90 ?? stStat?.max ?? c.stats?.max
                  )}
                </p>
              </div>
            </div>
            <p className="mt-3 text-xs text-inkSubtle">
              {stStat
                ? `${stStat.count ?? 0} list prices in ${stateName(stateAbbr)}`
                : `${c.stats?.count ?? 0} list prices nationwide`}
            </p>
          </Link>
        ))}
      </div>

      {filtered.length === 0 && (
        <div className="surface-card p-12 text-center">
          <p className="text-inkMuted">
            No results for that search. Try clearing the state filter, or search
            for something common like{" "}
            <code className="font-mono text-ink">colonoscopy</code> or
            <code className="font-mono text-ink"> 27447</code>.
          </p>
        </div>
      )}

      {filtered.length > 300 && (
        <p className="text-center text-xs text-inkSubtle">
          Showing the first 300 of {filtered.length} matches. Narrow the search
          to see more.
        </p>
      )}
    </div>
  );
}

function CategoryChip({
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
