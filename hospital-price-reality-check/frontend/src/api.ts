import type {
  CodeEntry,
  HospitalIndexRow,
  RunMeta,
  SpreadRow,
  StateCodeStat,
} from "./types";

const RAW_BASE = import.meta.env.BASE_URL || "/";
const BASE = RAW_BASE === "./" ? "/" : RAW_BASE;

function dataUrl(path: string): string {
  if (typeof window === "undefined") return `${BASE}data/${path}`;
  const root = BASE.endsWith("/") ? BASE : `${BASE}/`;
  return new URL(`${root}data/${path}`, window.location.origin).toString();
}

async function loadJson<T>(path: string): Promise<T> {
  const r = await fetch(dataUrl(path), { cache: "no-cache" });
  if (!r.ok) throw new Error(`Failed ${path}`);
  const ct = r.headers.get("content-type") || "";
  if (!ct.includes("json")) {
    const text = await r.text();
    if (text.trim().startsWith("<")) throw new Error(`Failed ${path}: server returned HTML`);
    return JSON.parse(text);
  }
  return r.json();
}

let cache: {
  codes: CodeEntry[];
  spread: SpreadRow[];
  meta: RunMeta;
  stateSummary: Record<string, { codes: Record<string, StateCodeStat> }>;
  hospitalIndex: HospitalIndexRow[];
} | null = null;

export async function loadAll() {
  if (cache) return cache;
  const [codes, spread, meta, stateSummary, hospitalIndex] = await Promise.all([
    loadJson<CodeEntry[]>("code_summary.json"),
    loadJson<SpreadRow[]>("spread_leaderboard.json").catch(() =>
      loadJson<SpreadRow[]>("chaos_leaderboard.json").catch(() => [])
    ),
    loadJson<RunMeta>("run_metadata.json"),
    loadJson<Record<string, { codes: Record<string, StateCodeStat> }>>(
      "state_summary.json"
    ),
    loadJson<HospitalIndexRow[]>("hospital_index.json"),
  ]);
  cache = { codes, spread, meta, stateSummary, hospitalIndex };
  return cache;
}
