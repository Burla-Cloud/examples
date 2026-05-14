import type {
  ChargemasterDoc,
  CodeEntry,
  HospitalDetail,
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

const hospitalDetailCache = new Map<string, Promise<HospitalDetail>>();

export function loadHospitalDetail(hospitalId: string): Promise<HospitalDetail> {
  let pending = hospitalDetailCache.get(hospitalId);
  if (!pending) {
    pending = loadJson<HospitalDetail>(`hospitals/${hospitalId}.json`);
    hospitalDetailCache.set(hospitalId, pending);
  }
  return pending;
}

const chargemasterCache = new Map<string, Promise<ChargemasterDoc>>();

/**
 * Fetch the per-hospital "Full chargemaster" JSON.gz bundle (every priced row
 * the hospital published in their MRF, capped at 50K). The file is gzipped
 * on disk because GitHub Pages doesn't auto-Content-Encoding .gz; we
 * decompress in the browser with DecompressionStream.
 */
export function loadFullChargemaster(
  hospitalId: string,
): Promise<ChargemasterDoc> {
  let pending = chargemasterCache.get(hospitalId);
  if (pending) return pending;
  pending = (async () => {
    const url = dataUrl(`chargemaster/${hospitalId}.json.gz`);
    const res = await fetch(url, { cache: "force-cache" });
    if (!res.ok) {
      if (res.status === 404) {
        throw new Error(
          "This hospital's full chargemaster isn't built yet.",
        );
      }
      throw new Error(`Failed to load chargemaster (HTTP ${res.status})`);
    }
    if (!res.body) throw new Error("No response body");
    if (typeof DecompressionStream === "undefined") {
      throw new Error(
        "Your browser can't decompress this bundle. Try a recent Chrome/Edge/Firefox/Safari.",
      );
    }
    const ds = new DecompressionStream("gzip");
    const decompressed = new Response(res.body.pipeThrough(ds));
    const doc = (await decompressed.json()) as ChargemasterDoc;
    return doc;
  })();
  chargemasterCache.set(hospitalId, pending);
  return pending;
}
