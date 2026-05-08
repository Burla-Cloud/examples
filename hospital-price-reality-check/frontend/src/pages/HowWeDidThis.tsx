import { useEffect, useState } from "react";
import { loadAll } from "../api";
import type { RunMeta } from "../types";
import { CaveatBanner } from "../components/CaveatBanner";
import { fmtNum } from "../format";

export function HowWeDidThis() {
  const [meta, setMeta] = useState<RunMeta | null>(null);

  useEffect(() => {
    loadAll().then((d) => setMeta(d.meta));
  }, []);

  const scale = meta?.scale_summary || {};
  const stats = [
    {
      label: "Hospitals indexed",
      value:
        meta?.hospitals_indexed != null ? fmtNum(meta.hospitals_indexed) : "...",
    },
    {
      label: "Hospitals with prices",
      value:
        meta?.hospitals_with_data != null ? fmtNum(meta.hospitals_with_data) : "...",
    },
    {
      label: "Real price points",
      value:
        scale.observation_rows_reported != null
          ? fmtNum(Number(scale.observation_rows_reported))
          : "...",
    },
    {
      label: "Run mode",
      value: typeof scale.mode === "string" ? scale.mode : "n/a",
    },
  ];

  return (
    <div className="space-y-16 max-w-4xl">
      <div>
        <p className="eyebrow">How we did this</p>
        <h1 className="display-2 mt-3 text-balance">
          From raw federal files to plain English.
        </h1>
        <p className="body-lead mt-5 text-pretty">
          Every word and number on this site comes from real US hospitals'
          machine readable price files. There is no fill in, no estimation, no
          synthetic data. If a hospital does not publish a code, that code does
          not appear in their column.
        </p>
      </div>

      <CaveatBanner />

      {meta && (
        <div className="grid gap-12 md:grid-cols-4 md:gap-8 border-t border-b border-line py-12">
          {stats.map((s) => (
            <div key={s.label}>
              <p className="eyebrow">{s.label}</p>
              <p className="font-display text-3xl md:text-4xl font-medium text-ink mt-3 tracking-[-0.02em]">
                {s.value}
              </p>
            </div>
          ))}
        </div>
      )}

      <Section title="The federal rule that makes this possible" n="01">
        <p>
          As of 2024, every Medicare-participating hospital in the US must
          publish a machine readable file of their standard charges online.
          This includes their gross charges, what uninsured patients pay in
          cash, and what each insurance company has negotiated. The rule is{" "}
          <span className="text-ink font-medium">45 CFR Part 180</span> and CMS
          publishes a v3.0 data dictionary that tells hospitals exactly how to
          format the file.
        </p>
        <p>
          The reality is messier. Files range from a few megabytes to over 3
          gigabytes each. Different hospitals use different layouts, sometimes
          very different. Some still post legacy CSV files predating the v3.0
          standard.
        </p>
      </Section>

      <Section title="Picking the things people actually need" n="02">
        <p>
          The full file from a hospital can have hundreds of thousands of line
          items. Most of those are bundled together when you actually get
          care. We picked roughly 360 specific procedures, lab tests,
          chemotherapy drugs, immunotherapy infusions, radiation oncology
          codes, and emergency room visits that real people commonly look up:
          childbirth, colonoscopy, MRI of the lower back, Keytruda, knee
          replacement, insulin, and so on.
        </p>
        <p>
          Each one is keyed to its medical billing code (CPT, HCPCS, MS-DRG,
          or NDC) and we show the plain English name alongside the code so you
          do not need a billing manual to understand what you are looking at.
        </p>
      </Section>

      <Section title="The pipeline (open source)" n="03">
        <p>
          One Python function takes a single hospital and returns the prices
          for our full code list. That function is the unit of work. We hand
          it to{" "}
          <Code>remote_parallel_map</Code> from{" "}
          <a
            href="https://burla.dev"
            target="_blank"
            rel="noreferrer"
            className="text-ink underline-offset-4 hover:underline font-medium"
          >
            Burla
          </a>
          , which fans the work out across a cluster.
        </p>
        <pre className="overflow-x-auto rounded-2xl border border-ink/10 bg-ink text-bg/95 p-6 text-[12.5px] leading-relaxed font-mono">
{`from burla import remote_parallel_map

def parse_hospital_mrf(hospital):
    raw = download_streaming(hospital["mrf_url"])
    parser = pick_parser(raw)
    rows = list(parser.iter_priced_items(raw, TARGET_CODES))
    write_jsonl(rows)
    return {"hospital_id": hospital["hospital_id"], "rows": len(rows)}

results = remote_parallel_map(
    parse_hospital_mrf,
    hospitals,
    func_cpu=1,
    func_ram=4,
    grow=True,
    max_parallelism=64,
)`}
        </pre>
        <p>
          Each worker streams its file, extracts the rows we care about, and
          writes them to a shared filesystem on the cluster. Then a single
          reduce step on one machine aggregates everything into the JSON files
          that this site reads on page load.
        </p>
      </Section>

      <Section title="What we measured" n="04">
        <ul className="space-y-3 list-disc pl-6">
          <li>The lowest price any hospital posts (cheapest hospital).</li>
          <li>The highest price any hospital posts (most expensive hospital).</li>
          <li>The typical price (median): half of hospitals charge less, half charge more.</li>
          <li>The average price (mean): the arithmetic average across hospitals.</li>
          <li>How widely prices spread from cheapest to most expensive.</li>
          <li>How those numbers change state by state.</li>
        </ul>
      </Section>

      <Section title="What we did not measure" n="05">
        <ul className="space-y-3 list-disc pl-6">
          <li>What you will actually pay. That depends on your insurance, deductible, and copay.</li>
          <li>Quality of care or hospital outcomes. Cheaper does not mean better.</li>
          <li>Doctor and anesthesia bills, which are usually billed separately by the providers themselves.</li>
          <li>Surprise billing, balance billing, or out-of-network charges.</li>
          <li>Implants and devices, which are often billed as a separate line item.</li>
        </ul>
      </Section>

      <Section title="A few honest caveats" n="06">
        <p>
          Not every hospital publishes a file we can read. Some files are
          corrupt, some require login, some use bespoke formats we have not
          parsed yet. When we cannot read a hospital's file we skip it and
          note it in the run log. We never make up a number.
        </p>
        <p>
          Files also age. The federal rule requires updates at least once a
          year, but in practice some hospitals are months behind. The "last
          updated" date for a hospital comes straight from their file when
          available.
        </p>
        <p>
          Finally, the same procedure code can mean slightly different things
          at different hospitals depending on what is bundled with it. We
          average across the noise, but the spread you see is partly real
          price variation and partly bundling differences.
        </p>
      </Section>

      <Section title="Run it yourself" n="07">
        <p>
          The pipeline is open source. Clone the repo, point the loader at any
          list of hospital MRF URLs, and rerun the scale step. The reduce,
          analysis, and frontend steps regenerate automatically. You can
          target a single state, a specific health system, or every hospital
          in the country.
        </p>
        <p className="text-sm">
          Stack: Python streaming parsers (ijson, pandas, openpyxl), Burla{" "}
          <Code>remote_parallel_map</Code>, GCSFuse-backed shared filesystem
          for intermediate results, Vite plus React plus Tailwind for the
          static site, Recharts for the distribution charts.
        </p>
      </Section>
    </div>
  );
}

function Code({ children }: { children: React.ReactNode }) {
  return (
    <code className="font-mono text-[13px] bg-section px-1.5 py-0.5 rounded text-ink">
      {children}
    </code>
  );
}

function Section({
  title,
  n,
  children,
}: {
  title: string;
  n: string;
  children: React.ReactNode;
}) {
  return (
    <section className="relative">
      <div className="flex items-center gap-5 mb-7">
        <span className="font-display text-2xl font-medium text-accent tracking-[-0.02em]">
          {n}
        </span>
        <div className="h-px flex-1 bg-line" />
      </div>
      <h2 className="display-2 mb-7 text-balance">{title}</h2>
      <div className="space-y-5 text-base text-inkMuted leading-relaxed text-pretty">
        {children}
      </div>
    </section>
  );
}
