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
          the list of 7,000+ hospitals to{" "}
          <Code>remote_parallel_map</Code> from{" "}
          <BurlaLink />, and a single laptop ends up driving an autoscaling
          fleet of cloud workers running that one function in parallel. No
          Kubernetes, no Lambda packaging, no queues, no manifest files.
          Regular Python from a single laptop.
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
    hospitals,           # ~7,000 hospital MRF URLs
    func_cpu=1,
    func_ram=2,
    grow=True,           # autoscale the cluster up to handle the queue
    max_parallelism=1500,
)`}
        </pre>
        <p>
          That single call spun up roughly 1,500 concurrent workers and
          churned through 7,188 hospital files in about 19 minutes start to
          finish. Files range from a few megabytes to over 3 gigabytes; on a
          single laptop a serial pass would take days. <BurlaLink /> handles
          the cluster, the packaging, the autoscaling, and the streaming
          stdout/stderr from every worker back to the local shell. The code
          is the same code we'd run locally on one hospital, just wrapped in
          one function call.
        </p>
        <p>
          Each worker streams its file, extracts the rows we care about, and
          writes them to a shared filesystem on the cluster. Then a single
          reduce step on one machine aggregates everything into the JSON
          files that this site reads on page load.
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

      <Section title="Comparing the same thing across hospitals" n="06">
        <p>
          Two hospitals can file a row under the same CPT code and mean
          completely different things. CPT 27447 is unilateral total knee
          replacement, but a chargemaster will sometimes list bilateral knee
          (which is two procedures) under the same code. We catch these
          before the cheapest / priciest podiums are computed:
        </p>
        <ul className="space-y-3 list-disc pl-6">
          <li>
            A hardware blocklist (plate, screw, suture anchor, drape, tubing,
            extender) prevents surgical supplies from sitting on a vaccine or
            ER visit page.
          </li>
          <li>
            Per-code positive keywords. The line item description must match
            the procedure or drug the code is for. We auto-derive these from
            each code's clinical name and hand-tune the ones where
            chargemaster shorthand disagrees with the official wording.
          </li>
          <li>
            Per-code negative keywords. CPT 27447 (total knee) excludes
            "bilateral", "revision", "partial knee", "unicompart". CPT 47562
            (lap chole) excludes "open" and "converted to open". CPT 45378
            (diagnostic colonoscopy) excludes biopsy, polypectomy, ablation,
            EMR. CPT 70450 (CT head without contrast) excludes "with contrast"
            and "without and with contrast" rows.
          </li>
        </ul>
      </Section>

      <Section title="Standardizing drug doses" n="07">
        <p>
          Hospitals publish drug prices at the vial level (one row for
          a 100 mg vial, another for a 1 mg vial), but the HCPCS code
          they're billed against is per-unit (per 1 mg, per 10 mg, per 25
          mcg, ...). If we just grabbed the vial price, a hospital with a
          big vial would look more expensive than a hospital with a small
          one even when their per-mg pricing is identical.
        </p>
        <p>
          We extract the dose from the chargemaster description (regex over
          common formats: "DOXORUBICIN 50 MG", "Keytruda 100mg/4ml",
          "PALONOSETRON 0.05 MG/ML 5 ML VIAL"), divide the published price
          by the dose, and multiply by the HCPCS billing unit. Every drug
          page on this site is per HCPCS billing unit. The card under each
          hospital shows both the raw vial price the hospital published AND
          the per-unit standardized price you see in the rankings.
        </p>
        <p>
          We anchor the standardized prices against the CMS Medicare Part B
          ASP (Average Sales Price) payment limit file, published quarterly.
          For a drug whose ASP is $60 per mg, a hospital chargemaster of $10
          per mg would be suspicious -- we flag it. For 33 of the 37 drug
          codes we have an ASP record for, our median chargemaster falls in
          the expected 2x-50x ASP range. The remaining 4 are off-patent
          generics (paclitaxel, oxaliplatin, granisetron, palonosetron)
          where ASP is fractions of a cent and the ratio loses signal --
          chargemasters notoriously lag generic erosion by years.
        </p>
      </Section>

      <Section title="LLM second opinion" n="08">
        <p>
          Regex and keyword filters can only do so much. We ran two
          independent passes with Claude (Sonnet 4.5, temperature 0):
        </p>
        <ul className="space-y-3 list-disc pl-6">
          <li>
            <span className="text-ink font-medium">218 drug podium cards</span>{" "}
            across all 39 HCPCS J-codes that bill per a unit dose. For each
            card the model independently extracted the dose from the
            description, checked it matched the HCPCS code, and verified the
            per-unit math. Result: 215 of 218 passed; 3 surfaced real bugs
            in our regex (a HCPCS-unit reminder being parsed as the dose,
            and a "concentration without volume" string being parsed as a
            total dose). Both have been fixed in the open-source
            <Code>dosage_extractor.py</Code>.
          </li>
          <li>
            <span className="text-ink font-medium">300 procedure podium
            cards</span> across the 50 most-trafficked non-drug codes (ER
            visits, imaging, colonoscopy, knee replacement, etc). The model
            checked whether the line item description matches the procedure
            code, whether it represents a clinical variant (bilateral,
            revision, with-contrast vs without-contrast), and whether the
            price is in a believable range. Real findings drove the negative
            keywords for CT contrast variants (CPT 70450, 71250, 74176) and
            an ultrasound exclusion for CPT 77063 (breast tomosynthesis).
          </li>
        </ul>
        <p className="text-sm">
          The audit report files (<Code>samples/dosage_audit_all_drugs.json</Code>{" "}
          and <Code>samples/procedure_audit.json</Code>) ship in the
          repository so anyone can inspect every flagged card and the
          model's reasoning.
        </p>
      </Section>

      <Section title="A few honest caveats" n="09">
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
          Hospital-system clustering is not collapsed. When five HCA sister
          hospitals copy-paste the same chargemaster, those are five data
          points in our podium even though the underlying decision was made
          once at the corporate level. We disclose this so a podium with
          three HCA siblings doesn't read as three independent
          observations.
        </p>
        <p>
          Coverage varies by code. A common test (CBC, CMP, basic chest
          x-ray) is published by 3,000+ hospitals. A niche surgical code
          may only have 80. Each code page now shows the count and the
          percent of total hospitals that publish that code. If the count
          is below 200 we display a "limited coverage" note above the
          podiums so a thin sample doesn't read as a national verdict.
        </p>
        <p>
          The same procedure code can still mean slightly different things
          at different hospitals depending on bundling. We average across
          the noise, but the spread you see is partly real price variation
          and partly bundling differences.
        </p>
      </Section>

      <Section title="Run it yourself" n="10">
        <p>
          The pipeline is open source. Clone the repo, point the loader at any
          list of hospital MRF URLs, and rerun the scale step. The reduce,
          analysis, and frontend steps regenerate automatically. You can
          target a single state, a specific health system, or every hospital
          in the country.
        </p>
        <p className="text-sm">
          Stack: Python streaming parsers (ijson, pandas, openpyxl),{" "}
          <BurlaLink /> <Code>remote_parallel_map</Code>, GCSFuse-backed
          shared filesystem for intermediate results, Vite plus React plus
          Tailwind for the static site, Recharts for the distribution charts.
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

function BurlaLink() {
  return (
    <a
      href="https://burla.dev"
      target="_blank"
      rel="noreferrer"
      className="text-accent font-medium underline decoration-accent/40 decoration-1 underline-offset-[3px] hover:decoration-accent transition-colors"
    >
      Burla
    </a>
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
