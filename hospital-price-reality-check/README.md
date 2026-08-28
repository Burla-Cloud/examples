# Hospital Price Reality Check

**Live site: <https://burla-cloud.github.io/examples/hospital-price-reality-check/>**

A total knee replacement at one US hospital is **$9,291**. At another it is **$83,902**. Same CPT 27447, same procedure description. Doxorubicin, the chemo drug, runs **$44 to $3,376** per 10 mg unit. Palonosetron, the anti-nausea drug given to chemo patients, spans **$35 to $5,499** for the same 25 mcg unit.

These are the pre-insurance list prices that 3,400 US hospitals are required by CMS (45 CFR Part 180) to publish in machine-readable files (MRFs). We pulled all 5,162 of those files, parsed them, normalized the pricing, and built a static React explorer of the chargemaster spread.

## The headline

|  |  |
|---|---:|
| MRFs parsed | **5,162** |
| Hospitals on the live site | **3,400** |
| Priced line items extracted | **1,285,268** |
| Standard codes covered | **361** (CPT, HCPCS, MS-DRG, NDC, CDT) |
| Map runtime | **1,168 sec** (~19 min) on 1,040 parallel CPUs |
| Cluster | **13 VMs x 80 vCPU** = 1,040 vCPU, 4,160 GB RAM |
| Docker image | **`python:3.12`** |
| LLMs used | **zero** |

The MRFs are 100 KB to 3 GB each, in five different formats (CMS v3 JSON, tall CSV, wide CSV, XLSX, ZIP), and several large hospital systems sit behind Akamai TLS bot walls. Nobody downloads them at scale. The whole point of this demo is showing how cheap that becomes once one Python function can fan out across a thousand parallel containers and share a filesystem with all of them.

## Repo layout

```
codes.py              loads the 361-code allow list with patient-facing fields
config/code_seeds.csv curated code allow-list configuration
hospital_index.py     dedup index across Oria + TPAFS + Dolthub MRF directories
parsers_inline.py     JSON / tall-CSV / wide-CSV / XLSX / ZIP parsers
pipeline.py           worker: download streaming + parse + write jsonl
scale.py              entrypoint: dispatch parse_hospital_mrf across the cluster
reduce.py             walks observation shards -> samples/hpt_reduced.json
description_filter.py drops chargemaster lines whose text does not match the code
analysis.py           builds frontend/public/data/*.json from the reduced data
run_reduce_burla.py   run reduce + analysis on a single worker
requirements.txt      requests, curl_cffi, ijson, pandas, openpyxl, ...

frontend/             Vite + React + Tailwind static site
  src/                routes, components, data loader
  public/data/*.json  generated artifacts: code summary, state summary,
                      leaderboard, hospital index, and run metadata
```

Generated data is ignored by Git.

## Reproduce

```bash
curl -fsSL https://raw.githubusercontent.com/Burla-Cloud/burla-agent-starter-kit/main/install.sh | sh
pip install -r requirements.txt

python scale.py                  # downloads the public hospital index, then maps
python run_reduce_burla.py       # reduce + analysis on one worker
                                 # writes frontend/public/data/*.json

cd frontend && npm install && npm run dev
```

## How we did this

The pipeline is two `remote_parallel_map` calls on a Burla cluster: one map across hospitals, one reduce on a single big container. `remote_parallel_map` ships your worker module to every node in the cluster and streams stdout back to your laptop; the dashboard's Settings tab is where you pick the VM hardware and image, the Jobs tab is where logs land, and `./shared` is a GCSFuse-backed network volume that every container in the cluster can read and write.

```python
from burla import remote_parallel_map
from pipeline import parse_hospital_mrf

results = remote_parallel_map(
    parse_hospital_mrf,
    hospitals,
    func_cpu=1,
    func_ram=2,
    max_parallelism=1500,
    spinner=True,
)
```

`func_cpu=1` and `func_ram=2` so each MRF parse only takes one CPU slot. On a 13 by 80-CPU cluster that is 1,040 calls running concurrently and Burla queues the rest. `print` from inside `parse_hospital_mrf` lands in the dashboard Jobs tab in real time, and Burla auto-installs whatever the worker imports (`requests`, `curl_cffi`, `ijson`, etc.) so we did not have to bake a custom image.

Workers write their parsed observations to `/workspace/shared/hpt/observations/<hospital_id>.jsonl`. The reduce step is a single 8 vCPU container that walks the shared tree, deduplicates `(hospital, code)` variants into one representative observation, and emits the static JSON the React site reads.

### Tricks worth keeping

- **`curl_cffi` with `impersonate="chrome"`.** Akamai-fronted hosts (Mount Sinai, Tufts, Select Medical) reject standard `requests` with 403 because their bot detection inspects the TLS JA3 fingerprint. `curl_cffi` is a libcurl-impersonate build with a `requests`-shaped API that matches Chrome's TLS handshake. Drop-in.
- **Streaming with HTTP Range fallback.** 1 GB MRFs hang up mid-stream on flaky CDNs. Resume from byte N, not byte 0.
- **Description match filter.** Real MRFs publish bone screws and surgical plates under flu vaccine and chemo J-codes. `description_filter.py` drops those before the cheapest/priciest podium and the leaderboard get computed, so every displayed comparison is verifiably the same procedure.
- **`./shared` on GCSFuse.** Every worker writes to the same path the reducer reads from. No fan-in plumbing, no intermediate object-store hops.

## Caveats

- **2,026 of the 7,188 attempted MRFs failed** (dead URLs, CDN throttling, malformed JSON we did not have a parser for, hospitals publishing under a different code namespace than they tell CMS). The site reflects the 3,400 hospitals that produced parseable rows.
- **Some hospitals publish CPT 27447 as the surgeon's professional fee only**, others as the all-in surgical package. We apply category-specific ratio guards plus the description match filter to drop placeholder rows and obvious mismatches, but the cheap and pricey ends still skew toward whichever band of reporting dominates a code.
- **Insurance-negotiated rates are intentionally dropped.** Every price on the site is `gross_charge` or `discounted_cash`. This is what an uninsured cash payer sees on a chargemaster, not what an insured patient pays.
- **One-shot snapshot.** MRFs change quietly. A real production version would re-run weekly.
