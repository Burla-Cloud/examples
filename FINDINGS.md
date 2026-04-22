# 1M READMEs — Findings

One million, two hundred thousand real GitHub README files, pulled from
`bigquery-public-data.github_repos`, streamed through a Burla cluster of
**500+ CPUs in parallel**, summarized with deterministic heuristics (no LLM),
then reduced and analyzed locally.

This document lists the nine findings surfaced by the pipeline, plus the raw
throughput numbers and caveats.

---

## Scale

| Thing                        | Number                  |
|------------------------------|-------------------------|
| Repos processed              | **1,200,000**           |
| Source rows scanned in BQ    | 2.3B files, 281M contents |
| Languages represented        | 30+                     |
| Heuristic categories         | 14                      |
| Pipeline shards (map phase)  | 600                     |
| Reduce buckets               | 16                      |
| Peak concurrent workers      | 500+ (Burla cluster)    |
| Map throughput               | ~25,000 repos / second  |
| Total Burla wall-clock       | 47.9 s (map) + 23.4 s (reduce) |
| LLMs used                    | **zero**                |

Every row in every finding is a real, attributable repo from the public
BigQuery dataset. Click any card in the UI to go straight to `github.com/<repo>`.

---

## Findings

### 1. The map of open-source GitHub

Every README was bucketed into **one of 14 categories** by keyword heuristics
(imports, install commands, domain vocabulary). The breakdown is *extremely*
top-heavy.

| Rank | Category                  | Repos     | Share   |
|------|---------------------------|-----------|---------|
| 1    | Web                       | 425,681   | 35.5%   |
| 2    | Other / uncategorized     | 134,251   | 11.2%   |
| 3    | DevOps                    | 130,604   | 10.9%   |
| 4    | CLI tools                 | 86,574    | 7.2%    |
| 5    | Libraries                 | 85,410    | 7.1%    |
| 6    | Mobile                    | 77,742    | 6.5%    |
| 7    | Documentation / lists     | 72,733    | 6.1%    |
| 8    | Databases                 | 64,088    | 5.3%    |
| 9    | OS / low-level            | 30,790    | 2.6%    |
| 10   | Machine Learning          | 25,728    | 2.1%    |
| 11   | Games                     | 25,361    | 2.1%    |
| 12   | Security                  | 17,333    | 1.4%    |
| 13   | Data engineering          | 16,591    | 1.4%    |
| 14   | Crypto / web3             | 7,114     | 0.6%    |

**One in three public repos is web.** The top 3 categories (web + devops +
"other") account for 57.5% of everything. Crypto, despite the noise, is 0.6%.

### 2. Which languages own which categories

Top languages within the top-ranked repos of each category:

| Category          | #1 language           | Share of top repos |
|-------------------|-----------------------|-------------------|
| Web               | JavaScript            | 58.0%             |
| Mobile            | Objective-C           | 36.5%             |
| ML                | Python                | 36.5%             |
| Libraries         | JavaScript            | 35.2%             |
| Games             | JavaScript            | 33.8%             |
| Data engineering  | Python                | 33.0%             |
| OS / low-level    | C                     | 32.5%             |
| Databases         | JavaScript            | 30.5%             |
| Security          | C                     | 29.8%             |
| CLI tools         | JavaScript            | 28.0%             |
| DevOps            | Go                    | 26.5%             |
| Docs / lists      | JavaScript            | 23.0%             |
| Crypto / web3     | JavaScript            | 40.5%             |

**Python owns ML + data. Go owns DevOps. Objective-C still dominates Mobile
(most of this data is 2016-era). JavaScript is the first or second language in
every single category.**

### 3. How does this thing install? Depends on the category.

Across the entire corpus, 74.6% of READMEs **never tell you how to install
the thing**. Among those that do, the install method is wildly
category-specific:

- ML: `pip install` — 31.0% of top ML repos
- Data eng: `pip install` — 38.0% of top data repos
- Web / libs / CLI / crypto: `npm install` — 28–62%
- OS / low-level: `apt-get` — 16.8%
- Mobile: `brew install` — 17.2%
- DevOps: `docker run` — 15.0%
- DevOps / docs / security: plain `git clone` — 5–11%

Install methods are a better category classifier than the language is.

### 4. The most lovingly documented repos on GitHub

Ranked by `badges × 3 + code fences`. These are the **README maximalists** —
people who put so much love into one markdown file that it could be its own
product.

| Rank | Repo                              | Badges | Code fences | Category |
|------|-----------------------------------|--------|-------------|----------|
| 1    | `mschile/terra-core`              | 186    | 1           | devops   |
| 2    | `cerner/terra-core`               | 177    | 1           | devops   |
| 3    | `SimonWaldherr/golibs`            | 130    | 50          | cli      |
| 4    | `CanaimaGNULinux/...web.policy`   | 124    | 0           | devops   |
| 5    | `*/turf` (4 forks tied)           | 122    | 4           | devops   |
| 6    | `mgrt/mgrt-php`                   | 2      | **348**     | devops   |
| 7    | `GoogleCloudPlatform/iap-active-directory-api` | 1 | **342** | docs |
| 8    | `christophehurpeau/babel-preset-modern-browsers` | 111 | 5 | lib |
| 9    | `code-troopers/ct`                | 0      | 268         | cli      |

Two wildly different README aesthetics emerge: the **badge peacocks** (Terra
Core: 186 badges, 1 code example) and the **code-dump handbooks** (mgrt-php:
2 badges, 348 code fences).

### 5. The loneliest READMEs on GitHub

READMEs under 4,600 characters with no code fences, no badges, and no
meaningful intro paragraph — **placeholders, TODOs, "coming soon"**.

Examples that made the cut:

- `ShareX/ShareX` and 10+ forks with identical 1 KB README stubs
- `Tatsh/xirvik-tools` — 297 chars total
- `fortesinformatica/MauticApi` — 498 chars, no description
- `CoreAPM/DotNetAgent` — "*(details coming soon)*"
- `rex64/unnamed-dungeon-crawler` — 852 chars, no description
- `jerboa88/168421` — "A simple placeholder/cover page for 168421.xyz"
- `coolstar/Capella` — 1.3 KB, "Social media client for Mastodon"
- `BeardlessBrady/Meat-Mod` — "*-Actual Name TBD-*"

Every one of these is a project someone meant to finish.

### 6. The words that define each category

TF-IDF over fourteen category "documents" (category = concatenation of all its
READMEs' text). The top distinctive words per category:

| Category  | Most distinctive words                                         |
|-----------|----------------------------------------------------------------|
| Web       | mean, angular, packages, mypackage, node, openshift, react     |
| DevOps    | kubernetes, openshift, docker, cluster, corefx, dotnet-ci      |
| Mobile    | swift, xcode, objective-c, cocoapods, carthage, afnetworking   |
| ML        | tensorflow, neural, learning, deep, model, keras, recurrent    |
| OS        | kernel, linux, boot, shadowsocks, libvirt, bootloader          |
| Games     | phaser, game, unity, minecraft, player, physics, sprite        |
| Security  | skipfish, security, scan, scanner, brute-force, cve-           |
| Data eng  | spark, science, hadoop, scientist, pandas, gensim, jupyter     |
| Crypto    | ethereum, wallet, contract, blockchain, bitcoin, truffle, geth |
| Databases | sails, cockroach, mysql, cockroachdb, waterline, postgresql    |

These words **don't just describe the category** — they describe what people
*disproportionately write about* in that category versus anywhere else. (Hence
"skipfish" and "mean" towering over their respective categories: they're
dominant project templates that get forked endlessly.)

### 7. The longest READMEs we found

The longest READMEs are almost entirely **curated lists** or **MEAN stack
template forks**:

- `judus/minimal-framework` — 31,983 chars
- **24 of the top 30 longest READMEs** are variants of the same MEAN stack
  template (`oritpersik/test`, `jeffj/example-mean-app`, `machnicki/mean`,
  `bulgar1/mean-app`, …). The original MEAN.io template is 31.9 KB of
  boilerplate, copied verbatim into hundreds of repos.
- `anthonyfok/cobra` — the Cobra CLI framework, 31,915 chars
- `PeterDaveHelloKitchen/nvm` — nvm fork, 31,812 chars
- `gyurisc/Humanizer` — .NET Humanizer fork, 31,760 chars

**If you've ever cloned a "framework starter," congratulations: you probably
contributed to this list.**

### 8. The winner-takes-all install ecosystems

For each category, what share of the top repos use the single most popular
install method?

| Category          | Winner         | Share   |
|-------------------|----------------|---------|
| Other             | (none listed)  | 94.5%   |
| Security          | (none listed)  | 72.5%   |
| Web               | `npm`          | 62.8%   |
| Mobile            | (none listed)  | 62.8%   |
| Docs / lists      | (none listed)  | 61.0%   |
| ML                | (none listed)  | 59.5%   |
| Games             | (none listed)  | 53.5%   |
| OS                | (none listed)  | 48.0%   |
| DevOps            | (none listed)  | 46.8%   |
| Data engineering  | (none listed)  | 42.0%   |
| Libraries         | `npm`          | 38.5%   |
| Crypto            | (none listed)  | 37.2%   |
| Databases         | (none listed)  | 36.5%   |
| CLI tools         | `npm`          | 32.8%   |

Two observations:

1. **Security README culture is allergic to install instructions** (72.5% have
   no install block). Half because the tools are pre-installed on Kali; half
   because the author assumes you can read C.
2. **Web is the only category where the install method is a monoculture**
   (npm owns 62.8% of the web top set).

### 9. The awesome-list epidemic

A genre of repo whose entire purpose is to be a curated list of other repos.
**Starting-with-"awesome" has become a branding strategy** — there are so many
that browsing them feels like an infinite hall of mirrors:

Categories colonized by awesome-lists:

- **Data eng** is mostly `awesome-datascience` forks — we counted **20+ forks**
  of the same ~28 KB "Awesome Data Science" README, all with near-identical
  content, differing only in contributor attribution.
- **Security** has `awesome-cyber-skills`, `awesome-mobile-CTF`,
  `awesome-security`, `awesome-hacking`.
- **Mobile** has `awesome-android` (29.9 KB).
- **Databases** has `awesome-scala`, `awesome-go`.
- **DevOps** has `awesome-keycloak`, `awesome-ciandcd`.
- **Docs** has `awesome-elasticsearch`, `awesome-amazon-alexa`.

A rough estimate: **1–2% of the top-quality repos across all 14 categories
are awesome-lists**, and within Data Eng the share of `awesome-datascience`
forks in the top-100 approaches 30%. *Curating other people's code has
become its own whole genre of open source.*

---

## Methodology

1. **Pull.** BigQuery query on `bigquery-public-data.github_repos` joining
   `files` (filter `path LIKE '%README%'`) + `contents` (content column) +
   `languages` (top language by bytes). One README per repo (pick the biggest
   if multiple). Export to a single zstd-compressed Parquet file on disk.
2. **Upload once.** Stream the parquet to the Burla cluster's shared
   filesystem (`/workspace/shared`) using a scatter-gather pattern: N parallel
   workers each write a chunk, one finalizer concatenates + decompresses.
3. **Fan out.** `remote_parallel_map(summarize_shard, range(600))` — each
   worker uses `pq.ParquetFile(...).iter_batches()` to stream its stripe
   without loading the full 1.3 GB file into RAM.
4. **Summarize.** For each README: pull the first non-empty heading as the
   *title*; the first non-empty non-heading line as the *tldr*; regex out
   install commands (`pip`, `npm`, `brew`, `go get`, `docker run`, …); scan
   for `![badge](...)` images, ` ``` ` code fences, and domain keywords
   (tensorflow, kubernetes, flutter, unity, ethereum, …) to assign one of the
   14 categories.
5. **Reduce.** 16 buckets, each merges ~40 shard files in parallel.
   `top_per_cat = 400`, `top_per_lang = 200`, `sample_cap = 6000` — enough for
   the UI without shipping 1.2M rows to the browser.
6. **Analyze locally.** TF-IDF over the 14 category "documents." Build the
   client-side search index. Emit `index.json`, `categories.json`,
   `findings.json`, `search.json`.
7. **Serve.** Static HTML + vanilla JS. Zero frameworks.

## Reproduce

```bash
# prereqs
gcloud auth application-default login
pip install -r requirements.txt

# local sample of 400k metadata rows (BigQuery — ~30 s, produces 1.3 GB parquet)
python prepare.py --out samples/readmes.parquet

# map phase on Burla (600 shards, 500+ workers)
python scale.py

# reduce phase on Burla (16 buckets)
python reduce.py

# TF-IDF + findings + search index (local, ~30 s)
python analysis.py

# serve
cd frontend && python -m http.server 8766
# then open http://localhost:8766
```

## Caveats

- **Categorization is keyword-based, not learned.** A repo talking about
  "kubernetes" in passing might still land in DevOps even if it's really a
  library; "web" is a catch-all whenever `npm` / `node` / `react` tokens
  appear. This is deliberate: we wanted a deterministic, reproducible,
  LLM-free pipeline that makes the scale — not the per-repo precision — the
  point.
- **The dataset is a 2016-era snapshot.** BigQuery's public
  `github_repos.contents` is frozen at that vintage for licensing reasons.
  That's why Objective-C is still the #1 mobile language and why Swift is at
  29% rather than 80%. Running the same pipeline on a fresher mirror would
  re-shuffle a few leaderboards but the *shape* of the landscape won't change
  much.
- **Duplicates are real.** Lots of the top-30 "longest READMEs" and "awesome-*
  list" entries are forks of the same starter template. We kept them in
  because *the forking itself is part of the finding*: open source runs on
  copy-paste.
- **Zero LLMs.** Every title, TLDR, category, install method, and ranked list
  was produced by regex and word-count. The only "model" in this pipeline is
  TF-IDF.
