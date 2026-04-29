---

## name: burla

description: Onboard users to Burla, run real parallel Python workloads at scale, and build/run Burla demos.
user-invocable: true

# Burla Skill

You are helping a user use **Burla** — a service that runs any Python function across many remote CPUs (or GPUs) in parallel via `burla.remote_parallel_map`.

This skill covers two flows:

1. **Onboarding** a new user end-to-end (account, login, cluster on, first 100k-input job).
2. **Building and running demos / pipelines** for an already-onboarded user.

> Before doing anything, read **"Per-user config & where to edit"** so you know which files you may edit and which you must not.

---

## Per-user config & where to edit

This skill file is **canonical, generic Burla knowledge**. Do **not** rewrite the canonical sections to match a specific user, cluster URL, email, or file path.

**All per-user values live under `~/.burla/<slug>/`** where `<slug>` is the email local-part lowercased with non-alphanumerics stripped (e.g. `joeyper23@gmail.com` → `joeyper23`). Each account gets its own sub-directory so multiple Burla accounts can coexist on the same machine:

1. `~/.burla/<slug>/user_config.json` — non-secret per-user paths, cluster URL, pinned client versions
2. `~/.burla/<slug>/.env` — secrets (`chmod 600`)
3. `~/.burla/<slug>/chrome-profile/` — Playwright persistent profile (saved Google session)
4. `~/.burla/<slug>/.venv/` — auto-provisioned Python venv matching the cluster's required Python + `burla` client version

Example `user_config.json`:

```json
{
  "email": "you@example.com",
  "burla_url": "https://<their-subdomain>.burla.dev/",
  "project_id": "burla-<id>",
  "auth_provider": "google",
  "venv_python": "/Users/<them>/.burla/<slug>/.venv/bin/python",
  "client_python_version": "3.12",
  "client_burla_version": "1.4.5",
  "cluster_python_version": "3.12",
  "default_first_run_inputs": 100000
}
```

Example `.env` (must be gitignored):

```dotenv
BURLA_EMAIL=user@example.com
BURLA_PASSWORD=********
BURLA_URL=https://<their-subdomain>.burla.dev
```

**Rules for the agent:**

- ✅ Edit `.env` and `user_config.json` freely.
- ✅ Modify cluster settings via the Burla UI / API on the **user's** cluster (image, VM, authorized users) — the user has granted full admin actions.
- ❌ **Do not** edit canonical sections of this file with a specific user's URL, email, paths, or VM specs.
- ❌ **Do not** commit `.env` or any file under `~/.burla/` to git.
- If `.env` does not exist, **create it** and ensure it is in `.gitignore`.

---

## What Burla Does

`remote_parallel_map(function_, inputs, ...)` takes any Python function and a list of inputs, and runs the function on each input in parallel — each on its own remote CPU. It returns a list of results.

```python
from burla import remote_parallel_map

def process(item):
    # runs on a remote machine
    return result

results = remote_parallel_map(process, [item1, item2, ...])
```

Key behaviors:

- Exceptions are re-raised on the client with full stack traces
- `print()` output is streamed back in real-time
- Returns a list (or generator if `generator=True`)
- Can queue millions of inputs reliably

Key parameters:

- `func_cpu` — CPUs per function instance (default 1)
- `func_ram` — RAM GB per instance (default 4)
- `max_parallelism` — cap concurrent instances
- `generator=True` — stream results as they arrive
- `grow=True` — auto-add nodes (up to 2560 CPUs)
- `detach=True` — keep running even if local process stops

Key constraints:

- Worker functions must be **top-level and picklable** — no lambdas, no closures over outer state.
- Non-Python system deps (ffmpeg, libGL, etc.) require updating the cluster's container image.

---

## First-time User Onboarding

**Preferred path: use the Burla Agent Starter Kit.**

If it's not already present in `agents/burla-agent-starter-kit/`, clone `https://github.com/Jmp1062/burla-agent-starter-kit` there. The kit was designed for exactly this skill and collapses the entire onboarding into one idempotent command:

```bash
cd agents/burla-agent-starter-kit
pip install -r requirements.txt
python -m playwright install chromium
python onboard.py --email <user-email> --demo demos/square.py
```

The kit handles every step below automatically:


| Step          | What the kit does                                                                                                                                                                                                                                                               |
| ------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 1. Auth       | Opens Chromium with a per-account persistent profile. Auto-clicks *Continue with Google*, waits for Google password entry (the **only** manual step), auto-clicks *Authorize* for the CLI. Saves the profile so every future run is zero-touch.                                 |
| 2. Config     | Writes `~/.burla/<slug>/user_config.json` and `~/.burla/<slug>/.env` with cluster URL + project id read from the CLI's saved credentials.                                                                                                                                       |
| 3. Venv       | Creates `~/.burla/<slug>/.venv` with the Python version (typically 3.12) and `burla` client version (typically 1.4.5) the cluster requires.                                                                                                                                     |
| 4. Cluster on | **Escalation ladder**: try `remote_parallel_map(..., grow=True)` first; on `VersionMismatch` or `NodeConflict` rebuild the venv; on `NoNodes` / cluster-off drive the dashboard UI via the saved Playwright profile and click *Start*; poll until at least one node is `READY`. |
| 5. First job  | Runs `demos/square.py` (100,000 inputs) in the provisioned venv as a sanity check.                                                                                                                                                                                              |


**Only use the manual flow below if the kit is unavailable or broken.** When you do use the kit, you still have to:

1. **Confirm identity** with the user (email, auth provider) before running `onboard.py`.
2. **Tell the user** exactly when to type their Google password (the kit logs a clear prompt; reiterate it).
3. **After success**, capture the cluster URL shown in the kit's output and tell the user what demo ran.

### Manual fallback (only if the kit cannot be used)

1. Confirm email + auth provider. Save to `~/.burla/<slug>/user_config.json` and `~/.burla/<slug>/.env`.
2. Open `https://login.burla.dev/` in a clean browser and complete OAuth. **Never type the user's password for them.**
3. Run `burla login` (the CLI opens the consent page). Click *Authorize*.
4. Read the cluster URL + project id from `~/Library/Application Support/burla/burla_credentials.json`.
5. If `remote_parallel_map(..., grow=True)` fails with `NoNodes`, go to the cluster URL in a browser and click **Start**.
6. Run a 100k-input job (see `demos/square.py` in the starter kit for a template).

> Password entry must always remain with the user. The agent never stores, logs, or types the user's Google password.

---

## Connect / Authenticate (returning users)

Prefer the starter kit's idempotent entry-point — it short-circuits when the session and venv are already valid:

```bash
cd agents/burla-agent-starter-kit
python onboard.py --email <user-email>             # re-auths only if needed, ensures cluster is on
python run_job.py --email <user-email> <script>.py  # runs in the per-account venv
```

If the kit is not available, the manual fallbacks are:

```bash
burla login                                   # manual Google OAuth in a browser
~/.burla/<slug>/.venv/bin/python <script>.py  # run jobs with the pinned client version
```

**Always ensure the cluster is ON before any `remote_parallel_map` call.** The kit's `ensure_cluster_ready(email, url)` does this automatically (grow → UI Start fallback). In the manual flow, open the cluster URL and click *Start* yourself.

When chaining auth + execution, the kit's `onboard.py --demo <path>` is the one-liner that does both.

---

## Cluster Configuration (user-administered)

The user has granted **full admin** rights to the agent for cluster changes. Common tasks:

- **Start/stop cluster** — Cluster Status page in the Burla UI, or `grow=True` for automatic start.
- **Change container image** — Settings UI → Image. Use a custom image when system deps are required (e.g. `osgeo/gdal:latest`, custom CUDA images).
- **Change VM size / GPU** — Settings UI → VM. Pick CPU/RAM/GPU appropriate for the workload.
- **Authorized users** — Settings UI → Users.
- **Region / disk** — Settings UI.

Rules:

- Only modify the **user's own cluster**. Never touch another tenant's settings.
- Document any cluster change in a short note for the user before applying it.
- Prefer the smallest config that works; scale up only if a job is bottlenecked.

> Never write the user's specific cluster config back into this skill file. Cluster config lives only on the cluster itself and (optionally) in `user_config.json`.

---

## Running Real Pipelines at Scale

For "real workload" runs (post-onboarding), recommend these patterns:

### A. Embarrassingly parallel transform

Hundreds of thousands of inputs, single function:

```python
from burla import remote_parallel_map

def transform(record):
    # do real work here
    return processed

results = remote_parallel_map(transform, records, grow=True)
```

### B. Streaming results

For long-running fan-outs where you want results as they arrive:

```python
for result in remote_parallel_map(transform, records, grow=True, generator=True):
    handle(result)
```

### C. Detached background jobs

For workloads larger than your local session lifetime:

```python
remote_parallel_map(transform, records, grow=True, detach=True)
```

### D. Map-Reduce via shared filesystem

`/workspace/shared` is a GCS-backed folder available on all worker nodes:

- Write to `/workspace/shared/...` → file appears in GCS bucket
- Read from `/workspace/shared/...` → reads from GCS bucket
- Visible in the Burla dashboard under **Filesystem**
- Use `Path(...).parent.mkdir(parents=True, exist_ok=True)` before writing nested paths

```python
from pathlib import Path
from burla import remote_parallel_map

def write_part(number):
    path = f"/workspace/shared/demo/parts/{number}.txt"
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    Path(path).write_text(f"{number}\n")
    return path

part_paths = remote_parallel_map(write_part, list(range(100)), grow=True)

def combine(paths):
    total = sum(int(Path(p).read_text().strip()) for p in paths)
    out = "/workspace/shared/demo/total.txt"
    Path(out).parent.mkdir(parents=True, exist_ok=True)
    Path(out).write_text(str(total))
    return out

remote_parallel_map(combine, [part_paths], func_cpu=8, func_ram=32, grow=True)
```

Wrap the reduce input in a list-of-one (`[part_paths]`) so it runs as a single call.

---

## Demo Ideas

When the user invokes `/burla` for a demo, suggest something practical:

1. **Parallel web scraping** — fan out 100+ URLs, persist results to `/workspace/shared`.
2. **Embarrassingly parallel compute** — primes, hashing, data transforms over 100k+ inputs.
3. **LLM batch inference** — run a prompt against many inputs simultaneously.
4. **Map-Reduce** — fan-out + single-reducer aggregation pattern above.
5. **API fan-out** — call an external API with many inputs concurrently.

## How to Write a Demo

1. Decide where the demo lives:
  - The user's standard demos directory (from `user_config.json` `demos_dir`), **or**
  - An agent-scoped repo under `agents/<repo>/` (preferred when it will be pushed to GitHub).
2. Import `from burla import remote_parallel_map`.
3. Define the worker function at **module top level** (picklable).
4. Validate locally on a few inputs first.
5. Use `/workspace/shared/` for any file I/O between steps.
6. Call `remote_parallel_map(fn, inputs, grow=True)`.
7. Print clear success markers (e.g., `LOCAL_OK`, `REMOTE_OK`, counts, samples).

## Rules for Building Demos

- **Validate locally first.** Burla cold starts take 3–5 minutes and errors are slow/opaque — don't use it as the first test of whether code works.
- **No GitHub push until the demo runs end-to-end on Burla.** No repo, no remote, no commit-and-push until you have a confirmed successful run.
- **No silent pivoting.** If the chosen data source or approach is infeasible, stop and tell the user — do not substitute a different one without asking.

---

## Troubleshooting

When a Burla call fails, do this in order before changing the user's code:

1. **Re-authenticate**: run `burla login` (or the project's auto-login script). Many failures are silent token expirations.
2. **Retry with `grow=True`**: confirms it isn't a "cluster off" issue.
3. **Look at the Cluster Status page** in the Burla UI to confirm nodes are healthy.
4. **Worker connection errors** (e.g. `Cannot connect to host <ip>:<port>`) are typically transient. Retry once after re-auth; if persistent, restart the cluster.
5. **Auth/JSON errors** are usually credential expiry — re-run auth.
6. **System dep errors** (missing native libs) — update the cluster's container image, then retry.

If the same error happens twice in a row after the steps above, stop and report the exact error to the user with a 1-line hypothesis.

---

## Workflow When Invoked

If the user has never been onboarded (no `~/.burla/<slug>/` directory for their email):

1. Ensure the starter kit is present at `agents/burla-agent-starter-kit/` (clone it if not).
2. Run `python onboard.py --email <them> --demo demos/square.py` from inside the kit. Tell the user exactly when to type their Google password.
3. Confirm success (look for `REMOTE_OK` + 100,000 results) and offer to build a real demo or pipeline.

If the user is already onboarded:

1. Ask what they want to build (or suggest from **Demo Ideas**).
2. Decide repo location (the starter kit's `demos/` directory, or a fresh `agents/<repo>/` for a demo that will be pushed to GitHub).
3. Write and locally validate the demo.
4. Run via `python run_job.py --email <them> <script>.py` from the starter kit so the correct venv + cluster-on escalation happens automatically.
5. Only after a confirmed successful run, ask whether to push to GitHub with a polished README.

---

## Hard Rules (do not violate)

- Never push to `https://github.com/Burla-Cloud/burla`.
- Never commit `.env` or anything under `~/.burla/` to git.
- Never edit the canonical sections of this skill file with a specific user's URL, email, paths, or VM specs — that information lives in `~/.burla/user_config.json` and `~/.burla/.env`.
- Never run a cluster modification on someone else's tenant — only on the authenticated user's own cluster.

