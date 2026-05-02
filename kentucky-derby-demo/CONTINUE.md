# Continuing the Kentucky Derby 1-Trillion Simulation

## What we're doing

Running **1,000,000,000,000 (1 trillion) Monte Carlo race simulations** for the 2026 Kentucky Derby on Burla, then updating the live demo website at https://jackburla.github.io/BurlaKentuckyDerby/ with the real results.

---

## Context (what's already done)

| Step | Status |
|------|--------|
| Full ML pipeline (scraper → features → model → sensitivity → montecarlo) | ✅ Complete |
| 1,000,000 simulation run (initial results) | ✅ Complete |
| Demo website pushed to GitHub Pages | ✅ Live at jackburla.github.io/BurlaKentuckyDerby |
| `derby_trillion.py` written & debugged | ✅ Ready to run |
| 1T run on Burla | ⏳ **Pick up here** |

---

## Setup on the new computer

### 1. Clone the repo
```bash
git clone https://github.com/JackBurla/BurlaKentuckyDerby.git
cd BurlaKentuckyDerby
```

### 2. Install Python 3.12 (MUST match Burla cluster — cluster runs 3.12)

The cluster containers run Python 3.12. If you run the script with 3.11 or 3.13, Burla will reject the job.

- Windows: download from https://www.python.org/ftp/python/3.12.9/python-3.12.9-amd64.exe
- Mac/Linux: `brew install python@3.12` or `pyenv install 3.12`

Verify: `python3.12 --version` should say `Python 3.12.x`

### 3. Install dependencies on Python 3.12
```bash
python3.12 -m pip install burla numpy pandas scikit-learn openpyxl requests beautifulsoup4
```

### 4. Log in to Burla
```bash
python3.12 -m burla login
# or: burla login
```
Use the same Burla account (JackBurla / Google/Microsoft login).

### 5. Make sure the Burla cluster is running
Go to the Burla dashboard and confirm the cluster is **started and healthy** (green nodes). If it was stopped, start it. Wait ~2 minutes for nodes to finish booting.

---

## Running the 1 trillion simulation

```bash
cd BurlaKentuckyDerby
python3.12 -u derby/derby_trillion.py
```

### What it does
- Dispatches **50,000 Burla workers**, each running **20,000,000 simulations**
- `max_parallelism=500` so only 500 run concurrently (safe under the 2,081 vCPU quota)
- Workers cycle through in ~100 rounds → estimated **15–30 minutes** total wall time
- Saves results to `derby/data/trillion_results.json`
- Prints a full results table at the end

### Expected output (success)
```
1 Trillion Simulation Kentucky Derby Model
==========================================
Workers   : 50,000
Sims/wrkr : 20,000,000
Chunk size: 100,000
Total sims: 1,000,000,000,000

Dispatching 50,000 workers to Burla cluster...
  grow=True, max_parallelism=500, func_cpu=1
✔ Done! Ran 50000 inputs through `simulate_race_batch` (50000/50000 completed)

Horse                  Win%     Place%   Show%   Value  Kelly
...
Done: 1,000,000,000,000 simulations in Xs via Burla
```

### If you hit quota errors (CPUS_PER_VM_FAMILY)
Open `derby/derby_trillion.py` and lower `max_parallelism`:
```python
max_parallelism=200,  # try 200 if 500 hits quota
```
The run will take ~2.5× longer but still completes.

### If you hit Python version mismatch
```
User is running python 3.x, containers in the cluster are running: 3.12
```
Make sure you're running with `python3.12`, not `python` or `python3`.

---

## After the simulation completes

### Step 1 — Update the website with real numbers

Run this to parse `trillion_results.json` and patch `docs/index.html`:

```bash
python3.12 derby/update_website.py
```

*(This script is described below — create it if not already there.)*

### Step 2 — Push to GitHub
```bash
git add derby/data/trillion_results.json docs/index.html
git commit -m "Update website with 1 trillion simulation results"
git push
```

GitHub Pages will rebuild in ~60 seconds. Site will be live at:
**https://jackburla.github.io/BurlaKentuckyDerby/**

---

## The update_website.py script

If `derby/update_website.py` doesn't exist yet, tell the Cursor agent:

> "The trillion sim finished and saved to `derby/data/trillion_results.json`. Read that file, extract the win/place/show percentages for all 20 horses, update the HORSES array in `docs/index.html` with the new numbers, change the hero stat from `1,000,000` to `1,000,000,000,000`, update the timing on the website (change '90 seconds' to the actual elapsed time), and push to GitHub."

---

## Key files

| File | Purpose |
|------|---------|
| `derby/derby_trillion.py` | **Run this** — 1T Burla simulation |
| `derby/data/trillion_results.json` | Output: per-horse win/place/show% from 1T sims |
| `docs/index.html` | The live demo website |
| `canvases/kentucky-derby-2026.canvas.tsx` | Cursor Canvas (uses 1M sim results) |
| `derby/derby_montecarlo.py` | Original 1M sim pipeline (reference) |

---

## Previous agent transcript

The full chat history for this session is at:
`agent-transcripts/8bab1942-d7f1-46cf-b782-ccaa6f94c916`

To give a new Cursor agent full context, paste this at the start:

> "Continue from the agent transcript at `8bab1942-d7f1-46cf-b782-ccaa6f94c916`. We built a Kentucky Derby prediction model, pushed it to GitHub at JackBurla/BurlaKentuckyDerby, built a Burla-style demo website at jackburla.github.io/BurlaKentuckyDerby, and were in the middle of running 1 trillion Monte Carlo simulations on Burla to update the website with real results. The script is `derby/derby_trillion.py`. Read `CONTINUE.md` for full instructions."

---

## Current simulation results (from 1,000,000 runs — to be replaced by 1T)

| Post | Horse | Win% | Place% | Show% | Value |
|------|-------|------|--------|-------|-------|
| 16 | **Further Ado** | 31.4% | 52.5% | 66.9% | BET |
| 11 | **Chief Wallabee** | 26.3% | 46.8% | 61.5% | BET |
| 8 | **The Puma** | 11.3% | 24.2% | 37.1% | BET |
| 13 | Emerging Market | 7.6% | 17.3% | 28.0% | BET |
| 5 | Commandment | 5.5% | 13.0% | 21.9% | FADE |
| 7 | So Happy | 4.6% | 11.2% | 19.1% | FADE |
| 4 | **Litmus Test** ⭐ longshot | 0.9% | 2.3% | 4.6% | BET |

*(These numbers come from `canvases/kentucky-derby-2026.canvas.tsx` and are the real model outputs.)*
