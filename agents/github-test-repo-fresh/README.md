# github-test-repo-fresh

A minimal but practical Burla demo that validates local logic first, then runs
the same function remotely with `remote_parallel_map(..., grow=True)`.

## What this demo does

- Builds simple numeric features for each input:
  - `square`
  - `cube`
  - `parity`
  - lightweight derived `score`
- Processes 50 inputs locally and remotely
- Prints run markers so success is obvious:
  - `LOCAL_OK`
  - `REMOTE_OK`

## Requirements

- Python with `burla` installed
- Burla account authenticated

## Run

From this repo:

```bash
python /Users/josephperry/claude/burla-demos/auto_login.py && python burla_basic_demo.py
```

## Expected output

You should see:

- `LOCAL_OK`
- `REMOTE_OK`
- `remote_count= 50`

If Burla worker connectivity is temporarily flaky, re-run the same command.

## Why this is useful

This pattern is a template for real workloads:

1. Validate transformations locally.
2. Keep worker functions top-level and picklable.
3. Scale the same function across many inputs using Burla.
