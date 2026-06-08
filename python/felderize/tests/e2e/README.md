# End-to-end tests (Spark → Feldera)

End-to-end tests that translate Spark SQL to Feldera SQL with felderize and check
it on a **real Feldera instance** with the **`run_tests.py`** runner:
translate → compile + run a pipeline → compare the output against the recorded
`.result`.

The unit tests (fast, hermetic) live separately under `../unit/` and run with
plain `pytest`.

## Layout

All test programs are under **`fixtures/`**, grouped by feature in
subdirectories:

- **Result-comparison fixtures** — files share a stem:
  `<id>.schema.sql`, `<id>.query.sql`, `<id>.data.sql`, `<id>.result`. E.g.
  `cast/`, `group-by/`, `window/`, and the multi-view suites `multiple_views/`
  and `multiple_views_dialect/`.
- **Behavior fixtures** (`fixtures/malformed/`, `fixtures/multiple_views_unsupported/`)
  — carry a `<id>.meta.json` (injected fault / unsupported feature) and **no
  `.result`**. They are **excluded** from `run_tests.py`; they exist to check
  that felderize degrades gracefully on bad input (flags the problem, emits a
  placeholder, never crashes).
- `support_status_<date>.yaml` — a snapshot of every fixture's outcome
  (pass/fail/error/skipped) with a brief reason, from a full run.

## What `run_tests.py` does

For every result-comparison test, in a single pass (no separate compile/run phases):

1. **Translate** the Spark SQL → Feldera SQL with felderize.
2. **Compile + start** a Feldera pipeline from the result.
3. **Insert** the test data and **query** the final view.
4. **Compare** the rows against the recorded `.result` (order-independent).

## Prerequisites

`run_tests.py` checks all of these on startup and reports what's missing:

| Need | How to get it |
|---|---|
| **Anthropic API key** | `export ANTHROPIC_API_KEY=...` or a `.env` file. Translation uses it. |
| **felderize** | `pip install -e .` from `python/felderize` (or `pip install felderize`). |
| **Feldera SQL compiler** | `felderize download-compiler`, then set `FELDERA_COMPILER` to the printed path (Java 19–21). |
| **Feldera Python SDK** | `pip install feldera` (or `pip install -e '.[test]'`). |
| **Docker** | Installed, with the daemon running (needed to start a Feldera instance). |
| **A running Feldera instance** | `docker run --rm -it -p 8080:8080 ghcr.io/feldera/pipeline-manager:latest` (see https://docs.feldera.com/get-started/). |

Config is read from `python/felderize/.env` (or the environment):
`ANTHROPIC_API_KEY`, `FELDERIZE_MODEL`, `FELDERA_COMPILER`.

## Usage

> **Run inside the project virtualenv.** `run_tests.py` imports the installed
> `feldera` SDK (which needs `pyarrow`) and `felderize`. Activate the venv
> (`source .venv/bin/activate`) or invoke `.venv/bin/python` explicitly — a bare
> system `python3` that lacks these will fail every test with
> `No module named 'pyarrow'` / `feldera`. Install them with
> `pip install -e '.[test]'`.

Paths below are from `python/felderize/` (the runners resolve fixtures/config by
their own location, so the working directory doesn't matter). They assume the
venv is active; otherwise prefix with `.venv/bin/python`:

```bash
# Verify the environment without running anything
python3 tests/e2e/run_tests.py --check-only

# Run the whole result-comparison suite
python3 tests/e2e/run_tests.py

# Just one subdirectory, or one test
python3 tests/e2e/run_tests.py --dir multiple_views
python3 tests/e2e/run_tests.py --file multiple_views_001

# One clean line per test by default; --verbose shows the translator / LLM /
# compiler chatter (useful when debugging a single test)
python3 tests/e2e/run_tests.py --file cast_001 --verbose

# Faster compile / parallel sharding / persisted results
# Run all 4 shards into one results dir (in parallel), then merge:
for k in 0 1 2 3; do
  python3 tests/e2e/run_tests.py --profile dev --results-dir out/ --shard $k/4 &
done
wait
python3 tests/e2e/run_tests.py --results-dir out/ --merge-summaries
```

`run_tests.py` prints one outcome per test plus a summary, and exits non-zero if
any test ended in `FAIL` or `ERROR`. The four outcomes mean:

| Outcome | Meaning |
|---|---|
| **PASS** | Translated, compiled, ran on Feldera, and the view's rows matched the recorded `.result` (compared order-independently). |
| **FAIL** | Translated and ran, but the result was **wrong** — a value differs or the row count doesn't match the `.result`. The translation is incorrect. |
| **ERROR** | Couldn't get a result to compare at all: translation threw, the SQL **failed to compile**, the pipeline failed to start, or no output view could be found in the translated SQL. |
| **SKIP** | Nothing to compare, so the test is neither passed nor failed: felderize produced **no query** (empty translation), or the final output is a `LOCAL VIEW` (not observable — e.g. an `INTERVAL` output column). |

Rule of thumb: `FAIL` = a translation **correctness** bug; `ERROR` = it didn't run
far enough to judge (translation/compile/runtime failure); `SKIP` = expected,
not something to fix.

Each test writes a per-test JSON to `out/<subdir>/<test_id>.json`. A
single (unsharded) run also writes `out/summary.json`. A **sharded** run writes
`out/summary.shard-K-of-M.json` per shard instead — each shard sees only its own
stride, so they must not clobber a shared `summary.json`. After all shards
finish, `--merge-summaries` rebuilds the canonical `out/summary.json` from the
per-test JSONs on disk (the source of truth).
