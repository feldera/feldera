#!/usr/bin/env python3
"""Run the Spark→Feldera tests end to end, in a single pass.

For each test this script does everything in one go — no separate compile and
run phases:

    1. translate the Spark SQL to Feldera SQL (felderize),
    2. compile + start a Feldera pipeline from it,
    3. insert the test data and query the final view,
    4. compare the rows against the recorded ``.result``.

It is meant to be run by external users, so it first checks that everything it
needs is in place: the Anthropic API key (translation), the Feldera SQL
compiler, Docker, and a reachable Feldera instance.

Usage:
    python3 run_tests.py                 # run every test under this folder
    python3 run_tests.py --dir cast      # only one subdirectory
    python3 run_tests.py --file cast_001 # only one test
    python3 run_tests.py --url http://localhost:8080
    python3 run_tests.py --profile dev   # fastest Feldera compile (default)
    python3 run_tests.py --results-dir out/   # write per-test JSON + summary.json
    python3 run_tests.py --check-only    # only verify prerequisites

See README.md in this folder for setup details.
"""

from __future__ import annotations

import argparse
import ast
import contextlib
import io
import json
import logging
import re
import shutil
import subprocess
import sys
import urllib.error
import urllib.request
from pathlib import Path

# This file lives at <felderize>/tests/e2e/run_tests.py; fixtures are in ./fixtures.
_THIS_DIR = Path(__file__).resolve().parent
_FIXTURES_DIR = _THIS_DIR / "fixtures"
_FELDERIZE_ROOT = _THIS_DIR.parents[1]  # python/felderize
_MONOREPO_PYTHON = _THIS_DIR.parents[2]  # python/  (holds the in-repo feldera SDK)

_PIPELINE_PREFIX = "felderize-e2e-"
_DEFAULT_URL = "http://localhost:8080"

# Robustness fixtures (malformed inputs / unsupported features) have no `.result`
# and are exercised by run_behavior.py, not this result-comparison runner.
_BEHAVIOR_DIRS = {"malformed", "multiple_views_unsupported"}


# ---------------------------------------------------------------------------
# Prerequisite checks
# ---------------------------------------------------------------------------


def _check_api_key(config) -> list[str]:
    if not config.api_key:
        return [
            "ANTHROPIC_API_KEY is not set. Translation needs it — put it in a .env "
            "file or export it in your shell."
        ]
    return []


def _check_compiler(config) -> list[str]:
    compiler = config.feldera_compiler
    if not compiler:
        return [
            "Feldera SQL compiler not configured. Set FELDERA_COMPILER in .env, or "
            "run `felderize download-compiler` and use the path it prints."
        ]
    path = Path(compiler).expanduser()
    if not path.is_file():
        return [
            f"Feldera compiler not found at {path}. "
            "Run `felderize download-compiler` to fetch it."
        ]
    if path.suffix == ".jar" and not shutil.which("java"):
        return ["Java not found — install Java 19–21 to run the compiler JAR."]
    # Reject a compiler older than felderize's minimum (when the version is
    # parseable from the filename) — older releases lack SQL features felderize
    # relies on, so validation against them is unreliable.
    try:
        from felderize.install_feldera_sql_compiler import (
            MINIMUM_COMPILER_VERSION,
            is_supported_version,
            jar_version,
        )

        tag = jar_version(str(path))
        if tag and not is_supported_version(tag):
            return [
                f"Feldera compiler {tag} is older than the minimum supported "
                f"{MINIMUM_COMPILER_VERSION}. Run `felderize download-compiler` to update."
            ]
    except ModuleNotFoundError:
        pass  # felderize not importable here; the SDK prerequisite will flag the env
    return []


def _check_docker() -> list[str]:
    if not shutil.which("docker"):
        return [
            "Docker not found. Install Docker — Feldera runs as a Docker container."
        ]
    try:
        result = subprocess.run(
            ["docker", "info"], capture_output=True, timeout=20, text=True
        )
    except Exception as e:  # noqa: BLE001 - report any failure to the user
        return [f"Could not run `docker info`: {e}"]
    if result.returncode != 0:
        return [
            "Docker is installed but its daemon is not running. Start Docker Desktop / the docker service."
        ]
    return []


def _check_feldera(url: str) -> list[str]:
    """A reachable server (any HTTP response) is good enough; refused/timeout is not."""
    try:
        urllib.request.urlopen(url, timeout=5)
        return []
    except urllib.error.HTTPError:
        return []  # server answered (some status) — it is up
    except Exception:  # noqa: BLE001 - connection refused, DNS, timeout, ...
        return [
            f"No Feldera instance reachable at {url}. Start one with Docker, e.g.:\n"
            "      docker run --rm -it -p 8080:8080 ghcr.io/feldera/pipeline-manager:latest\n"
            "    then re-run (see https://docs.feldera.com/get-started/)."
        ]


def _check_feldera_sdk() -> list[str]:
    """Import the full SDK client chain the runner uses at run time.

    Done here so a missing dependency fails fast with one clear message, instead
    of erroring identically on every test (a bare system python that lacks the
    SDK's deps — pyarrow, requests, … — is the usual cause).
    """
    try:
        _feldera_imports()
        return []
    except Exception as e:  # noqa: BLE001 - any import failure is actionable
        return [
            f"Feldera Python SDK is not fully importable ({type(e).__name__}: {e}).\n"
            "      Run inside the project venv — `.venv/bin/python tests/e2e/run_tests.py`\n"
            "      or `source .venv/bin/activate` first — or `pip install feldera`\n"
            "      into the interpreter you are using. A bare system python is the usual cause."
        ]


def check_prerequisites(config, url: str) -> bool:
    """Print a checklist; return True only if every prerequisite is satisfied."""
    checks = [
        ("Anthropic API key", _check_api_key(config)),
        ("Feldera SQL compiler", _check_compiler(config)),
        ("Feldera Python SDK", _check_feldera_sdk()),
        ("Docker", _check_docker()),
        (f"Feldera instance ({url})", _check_feldera(url)),
    ]
    ok = True
    print("Checking prerequisites:")
    for label, problems in checks:
        if problems:
            ok = False
            print(f"  ✗ {label}")
            for p in problems:
                print(f"      {p}")
        else:
            print(f"  ✓ {label}")
    print()
    return ok


# ---------------------------------------------------------------------------
# SQL + result helpers
# ---------------------------------------------------------------------------


# CREATE VIEW with any modifier Feldera/Spark may carry. Group 1 captures the
# modifier (LOCAL / MATERIALIZED / TEMP[ORARY]) when present, group 2 the name.
# A LOCAL view is deliberately not observable (e.g. an INTERVAL output column,
# which a connector-exposed view cannot have), so it is detected, not rewritten.
_CREATE_VIEW_RE = re.compile(
    r"CREATE\s+(?:OR\s+REPLACE\s+)?"
    r"((?:LOCAL|MATERIALIZED|TEMP(?:ORARY)?)\s+)?VIEW\s+([\w`\".-]+)",
    re.IGNORECASE,
)


def _output_view(query_sql: str) -> tuple[str | None, bool]:
    """Return (name, is_local) for the LAST CREATE VIEW — the final output view.

    `is_local` is True when that view is a LOCAL view, which cannot be queried.
    Returns (None, False) when no CREATE VIEW is present.
    """
    code = "\n".join(
        ln for ln in query_sql.splitlines() if not ln.lstrip().startswith("--")
    )
    matches = _CREATE_VIEW_RE.findall(code)
    if not matches:
        return None, False
    modifier, name = matches[-1]
    return name.strip('`"'), "LOCAL" in modifier.upper()


def _extract_view_name(query_sql: str) -> str | None:
    """Return the name of the final output view (see `_output_view`)."""
    return _output_view(query_sql)[0]


# Make views materialized so they can be queried — but leave LOCAL views alone:
# they are intentionally non-observable and forcing MATERIALIZED would break
# compilation (e.g. INTERVAL columns are illegal in a connector-exposed view).
_MATERIALIZE_RE = re.compile(
    r"CREATE\s+(?:OR\s+REPLACE\s+)?(?:(?:MATERIALIZED|TEMP(?:ORARY)?)\s+)?VIEW",
    re.IGNORECASE,
)


def _normalize_feldera_sql(feldera_schema: str, feldera_query: str) -> str:
    """Combine schema + query, making non-local views materialized so they can be queried."""
    query = _MATERIALIZE_RE.sub("CREATE MATERIALIZED VIEW", feldera_query.strip())
    return feldera_schema.strip() + "\n\n" + query


def _parse_spark_result(result_text: str) -> list[list[str]]:
    rows = []
    for line in result_text.strip().splitlines():
        if line.strip():
            rows.append(line.split("\t"))
    return rows


def _rows_match(
    feldera_rows: list[dict], spark_rows: list[list[str]]
) -> tuple[bool, str]:
    """Compare Feldera output against the recorded result, order-independent."""
    if not spark_rows and not feldera_rows:
        return True, "both empty"

    def _norm(v: str) -> str:
        v = v.strip()
        # Booleans: Feldera (Python) renders True/False; Spark renders true/false.
        if v.lower() in ("true", "false"):
            return v.lower()
        row_m = re.match(r"Row\((.+)\)$", v)
        dict_m = re.match(r"\{(.+)\}$", v)
        if row_m or dict_m:
            inner = row_m.group(1) if row_m else dict_m.group(1)
            try:
                pairs = re.findall(r"[\'\"]?(\w+)[\'\"]?\s*[=:]\s*([^,]+)", inner)
                return str(sorted((k.strip(), _norm(vv.strip())) for k, vv in pairs))
            except Exception:  # noqa: BLE001
                pass
        if v.startswith("[") and v.endswith("]"):
            try:
                v_clean = re.sub(r"Decimal\('([^']+)'\)", r"\1", v)
                lst = ast.literal_eval(v_clean)
                if isinstance(lst, list):
                    return str(sorted(_norm(str(x)) for x in lst))
            except Exception:  # noqa: BLE001
                pass
        v_ts = v.replace("T", " ")
        if v_ts != v and re.match(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}", v_ts):
            return v_ts
        try:
            f = float(v)
            return str(int(f)) if f == int(f) else f"{f:.10g}"
        except (ValueError, OverflowError):
            pass
        return v

    def row_to_key(row: dict) -> tuple:
        return tuple(_norm(str(v) if v is not None else "None") for v in row.values())

    feldera_keys = sorted(row_to_key(r) for r in feldera_rows)
    spark_keys = sorted(tuple(_norm(c) for c in r) for r in spark_rows)
    if len(feldera_keys) != len(spark_keys):
        return (
            False,
            f"row count mismatch: feldera={len(feldera_keys)} expected={len(spark_keys)}",
        )
    mismatches = [(f, s) for f, s in zip(feldera_keys, spark_keys) if f != s]
    if mismatches:
        f, s = mismatches[0]
        return False, f"{len(mismatches)} row(s) differ, e.g. got {f} vs expected {s}"
    return True, f"{len(feldera_keys)} rows match"


# ---------------------------------------------------------------------------
# Feldera execution
# ---------------------------------------------------------------------------


def _feldera_imports():
    """Import the Feldera Python SDK (installed, or from the monorepo as fallback)."""
    try:
        from feldera.rest.feldera_client import FelderaClient
        from feldera.pipeline_builder import PipelineBuilder
        from feldera.enums import CompilationProfile, PipelineStatus
    except ModuleNotFoundError:
        sys.path.insert(0, str(_MONOREPO_PYTHON))
        from feldera.rest.feldera_client import FelderaClient
        from feldera.pipeline_builder import PipelineBuilder
        from feldera.enums import CompilationProfile, PipelineStatus
    return FelderaClient, PipelineBuilder, CompilationProfile, PipelineStatus


def _to_feldera_dml(stmt: str) -> str:
    """Convert Spark collection literals in a data statement to Feldera syntax.

    Feldera's ad-hoc INSERT (DataFusion) rejects Spark's `array(...)`/`map(...)`
    literals; sqlglot transpiles them to `ARRAY[...]` / `MAP(...)`. Only touched
    when such a literal is present; falls back to the original on any failure.
    """
    if not re.search(r"\b(?:array|map)\s*\(", stmt, re.IGNORECASE):
        return stmt
    try:
        import sqlglot

        return sqlglot.transpile(stmt, read="spark", write="postgres")[0]
    except Exception:  # noqa: BLE001 - keep the original statement on any failure
        return stmt


def run_on_feldera(
    test_id: str,
    feldera_sql: str,
    view_name: str,
    data_sql: str,
    url: str,
    profile: str = "dev",
) -> list[dict]:
    """Compile + start a pipeline, insert data, query the view, then stop it.

    profile is the Feldera compilation profile ("dev" is fastest to compile).
    """
    import sqlparse

    FelderaClient, PipelineBuilder, CompilationProfile, PipelineStatus = (
        _feldera_imports()
    )
    pipeline_name = _PIPELINE_PREFIX + test_id.replace("_", "-")
    client = FelderaClient(url)

    pipeline = None
    try:
        pipeline = PipelineBuilder(
            client,
            name=pipeline_name,
            sql=feldera_sql,
            compilation_profile=CompilationProfile(profile),
        ).create_or_replace(wait=True)
        pipeline.start()
        pipeline.wait_for_status(PipelineStatus.RUNNING, timeout=120)
        for stmt in sqlparse.split(data_sql):
            stmt = stmt.strip().rstrip(";")
            if not stmt:
                continue
            try:
                pipeline.execute(_to_feldera_dml(stmt))
            except Exception as e:  # noqa: BLE001
                # Tolerate inserts into tables trimmed from the schema (they are
                # not referenced by the query, so their data is irrelevant).
                print(
                    f"    insert skipped: {str(e).splitlines()[0][:90]}",
                    file=sys.stderr,
                )
        pipeline.pause()
        pipeline.wait_for_status(PipelineStatus.PAUSED, timeout=60)
        return list(pipeline.query(f'SELECT * FROM "{view_name}"'))
    finally:
        # Always remove the pipeline so they do not accumulate across tests —
        # even when compilation or startup failed above.
        _cleanup_pipeline(client, pipeline, pipeline_name)


def _cleanup_pipeline(client, pipeline, name) -> None:
    """Best-effort: stop, clear storage, and delete the pipeline; never raises.

    Feldera refuses to delete a pipeline until it is stopped and its storage is
    cleared, so we use pipeline.delete(clear_storage=True). When the pipeline
    object is unavailable (e.g. compilation failed), fetch a handle by name.
    """
    if pipeline is None:
        try:
            from feldera.pipeline import Pipeline

            pipeline = Pipeline.get(name, client)
        except Exception:  # noqa: BLE001 - nothing to clean up
            return
    if pipeline is None:
        return
    try:
        pipeline.stop(force=True)
    except Exception:  # noqa: BLE001 - cleanup must not mask the real result
        pass
    try:
        pipeline.delete(clear_storage=True)
    except Exception:  # noqa: BLE001
        pass


# ---------------------------------------------------------------------------
# Test collection + single-pass execution
# ---------------------------------------------------------------------------


def _collect_tests(
    subdir_filter: str | None,
    file_filter: str | None,
):
    tests = []
    subdirs = sorted(
        d
        for d in _FIXTURES_DIR.iterdir()
        if d.is_dir() and d.name not in _BEHAVIOR_DIRS
    )
    if subdir_filter:
        subdirs = [d for d in subdirs if d.name == subdir_filter]
    for subdir in subdirs:
        schema_files = sorted(subdir.glob("*.schema.sql")) + sorted(
            subdir.glob("*_schema.sql")
        )
        for schema_path in schema_files:
            if schema_path.name.endswith(".schema.sql"):
                name = schema_path.name[: -len(".schema.sql")]
                query_path = subdir / f"{name}.query.sql"
                data_path = subdir / f"{name}.data.sql"
                result_path = subdir / f"{name}.result"
            else:
                name = schema_path.name[: -len("_schema.sql")]
                query_path = subdir / f"{name}_query.sql"
                data_path = subdir / f"{name}_data.sql"
                result_path = subdir / f"{name}_result"
            if file_filter and name != file_filter:
                continue
            if not query_path.exists():
                continue
            tests.append(
                (subdir.name, name, schema_path, query_path, data_path, result_path)
            )
    return tests


def run_one_test(
    subdir,
    test_id,
    schema_path,
    query_path,
    data_path,
    result_path,
    config,
    url,
    profile,
) -> dict:
    """Translate, compile, run, and compare one test — the whole pipeline.

    Returns a JSON-serializable record describing the outcome and the artifacts.
    """
    from felderize.translator import translate_spark_to_feldera

    record: dict = {
        "subdir": subdir,
        "test_id": test_id,
        "outcome": "error",
        "detail": "",
    }

    schema_sql = schema_path.read_text() if schema_path.exists() else ""
    query_sql = query_path.read_text()

    try:
        result = translate_spark_to_feldera(
            schema_sql, query_sql, config, validate=True
        )
    except Exception as e:  # noqa: BLE001 - a transient LLM/network error must not abort the run
        record["detail"] = f"translation error: {e}"
        return record
    record["translation_status"] = result.status.value
    record["feldera_schema"] = result.feldera_schema
    record["feldera_query"] = result.feldera_query
    if result.unsupported:
        record["unsupported"] = result.unsupported

    if not result.feldera_query.strip():
        record["outcome"] = "skipped"
        record["detail"] = f"no query translated ({result.status.value})"
        return record

    view, is_local = _output_view(result.feldera_query)
    if not view:
        record["detail"] = "could not find the output view name"
        return record
    if is_local:
        # A LOCAL view is not observable, so its result cannot be compared. This
        # is a genuine Feldera limitation (e.g. an INTERVAL output column), not a
        # translation failure — record it as skipped rather than an error.
        record["outcome"] = "skipped"
        record["detail"] = (
            "final output is a LOCAL VIEW (not observable, e.g. INTERVAL output)"
        )
        record["view"] = view
        return record
    record["view"] = view

    feldera_sql = _normalize_feldera_sql(result.feldera_schema, result.feldera_query)
    data_sql = data_path.read_text() if data_path and data_path.exists() else ""
    expected = _parse_spark_result(
        result_path.read_text() if result_path.exists() else ""
    )
    record["expected_row_count"] = len(expected)

    try:
        rows = run_on_feldera(test_id, feldera_sql, view, data_sql, url, profile)
    except Exception as e:  # noqa: BLE001 - surface any Feldera error per test
        record["detail"] = str(e)
        return record

    record["feldera_row_count"] = len(rows)
    match, detail = _rows_match(rows, expected)
    record["outcome"] = "pass" if match else "fail"
    record["detail"] = detail
    return record


# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------

_SUMMARY_FIELDS = ("subdir", "test_id", "outcome", "detail", "translation_status")
_OUTCOMES = ("pass", "fail", "error", "skipped")

# ---------------------------------------------------------------------------
# Console output
# ---------------------------------------------------------------------------

_LABELS = {"pass": "PASS", "fail": "FAIL", "error": "ERROR", "skipped": "SKIP"}
_COLORS = {"pass": "32", "fail": "31", "error": "31", "skipped": "33"}  # ANSI


def _color(text: str, outcome: str, enabled: bool) -> str:
    code = _COLORS.get(outcome)
    return f"\033[{code}m{text}\033[0m" if (enabled and code) else text


def _short_detail(detail: str | None, width: int = 72) -> str:
    """Collapse a possibly multi-line detail to one trimmed line."""
    one = " ".join((detail or "").split())
    return one if len(one) <= width else one[: width - 1] + "…"


def _build_summary(records: list[dict], profile: str, url: str) -> dict:
    """Assemble a summary document from per-test records."""
    counts = {o: 0 for o in _OUTCOMES}
    for r in records:
        outcome = r.get("outcome")
        if outcome in counts:
            counts[outcome] += 1
    return {
        "total": len(records),
        "counts": counts,
        "profile": profile,
        "url": url,
        "results": [{k: r.get(k) for k in _SUMMARY_FIELDS} for r in records],
    }


def _merge_summaries(results_dir: Path, profile: str, url: str) -> dict:
    """Rebuild the canonical summary by scanning every per-test JSON on disk.

    The per-test JSON files are the source of truth, so this reconstructs a
    complete summary regardless of how many shards produced them. Records are
    sorted by (subdir, test_id) for a stable, diff-friendly ordering.
    """
    records: list[dict] = []
    for path in results_dir.glob("*/*.json"):
        try:
            records.append(json.loads(path.read_text()))
        except (OSError, json.JSONDecodeError):
            continue  # skip unreadable/partial files rather than abort the merge
    records.sort(key=lambda r: (r.get("subdir", ""), r.get("test_id", "")))
    return _build_summary(records, profile, url)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run Spark→Feldera tests end to end (translate + compile + run + compare)."
    )
    parser.add_argument(
        "--dir", metavar="SUBDIR", help="Only this subdirectory (e.g. cast)"
    )
    parser.add_argument("--file", metavar="NAME", help="Only this test (e.g. cast_001)")
    parser.add_argument(
        "--url", default=_DEFAULT_URL, help=f"Feldera API URL (default: {_DEFAULT_URL})"
    )
    parser.add_argument(
        "--profile",
        default="dev",
        choices=["dev", "unoptimized", "optimized"],
        help="Feldera compilation profile (default: dev — fastest to compile)",
    )
    parser.add_argument(
        "--results-dir",
        metavar="DIR",
        default=None,
        help="Write a JSON record per test (under DIR/<subdir>/) plus a summary "
        "(summary.json, or summary.shard-K-of-M.json under --shard)",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Skip tests already recorded under --results-dir (continue a prior run)",
    )
    parser.add_argument(
        "--shard",
        metavar="K/M",
        help="Run only shard K of M (0-based); for parallel runs across processes",
    )
    parser.add_argument(
        "--merge-summaries",
        action="store_true",
        help="Rebuild summary.json from the per-test JSONs under --results-dir, "
        "then exit (use after sharded runs complete)",
    )
    parser.add_argument(
        "--check-only", action="store_true", help="Only verify prerequisites, then exit"
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Show the translator/LLM/compiler chatter (suppressed by default)",
    )
    args = parser.parse_args()

    # Quiet the SDK's client/server version-mismatch warning (noise per pipeline).
    logging.getLogger("feldera").setLevel(logging.ERROR)

    # Merging only reads files already on disk — no Feldera, no prereq checks.
    if args.merge_summaries:
        if not args.results_dir:
            print("--merge-summaries requires --results-dir")
            sys.exit(2)
        results_dir = Path(args.results_dir).expanduser().resolve()
        summary = _merge_summaries(results_dir, args.profile, args.url)
        (results_dir / "summary.json").write_text(json.dumps(summary, indent=2))
        c = summary["counts"]
        print(f"Merged {summary['total']} test(s) into {results_dir}/summary.json")
        print(
            f"PASS: {c['pass']}  FAIL: {c['fail']}  "
            f"ERROR: {c['error']}  SKIPPED: {c['skipped']}"
        )
        return

    # felderize is importable when installed; fall back to the in-repo source.
    try:
        from felderize.config import Config
    except ModuleNotFoundError:
        sys.path.insert(0, str(_FELDERIZE_ROOT / "felderize"))
        from felderize.config import Config

    config = Config.from_env()

    if not check_prerequisites(config, args.url):
        print("Prerequisites not met — fix the items marked ✗ above and re-run.")
        sys.exit(2)
    if args.check_only:
        print("All prerequisites satisfied.")
        return

    tests = _collect_tests(args.dir, args.file)
    if not tests:
        print("No tests found.")
        sys.exit(1)

    # Sharding for parallel runs: each process takes a disjoint stride of tests.
    if args.shard:
        k, m = (int(x) for x in args.shard.split("/"))
        tests = tests[k::m]
        print(f"[shard {k}/{m}] handling {len(tests)} tests")

    results_dir = (
        Path(args.results_dir).expanduser().resolve() if args.results_dir else None
    )

    total = len(tests)
    counts = {"pass": 0, "fail": 0, "error": 0, "skipped": 0}
    records: list[dict] = []
    use_color = sys.stdout.isatty()
    idx_w = len(str(total))
    print(
        f"Running {total} test(s) [profile={args.profile}"
        + (f", results-dir={results_dir}" if results_dir else "")
        + "]...\n"
    )
    for i, (
        subdir,
        test_id,
        schema_path,
        query_path,
        data_path,
        result_path,
    ) in enumerate(tests, 1):
        prefix = f"[{i:>{idx_w}}/{total}] {subdir}/{test_id}"
        cached = results_dir / subdir / f"{test_id}.json" if results_dir else None
        if args.resume and cached and cached.is_file():
            try:
                res = json.loads(cached.read_text())
                counts[res["outcome"]] += 1
                records.append(res)
                badge = _color(_LABELS[res["outcome"]], res["outcome"], use_color)
                print(f"{prefix:<58} {badge}  (resumed)")
                continue
            except Exception:  # noqa: BLE001 - fall through and re-run if the cache is unreadable
                pass
        print(f"{prefix:<58} ", end="", flush=True)
        try:
            # The translator/LLM/compiler write progress to stderr; hide it unless
            # --verbose so the per-test result stays one clean line.
            stderr_sink = io.StringIO() if not args.verbose else sys.stderr
            with contextlib.redirect_stderr(stderr_sink):
                res = run_one_test(
                    subdir,
                    test_id,
                    schema_path,
                    query_path,
                    data_path,
                    result_path,
                    config,
                    args.url,
                    args.profile,
                )
        except Exception as e:  # noqa: BLE001 - backstop: never let one test abort the whole run
            res = {
                "subdir": subdir,
                "test_id": test_id,
                "outcome": "error",
                "detail": f"unexpected error: {e}",
            }
        counts[res["outcome"]] += 1
        records.append(res)
        if results_dir:
            out = results_dir / subdir
            out.mkdir(parents=True, exist_ok=True)
            (out / f"{test_id}.json").write_text(json.dumps(res, indent=2))
        badge = _color(f"{_LABELS[res['outcome']]:<5}", res["outcome"], use_color)
        detail = _short_detail(res.get("detail"))
        print(f"{badge}  {detail}" if detail else badge)

    print(f"\n{'=' * 60}")
    parts = [
        _color(f"PASS {counts['pass']}", "pass", use_color),
        _color(f"FAIL {counts['fail']}", "fail", use_color),
        _color(f"ERROR {counts['error']}", "error", use_color),
        _color(f"SKIP {counts['skipped']}", "skipped", use_color),
        f"TOTAL {total}",
    ]
    print("   ".join(parts))

    if results_dir:
        summary = _build_summary(records, args.profile, args.url)
        # A sharded run covers only its own stride, so each shard writes a
        # distinct file rather than clobbering the others' summary.json. Rebuild
        # the canonical summary.json with --merge-summaries once all shards finish.
        if args.shard:
            k, m = (int(x) for x in args.shard.split("/"))
            name = f"summary.shard-{k}-of-{m}.json"
        else:
            name = "summary.json"
        (results_dir / name).write_text(json.dumps(summary, indent=2))
        print(f"Results written to {results_dir}/ ({name})")
        if args.shard:
            print("Run with --merge-summaries to rebuild the full summary.json.")

    if counts["fail"] or counts["error"]:
        sys.exit(1)


if __name__ == "__main__":
    main()
