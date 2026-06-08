"""Unit tests for the e2e runner's summary helpers.

These cover the shard-safe summary writing: `_build_summary` rolls per-test
records into counts, and `_merge_summaries` reconstructs a complete summary from
the per-test JSON files left on disk by independent shard processes.

`run_tests.py` lives under tests/e2e (not in the felderize package), so it is
loaded by path. Its top-level imports are stdlib only, so importing is hermetic.
"""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path

_RUN_TESTS = Path(__file__).resolve().parents[1] / "e2e" / "run_tests.py"
_spec = importlib.util.spec_from_file_location("e2e_run_tests", _RUN_TESTS)
run_tests = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(run_tests)


def _record(subdir: str, test_id: str, outcome: str, **extra) -> dict:
    return {"subdir": subdir, "test_id": test_id, "outcome": outcome, **extra}


# ---------------------------------------------------------------------------
# _build_summary
# ---------------------------------------------------------------------------


def test_build_summary_counts_every_outcome():
    records = [
        _record("cast", "cast_001", "pass"),
        _record("cast", "cast_002", "fail", detail="1 row differs"),
        _record("window", "window_001", "error"),
        _record("union", "union_001", "skipped"),
    ]
    summary = run_tests._build_summary(records, "dev", "http://localhost:8080")
    assert summary["total"] == 4
    assert summary["counts"] == {"pass": 1, "fail": 1, "error": 1, "skipped": 1}
    assert summary["profile"] == "dev"
    assert summary["url"] == "http://localhost:8080"


def test_build_summary_keeps_only_summary_fields():
    records = [
        _record(
            "cast",
            "cast_001",
            "pass",
            detail="ok",
            translation_status="success",
            secret="dropme",
        )
    ]
    summary = run_tests._build_summary(records, "dev", "url")
    assert summary["results"][0] == {
        "subdir": "cast",
        "test_id": "cast_001",
        "outcome": "pass",
        "detail": "ok",
        "translation_status": "success",
    }
    assert "secret" not in summary["results"][0]


def test_build_summary_ignores_unknown_outcome():
    """A malformed/missing outcome must not crash counting or inflate totals."""
    records = [_record("cast", "cast_001", "bogus"), _record("cast", "cast_002", None)]
    summary = run_tests._build_summary(records, "dev", "url")
    assert summary["counts"] == {"pass": 0, "fail": 0, "error": 0, "skipped": 0}
    assert summary["total"] == 2  # still reported as attempted


def test_build_summary_empty():
    summary = run_tests._build_summary([], "dev", "url")
    assert summary["total"] == 0
    assert summary["counts"] == {"pass": 0, "fail": 0, "error": 0, "skipped": 0}
    assert summary["results"] == []


# ---------------------------------------------------------------------------
# _merge_summaries
# ---------------------------------------------------------------------------


def _write_test_json(results_dir: Path, subdir: str, test_id: str, rec: dict) -> None:
    out = results_dir / subdir
    out.mkdir(parents=True, exist_ok=True)
    (out / f"{test_id}.json").write_text(json.dumps(rec))


def test_merge_summaries_aggregates_all_shards(tmp_path):
    """Per-test JSONs from several shards merge into one complete summary."""
    # Shard 0 wrote these; shard 1 wrote the others — different dirs, same root.
    _write_test_json(tmp_path, "cast", "cast_001", _record("cast", "cast_001", "pass"))
    _write_test_json(tmp_path, "cast", "cast_002", _record("cast", "cast_002", "fail"))
    _write_test_json(
        tmp_path, "window", "window_001", _record("window", "window_001", "error")
    )

    summary = run_tests._merge_summaries(tmp_path, "dev", "url")
    assert summary["total"] == 3
    assert summary["counts"] == {"pass": 1, "fail": 1, "error": 1, "skipped": 0}


def test_merge_summaries_sorts_records_stably(tmp_path):
    _write_test_json(
        tmp_path, "window", "window_002", _record("window", "window_002", "pass")
    )
    _write_test_json(tmp_path, "cast", "cast_010", _record("cast", "cast_010", "pass"))
    _write_test_json(tmp_path, "cast", "cast_002", _record("cast", "cast_002", "pass"))

    summary = run_tests._merge_summaries(tmp_path, "dev", "url")
    keys = [(r["subdir"], r["test_id"]) for r in summary["results"]]
    assert keys == [
        ("cast", "cast_002"),
        ("cast", "cast_010"),
        ("window", "window_002"),
    ]


def test_merge_summaries_skips_unreadable_json(tmp_path):
    """A partially written/corrupt file must not abort the merge."""
    _write_test_json(tmp_path, "cast", "cast_001", _record("cast", "cast_001", "pass"))
    bad = tmp_path / "cast" / "cast_002.json"
    bad.write_text("{not valid json")

    summary = run_tests._merge_summaries(tmp_path, "dev", "url")
    assert summary["total"] == 1
    assert summary["counts"]["pass"] == 1


def test_merge_summaries_ignores_top_level_summary_file(tmp_path):
    """summary.json sits at the root, not under */ — the glob must skip it."""
    _write_test_json(tmp_path, "cast", "cast_001", _record("cast", "cast_001", "pass"))
    (tmp_path / "summary.json").write_text(json.dumps({"total": 999}))

    summary = run_tests._merge_summaries(tmp_path, "dev", "url")
    assert summary["total"] == 1


def test_merge_summaries_empty_dir(tmp_path):
    summary = run_tests._merge_summaries(tmp_path, "dev", "url")
    assert summary["total"] == 0
    assert summary["results"] == []


# ---------------------------------------------------------------------------
# _output_view — final view name + LOCAL detection
# ---------------------------------------------------------------------------


def test_output_view_plain():
    assert run_tests._output_view("CREATE VIEW v AS SELECT 1;") == ("v", False)


def test_output_view_local_is_flagged():
    name, is_local = run_tests._output_view("CREATE LOCAL VIEW cast_009 AS SELECT 1;")
    assert (name, is_local) == ("cast_009", True)


def test_output_view_materialized_and_temp_not_local():
    assert run_tests._output_view("CREATE MATERIALIZED VIEW m AS SELECT 1;") == (
        "m",
        False,
    )
    assert run_tests._output_view("CREATE TEMP VIEW t AS SELECT 1;") == ("t", False)


def test_output_view_takes_last_view():
    sql = "CREATE VIEW a AS SELECT 1;\nCREATE LOCAL VIEW b AS SELECT 2;"
    assert run_tests._output_view(sql) == ("b", True)
    sql2 = "CREATE LOCAL VIEW a AS SELECT 1;\nCREATE VIEW b AS SELECT 2;"
    assert run_tests._output_view(sql2) == ("b", False)


def test_output_view_strips_quotes_and_skips_comments():
    sql = '-- a comment\nCREATE VIEW "my-view" AS SELECT 1;'
    assert run_tests._output_view(sql) == ("my-view", False)


def test_output_view_none_when_absent():
    assert run_tests._output_view("SELECT 1;") == (None, False)


def test_extract_view_name_delegates():
    assert run_tests._extract_view_name("CREATE LOCAL VIEW v AS SELECT 1;") == "v"


# ---------------------------------------------------------------------------
# _normalize_feldera_sql — materialize all but LOCAL views
# ---------------------------------------------------------------------------


def test_normalize_materializes_plain_view():
    out = run_tests._normalize_feldera_sql("", "CREATE VIEW v AS SELECT 1;")
    assert "CREATE MATERIALIZED VIEW v" in out


def test_normalize_materializes_temp_view():
    out = run_tests._normalize_feldera_sql("", "CREATE TEMP VIEW v AS SELECT 1;")
    assert "CREATE MATERIALIZED VIEW v" in out


def test_normalize_leaves_local_view_alone():
    """A LOCAL view must stay LOCAL — forcing MATERIALIZED breaks compilation
    (e.g. an INTERVAL output column is illegal in a connector-exposed view)."""
    out = run_tests._normalize_feldera_sql("", "CREATE LOCAL VIEW v AS SELECT 1;")
    assert "CREATE LOCAL VIEW v" in out
    assert "MATERIALIZED" not in out
