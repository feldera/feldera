"""Unit tests for the prompt builders (template loading + formatting)."""

from __future__ import annotations

from felderize import prompts


def test_build_user_prompt_embeds_schema_and_query():
    out = prompts.build_user_prompt(
        "  CREATE TABLE t (id INT);  ", "  CREATE VIEW v AS SELECT id FROM t;  "
    )
    assert "CREATE TABLE t (id INT);" in out
    assert "CREATE VIEW v AS SELECT id FROM t;" in out
    # Inputs are stripped before formatting.
    assert "  CREATE TABLE" not in out


def test_build_force_query_prompt_lists_unsupported():
    out = prompts.build_force_query_prompt(
        "CREATE TABLE t (id INT);",
        "CREATE VIEW v AS SELECT reverse(s) FROM t;",
        ["reverse(s)", "udf_x()"],
    )
    assert "- reverse(s)" in out
    assert "- udf_x()" in out


def test_build_repair_prompt_lists_errors_and_prior_sql():
    out = prompts.build_repair_prompt(
        "CREATE TABLE t (id INT);",
        "CREATE VIEW v AS SELECT id FROM t;",
        "CREATE VIEW v AS SELECT BROKEN;",
        ["error: BROKEN is not defined", "line 1: parse error"],
    )
    assert "- error: BROKEN is not defined" in out
    assert "- line 1: parse error" in out
    assert "CREATE VIEW v AS SELECT BROKEN;" in out
