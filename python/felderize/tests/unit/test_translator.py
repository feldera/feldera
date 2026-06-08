"""Unit tests for the translator: helpers + the whole-program translate flow."""

from __future__ import annotations

import json

from felderize.config import Config
from felderize.llm import PromptTooLargeError
from felderize.models import Status
from felderize import translator
from felderize.translator import (
    split_combined_sql,
    trim_schema_to_referenced,
    validate_inputs,
    validate_query,
    validate_schema,
)


# ---------------------------------------------------------------------------
# trim_schema_to_referenced
# ---------------------------------------------------------------------------

_SCHEMA = """\
CREATE TABLE users (id BIGINT, name STRING);
CREATE TABLE orders (id BIGINT, user_id BIGINT, amount DECIMAL);
CREATE TABLE products (id BIGINT, sku STRING);
CREATE TABLE irrelevant (id BIGINT, junk STRING);
CREATE VIEW active_orders AS SELECT * FROM orders WHERE amount > 0;
"""

_QUERY = """\
CREATE VIEW result AS
SELECT u.name, o.amount
FROM users u JOIN active_orders o ON u.id = o.user_id
"""


def test_drops_unreferenced_tables():
    result = trim_schema_to_referenced(_SCHEMA, _QUERY)
    assert "irrelevant" not in result and "products" not in result


def test_keeps_directly_referenced_tables():
    assert "CREATE TABLE users" in trim_schema_to_referenced(_SCHEMA, _QUERY)


def test_keeps_tables_referenced_inside_schema_view():
    # orders is only used inside the active_orders view body, not the query
    assert "CREATE TABLE orders" in trim_schema_to_referenced(_SCHEMA, _QUERY)


def test_views_always_pass_through():
    assert "CREATE VIEW active_orders" in trim_schema_to_referenced(_SCHEMA, _QUERY)


def test_table_name_not_matched_as_substring():
    schema = "CREATE TABLE users (id BIGINT);\nCREATE TABLE user_prefs (id BIGINT);"
    result = trim_schema_to_referenced(schema, "SELECT * FROM user_prefs")
    assert "user_prefs" in result and "CREATE TABLE users" not in result


# ---------------------------------------------------------------------------
# validate_schema / validate_query / validate_inputs
# ---------------------------------------------------------------------------


def test_validate_schema_empty():
    assert any("empty" in e.lower() for e in validate_schema(""))


def test_validate_schema_no_create_table():
    assert any("CREATE TABLE" in e for e in validate_schema("SELECT 1"))


def test_validate_schema_valid():
    assert validate_schema("CREATE TABLE t (id BIGINT);") == []


def test_validate_query_empty():
    assert any("empty" in e.lower() for e in validate_query(""))


def test_validate_query_with_create_view():
    assert validate_query("CREATE VIEW v AS SELECT 1;") == []


def test_validate_query_with_bare_select():
    assert validate_query("SELECT * FROM t") == []


def test_validate_inputs_collects_both_errors():
    assert len(validate_inputs("", "")) == 2


# ---------------------------------------------------------------------------
# split_combined_sql
# ---------------------------------------------------------------------------


def test_split_tables_and_views():
    schema, query = split_combined_sql(
        "CREATE TABLE t (id BIGINT);\nCREATE VIEW v AS SELECT id FROM t;"
    )
    assert "CREATE TABLE t" in schema and "CREATE VIEW" not in schema
    assert "CREATE VIEW v" in query and "CREATE TABLE" not in query


def test_split_or_replace_temp_view_to_query():
    _, query = split_combined_sql("CREATE OR REPLACE TEMP VIEW v AS SELECT 1;")
    assert "VIEW v" in query


def test_split_unrecognised_goes_to_schema():
    schema, _ = split_combined_sql("SET timezone = 'UTC';\nCREATE TABLE t (id BIGINT);")
    assert "SET timezone" in schema and "CREATE TABLE t" in schema


# ---------------------------------------------------------------------------
# translate_spark_to_feldera — whole-program, LLM-only / optional validate
# ---------------------------------------------------------------------------


class _FakeClient:
    def __init__(self, responder):
        self.calls = 0
        self._responder = responder

    def translate(self, system_prompt, user_prompt):
        self.calls += 1
        return self._responder(self.calls, user_prompt)


def _cfg():
    return Config(model="m", api_key="x", feldera_compiler="/fake/compiler")


def _json(**kw):
    base = {"feldera_schema": "", "feldera_query": "", "unsupported": []}
    base.update(kw)
    return json.dumps(base)


def test_translate_llm_only_no_validate(monkeypatch):
    """validate=False → one LLM call, parsed result, no compiler involvement."""
    client = _FakeClient(
        lambda n, p: _json(
            feldera_schema="CREATE TABLE t (id BIGINT);",
            feldera_query="CREATE VIEW v AS SELECT id FROM t;",
        )
    )
    monkeypatch.setattr(translator, "create_client", lambda c, verbose=False: client)
    # Guard: the compiler must not be touched when validate is off.
    monkeypatch.setattr(
        translator,
        "validate_sql",
        lambda *a, **k: (_ for _ in ()).throw(AssertionError("compiler used")),
    )
    r = translator.translate_spark_to_feldera(
        "CREATE TABLE t (id BIGINT);", "CREATE VIEW v AS SELECT id FROM t;", _cfg()
    )
    assert r.status == Status.SUCCESS
    assert "CREATE VIEW v" in r.feldera_query
    assert client.calls == 1


def test_translate_flags_unsupported(monkeypatch):
    client = _FakeClient(
        lambda n, p: _json(
            feldera_query="CREATE VIEW v AS SELECT CAST(NULL AS INT) AS x;",
            unsupported=["reverse(s)"],
        )
    )
    monkeypatch.setattr(translator, "create_client", lambda c, verbose=False: client)
    r = translator.translate_spark_to_feldera("CREATE TABLE t (id INT);", "Q", _cfg())
    assert r.status == Status.UNSUPPORTED and r.unsupported == ["reverse(s)"]


def test_translate_validate_repairs_then_succeeds(monkeypatch):
    """validate=True: first SQL fails to compile, repair fixes it."""

    def responder(n, p):
        if "compiler errors" in p:  # repair prompt
            return _json(feldera_query="CREATE VIEW v AS SELECT 1;")
        return _json(feldera_query="CREATE VIEW v AS SELECT BROKEN;")

    client = _FakeClient(responder)
    monkeypatch.setattr(translator, "create_client", lambda c, verbose=False: client)
    monkeypatch.setattr(
        translator,
        "validate_sql",
        lambda sql, path=None: ["error"] if "BROKEN" in sql else [],
    )
    r = translator.translate_spark_to_feldera(
        "CREATE TABLE t (id INT);", "Q", _cfg(), validate=True, include_docs=False
    )
    assert r.status == Status.SUCCESS and "BROKEN" not in r.feldera_query


def test_translate_validate_errors_after_retries(monkeypatch):
    client = _FakeClient(
        lambda n, p: _json(feldera_query="CREATE VIEW v AS SELECT BROKEN;")
    )
    monkeypatch.setattr(translator, "create_client", lambda c, verbose=False: client)
    monkeypatch.setattr(
        translator, "validate_sql", lambda sql, path=None: ["compile error"]
    )
    r = translator.translate_spark_to_feldera(
        "CREATE TABLE t (id INT);",
        "Q",
        _cfg(),
        validate=True,
        include_docs=False,
        max_retries=2,
    )
    assert r.status == Status.ERROR


def test_translate_invalid_json_is_error(monkeypatch):
    client = _FakeClient(lambda n, p: "not json")
    monkeypatch.setattr(translator, "create_client", lambda c, verbose=False: client)
    r = translator.translate_spark_to_feldera("CREATE TABLE t (id INT);", "Q", _cfg())
    assert r.status == Status.ERROR


def test_parse_response_strips_markdown_fence():
    out = translator._parse_response('```json\n{"feldera_query": "Q"}\n```')
    assert out["feldera_query"] == "Q"


def test_as_str_joins_list():
    assert translator._as_str(["a", "b"]) == "a\nb"
    assert translator._as_str("x") == "x"
    assert translator._as_str(None) == ""


def test_as_list_normalizes():
    assert translator._as_list(["a", "b"]) == ["a", "b"]
    assert translator._as_list("x") == ["x"]
    assert translator._as_list("") == []


def test_salvage_sql_from_fence():
    raw = "Here is the translation:\n```sql\nCREATE VIEW v AS SELECT 1;\n```\nDone."
    schema, query = translator._salvage_sql(raw)
    assert "CREATE VIEW v AS SELECT 1;" in query
    assert schema == ""


def test_salvage_sql_splits_schema_and_query():
    raw = "CREATE TABLE t (id INT);\nCREATE VIEW v AS SELECT id FROM t;"
    schema, query = translator._salvage_sql(raw)
    assert "CREATE TABLE t" in schema
    assert "CREATE VIEW v" in query


def test_salvage_sql_no_sql_returns_empty():
    assert translator._salvage_sql("I cannot translate this, sorry.") == ("", "")


def test_salvage_sql_rejects_reasoning_prose():
    """Prose that merely mentions CREATE VIEW must not be scraped in as SQL.

    Regression: the model's chain-of-thought leaked into the query, e.g.
    "CREATE VIEW statement from the previous attempt's reasoning ...".
    """
    prose = (
        "CREATE VIEW statement from the previous attempt's reasoning text "
        "leaking into the SQL. I need to produce clean, valid Feldera SQL."
    )
    assert translator._salvage_sql(prose) == ("", "")


def test_salvage_sql_rejects_prose_with_backtick_artifact():
    prose = (
        "CREATE VIEW` — must use `CREATE LOCAL VIEW`\n\nThe interval value maps to X."
    )
    assert translator._salvage_sql(prose) == ("", "")


def test_salvage_sql_requires_as_after_view():
    """A VIEW without an AS clause is not a usable statement."""
    assert translator._salvage_sql("CREATE VIEW v;") == ("", "")


# ---------------------------------------------------------------------------
# _normalize_escapes — undo doubly-escaped responses
# ---------------------------------------------------------------------------


def test_normalize_escapes_restores_collapsed_newlines():
    raw = r"CREATE VIEW v AS\nSELECT CAST(NULL AS VARCHAR);"
    out = translator._normalize_escapes(raw)
    assert "\n" in out
    assert "\\n" not in out
    assert out == "CREATE VIEW v AS\nSELECT CAST(NULL AS VARCHAR);"


def test_normalize_escapes_restores_escaped_quotes():
    raw = r"CREATE VIEW \"my-view\" AS\nSELECT 1;"
    out = translator._normalize_escapes(raw)
    assert '\\"' not in out
    assert '"my-view"' in out


def test_normalize_escapes_leaves_real_multiline_untouched():
    raw = "CREATE VIEW v AS\nSELECT 1;"
    assert translator._normalize_escapes(raw) == raw


def test_normalize_escapes_leaves_plain_sql_untouched():
    raw = "CREATE VIEW v AS SELECT 1;"
    assert translator._normalize_escapes(raw) == raw


def test_build_result_normalizes_escaped_query():
    data = {"feldera_query": r"CREATE VIEW v AS\nSELECT 1;", "feldera_schema": ""}
    result = translator._build_result(data)
    assert "\n" in result.feldera_query
    assert "\\n" not in result.feldera_query


def test_parse_or_salvage_clean_json():
    data, warns = translator._parse_or_salvage(_json(feldera_query="Q"))
    assert data["feldera_query"] == "Q"
    assert warns == []


def test_parse_or_salvage_recovers_sql_from_prose():
    data, warns = translator._parse_or_salvage(
        "Sure!\n```sql\nCREATE VIEW v AS SELECT 1;\n```"
    )
    assert "CREATE VIEW v" in data["feldera_query"]
    assert warns and "salvaged" in warns[0].lower()


def test_parse_or_salvage_unrecoverable():
    data, warns = translator._parse_or_salvage("total nonsense, no sql")
    assert data == {}
    assert warns and "no SQL could be salvaged" in warns[0]


def test_translate_salvages_when_response_not_json(monkeypatch):
    """A non-JSON response with SQL still yields a best-effort query, never empty."""
    client = _FakeClient(
        lambda n, p: "Here you go:\n```sql\nCREATE VIEW v AS SELECT 1 AS x;\n```"
    )
    monkeypatch.setattr(translator, "create_client", lambda c, verbose=False: client)
    monkeypatch.setattr(translator, "validate_sql", lambda sql, path=None: [])
    r = translator.translate_spark_to_feldera(
        "CREATE TABLE t (id INT);", "Q", _cfg(), validate=True, include_docs=False
    )
    assert "CREATE VIEW v" in r.feldera_query
    assert any("salvaged" in w.lower() for w in r.warnings)


def test_translate_force_query_retry_after_empty(monkeypatch):
    """An empty first query triggers a force-retry that fills it in."""

    def responder(n, p):
        if n == 1:
            return _json(feldera_query="", unsupported=["udf()"])
        return _json(feldera_query="CREATE VIEW v AS SELECT CAST(NULL AS INT) AS x;")

    client = _FakeClient(responder)
    monkeypatch.setattr(translator, "create_client", lambda c, verbose=False: client)
    r = translator.translate_spark_to_feldera("CREATE TABLE t (id INT);", "Q", _cfg())
    assert "CREATE VIEW v" in r.feldera_query
    assert any("force-retry" in w for w in r.warnings)
    assert client.calls == 2


def test_translate_compiler_not_found_warns(monkeypatch):
    client = _FakeClient(lambda n, p: _json(feldera_query="CREATE VIEW v AS SELECT 1;"))
    monkeypatch.setattr(translator, "create_client", lambda c, verbose=False: client)
    monkeypatch.setattr(
        translator, "validate_sql", lambda sql, path=None: ["Compiler not found: x"]
    )
    r = translator.translate_spark_to_feldera(
        "CREATE TABLE t (id INT);", "Q", _cfg(), validate=True, include_docs=False
    )
    assert r.status == Status.SUCCESS
    assert any("Compiler not found" in w for w in r.warnings)


def test_translate_schema_only_is_unsupported(monkeypatch):
    """No view generated under validate → flagged unsupported, compiler untouched."""
    client = _FakeClient(
        lambda n, p: _json(feldera_schema="CREATE TABLE t (id INT);", feldera_query="")
    )
    monkeypatch.setattr(translator, "create_client", lambda c, verbose=False: client)
    monkeypatch.setattr(
        translator,
        "validate_sql",
        lambda *a, **k: (_ for _ in ()).throw(AssertionError("compiler used")),
    )
    r = translator.translate_spark_to_feldera(
        "CREATE TABLE t (id INT);", "Q", _cfg(), validate=True, include_docs=False
    )
    assert r.status == Status.UNSUPPORTED
    assert any("incomplete" in u.lower() for u in r.unsupported)


def test_translate_docs_pass_resolves(monkeypatch):
    """First pass unsupported, docs pass returns a clean result."""

    def responder(n, p):
        if n == 1:
            return _json(
                feldera_query="CREATE VIEW v AS SELECT CAST(NULL AS INT) AS x;",
                unsupported=["udf()"],
            )
        return _json(feldera_query="CREATE VIEW v AS SELECT 1 AS x;")

    client = _FakeClient(responder)
    monkeypatch.setattr(translator, "create_client", lambda c, verbose=False: client)
    monkeypatch.setattr(translator, "validate_sql", lambda sql, path=None: [])
    r = translator.translate_spark_to_feldera(
        "CREATE TABLE t (id INT);", "Q", _cfg(), validate=True, include_docs=True
    )
    assert r.status == Status.SUCCESS
    assert any("docs retry" in w.lower() for w in r.warnings)


def test_translate_repair_invalid_json_warns(monkeypatch):
    """A repair attempt that returns junk is recorded but does not crash."""

    def responder(n, p):
        if "compiler errors" in p:
            return "not json"
        return _json(feldera_query="CREATE VIEW v AS SELECT BROKEN;")

    client = _FakeClient(responder)
    monkeypatch.setattr(translator, "create_client", lambda c, verbose=False: client)
    monkeypatch.setattr(translator, "validate_sql", lambda sql, path=None: ["err"])
    r = translator.translate_spark_to_feldera(
        "CREATE TABLE t (id INT);",
        "Q",
        _cfg(),
        validate=True,
        include_docs=False,
        max_retries=1,
    )
    assert r.status == Status.ERROR
    assert any("Repair attempt" in w and "parse" in w.lower() for w in r.warnings)


def _raise_too_large(*_a):
    raise PromptTooLargeError("prompt is too long: 250000 tokens > 200000 maximum")


def test_translate_too_large_returns_shorten_message(monkeypatch):
    """A context-window overflow on the first pass yields a clear ERROR, not a crash."""
    client = _FakeClient(_raise_too_large)
    monkeypatch.setattr(translator, "create_client", lambda c, verbose=False: client)
    r = translator.translate_spark_to_feldera("CREATE TABLE t (id INT);", "Q", _cfg())
    assert r.status == Status.ERROR
    assert len(r.warnings) == 1
    assert "too large" in r.warnings[0].lower()
    assert "shorten" in r.warnings[0].lower()


def test_translate_docs_retry_too_large_keeps_first_pass(monkeypatch):
    """If only the (larger) docs retry overflows, keep the first-pass result."""

    def responder(n, p):
        if n == 1:  # first pass: a usable but unsupported result
            return _json(
                feldera_query="CREATE VIEW v AS SELECT CAST(NULL AS INT) AS x;",
                unsupported=["reverse(s)"],
            )
        raise PromptTooLargeError("prompt is too long: 250000 tokens > 200000 maximum")

    client = _FakeClient(responder)
    monkeypatch.setattr(translator, "create_client", lambda c, verbose=False: client)
    r = translator.translate_spark_to_feldera(
        "CREATE TABLE t (id INT);", "Q", _cfg(), include_docs=True
    )
    assert r.status == Status.UNSUPPORTED
    assert r.unsupported == ["reverse(s)"]
    assert any("docs retry skipped" in w.lower() for w in r.warnings)
