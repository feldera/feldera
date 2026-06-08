from __future__ import annotations

import json
import re
import sys
from pathlib import Path

import sqlparse

from felderize.config import Config
from felderize.constants import DEFAULT_MAX_RETRIES
from felderize.feldera_client import validate_sql
from felderize.llm import LLMClient, PromptTooLargeError, create_client
from felderize.models import Status, TranslationResult
from felderize.prompts import (
    build_force_query_prompt,
    build_repair_prompt,
    build_user_prompt,
)
from felderize.skills import build_system_prompt


_CREATE_TABLE_RE = re.compile(r"\bCREATE\s+TABLE\b", re.IGNORECASE)
_CREATE_VIEW_RE = re.compile(
    r"\bCREATE\s+(OR\s+REPLACE\s+)?(TEMP(ORARY)?\s+)?VIEW\b", re.IGNORECASE
)
_SELECT_RE = re.compile(r"\bSELECT\b", re.IGNORECASE)


def trim_schema_to_referenced(schema_sql: str, query_sql: str) -> str:
    """Return schema_sql with unreferenced CREATE TABLE statements removed.

    Builds a reference set from the query SQL and the SELECT bodies of any
    CREATE VIEW statements in the schema. CREATE TABLE definitions themselves
    are excluded from the scan so a table's own name doesn't count as a
    self-reference. Non-table statements (views) always pass through.

    This reduces LLM prompt size when a schema has many shared tables that the
    specific query doesn't use.
    """
    stmts = [s.strip() for s in sqlparse.split(schema_sql) if s.strip()]

    scan_parts: list[str] = [query_sql]
    for stmt in stmts:
        s = stmt.rstrip(";")
        vm = re.match(
            r"CREATE\s+(?:OR\s+REPLACE\s+)?(?:TEMP(?:ORARY)?\s+)?VIEW\s+\S+"
            r"(?:\s*\([^)]*\))?"  # optional column list
            r"(?:\s|--[^\n]*\n)*"  # optional whitespace and line comments
            r"AS\s+(.*)",
            s,
            re.IGNORECASE | re.DOTALL,
        )
        if vm:
            scan_parts.append(vm.group(1))

    ref_tokens: set[str] = set(
        re.findall(r"\b[a-z_]\w*\b", "\n".join(scan_parts).lower())
    )

    kept: list[str] = []
    for stmt in stmts:
        s = stmt.rstrip(";")
        m = re.match(
            r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(\w+)", s, re.IGNORECASE
        )
        if m:
            if m.group(1).lower() in ref_tokens:
                kept.append(stmt if stmt.endswith(";") else stmt + ";")
        else:
            kept.append(stmt if stmt.endswith(";") else stmt + ";")

    return "\n\n".join(kept)


def validate_schema(schema_sql: str) -> list[str]:
    """Return validation errors for the schema SQL, empty if valid."""
    if not schema_sql.strip():
        return ["Schema file is empty."]
    if not _CREATE_TABLE_RE.search(schema_sql):
        return [
            "Schema file does not contain any CREATE TABLE statement. "
            "Expected a SQL schema, not arbitrary text."
        ]
    return []


def validate_query(query_sql: str) -> list[str]:
    """Return validation errors for the query SQL, empty if valid."""
    if not query_sql.strip():
        return ["Query file is empty."]
    if not _CREATE_VIEW_RE.search(query_sql) and not _SELECT_RE.search(query_sql):
        return [
            "Query file does not contain any CREATE VIEW or SELECT statement. "
            "Expected a SQL query, not arbitrary text."
        ]
    return []


def validate_inputs(schema_sql: str, query_sql: str) -> list[str]:
    """Return validation errors for both schema and query SQL."""
    return validate_schema(schema_sql) + validate_query(query_sql)


def split_combined_sql(sql: str) -> tuple[str, str]:
    """Split a combined SQL file into (schema_sql, query_sql).

    Schema: CREATE TABLE statements. Query: CREATE [OR REPLACE] [TEMP] VIEW
    statements. Unrecognised statements are placed in schema_sql.
    """
    raw_stmts = [s.strip().rstrip(";") for s in sqlparse.split(sql) if s.strip()]

    schema_parts: list[str] = []
    query_parts: list[str] = []

    for stripped in raw_stmts:
        if not stripped:
            continue
        first_kw = next(
            (
                ln.strip()
                for ln in stripped.splitlines()
                if ln.strip() and not ln.strip().startswith("--")
            ),
            "",
        ).upper()
        if not first_kw:
            continue
        if _CREATE_VIEW_RE.match(first_kw):
            query_parts.append(stripped + ";")
        else:
            schema_parts.append(stripped + ";")

    return "\n\n".join(schema_parts), "\n\n".join(query_parts)


# ---------------------------------------------------------------------------
# LLM response parsing
# ---------------------------------------------------------------------------


def _parse_response(raw: str) -> dict:
    """Extract JSON from an LLM response, handling markdown fences."""
    text = raw.strip()
    match = re.search(r"```(?:json)?\s*\n(.*?)```", text, re.DOTALL)
    if match:
        text = match.group(1).strip()
    return json.loads(text)


_SQL_FENCE_RE = re.compile(r"```(?:sql)?\s*\n(.*?)```", re.DOTALL | re.IGNORECASE)

# A salvageable statement must be a *structured* CREATE — a VIEW followed by AS,
# or a TABLE followed by its column list — not merely the word "CREATE". The
# strict shape stops the model's reasoning prose (e.g. "CREATE VIEW statement
# from the previous attempt's reasoning...") from being scraped in as SQL.
_CREATE_STMT_RE = re.compile(
    r"""\bCREATE\s+(?:OR\s+REPLACE\s+)?
        (?:(?:LOCAL|MATERIALIZED|TEMP(?:ORARY)?)\s+)?
        (?:VIEW\s+[\w`".-]+\s+AS\b | TABLE\s+[\w`".-]+\s*\()
        .*?;""",
    re.DOTALL | re.IGNORECASE | re.VERBOSE,
)


def _salvage_sql(raw: str) -> tuple[str, str]:
    """Best-effort extraction of (schema_sql, query_sql) from a non-JSON response.

    Some responses come back as prose or bare SQL instead of the requested JSON
    (or as truncated JSON). Rather than give up and return nothing, pull whatever
    SQL we can: prefer ```sql fences, else scan for well-formed CREATE statements,
    then split them into schema (CREATE TABLE) vs query (CREATE VIEW). Returns
    ("", "") when no SQL-looking statements are present.
    """
    fences = _SQL_FENCE_RE.findall(raw)
    text = "\n".join(f.strip() for f in fences) if fences else raw
    statements = _CREATE_STMT_RE.findall(text)
    if not statements:
        return "", ""
    return split_combined_sql("\n".join(statements))


def _parse_or_salvage(raw: str) -> tuple[dict, list[str]]:
    """Parse the JSON response, or salvage best-effort SQL when that fails.

    Returns (data, warnings). On a clean parse, warnings is empty. On failure,
    salvaged SQL (if any) is returned as a result dict so the caller always has
    something to validate/repair rather than an empty translation.
    """
    try:
        return _parse_response(raw), []
    except (json.JSONDecodeError, KeyError):
        schema_sql, query_sql = _salvage_sql(raw)
        if schema_sql.strip() or query_sql.strip():
            return (
                {
                    "feldera_schema": schema_sql,
                    "feldera_query": query_sql,
                    "unsupported": [],
                },
                ["LLM response was not valid JSON — salvaged SQL from the raw text"],
            )
        return {}, ["Failed to parse LLM response and no SQL could be salvaged"]


def _as_str(val: object) -> str:
    """Normalize a value to string — handles lists from LLM responses."""
    if isinstance(val, list):
        return "\n".join(str(v) for v in val)
    return str(val) if val else ""


def _as_list(val: object) -> list[str]:
    if isinstance(val, list):
        return [str(v) for v in val]
    if isinstance(val, str) and val:
        return [val]
    return []


def _normalize_escapes(sql: str) -> str:
    """Undo a doubly-escaped LLM response.

    Some responses arrive escaped a second time, so the SQL body holds the
    literal two-character sequences ``\\n`` and ``\\"`` instead of real newlines
    and quotes; the compiler then fails on the stray backslash. When the text
    carries no real newline yet contains a literal ``\\n``, the whole statement
    has collapsed onto one line — treat it as doubly escaped and restore the
    intended characters. Genuinely multi-line SQL is left untouched.
    """
    if "\n" in sql or "\\n" not in sql:
        return sql
    return (
        sql.replace("\\n", "\n")
        .replace("\\t", "\t")
        .replace("\\r", "\r")
        .replace('\\"', '"')
    )


def _build_result(data: dict) -> TranslationResult:
    unsupported = _as_list(data.get("unsupported", []))
    return TranslationResult(
        feldera_schema=_normalize_escapes(_as_str(data.get("feldera_schema", ""))),
        feldera_query=_normalize_escapes(_as_str(data.get("feldera_query", ""))),
        unsupported=unsupported,
        warnings=_as_list(data.get("warnings", [])),
        explanations=_as_list(data.get("explanations", [])),
        status=Status.UNSUPPORTED if unsupported else Status.SUCCESS,
    )


def _too_large_message(detail: str) -> str:
    """User-facing guidance for a program that overflows the model's context window."""
    return (
        "The program is too large to fit the model's context window "
        f"({detail}). Shorten the input — translate fewer views at a time, drop "
        "unused tables, or split it into smaller files — or set a model with a "
        "larger context window via FELDERIZE_MODEL / --model."
    )


def _log_verbose_sql(full_sql: str, attempt: int) -> None:
    print(f"\n--- SQL submitted to validator (attempt {attempt}) ---", file=sys.stderr)
    print(full_sql, file=sys.stderr)
    print("---", file=sys.stderr)


# ---------------------------------------------------------------------------
# Translation: one LLM call on the whole program, with optional compiler repair
# ---------------------------------------------------------------------------


def _translate_with_repair(
    schema_sql: str,
    query_sql: str,
    config: Config,
    client: LLMClient,
    system_prompt: str,
    validate: bool,
    max_retries: int,
    verbose: bool = False,
) -> TranslationResult:
    """Translate the whole program with one LLM call, then optionally validate + repair."""
    raw = client.translate(system_prompt, build_user_prompt(schema_sql, query_sql))
    data, parse_warnings = _parse_or_salvage(raw)
    result = _build_result(data)
    result.warnings.extend(parse_warnings)

    # If the query came back empty, force one retry with NULL placeholders.
    if not result.feldera_query.strip():
        raw2 = client.translate(
            system_prompt,
            build_force_query_prompt(schema_sql, query_sql, result.unsupported),
        )
        data2, warn2 = _parse_or_salvage(raw2)
        result2 = _build_result(data2)
        if result2.feldera_query.strip():
            result = result2
            result.warnings.extend(warn2)
            result.warnings.append(
                "Query produced on force-retry after empty first response"
            )

    # A response we could not parse or salvage is a genuine failure, not a clean
    # translation — mark it so callers don't treat an empty query as success.
    if parse_warnings and not result.feldera_query.strip():
        result.status = Status.ERROR

    if not validate:
        return result

    full_sql = result.feldera_schema + "\n\n" + result.feldera_query
    # Schema-only or empty SQL always compiles, which is not a meaningful success.
    if not full_sql.strip() or not result.feldera_query.strip():
        if not result.unsupported:
            result.status = Status.UNSUPPORTED
            result.unsupported = ["No query generated — translation incomplete"]
        return result

    for attempt in range(max_retries):
        if verbose:
            _log_verbose_sql(full_sql, attempt + 1)
        errors = validate_sql(full_sql, config.compiler_path)
        if errors and any("Compiler not found" in e for e in errors):
            print(
                "Warning: Feldera compiler not found — skipping validation.",
                file=sys.stderr,
            )
            result.warnings.append("Compiler not found — output SQL is not validated")
            result.status = Status.UNSUPPORTED if result.unsupported else Status.SUCCESS
            return result
        if not errors:
            result.warnings.append(f"Validated successfully (attempt {attempt + 1})")
            result.status = Status.UNSUPPORTED if result.unsupported else Status.SUCCESS
            return result

        print(
            f"Validation attempt {attempt + 1}/{max_retries} failed: "
            f"{len(errors)} error(s)",
            file=sys.stderr,
        )
        for err in errors:
            print(f"  {err}", file=sys.stderr)

        repair = build_repair_prompt(schema_sql, query_sql, full_sql, errors)
        raw = client.translate(system_prompt, repair)
        data, repair_warnings = _parse_or_salvage(raw)
        if repair_warnings:
            result.warnings.append(
                f"Repair attempt {attempt + 1}: {repair_warnings[0]}"
            )
        # When parsing/salvage yields nothing, data is empty and these .get calls
        # keep the previous best-effort SQL rather than blanking it out.
        result.feldera_schema = _as_str(
            data.get("feldera_schema", result.feldera_schema)
        )
        result.feldera_query = _as_str(data.get("feldera_query", result.feldera_query))
        result.unsupported = _as_list(data.get("unsupported", result.unsupported))
        result.explanations = _as_list(data.get("explanations", result.explanations))
        full_sql = result.feldera_schema + "\n\n" + result.feldera_query

    if verbose:
        _log_verbose_sql(full_sql, max_retries + 1)
    errors = validate_sql(full_sql, config.compiler_path)
    if not errors:
        result.warnings.append(f"Validated successfully (attempt {max_retries + 1})")
        result.status = Status.UNSUPPORTED if result.unsupported else Status.SUCCESS
    else:
        result.status = Status.ERROR
        result.warnings.extend(
            f"Still failing after {max_retries} repairs: {e}" for e in errors
        )

    return result


def translate_spark_to_feldera(
    schema_sql: str,
    query_sql: str,
    config: Config,
    validate: bool = False,
    max_retries: int = DEFAULT_MAX_RETRIES,
    include_docs: bool = True,
    verbose: bool = False,
    extra_rules: list[Path] | None = None,
    extra_examples_dirs: list[Path] | None = None,
    extra_examples_files: list[Path] | None = None,
) -> TranslationResult:
    """Translate a Spark SQL schema + query (the whole program) to Feldera SQL.

    A single LLM call translates the entire program. When ``validate`` is set the
    output is checked against the Feldera compiler and repaired with error
    feedback for up to ``max_retries`` attempts. On a non-success first pass the
    translation is retried once more with Feldera documentation added to the
    prompt (unless ``include_docs`` is false).
    """
    schema_sql = trim_schema_to_referenced(schema_sql, query_sql)
    combined_sql = schema_sql + "\n" + query_sql
    client = create_client(config, verbose=verbose)

    # First pass: rules + validated examples (no docs).
    system_prompt = build_system_prompt(
        spark_sql=combined_sql,
        with_docs=False,
        with_examples=True,
        extra_rules=extra_rules,
        extra_examples_dirs=extra_examples_dirs,
        extra_examples_files=extra_examples_files,
    )
    try:
        result = _translate_with_repair(
            schema_sql,
            query_sql,
            config,
            client,
            system_prompt,
            validate,
            max_retries,
            verbose,
        )
    except PromptTooLargeError as e:
        message = _too_large_message(str(e))
        print(f"Error: {message}", file=sys.stderr)
        return TranslationResult(status=Status.ERROR, warnings=[message])

    # Retry with docs when the first pass was not fully successful.
    if not include_docs or result.status == Status.SUCCESS:
        return result

    print("Retrying with skills + docs prompt...", file=sys.stderr)
    system_prompt_docs = build_system_prompt(
        spark_sql=combined_sql,
        with_docs=True,
        with_examples=True,
        with_skills=True,
        retry_unsupported=result.unsupported or None,
        extra_rules=extra_rules,
        extra_examples_dirs=extra_examples_dirs,
        extra_examples_files=extra_examples_files,
    )
    try:
        docs_result = _translate_with_repair(
            schema_sql,
            query_sql,
            config,
            client,
            system_prompt_docs,
            validate,
            max_retries,
            verbose,
        )
    except PromptTooLargeError as e:
        # The docs prompt is the largest; keep the first-pass result and explain
        # why the docs retry was skipped.
        message = _too_large_message(str(e))
        print(f"Warning: docs retry skipped — {message}", file=sys.stderr)
        result.warnings.append(f"Docs retry skipped — {message}")
        return result

    result = docs_result
    if result.status != Status.ERROR:
        result.warnings.append("Resolved with skills + docs retry")
    return result
