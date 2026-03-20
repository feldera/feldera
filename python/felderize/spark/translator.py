from __future__ import annotations

import json
import re
import sys
from pathlib import Path

from felderize.config import Config
from felderize.feldera_client import validate_sql
from felderize.llm import create_client
from felderize.models import Status, TranslationResult
from felderize.skills import build_system_prompt


def _parse_response(raw: str) -> dict:
    """Extract JSON from LLM response, handling markdown fences."""
    text = raw.strip()
    # Strip markdown code fences if present
    match = re.search(r"```(?:json)?\s*\n(.*?)```", text, re.DOTALL)
    if match:
        text = match.group(1).strip()
    return json.loads(text)


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


def _build_result(data: dict) -> TranslationResult:
    unsupported = _as_list(data.get("unsupported", []))
    return TranslationResult(
        feldera_schema=_as_str(data.get("feldera_schema", "")),
        feldera_query=_as_str(data.get("feldera_query", "")),
        unsupported=unsupported,
        warnings=_as_list(data.get("warnings", [])),
        explanations=_as_list(data.get("explanations", [])),
        status=Status.UNSUPPORTED if unsupported else Status.SUCCESS,
    )


def _build_user_prompt(schema_sql: str, query_sql: str) -> str:
    return f"""\
Translate the following Spark SQL schema and query to Feldera SQL.

--- Spark Schema ---
{schema_sql.strip()}

--- Spark Query ---
{query_sql.strip()}
"""


def _build_repair_prompt(
    schema_sql: str, query_sql: str, feldera_sql: str, errors: list[str]
) -> str:
    error_text = "\n".join(f"- {e}" for e in errors)
    return f"""\
The following Feldera SQL was generated from a Spark SQL translation but has compiler errors.
Fix the Feldera SQL to resolve these errors while keeping it semantically equivalent to the original Spark SQL.

--- Original Spark Schema ---
{schema_sql.strip()}

--- Original Spark Query ---
{query_sql.strip()}

--- Failed Feldera SQL ---
{feldera_sql.strip()}

--- Compiler Errors ---
{error_text}
"""


def _translate_with_repair(
    schema_sql: str,
    query_sql: str,
    config: Config,
    client,
    system_prompt: str,
    validate: bool,
    max_retries: int,
    verbose: bool = False,
) -> TranslationResult:
    """Run one translation attempt with optional validation + repair loop."""
    user_prompt = _build_user_prompt(schema_sql, query_sql)
    raw = client.translate(system_prompt, user_prompt)

    try:
        data = _parse_response(raw)
    except (json.JSONDecodeError, KeyError) as e:
        return TranslationResult(
            status=Status.ERROR,
            warnings=[f"Failed to parse LLM response: {e}", raw[:500]],
        )

    result = _build_result(data)

    if not validate:
        return result

    # Validation + repair loop
    full_sql = result.feldera_schema + "\n\n" + result.feldera_query
    for attempt in range(max_retries):
        if verbose:
            print(
                f"\n--- SQL submitted to validator (attempt {attempt + 1}) ---",
                file=sys.stderr,
            )
            print(full_sql, file=sys.stderr)
            print("---", file=sys.stderr)
        errors = validate_sql(full_sql, config.feldera_compiler or None)
        if not errors:
            result.warnings.append(f"Validated successfully (attempt {attempt + 1})")
            result.status = Status.UNSUPPORTED if result.unsupported else Status.SUCCESS
            return result

        print(
            f"Validation attempt {attempt + 1}/{max_retries} failed: {len(errors)} error(s)",
            file=sys.stderr,
        )
        for err in errors:
            print(f"  {err}", file=sys.stderr)

        repair_prompt = _build_repair_prompt(schema_sql, query_sql, full_sql, errors)
        raw = client.translate(system_prompt, repair_prompt)
        try:
            data = _parse_response(raw)
            result.feldera_schema = _as_str(
                data.get("feldera_schema", result.feldera_schema)
            )
            result.feldera_query = _as_str(
                data.get("feldera_query", result.feldera_query)
            )
            result.unsupported = _as_list(data.get("unsupported", result.unsupported))
            result.warnings = _as_list(data.get("warnings", result.warnings))
            result.explanations = _as_list(
                data.get("explanations", result.explanations)
            )
            full_sql = result.feldera_schema + "\n\n" + result.feldera_query
        except (json.JSONDecodeError, KeyError):
            result.warnings.append(
                f"Repair attempt {attempt + 1} produced invalid JSON"
            )

    # Final validation after all retries
    if verbose:
        print(
            f"\n--- SQL submitted to validator (attempt {max_retries + 1}) ---",
            file=sys.stderr,
        )
        print(full_sql, file=sys.stderr)
        print("---", file=sys.stderr)
    errors = validate_sql(full_sql, config.feldera_compiler or None)
    if not errors:
        result.warnings.append(f"Validated successfully (attempt {max_retries + 1})")
        result.status = Status.UNSUPPORTED if result.unsupported else Status.SUCCESS
    else:
        result.status = Status.ERROR
        result.warnings.extend(
            [f"Still failing after {max_retries} repairs: {e}" for e in errors]
        )

    return result


def split_combined_sql(sql: str) -> tuple[str, str]:
    """Split a combined SQL file into (schema_sql, query_sql).

    Schema: CREATE TABLE statements.
    Query: CREATE [OR REPLACE] [TEMP[ORARY]] VIEW statements.
    Comments and blank lines are preserved with their associated statement.
    Unrecognised statements are placed in schema_sql.
    """
    import re as _re

    # Split on statement boundaries (semicolon at end of line or followed by whitespace).
    raw_stmts = _re.split(r";\s*", sql)

    schema_parts: list[str] = []
    query_parts: list[str] = []

    for stmt in raw_stmts:
        stripped = stmt.strip()
        if not stripped:
            continue
        # Find first non-comment, non-blank line to identify statement type.
        first_kw = next(
            (
                ln.strip()
                for ln in stripped.splitlines()
                if ln.strip() and not ln.strip().startswith("--")
            ),
            "",
        ).upper()
        if not first_kw:
            continue  # comment-only block
        if _re.match(r"CREATE\s+(OR\s+REPLACE\s+)?(TEMP(ORARY)?\s+)?VIEW\b", first_kw):
            query_parts.append(stripped + ";")
        else:
            schema_parts.append(stripped + ";")

    return "\n\n".join(schema_parts), "\n\n".join(query_parts)


def translate_spark_to_feldera(
    schema_sql: str,
    query_sql: str,
    config: Config,
    validate: bool = False,
    max_retries: int = 3,
    docs_only_fallback: bool = True,
    skills_dir: str | None = None,
    docs_dir: str | None = None,
    include_docs: bool = True,
    verbose: bool = False,
) -> TranslationResult:
    combined_sql = schema_sql + "\n" + query_sql
    docs_dir_path = Path(docs_dir) if docs_dir else None
    client = create_client(config)

    # First pass: skills + examples (no docs).
    system_prompt_skills = build_system_prompt(
        skills_dir,
        docs_dir=docs_dir_path,
        spark_sql=combined_sql,
        with_docs=False,
    )
    result = _translate_with_repair(
        schema_sql,
        query_sql,
        config,
        client,
        system_prompt_skills,
        validate,
        max_retries,
        verbose,
    )

    if result.status != Status.ERROR:
        return result

    # Final fallback: docs only (no skills, no examples).
    if include_docs and docs_only_fallback:
        print("Retrying with docs-only prompt...", file=sys.stderr)
        system_prompt_docs = build_system_prompt(
            skills_dir,
            docs_dir=docs_dir_path,
            spark_sql=combined_sql,
            with_docs=True,
            with_examples=False,
            with_skills=False,
        )
        result = _translate_with_repair(
            schema_sql,
            query_sql,
            config,
            client,
            system_prompt_docs,
            validate,
            max_retries,
            verbose,
        )
        if result.status != Status.ERROR:
            result.warnings.append("Resolved with docs-only fallback")

    return result
