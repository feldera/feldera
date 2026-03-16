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


def _translate_once(
    schema_sql: str,
    query_sql: str,
    config: Config,
    client,
    system_prompt: str,
    validate: bool,
    max_retries: int,
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
        errors = validate_sql(full_sql, config.feldera_compiler or None)
        if not errors:
            result.warnings.append(f"Validated successfully (attempt {attempt + 1})")
            result.status = Status.UNSUPPORTED if result.unsupported else Status.SUCCESS
            return result

        print(
            f"Validation attempt {attempt + 1}/{max_retries} failed: {len(errors)} error(s)",
            file=sys.stderr,
        )

        repair_prompt = _build_repair_prompt(schema_sql, query_sql, full_sql, errors)
        raw = client.translate(system_prompt, repair_prompt)
        try:
            data = _parse_response(raw)
            result.feldera_schema = _as_str(data.get("feldera_schema", result.feldera_schema))
            result.feldera_query = _as_str(data.get("feldera_query", result.feldera_query))
            result.unsupported = _as_list(data.get("unsupported", result.unsupported))
            result.warnings = _as_list(data.get("warnings", result.warnings))
            result.explanations = _as_list(data.get("explanations", result.explanations))
            full_sql = result.feldera_schema + "\n\n" + result.feldera_query
        except (json.JSONDecodeError, KeyError):
            result.warnings.append(f"Repair attempt {attempt + 1} produced invalid JSON")

    # Final validation after all retries
    errors = validate_sql(full_sql, config.feldera_compiler or None)
    if not errors:
        result.warnings.append(f"Validated successfully (attempt {max_retries + 1})")
        result.status = Status.UNSUPPORTED if result.unsupported else Status.SUCCESS
    else:
        result.status = Status.ERROR
        result.warnings.extend([f"Still failing after {max_retries} repairs: {e}" for e in errors])

    return result


def translate_spark_to_feldera(
    schema_sql: str,
    query_sql: str,
    config: Config,
    validate: bool = False,
    max_retries: int = 3,
    skills_dir: str | None = None,
    docs_dir: str | None = None,
    include_docs: bool = True,
) -> TranslationResult:
    combined_sql = schema_sql + "\n" + query_sql if include_docs else ""
    docs_dir_path = Path(docs_dir) if docs_dir else None
    client = create_client(config)

    # First pass: skills + examples only (no docs)
    system_prompt = build_system_prompt(
        skills_dir, docs_dir=docs_dir_path, spark_sql=combined_sql, with_docs=False,
    )
    result = _translate_once(
        schema_sql, query_sql, config, client, system_prompt, validate, max_retries,
    )

    # If first pass failed and docs are enabled, retry with docs
    if result.status == Status.ERROR and include_docs:
        print("Retrying with Feldera docs...", file=sys.stderr)
        system_prompt_with_docs = build_system_prompt(
            skills_dir, docs_dir=docs_dir_path, spark_sql=combined_sql, with_docs=True,
        )
        result = _translate_once(
            schema_sql, query_sql, config, client, system_prompt_with_docs,
            validate, max_retries,
        )
        if result.status != Status.ERROR:
            result.warnings.append("Resolved with docs fallback")

    return result
