from __future__ import annotations

from felderize.constants import PROMPTS_DIR


def _load(name: str) -> str:
    """Load a prompt template by filename from the prompts directory."""
    return (PROMPTS_DIR / name).read_text()


def build_user_prompt(schema_sql: str, query_sql: str) -> str:
    """Build the initial translation prompt for the given schema and query."""
    return _load("user_prompt.md").format(
        schema_sql=schema_sql.strip(),
        query_sql=query_sql.strip(),
    )


def build_force_query_prompt(
    schema_sql: str, query_sql: str, unsupported: list[str]
) -> str:
    """Build a follow-up prompt when the LLM returned an empty feldera_query."""
    unsup_text = "\n".join(f"- {u}" for u in unsupported)
    return _load("force_query_prompt.md").format(
        unsup_text=unsup_text,
        schema_sql=schema_sql.strip(),
        query_sql=query_sql.strip(),
    )


def build_repair_prompt(
    schema_sql: str, query_sql: str, feldera_sql: str, errors: list[str]
) -> str:
    """Build a repair prompt containing the compiler errors from the previous attempt."""
    error_text = "\n".join(f"- {e}" for e in errors)
    return _load("repair_prompt.md").format(
        schema_sql=schema_sql.strip(),
        query_sql=query_sql.strip(),
        feldera_sql=feldera_sql.strip(),
        error_text=error_text,
    )
