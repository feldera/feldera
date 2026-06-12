from __future__ import annotations

import json
import sys
from pathlib import Path

import click

from felderize.constants import MINIMUM_COMPILER_VERSION
from felderize.install_feldera_sql_compiler import (
    download_compiler,
    ensure_compiler,
    is_supported_version,
    jar_version,
)
from felderize.config import Config
from felderize.models import Status, TranslationResult
from felderize.translator import (
    split_combined_sql,
    translate_spark_to_feldera,
    validate_inputs,
    validate_query,
    validate_schema,
)


def _warn_if_unsupported_compiler(compiler_path: str | None) -> None:
    """Warn when the configured compiler JAR is older than the minimum supported version."""
    if not compiler_path:
        return
    tag = jar_version(compiler_path)
    if tag and not is_supported_version(tag):
        click.echo(
            f"Warning: compiler {tag} is older than the minimum supported "
            f"{MINIMUM_COMPILER_VERSION}; run 'felderize download-compiler' to "
            "update. Translations validated against it may be inaccurate.",
            err=True,
        )


def _prepare_config(compiler: str | None, model: str | None, validate: bool) -> Config:
    """Build the run config from env, apply CLI overrides, and resolve a compiler.

    An explicit ``--compiler`` (or ``FELDERA_COMPILER``) always wins. Otherwise,
    when validating, felderize reuses a compiler JAR cached in ``~/.felderize/``
    or downloads the latest release — unless auto-download is disabled via
    ``FELDERIZE_AUTO_DOWNLOAD=0``. Download progress goes to stderr so ``--json-output``
    stays clean.
    """
    config = Config.from_env()
    if compiler:
        config.feldera_compiler = compiler
    if model:
        config.model = model
    if validate:
        if not config.feldera_compiler:
            resolved = ensure_compiler(
                auto_download=config.auto_download_compiler, logs=sys.stderr
            )
            if resolved is not None:
                config.feldera_compiler = str(resolved)
        _warn_if_unsupported_compiler(config.feldera_compiler)
    return config


def _split_examples(paths: tuple[str, ...]) -> tuple[list[Path], list[Path]]:
    """Split --examples paths into (dirs, files)."""
    dirs, files = [], []
    for p in paths:
        path = Path(p)
        (dirs if path.is_dir() else files).append(path)
    return dirs, files


def _expand_query_paths(paths: tuple[Path, ...]) -> list[Path]:
    """Expand query file arguments: directories become sorted *.sql files within them."""
    result: list[Path] = []
    for path in paths:
        if path.is_dir():
            result.extend(sorted(path.glob("*.sql")))
        else:
            result.append(path)
    return result


def _comment_lines(prefix: str, items: list[str]) -> str:
    """Render items as SQL line comments, collapsing each to a single line so a
    multi-line note can't leak uncommented (invalid) text into the file."""
    out = ""
    for item in items:
        one = " ".join(str(item).split())
        if one:
            out += f"-- {prefix}{one}\n"
    return out


def _read_text(path) -> str:
    """Read a user-supplied SQL file, exiting cleanly on binary/unreadable input."""
    try:
        return Path(path).read_text()
    except (UnicodeDecodeError, OSError) as e:
        click.echo(
            f"Error: cannot read {path} as a UTF-8 text file ({type(e).__name__}) — "
            "expected a SQL file, not binary.",
            err=True,
        )
        sys.exit(1)


def _write_sql_file(result, path: Path) -> None:
    """Write the translated schema + query to a deployable .sql file.

    A header comment block carries the translation's caveats (status, any
    unsupported constructs, and warnings) so the file is self-documenting.
    """
    with Path(path).open("w") as f:
        f.write(f"-- Translated by felderize — status: {result.status.value}\n")
        if result.unsupported:
            f.write(
                "-- Unsupported Spark constructs (emitted as CAST(NULL ...) placeholders):\n"
            )
            f.write(_comment_lines("  - ", result.unsupported))
        if result.warnings:
            f.write("-- Warnings:\n")
            f.write(_comment_lines("  - ", result.warnings))
        f.write("\n")
        if result.feldera_schema:
            f.write("-- Schema --\n" + result.feldera_schema.strip() + "\n\n")
        if result.feldera_query:
            f.write("-- Query --\n" + result.feldera_query.strip() + "\n")


@click.group()
def cli():
    """Feldera SQL translator — converts source SQL dialects to Feldera SQL."""


@cli.command("download-compiler")
@click.option(
    "--version", default=None, help="Release tag (e.g. v0.291.0); default: latest"
)
@click.option(
    "--output-dir",
    type=click.Path(),
    default=None,
    help="Directory to save the JAR (default: ~/.felderize/)",
)
@click.option(
    "--force", is_flag=True, help="Re-download even if the file already exists"
)
def download_compiler_cmd(version: str | None, output_dir: str | None, force: bool):
    """Download the Feldera SQL compiler JAR from GitHub releases.

    After downloading, set FELDERA_COMPILER in your .env to the printed path.
    """
    dest_dir = Path(output_dir) if output_dir else None
    try:
        jar_path = download_compiler(output_dir=dest_dir, version=version, force=force)
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)

    click.echo(f"Compiler ready: {jar_path}")
    click.echo(f"\nAdd to your .env:\n  FELDERA_COMPILER={jar_path}")


@cli.group()
def spark():
    """Translate Spark SQL to Feldera SQL."""


@spark.command()
@click.argument("schema_file", type=click.Path(exists=True))
@click.argument("query_file", type=click.Path(exists=True))
@click.option("--validate", is_flag=True, help="Validate against Feldera instance")
@click.option("--compiler", type=click.Path(), help="Path to Feldera compiler binary")
@click.option("--model", help="LLM model to use (overrides FELDERIZE_MODEL env var)")
@click.option("--json-output", is_flag=True, help="Output as JSON")
@click.option(
    "-o",
    "--output",
    "output",
    type=click.Path(),
    help="Write the translated SQL (schema + views) to this .sql file",
)
@click.option("--no-docs", is_flag=True, help="Disable Feldera doc inclusion in prompt")
@click.option(
    "--rules",
    type=click.Path(exists=True),
    multiple=True,
    help="Extra translation rule .md files (repeatable)",
)
@click.option(
    "--examples",
    type=click.Path(exists=True),
    multiple=True,
    help="Extra example .md files or directories (repeatable)",
)
@click.option(
    "--verbose", is_flag=True, help="Log SQL submitted to validator at each attempt"
)
def translate(
    schema_file: str,
    query_file: str,
    validate: bool,
    compiler: str | None,
    model: str | None,
    json_output: bool,
    output: str | None,
    no_docs: bool,
    rules: tuple[str, ...],
    examples: tuple[str, ...],
    verbose: bool,
):
    """Translate a single Spark SQL schema + query/views pair to Feldera SQL."""
    if not validate:
        click.echo(
            "Warning: running without validation — output SQL is not verified against the Feldera compiler.",
            err=True,
        )
    config = _prepare_config(compiler, model, validate)
    schema_sql = _read_text(schema_file)
    query_sql = _read_text(query_file)

    errors = validate_inputs(schema_sql, query_sql)
    if errors:
        for e in errors:
            click.echo(f"Error: {e}", err=True)
        sys.exit(1)

    ex_dirs, ex_files = _split_examples(examples)
    result = translate_spark_to_feldera(
        schema_sql,
        query_sql,
        config,
        validate=validate,
        include_docs=not no_docs,
        verbose=verbose,
        extra_rules=[Path(r) for r in rules] or None,
        extra_examples_dirs=ex_dirs or None,
        extra_examples_files=ex_files or None,
    )

    if output:
        _write_sql_file(result, Path(output))
        click.echo(f"Wrote {output} (status: {result.status.value})", err=True)
    if json_output:
        click.echo(json.dumps(result.to_dict(), indent=2))
    elif not output:
        _print_result(result)


@spark.command("translate-file")
@click.argument("sql_file", type=click.Path(exists=True))
@click.option("--validate", is_flag=True, help="Validate against Feldera instance")
@click.option("--compiler", type=click.Path(), help="Path to Feldera compiler binary")
@click.option("--model", help="LLM model to use (overrides FELDERIZE_MODEL env var)")
@click.option("--json-output", is_flag=True, help="Output as JSON")
@click.option(
    "-o",
    "--output",
    "output",
    type=click.Path(),
    help="Write the translated SQL (schema + views) to this .sql file",
)
@click.option("--no-docs", is_flag=True, help="Disable Feldera doc inclusion in prompt")
@click.option(
    "--rules",
    type=click.Path(exists=True),
    multiple=True,
    help="Extra translation rule .md files (repeatable)",
)
@click.option(
    "--examples",
    type=click.Path(exists=True),
    multiple=True,
    help="Extra example .md files or directories (repeatable)",
)
@click.option(
    "--verbose", is_flag=True, help="Log SQL submitted to validator at each attempt"
)
def translate_file(
    sql_file: str,
    validate: bool,
    compiler: str | None,
    model: str | None,
    json_output: bool,
    output: str | None,
    no_docs: bool,
    rules: tuple[str, ...],
    examples: tuple[str, ...],
    verbose: bool,
):
    """Translate a single combined Spark SQL file (schema + views) to Feldera SQL."""
    if not validate:
        click.echo(
            "Warning: running without validation — output SQL is not verified against the Feldera compiler.",
            err=True,
        )
    config = _prepare_config(compiler, model, validate)
    combined_sql = _read_text(sql_file)
    schema_sql, query_sql = split_combined_sql(combined_sql)

    errors = validate_inputs(schema_sql, query_sql)
    if errors:
        for e in errors:
            click.echo(f"Error: {e}", err=True)
        sys.exit(1)

    ex_dirs, ex_files = _split_examples(examples)
    result = translate_spark_to_feldera(
        schema_sql,
        query_sql,
        config,
        validate=validate,
        include_docs=not no_docs,
        verbose=verbose,
        extra_rules=[Path(r) for r in rules] or None,
        extra_examples_dirs=ex_dirs or None,
        extra_examples_files=ex_files or None,
    )

    if output:
        _write_sql_file(result, Path(output))
        click.echo(f"Wrote {output} (status: {result.status.value})", err=True)
    if json_output:
        click.echo(json.dumps(result.to_dict(), indent=2))
    elif not output:
        _print_result(result)


@spark.command("translate-batch")
@click.argument("schema_file", type=click.Path(exists=True))
@click.argument(
    "query_files", nargs=-1, required=True, type=click.Path(exists=True, path_type=Path)
)
@click.option("--validate", is_flag=True, help="Validate against Feldera compiler")
@click.option("--compiler", type=click.Path(), help="Path to Feldera compiler binary")
@click.option("--model", help="LLM model to use (overrides FELDERIZE_MODEL env var)")
@click.option(
    "--output-dir",
    type=click.Path(),
    help="Directory to write translated SQL files (one per query)",
)
@click.option("--no-docs", is_flag=True, help="Disable Feldera doc inclusion in prompt")
@click.option(
    "--rules",
    type=click.Path(exists=True),
    multiple=True,
    help="Extra translation rule .md files (repeatable)",
)
@click.option(
    "--examples",
    type=click.Path(exists=True),
    multiple=True,
    help="Extra example .md files or directories (repeatable)",
)
@click.option(
    "--verbose", is_flag=True, help="Log SQL submitted to validator at each attempt"
)
def translate_batch(
    schema_file: str,
    query_files: tuple[Path, ...],
    validate: bool,
    compiler: str | None,
    model: str | None,
    output_dir: str | None,
    no_docs: bool,
    rules: tuple[str, ...],
    examples: tuple[str, ...],
    verbose: bool,
):
    """Translate multiple Spark SQL query files against a shared schema.

    Runs all queries in a single process so doc and example caches stay warm,
    making batch translation faster than calling translate in a shell loop.

    \b
    Usage:
      felderize spark translate-batch schema.sql q1.sql q2.sql q3.sql --validate
      felderize spark translate-batch schema.sql queries/ --validate --output-dir out/
    """
    if not validate:
        click.echo(
            "Warning: running without validation — output SQL is not verified against the Feldera compiler.",
            err=True,
        )
    config = _prepare_config(compiler, model, validate)

    schema_sql = _read_text(schema_file)
    schema_errors = validate_schema(schema_sql)
    if schema_errors:
        for e in schema_errors:
            click.echo(f"Error: {e}", err=True)
        sys.exit(1)

    out_dir = Path(output_dir) if output_dir else None
    if out_dir:
        out_dir.mkdir(parents=True, exist_ok=True)

    ex_dirs, ex_files = _split_examples(examples)
    expanded_queries = _expand_query_paths(query_files)
    if not expanded_queries:
        click.echo("Error: no .sql query files found.", err=True)
        sys.exit(1)
    results: list[tuple[str, TranslationResult]] = []

    for qpath in expanded_queries:
        try:
            query_sql = qpath.read_text()
        except (UnicodeDecodeError, OSError) as e:
            msg = f"cannot read as a UTF-8 text file ({type(e).__name__}) — not a SQL file"
            click.echo(f"Error ({qpath.name}): {msg}", err=True)
            results.append(
                (qpath.stem, TranslationResult(status=Status.ERROR, warnings=[msg]))
            )
            continue

        errors = validate_query(query_sql)
        if errors:
            for e in errors:
                click.echo(f"Error ({qpath.name}): {e}", err=True)
            results.append(
                (qpath.stem, TranslationResult(status=Status.ERROR, warnings=errors))
            )
            continue

        result = translate_spark_to_feldera(
            schema_sql,
            query_sql,
            config,
            validate=validate,
            include_docs=not no_docs,
            verbose=verbose,
            extra_rules=[Path(r) for r in rules] or None,
            extra_examples_dirs=ex_dirs or None,
            extra_examples_files=ex_files or None,
        )

        if out_dir:
            _write_sql_file(result, out_dir / f"{qpath.stem}_feldera.sql")
        else:
            click.echo(f"\n=== {qpath.name} ===")
            _print_result(result)

        click.echo(f"{qpath.stem}: {result.status.value}", err=True)
        results.append((qpath.stem, result))

    n_success = sum(1 for _, r in results if r.status == Status.SUCCESS)
    click.echo(f"\n{n_success}/{len(results)} translated cleanly.", err=True)
    counts: dict[str, int] = {}
    for _, r in results:
        counts[r.status.value] = counts.get(r.status.value, 0) + 1
    breakdown = ", ".join(f"{n} {status}" for status, n in sorted(counts.items()))
    click.echo(f"  Status breakdown: {breakdown}", err=True)


_EXAMPLES_DIR = Path(__file__).resolve().parent / "spark_sql"


@spark.command()
@click.argument("name", required=False)
@click.option(
    "--validate/--no-validate",
    default=True,
    help="Validate against Feldera instance (default: on)",
)
@click.option("--compiler", type=click.Path(), help="Path to Feldera compiler binary")
@click.option("--model", help="LLM model to use (overrides FELDERIZE_MODEL env var)")
@click.option("--json-output", is_flag=True, help="Output as JSON")
@click.option("--no-docs", is_flag=True, help="Disable Feldera doc inclusion in prompt")
@click.option(
    "--verbose", is_flag=True, help="Log SQL submitted to validator at each attempt"
)
def example(
    name: str | None,
    validate: bool,
    compiler: str | None,
    model: str | None,
    json_output: bool,
    no_docs: bool,
    verbose: bool,
):
    """Run a built-in example translation.

    Without NAME, lists available examples. With NAME, translates that example.

    \b
    Usage:
      felderize spark example              # list available examples
      felderize spark example simple       # translate the 'simple' example
    """
    pairs: dict[str, tuple[Path, Path] | Path] = {}
    for schema_file in sorted(_EXAMPLES_DIR.glob("*_schema.sql")):
        example_name = schema_file.name.replace("_schema.sql", "")
        query_file = _EXAMPLES_DIR / f"{example_name}_query.sql"
        if query_file.is_file():
            pairs[example_name] = (schema_file, query_file)
    for combined_file in sorted(_EXAMPLES_DIR.glob("*_combined.sql")):
        example_name = combined_file.name.replace("_combined.sql", "")
        pairs[example_name] = combined_file

    if not name:
        click.echo("Available examples:\n")
        for ex_name, files in pairs.items():
            tag = "[combined]" if isinstance(files, Path) else "[schema+query]"
            click.echo(f"  {ex_name:20s} {tag}")
        click.echo("\nRun one with: felderize spark example <name>")
        return

    if name not in pairs:
        click.echo(f"Unknown example '{name}'. Available: {', '.join(pairs)}", err=True)
        sys.exit(1)

    files = pairs[name]
    if isinstance(files, Path):
        combined_sql = files.read_text()
        schema_sql, query_sql = split_combined_sql(combined_sql)
        click.echo(f"-- Spark SQL ({name}) --", err=True)
        click.echo(combined_sql.strip(), err=True)
    else:
        schema_file, query_file = files
        schema_sql = schema_file.read_text()
        query_sql = query_file.read_text()
        click.echo(f"-- Spark Schema ({name}) --", err=True)
        click.echo(schema_sql.strip(), err=True)
        click.echo(f"\n-- Spark Query ({name}) --", err=True)
        click.echo(query_sql.strip(), err=True)
    click.echo("\nTranslating...\n", err=True)

    if not validate:
        click.echo(
            "Warning: running without validation — output SQL is not verified against the Feldera compiler.",
            err=True,
        )
    config = _prepare_config(compiler, model, validate)
    result = translate_spark_to_feldera(
        schema_sql,
        query_sql,
        config,
        validate=validate,
        include_docs=not no_docs,
        verbose=verbose,
    )

    if json_output:
        click.echo(json.dumps(result.to_dict(), indent=2))
    else:
        _print_result(result)


_CONTACT_MESSAGE = (
    "\n  Some Spark SQL features are not yet supported in Feldera.\n"
    "  Contact us at support@feldera.com to request support for these features or "
    "  consider filing an issue at github.com/feldera/feldera/issue. "
)

_ERROR_CONTACT_MESSAGE = "\n  Contact us at support@feldera.com for assistance."


def _print_result(result: TranslationResult) -> None:
    """Pretty-print a translation result."""
    if result.status == Status.ERROR:
        click.echo("-- Translation Failed --", err=True)
        click.echo(
            "  The translation could not be validated against the Feldera compiler.",
            err=True,
        )
        if result.warnings:
            click.echo("  Errors:", err=True)
            for item in result.warnings:
                click.echo(f"    - {item}", err=True)
        click.echo(_ERROR_CONTACT_MESSAGE, err=True)
        click.echo(f"\nStatus: {result.status.value}", err=True)
        return

    if result.feldera_schema:
        click.echo("-- Schema --")
        click.echo(result.feldera_schema)
        click.echo()

    if result.feldera_query:
        click.echo("-- Query --")
        click.echo(result.feldera_query)
        click.echo()

    if result.explanations:
        click.echo("-- Transformations Applied --", err=True)
        for item in result.explanations:
            click.echo(f"  - {item}", err=True)
        click.echo(err=True)

    if result.unsupported:
        click.echo("-- Unsupported --", err=True)
        for item in result.unsupported:
            click.echo(f"  - {item}", err=True)
        click.echo(_CONTACT_MESSAGE, err=True)
        click.echo(err=True)

    if result.warnings:
        click.echo("-- Warnings --", err=True)
        for item in result.warnings:
            click.echo(f"  - {item}", err=True)

    click.echo(f"\nStatus: {result.status.value}", err=True)
