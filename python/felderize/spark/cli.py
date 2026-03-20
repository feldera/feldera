from __future__ import annotations

import json
import sys
from pathlib import Path

import click

from felderize.config import Config
from felderize.models import TranslationResult
from felderize.translator import split_combined_sql, translate_spark_to_feldera


@click.group()
def cli():
    """Spark SQL to Feldera SQL translator."""


@cli.command()
@click.argument("schema_file", type=click.Path(exists=True))
@click.argument("query_file", type=click.Path(exists=True))
@click.option("--validate", is_flag=True, help="Validate against Feldera instance")
@click.option("--compiler", type=click.Path(), help="Path to Feldera compiler binary")
@click.option("--json-output", is_flag=True, help="Output as JSON")
@click.option("--no-docs", is_flag=True, help="Disable Feldera doc inclusion in prompt")
@click.option(
    "--verbose", is_flag=True, help="Log SQL submitted to validator at each attempt"
)
def translate(
    schema_file: str,
    query_file: str,
    validate: bool,
    compiler: str | None,
    json_output: bool,
    no_docs: bool,
    verbose: bool,
):
    """Translate a single Spark SQL schema + query pair to Feldera SQL."""
    if not validate:
        click.echo(
            "Warning: running without validation — output SQL is not verified against the Feldera compiler.",
            err=True,
        )
    config = Config.from_env()
    if compiler:
        config.feldera_compiler = compiler
    schema_sql = Path(schema_file).read_text()
    query_sql = Path(query_file).read_text()

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


@cli.command("translate-file")
@click.argument("sql_file", type=click.Path(exists=True))
@click.option("--validate", is_flag=True, help="Validate against Feldera instance")
@click.option("--compiler", type=click.Path(), help="Path to Feldera compiler binary")
@click.option("--json-output", is_flag=True, help="Output as JSON")
@click.option("--no-docs", is_flag=True, help="Disable Feldera doc inclusion in prompt")
@click.option(
    "--verbose", is_flag=True, help="Log SQL submitted to validator at each attempt"
)
def translate_file(
    sql_file: str, validate: bool, compiler: str | None, json_output: bool, no_docs: bool, verbose: bool
):
    """Translate a single combined Spark SQL file (schema + views) to Feldera SQL."""
    if not validate:
        click.echo(
            "Warning: running without validation — output SQL is not verified against the Feldera compiler.",
            err=True,
        )
    config = Config.from_env()
    if compiler:
        config.feldera_compiler = compiler
    combined_sql = Path(sql_file).read_text()
    schema_sql, query_sql = split_combined_sql(combined_sql)

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


@cli.command()
@click.argument("data_dir", type=click.Path(exists=True))
@click.option("--validate", is_flag=True, help="Validate against Feldera instance")
@click.option("--output-dir", type=click.Path(), help="Write results to directory")
@click.option("--no-docs", is_flag=True, help="Disable Feldera doc inclusion in prompt")
def batch(data_dir: str, validate: bool, output_dir: str | None, no_docs: bool):
    """Translate all Spark SQL pairs in a directory."""
    config = Config.from_env()
    data_path = Path(data_dir)
    results: dict[str, dict] = {}

    # Find all benchmark directories
    dirs = sorted(d for d in data_path.iterdir() if d.is_dir())

    if not dirs:
        click.echo("No benchmark directories found.", err=True)
        sys.exit(1)

    for bm_dir in dirs:
        name = bm_dir.name
        schema_files = list(bm_dir.glob("*_schema.sql"))
        query_files = list(bm_dir.glob("*_query.sql"))

        if not schema_files or not query_files:
            click.echo(f"Skipping {name}: missing schema or query file", err=True)
            continue

        schema_sql = schema_files[0].read_text()
        query_sql = query_files[0].read_text()

        click.echo(f"Translating {name}...", err=True)
        result = translate_spark_to_feldera(
            schema_sql,
            query_sql,
            config,
            validate=validate,
            include_docs=not no_docs,
        )
        results[name] = result.to_dict()

        if output_dir:
            out_path = Path(output_dir)
            out_path.mkdir(parents=True, exist_ok=True)
            (out_path / f"{name}.sql").write_text(
                result.feldera_schema + "\n\n" + result.feldera_query
            )
            (out_path / f"{name}.json").write_text(
                json.dumps(result.to_dict(), indent=2)
            )

    # Summary
    total = len(results)
    success = sum(1 for r in results.values() if r["status"] == "success")
    click.echo(f"\nResults: {success}/{total} successful", err=True)

    # Print full results as JSON to stdout
    click.echo(json.dumps(results, indent=2))


_EXAMPLES_DIR = Path(__file__).resolve().parent / "data" / "demo"


@cli.command()
@click.argument("name", required=False)
@click.option(
    "--validate/--no-validate",
    default=True,
    help="Validate against Feldera instance (default: on)",
)
@click.option("--compiler", type=click.Path(), help="Path to Feldera compiler binary")
@click.option("--json-output", is_flag=True, help="Output as JSON")
@click.option("--no-docs", is_flag=True, help="Disable Feldera doc inclusion in prompt")
@click.option(
    "--verbose", is_flag=True, help="Log SQL submitted to validator at each attempt"
)
def example(
    name: str | None, validate: bool, compiler: str | None, json_output: bool, no_docs: bool, verbose: bool
):
    """Run a built-in example translation.

    Without NAME, lists available examples. With NAME, translates that example.

    \b
    Usage:
      felderize example              # list available examples
      felderize example simple       # translate the 'simple' example
    """
    # Discover available examples: schema+query pairs and combined files
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
        click.echo("\nRun one with: felderize example <name>")
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
    config = Config.from_env()
    if compiler:
        config.feldera_compiler = compiler
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
    "  Contact us at support@feldera.com to request support for these features."
)

_ERROR_CONTACT_MESSAGE = "\n  Contact us at support@feldera.com for assistance."


def _print_result(result: TranslationResult):
    """Pretty-print a translation result."""
    from felderize.models import Status

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
