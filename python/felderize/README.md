# Felderize — Spark SQL to Feldera SQL Translator

felderize attempts to translate Spark SQL schemas and queries into valid [Feldera](https://www.feldera.com/) SQL using LLM-based translation with optional compiler validation.

## Setup

```bash
cd python/felderize
python3 -m venv .venv
source .venv/bin/activate
pip install -e .
```

> **Note:** `pip install -e .` is required before running `felderize`. It registers the package and CLI command.

Create a `.env` file:

```bash
ANTHROPIC_API_KEY=your-key-here
FELDERA_COMPILER=/path/to/sql-to-dbsp  # default: ../../sql-to-dbsp-compiler/SQL-compiler/sql-to-dbsp
FELDERIZE_MODEL=claude-sonnet-4-5
```

The `FELDERA_COMPILER` path is required for validation. Without it, translation still works but output SQL is not verified. You can also pass it per-command with `--compiler PATH`.

The compiler must be built before use (requires Java 19–21 and Maven):

```bash
cd sql-to-dbsp-compiler
./build.sh
```

## Usage

### Run a built-in example

```bash
# List available examples
felderize example

# Translate an example (validates by default)
felderize example simple

# Without compiler validation
felderize example simple --no-validate

# Log SQL submitted to the validator at each attempt
felderize example json --verbose

# Use a specific compiler binary
felderize example simple --compiler /path/to/sql-to-dbsp

# Output as JSON
felderize example simple --json-output
```

Available examples:

| Name | Description |
|------|-------------|
| `simple` | Date truncation, GROUP BY |
| `strings` | INITCAP, LPAD, NVL, CONCAT_WS |
| `arrays` | array_contains, size, element_at |
| `joins` | Null-safe equality (`<=>`) |
| `windows` | LAG, running SUM OVER |
| `aggregations` | COUNT DISTINCT, HAVING (includes unsupported: COLLECT_LIST, PERCENTILE_APPROX) |
| `json` | get_json_object → PARSE_JSON + VARIANT access *(combined file)* |
| `topk` | ROW_NUMBER TopK, QUALIFY, DATEDIFF → TIMESTAMPDIFF *(combined file)* |

The JSON output contains:

```json
{
  "feldera_schema": "...",   // translated DDL (CREATE TABLE statements)
  "feldera_query": "...",    // translated query (CREATE VIEW statements)
  "unsupported": [...],      // unsupported Spark features found
  "warnings": [...],         // non-fatal issues
  "explanations": [...],     // explanations for translation decisions
  "status": "success|unsupported|error"
}
```

### Translate your own SQL

Two input formats are supported:

**Separate schema and query files:**
```bash
felderize translate path/to/schema.sql path/to/query.sql
felderize translate path/to/schema.sql path/to/query.sql --validate
```

**Single combined file** (CREATE TABLE and CREATE VIEW statements in one file):
```bash
felderize translate-file path/to/combined.sql
felderize translate-file path/to/combined.sql --validate
```

> **Note:** Running without `--validate` prints a warning — the output SQL has not been verified against the Feldera compiler.

Both commands accept:
- `--verbose` to log the SQL submitted to the validator at each repair attempt
- `--compiler PATH` to specify the path to the Feldera compiler binary (overrides `FELDERA_COMPILER` env var)
- `--model` to specify the LLM model (overrides `FELDERIZE_MODEL` env var)

### Batch translation

```bash
felderize batch path/to/data_dir/ --output-dir results/
```

Each subdirectory should contain `*_schema.sql` and `*_query.sql` files.

## Configuration

Environment variables (set in `.env`):

| Variable | Description | Default |
|---|---|---|
| `ANTHROPIC_API_KEY` | Anthropic API key | (required) |
| `FELDERIZE_MODEL` | LLM model to use (can also be set with `--model`) | (required, set in `.env`) |
| `FELDERA_COMPILER` | Path to sql-to-dbsp compiler (can also be set with `--compiler`) | (required for validation) |

## How it works

1. Loads translation rules from a single skill file (`spark/data/skills/spark_skills.md`)
2. Sends Spark SQL to the LLM with rules, validated examples, and relevant Feldera SQL documentation (from `docs.feldera.com/docs/sql/`)
3. Parses the translated Feldera SQL from the LLM response
4. Optionally validates output against the Feldera compiler, retrying with error feedback if needed

## Support

Contact us at support@feldera.com for assistance with unsupported Spark SQL features.
