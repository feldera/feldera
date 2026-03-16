# Felderize — Spark SQL to Feldera SQL Translator

felderize translates Spark SQL schemas and queries into valid [Feldera](https://www.feldera.com/) SQL using LLM-based translation with optional compiler validation.

## Setup

```bash
cd python/felderize
python3 -m venv .venv
source .venv/bin/activate
pip install -e .
```

> **Note:** `pip install -e .` is required before running `felderize`. It registers the package and CLI command.

Create a `.env` file with your API key:

```bash
echo 'ANTHROPIC_API_KEY=your-key-here' > .env
```

## Usage

### Run a built-in example

```bash
# List available examples
felderize example

# Translate an example
felderize example simple

# With compiler validation
felderize example simple --validate

# JSON output
felderize example simple --json-output
```

### Translate your own SQL

```bash
felderize translate path/to/schema.sql path/to/query.sql
felderize translate path/to/schema.sql path/to/query.sql --validate
```

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
| `FELDERIZE_LLM_PROVIDER` | `anthropic` or `openai` | `anthropic` |
| `FELDERIZE_MODEL` | LLM model to use | `claude-sonnet-4-20250514` |
| `OPENAI_API_KEY` | OpenAI API key (if using openai provider) | — |
| `FELDERA_COMPILER` | Path to sql-to-dbsp compiler | `<repo-root>/sql-to-dbsp-compiler/SQL-compiler/sql-to-dbsp` |

## How it works

1. Loads translation rules from skill files (`spark/data/skills/`)
2. Sends Spark SQL to the LLM with rules and validated examples
3. Parses the translated Feldera SQL from the LLM response
4. Optionally validates output against the Feldera compiler, retrying with error feedback if needed

## Support

Contact us at support@feldera.com for assistance with unsupported Spark SQL features.
