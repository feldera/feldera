# Felderize — SQL to Feldera SQL Translator

felderize translates SQL from various dialects into valid [Feldera](https://www.feldera.com/) SQL using LLM-based translation with optional compiler validation.

> **Dialects:** Spark SQL is currently the only supported dialect. Support for additional dialects is planned.

## Setup

```bash
cd python/felderize
python3 -m venv .venv
source .venv/bin/activate
pip install -e .
```

> **Note:** `pip install -e .` is required before running `felderize`. It registers the package and CLI command.

**The Feldera SQL compiler JAR** (used only for `--validate`; requires Java 19–21 installed):

felderize downloads it for you. The first time you run a command with `--validate`
and no compiler configured, felderize fetches the latest
`sql2dbsp-jar-with-dependencies-*.jar` from
[GitHub Releases](https://github.com/feldera/feldera/releases) into `~/.felderize/`
and reuses it on later runs. To opt out (e.g. in CI or offline), set
`FELDERIZE_AUTO_DOWNLOAD=0`; validation is then skipped unless you point
`FELDERA_COMPILER` / `--compiler` at a JAR.

To fetch or update it explicitly:

```bash
felderize download-compiler
```

This saves the latest JAR to `~/.felderize/` and prints its path. Re-run it any
time to pick up a newer release; it reports whether you are already on the latest
one. felderize automatically uses the newest JAR cached in `~/.felderize/`, so you
do not need to set `FELDERA_COMPILER` unless you want a specific JAR.

> **Requirement:** felderize needs compiler **v0.304.0 or newer** — earlier releases lack SQL features felderize relies on (e.g. `div_null`, `MAKE_DATE`). `download-compiler` always fetches the latest release, and felderize warns at validation time if the configured compiler is older than v0.304.0.

> **Note:** Java 19, 20, or 21 must be installed and on your `PATH` before running validation. Check with `java -version`. Later versions (22+) are not supported.

**Create a `.env` file in the `python/felderize/` directory:**

```bash
ANTHROPIC_API_KEY=your-key-here
FELDERA_COMPILER=~/.felderize/sql2dbsp-jar-with-dependencies-vX.Y.Z.jar
FELDERIZE_MODEL=claude-sonnet-4-6
```

`ANTHROPIC_API_KEY` and `FELDERIZE_MODEL` are required. `FELDERA_COMPILER` is optional: it is used only for validation, and when unset felderize auto-downloads (and caches) the compiler on first `--validate`. Set it to pin a specific JAR. You can also pass `--compiler PATH` and `--model MODEL` per command.

> **Note:** felderize currently requires an Anthropic API key — only Claude models are supported.

> **Tip — use a large-context-window model.** Each request bundles the full rule
> set (`felderize/skills/spark_skills.md`), validated examples, and (on the retry pass)
> Feldera reference documentation *on top of* your schema and query — easily tens
> of thousands of tokens, and the `--validate` repair and docs passes add more.
> Prefer a model with a large context window (e.g. a recent Claude Sonnet/Opus)
> so nothing is truncated and the model has the full rules in view; set it via
> `FELDERIZE_MODEL` or `--model`. If a program still doesn't fit, felderize stops
> with an `error` asking you to shorten the input (translate fewer views at a
> time, drop unused tables, or split it into smaller files).

## Usage

### Run a built-in example

```bash
# List available examples
felderize spark example

# Translate an example (validates by default)
felderize spark example simple

# Without compiler validation
felderize spark example simple --no-validate

# Log SQL submitted to the validator at each attempt
felderize spark example json --verbose

# Use a specific compiler binary
felderize spark example simple --compiler /path/to/sql-to-dbsp

# Output as JSON
felderize spark example simple --json-output
```

Available examples:

| Name | Description |
|------|-------------|
| `simple` | Date truncation, GROUP BY |
| `strings` | INITCAP, LPAD, NVL, CONCAT_WS |
| `arrays` | array_contains, size, element_at |
| `joins` | Null-safe equality (`<=>`) |
| `windows` | LAG, running SUM OVER |
| `aggregations` | COUNT DISTINCT, AVG, SUM, HAVING |
| `json` | get_json_object → PARSE_JSON + VARIANT access *(combined file)* |
| `topk` | ROW_NUMBER TopK, QUALIFY, datediff *(combined file)* |
| `dates` | to_date → PARSE_DATE, date_format → FORMAT_DATE/EXTRACT *(combined file)* |
| `arithmetic` | pmod, NULLIF division, subtraction *(combined file)* |

The JSON output contains:

```json
{
  "feldera_schema": "...",   // translated DDL (CREATE TABLE statements)
  "feldera_query": "...",    // translated query (CREATE VIEW statements)
  "unsupported": [...],      // Spark features with no Feldera equivalent
  "warnings": [...],         // non-fatal notes (compiler repairs, validation result)
  "explanations": [...],     // explanations for translation decisions
  "status": "success|unsupported|error"
}
```

**`status`:**
- `success` — translated, and (with `--validate`) compiled cleanly.
- `unsupported` — translated, but some constructs have no Feldera equivalent and
  were emitted as `CAST(NULL AS <type>)` placeholders (listed in `unsupported`).
- `error` — the LLM response couldn't be parsed, or (with `--validate`) the output
  still failed to compile after the repair attempts. felderize always returns the
  best-effort SQL in `feldera_query` (salvaging it from the raw response when the
  reply wasn't valid JSON), with the compiler errors in `warnings` — it may not
  compile, but it always attempts a translation.

### Translate your own SQL

Each form below writes the translated, deployable Feldera SQL to a file. The
file leads with a comment header recording the translation status and any
unsupported constructs / warnings, so it is self-documenting; the status is also
printed to stderr.

**Separate schema and query files:**
```bash
felderize spark translate schema.sql query.sql --validate -o out.sql
# → out.sql   (the translated CREATE TABLE + CREATE VIEW)
```

**Single combined file** (CREATE TABLE and CREATE VIEW statements in one file):
```bash
felderize spark translate-file combined.sql --validate -o out.sql
# → out.sql
```

**Multiple query files against a shared schema** (batch — faster than a shell loop):
```bash
felderize spark translate-batch schema.sql queries/*.sql --validate --output-dir out/
# → out/<query>_feldera.sql, one per query
```

**Structured JSON instead of a `.sql` file** (for automation — parse with `jq`):
```bash
felderize spark translate schema.sql query.sql --validate --json-output > result.json
# → result.json with feldera_schema, feldera_query, status, unsupported, warnings
```

`translate-batch` processes all queries in a single process so doc and example
caches stay warm across queries. Omitting `-o` / `--output-dir` / `--json-output`
prints the result as readable sections to the terminal instead.

> **Note:** Running without `--validate` prints a warning — the output SQL has not been verified against the Feldera compiler.

All commands accept:
- `--validate` to validate output against the Feldera compiler (opt-in; `example` validates by default, use `--no-validate` to skip)
- `--compiler PATH` to specify the path to the Feldera compiler binary (overrides `FELDERA_COMPILER` env var)
- `--model MODEL` to specify the LLM model (overrides `FELDERIZE_MODEL` env var)
- `--no-docs` to disable Feldera SQL reference docs in the prompt
- `--verbose` to log the SQL submitted to the validator at each repair attempt
- `--json-output` to output results as JSON (the structured machine interface)
- `-o, --output PATH` (`translate` / `translate-file`) to write the translated
  schema + views to a deployable `.sql` file; the status prints to stderr so
  stdout/the file stay clean. (`translate-batch` uses `--output-dir` instead.)

### Programmatic API (Python)

To call felderize from your own code instead of shelling out to the CLI, use the
single entry point `translate_spark_to_feldera`:

```python
from felderize import translate_spark_to_feldera, Config, Status

cfg = Config.from_env()          # reads ANTHROPIC_API_KEY, FELDERA_COMPILER, FELDERIZE_MODEL
result = translate_spark_to_feldera(
    schema_sql,                  # Spark CREATE TABLE ... DDL (str)
    query_sql,                   # Spark CREATE VIEW / SELECT ... (str)
    cfg,
    validate=True,               # compile against the Feldera compiler and repair (default: False)
)

if result.status is Status.SUCCESS:
    deploy(result.feldera_schema, result.feldera_query)
else:
    # UNSUPPORTED -> NULL-placeholder views (see result.unsupported);
    # ERROR       -> best-effort SQL that did not compile.
    review(result.unsupported, result.warnings)
```

`TranslationResult` exposes `feldera_schema`, `feldera_query`, `status`,
`unsupported`, `warnings`, `explanations`, and `to_dict()`. `validate=False`
skips the compiler (faster, but the output is not verified).

Runnable examples:

```bash
.venv/bin/python examples/api_usage.py            # translate one schema + query
.venv/bin/python examples/translate_all_examples.py   # translate all built-in examples + summary
```

## Configuration

Environment variables (set in `.env`):

| Variable | Description | Default |
|---|---|---|
| `ANTHROPIC_API_KEY` | Anthropic API key | (required) |
| `FELDERIZE_MODEL` | LLM model to use (can also be set with `--model`) | (required, set in `.env`) |
| `FELDERA_COMPILER` | Path to sql-to-dbsp compiler (can also be set with `--compiler`) | (optional; auto-downloaded when unset) |
| `FELDERIZE_AUTO_DOWNLOAD` | Auto-download the compiler on first `--validate` when none is configured. Set to `0`/`false` to disable. | `1` |
| `ANTHROPIC_BASE_URL` | Override Anthropic API base URL (for proxies or alternate endpoints) | (optional) |

## Customizing translation

You can teach felderize your project-specific patterns by adding **rules** and **examples**.

### Rules

Rules tell the LLM how to rewrite specific Spark constructs. Each `.md` file should start with a YAML frontmatter block with a `name:` and `description:` field, followed by plain-markdown bullet points:

```markdown
---
name: my-project-rules
description: Project-specific Spark-to-Feldera rewrites.
---

- **[PROJ-HASH] Internal UDF `my_hash(col)`:** Rewrite as `MD5(CAST(col AS VARCHAR))`.

- **[PROJ-ID] `CUSTOM_ID` columns:** Always map to `BIGINT NOT NULL` in Feldera.
```

> **Note:** Frontmatter is recommended. Files without it are still loaded but produce a warning.

Place `.md` files in one of these locations — all are loaded **automatically**, no flag needed:

| Location | Scope |
|---|---|
| `~/.felderize/rules/` | All your projects (survives pip upgrades) |
| `.felderize/rules/` in your project dir | This project only (commit to git) |

Or pass one or more files explicitly (repeatable):

```bash
felderize spark translate schema.sql query.sql --rules rules1.md --rules rules2.md
```

### Examples

Examples are validated Spark → Feldera pairs shown to the LLM alongside the built-in ones. The more precise your examples, the better the translation quality for your specific SQL patterns.

Each `.md` file must start with a YAML frontmatter block. Use `categories:` to load the example only when those SQL constructs are detected in the query being translated. Omit `categories:` (but keep the frontmatter) to always include the example:

```markdown
---
categories: [datetime]
---

### Example: Monthly revenue

**Spark SQL:**
```sql
SELECT date_trunc('MONTH', ts) AS month, SUM(amount) AS revenue
FROM sales GROUP BY date_trunc('MONTH', ts);
\```

**Feldera SQL:**
```sql
SELECT FLOOR(ts TO MONTH) AS month, SUM(amount) AS revenue
FROM sales GROUP BY FLOOR(ts TO MONTH);
\```
```

> **Note:** The frontmatter block (`---`) is required. Files without it are skipped.

Valid categories: `aggregates`, `string`, `datetime`, `array`, `json`, `map`, `types`.

Place `.md` files in one of these locations — loaded automatically, no flag needed:

| Location | Scope |
|---|---|
| `~/.felderize/examples/` | All your projects (survives pip upgrades) |
| `.felderize/examples/` in your project dir | This project only (commit to git) |

Or pass individual files or directories explicitly (repeatable, accepts both):

```bash
felderize spark translate schema.sql query.sql --examples ex1.md --examples my_examples/
```

## How it works

felderize translates the whole program (schema + all views) in a single LLM call:

1. Loads translation rules from the skill file (`felderize/skills/spark_skills.md`).
2. Trims the schema to the tables the query actually references, then sends the
   Spark schema + query to the LLM with the rules and validated examples.
3. Parses the translated Feldera SQL from the response. Constructs with no Feldera
   equivalent are emitted as `CAST(NULL AS <type>)` placeholders and listed in
   `unsupported`.
4. With `--validate`, compiles the output against the Feldera compiler and repairs
   it using the compiler's error feedback for up to a few attempts. If that first
   pass still doesn't compile, it retries once more with relevant Feldera
   documentation added to the prompt (from `docs.feldera.com/docs/sql/`); use
   `--no-docs` to skip the documentation pass.

## Support

Contact us at support@feldera.com for assistance with unsupported Spark SQL features.
