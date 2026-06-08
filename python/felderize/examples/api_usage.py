#!/usr/bin/env python3
"""Example: translate Spark SQL to Feldera SQL with the felderize Python API.

Run from `python/felderize/` with the project venv (so `felderize` is importable
and ANTHROPIC_API_KEY is available, e.g. from a .env file):

    .venv/bin/python examples/api_usage.py
"""

from felderize import Config, Status, translate_spark_to_feldera

SCHEMA = """
CREATE TABLE orders (
  id BIGINT,
  region STRING,
  status STRING,
  amount DECIMAL(10, 2),
  created_at TIMESTAMP
) USING parquet;
"""

QUERY = """
CREATE OR REPLACE TEMP VIEW revenue_by_region AS
SELECT
  region,
  date_trunc('MONTH', created_at) AS month,
  SUM(amount) AS total
FROM orders
WHERE status = 'PAID'
GROUP BY region, date_trunc('MONTH', created_at);
"""


def main() -> None:
    # Reads ANTHROPIC_API_KEY, FELDERIZE_MODEL, and (for validate=True)
    # FELDERA_COMPILER from the environment / a .env file.
    cfg = Config.from_env()

    # validate=True compiles the output against the Feldera compiler and repairs
    # it using the compiler's feedback. Set validate=False to skip the compiler
    # (faster, but the output SQL is not verified).
    result = translate_spark_to_feldera(SCHEMA, QUERY, cfg, validate=False)

    print(f"status: {result.status.value}\n")
    print("-- Schema --")
    print(result.feldera_schema.strip())
    print("\n-- Query --")
    print(result.feldera_query.strip())

    if result.unsupported:
        print("\nunsupported:")
        for item in result.unsupported:
            print(f"  - {item}")
    if result.warnings:
        print("\nwarnings:")
        for warning in result.warnings:
            print(f"  - {warning}")

    # Branch on status the way a real integration would.
    if result.status is Status.SUCCESS:
        print("\nOK — the translated SQL is ready to deploy to a Feldera pipeline.")
    else:
        # Status.UNSUPPORTED -> some constructs became CAST(NULL ...) placeholders;
        # Status.ERROR       -> best-effort SQL that did not compile.
        print("\nReview needed — see the unsupported/warnings above.")


if __name__ == "__main__":
    main()
