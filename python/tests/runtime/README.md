## Running tests that compare against PostgreSQL locally

### Prerequisites

- Running Feldera instance (`FELDERA_HOST`, default `http://localhost:8080`)
- PostgreSQL instance for comparison tests

### Run

```bash
cd python
export INTEGRATION_TESTS_POSTGRES_URI="postgresql://postgres:password@localhost:5432/postgres"

# Spill test (200K rows):
uv run pytest tests/runtime/test_pg_percentile_spill.py -v --timeout=600

# Multi-table program test:
uv run pytest tests/runtime/test_pg_percentile_programs.py -v --timeout=300

# Both tests together:
uv run pytest tests/runtime/test_pg_percentile_spill.py tests/runtime/test_pg_percentile_programs.py -v --timeout=600
```

Without `INTEGRATION_TESTS_POSTGRES_URI` both tests are skipped.

### Audit: inspect the PostgreSQL tables and data without Feldera

Set `PG_TESTS_AUDIT=1` to populate PostgreSQL and skip the Feldera comparison.
Each test creates a well-known schema (`audit_spill`, `audit_programs`)
that persists after the run:

```bash
cd python
export INTEGRATION_TESTS_POSTGRES_URI="postgresql://postgres:password@localhost:5432/postgres"
export PG_TESTS_AUDIT=1

# Populate both schemas:
uv run pytest tests/runtime/test_pg_percentile_spill.py tests/runtime/test_pg_percentile_programs.py -v
```

Then in `psql`:

```sql
SET search_path TO audit_spill;
SELECT * FROM percentile_view;
SELECT COUNT(*) FROM data_table;          -- 200 000 rows

SET search_path TO audit_programs;
SELECT * FROM vw_company_monthly_salary_distribution LIMIT 5;
SELECT * FROM vw_company_salary_inequality LIMIT 5;
SELECT * FROM vw_company_growth_distribution;

-- clean up
DROP SCHEMA audit_spill CASCADE;
DROP SCHEMA audit_programs CASCADE;
```
