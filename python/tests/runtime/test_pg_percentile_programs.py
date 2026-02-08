"""
Test realistic SQL programs with PERCENTILE_CONT against PostgreSQL.

Compares Feldera output against PostgreSQL for a multi-table salary program
with JOINs, GROUP BY, CTEs, window functions, and PERCENTILE_CONT.

Requires INTEGRATION_TESTS_POSTGRES_URI (skips otherwise).
"""

import unittest
import uuid

from feldera import PipelineBuilder
from feldera.runtime_config import RuntimeConfig
from feldera.testutils import FELDERA_TEST_NUM_WORKERS, FELDERA_TEST_NUM_HOSTS
from tests import TEST_CLIENT, unique_pipeline_name
from tests.runtime.pg_utils import pg_audit_mode, pg_connect, pg_insert, send_rows

# ---------------------------------------------------------------------------
# Shared SQL fragments
# ---------------------------------------------------------------------------

TABLE_DDL = """\
CREATE TABLE company (
  company_id   INTEGER NOT NULL PRIMARY KEY,
  company_name TEXT NOT NULL
);

CREATE TABLE worker (
  worker_id    INTEGER NOT NULL PRIMARY KEY,
  company_id   INTEGER NOT NULL,
  worker_name  TEXT NOT NULL,
  hire_date    DATE NOT NULL
);

CREATE TABLE worker_salary (
  worker_id     INTEGER NOT NULL,
  salary_month  DATE    NOT NULL,
  salary_amount NUMERIC(12,2) NOT NULL,
  PRIMARY KEY (worker_id, salary_month)
);
"""

# (name, select_body) — order matters: view 2 depends on view 1.
VIEWS = [
    (
        "vw_company_monthly_salary_distribution",
        """\
SELECT
  c.company_id,
  c.company_name,
  ws.salary_month,
  COUNT(*)                                                         AS workers_paid,
  AVG(ws.salary_amount)                                            AS avg_salary,
  PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY ws.salary_amount)   AS p50_median_salary,
  PERCENTILE_CONT(0.10) WITHIN GROUP (ORDER BY ws.salary_amount)   AS p10_salary,
  PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY ws.salary_amount)   AS p90_salary
FROM company c
JOIN worker w       ON w.company_id = c.company_id
JOIN worker_salary ws ON ws.worker_id = w.worker_id
GROUP BY c.company_id, c.company_name, ws.salary_month""",
    ),
    (
        "vw_company_salary_inequality",
        """\
SELECT
  company_id,
  company_name,
  salary_month,
  workers_paid,
  avg_salary,
  p50_median_salary,
  p10_salary,
  p90_salary,
  (p90_salary - p10_salary)            AS p90_minus_p10,
  (p90_salary / NULLIF(p10_salary, 0)) AS p90_to_p10_ratio,
  (p90_salary - p50_median_salary)     AS p90_minus_median,
  (p50_median_salary - p10_salary)     AS median_minus_p10
FROM vw_company_monthly_salary_distribution""",
    ),
    (
        "vw_company_growth_distribution",
        """\
WITH per_worker AS (
  SELECT
    w.company_id,
    w.worker_id,
    MIN(ws.salary_month) AS first_month,
    MAX(ws.salary_month) AS last_month,
    FIRST_VALUE(ws.salary_amount) OVER (
      PARTITION BY w.worker_id ORDER BY ws.salary_month
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS first_salary,
    FIRST_VALUE(ws.salary_amount) OVER (
      PARTITION BY w.worker_id ORDER BY ws.salary_month DESC
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS last_salary
  FROM worker w
  JOIN worker_salary ws ON ws.worker_id = w.worker_id
  GROUP BY w.company_id, w.worker_id, ws.salary_month, ws.salary_amount
),
growth AS (
  SELECT DISTINCT
    company_id,
    worker_id,
    first_month,
    last_month,
    first_salary,
    last_salary,
    (last_salary - first_salary)                    AS abs_growth,
    (last_salary / NULLIF(first_salary, 0) - 1.0)  AS pct_growth
  FROM per_worker
)
SELECT
  c.company_id,
  c.company_name,
  COUNT(*) AS workers_count,
  PERCENTILE_CONT(0.10) WITHIN GROUP (ORDER BY pct_growth) AS p10_pct_growth,
  PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY pct_growth) AS p50_pct_growth,
  PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY pct_growth) AS p90_pct_growth,
  AVG(pct_growth) AS avg_pct_growth
FROM growth g
JOIN company c ON c.company_id = g.company_id
GROUP BY c.company_id, c.company_name""",
    ),
]

AUDIT_SCHEMA = "audit_programs"

VIEW_SORT_KEYS = {
    "vw_company_monthly_salary_distribution": ["company_id", "salary_month"],
    "vw_company_salary_inequality": ["company_id", "salary_month"],
    "vw_company_growth_distribution": ["company_id"],
}

# Build Feldera SQL (MATERIALIZED VIEW for ad-hoc query access).
FELDERA_SQL = TABLE_DDL + "\n".join(
    f"CREATE MATERIALIZED VIEW {name} AS\n{body};\n" for name, body in VIEWS
)

# ---------------------------------------------------------------------------
# Test data (matches the original INSERT statements)
# ---------------------------------------------------------------------------

COMPANIES = [
    {"company_id": 1, "company_name": "Acme Manufacturing"},
    {"company_id": 2, "company_name": "Globex Logistics"},
]

WORKERS = [
    {"worker_id": 1,  "company_id": 1, "worker_name": "Ava Carter",    "hire_date": "2022-03-15"},
    {"worker_id": 2,  "company_id": 1, "worker_name": "Noah Brooks",   "hire_date": "2021-07-01"},
    {"worker_id": 3,  "company_id": 1, "worker_name": "Mia Nguyen",    "hire_date": "2020-11-20"},
    {"worker_id": 4,  "company_id": 1, "worker_name": "Liam Patel",    "hire_date": "2023-01-10"},
    {"worker_id": 5,  "company_id": 1, "worker_name": "Ethan Kim",     "hire_date": "2019-09-05"},
    {"worker_id": 6,  "company_id": 1, "worker_name": "Sophia Reed",   "hire_date": "2020-02-28"},
    {"worker_id": 7,  "company_id": 1, "worker_name": "Olivia Diaz",   "hire_date": "2021-12-12"},
    {"worker_id": 8,  "company_id": 1, "worker_name": "Lucas Allen",   "hire_date": "2022-06-30"},
    {"worker_id": 9,  "company_id": 1, "worker_name": "Isabella Fox",  "hire_date": "2018-04-18"},
    {"worker_id": 10, "company_id": 1, "worker_name": "James Ward",    "hire_date": "2023-05-08"},
    {"worker_id": 11, "company_id": 2, "worker_name": "Amelia Stone",  "hire_date": "2020-08-03"},
    {"worker_id": 12, "company_id": 2, "worker_name": "Benjamin Lee",  "hire_date": "2019-01-14"},
    {"worker_id": 13, "company_id": 2, "worker_name": "Charlotte Yu",  "hire_date": "2021-10-25"},
    {"worker_id": 14, "company_id": 2, "worker_name": "Henry Scott",   "hire_date": "2022-02-11"},
    {"worker_id": 15, "company_id": 2, "worker_name": "Harper Evans",  "hire_date": "2018-12-01"},
    {"worker_id": 16, "company_id": 2, "worker_name": "Jack Turner",   "hire_date": "2023-03-19"},
    {"worker_id": 17, "company_id": 2, "worker_name": "Evelyn Baker",  "hire_date": "2020-05-22"},
    {"worker_id": 18, "company_id": 2, "worker_name": "Daniel Clark",  "hire_date": "2021-04-09"},
    {"worker_id": 19, "company_id": 2, "worker_name": "Ella Rivera",   "hire_date": "2019-06-17"},
    {"worker_id": 20, "company_id": 2, "worker_name": "Michael Young", "hire_date": "2022-09-27"},
]

# Salary generation from deterministic pattern.
_MONTHLY_OFFSETS = [0, 35, 70, 55, 50, 90, 135, 130, 110, 105, 130, 165]
_WORKER_BASES = {
    1: 4250, 2: 4500, 3: 4750, 4: 5000, 5: 5250,
    6: 5500, 7: 5750, 8: 6000, 9: 6250, 10: 6500,
    11: 4750, 12: 5000, 13: 5250, 14: 5500, 15: 5750,
    16: 6000, 17: 6250, 18: 6500, 19: 6750, 20: 7000,
}
_YEAR_BUMP = 300


def _generate_salaries():
    rows = []
    for wid in sorted(_WORKER_BASES):
        base = _WORKER_BASES[wid]
        for yi, year in enumerate([2024, 2025]):
            bump = _YEAR_BUMP * yi
            for mi in range(12):
                rows.append({
                    "worker_id": wid,
                    "salary_month": f"{year}-{mi + 1:02d}-01",
                    "salary_amount": base + bump + _MONTHLY_OFFSETS[mi],
                })
    return rows


SALARIES = _generate_salaries()


# ---------------------------------------------------------------------------
# Test class
# ---------------------------------------------------------------------------


class TestPgPercentilePrograms(unittest.TestCase):
    """Compare Feldera PERCENTILE_CONT output against PostgreSQL."""

    def setUp(self):
        self.audit = pg_audit_mode()
        self.pg_conn = pg_connect()
        if self.pg_conn is None:
            self.skipTest(
                "PostgreSQL not available (set INTEGRATION_TESTS_POSTGRES_URI)"
            )

        if self.audit:
            self.pg_schema = AUDIT_SCHEMA
        else:
            # Isolated PG schema per test run to avoid conflicts.
            self.pg_schema = f"test_{uuid.uuid4().hex[:8]}"
        self._setup_postgres()

        if not self.audit:
            self.pipeline = PipelineBuilder(
                TEST_CLIENT,
                name=unique_pipeline_name("test_pg_percentile"),
                sql=FELDERA_SQL,
                runtime_config=RuntimeConfig(
                    workers=FELDERA_TEST_NUM_WORKERS,
                    hosts=FELDERA_TEST_NUM_HOSTS,
                ),
            ).create_or_replace()

    def tearDown(self):
        if hasattr(self, "pipeline"):
            self.pipeline.stop(force=True)
        if hasattr(self, "pg_conn") and self.pg_conn is not None:
            try:
                if not getattr(self, "audit", False):
                    with self.pg_conn.cursor() as cur:
                        cur.execute(f"DROP SCHEMA IF EXISTS {self.pg_schema} CASCADE")
            finally:
                self.pg_conn.close()

    # -- PostgreSQL setup ---------------------------------------------------

    def _setup_postgres(self):
        with self.pg_conn.cursor() as cur:
            cur.execute(f"CREATE SCHEMA {self.pg_schema}")
            cur.execute(f"SET search_path TO {self.pg_schema}")
            cur.execute(TABLE_DDL)
            for name, body in VIEWS:
                cur.execute(f"CREATE VIEW {name} AS {body}")
        pg_insert(self.pg_conn, "company", COMPANIES)
        pg_insert(self.pg_conn, "worker", WORKERS)
        pg_insert(self.pg_conn, "worker_salary", SALARIES)

    # -- Comparison helper --------------------------------------------------

    def _compare_view(self, view_name):
        sort_keys = VIEW_SORT_KEYS[view_name]

        # Feldera
        feldera_rows = list(
            self.pipeline.query(f"SELECT * FROM {view_name}")
        )
        # Normalize column names to lowercase.
        feldera_rows = [
            {k.lower(): v for k, v in row.items()} for row in feldera_rows
        ]

        # PostgreSQL
        with self.pg_conn.cursor() as cur:
            cur.execute(f"SET search_path TO {self.pg_schema}")
            cur.execute(f"SELECT * FROM {view_name}")
            col_names = [desc[0].lower() for desc in cur.description]
            pg_rows = [dict(zip(col_names, row)) for row in cur.fetchall()]

        # Sort
        def _sort_key(row):
            return tuple(str(row.get(k, "")) for k in sort_keys)

        feldera_rows.sort(key=_sort_key)
        pg_rows.sort(key=_sort_key)

        self.assertEqual(
            len(feldera_rows), len(pg_rows),
            f"{view_name}: row count mismatch "
            f"({len(feldera_rows)} feldera vs {len(pg_rows)} pg)",
        )

        for i, (f_row, p_row) in enumerate(zip(feldera_rows, pg_rows)):
            for col in sorted(f_row.keys()):
                f_val = f_row[col]
                p_val = p_row.get(col)
                if f_val is None and p_val is None:
                    continue
                if f_val is None or p_val is None:
                    self.fail(
                        f"{view_name}[{i}].{col}: NULL mismatch "
                        f"(feldera={f_val!r}, pg={p_val!r})"
                    )
                try:
                    self.assertAlmostEqual(
                        float(f_val), float(p_val), places=4,
                        msg=f"{view_name}[{i}].{col}",
                    )
                except (ValueError, TypeError):
                    self.assertEqual(
                        str(f_val), str(p_val),
                        msg=f"{view_name}[{i}].{col}",
                    )

    # -- Tests --------------------------------------------------------------

    def test_salary_program(self):
        """Load salary data, compare all views against PostgreSQL."""
        if self.audit:
            views = ", ".join(name for name, _ in VIEWS)
            self.skipTest(
                f"PG_TESTS_AUDIT: {AUDIT_SCHEMA} schema ready — "
                f"tables: company, worker, worker_salary; views: {views}"
            )

        self.pipeline.start()

        send_rows(self.pipeline, "company", COMPANIES)
        send_rows(self.pipeline, "worker", WORKERS)
        send_rows(self.pipeline, "worker_salary", SALARIES)
        self.pipeline.wait_for_idle()

        for name, _ in VIEWS:
            with self.subTest(view=name):
                self._compare_view(name)


if __name__ == "__main__":
    unittest.main()
