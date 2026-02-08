"""
Test PERCENTILE_CONT with spill-to-disk via NodeStorage against PostgreSQL.

Generates 200K rows to exceed the 1 MiB spill threshold, then exercises the
B+ tree through multiple phases: value modification across different regions,
bulk deletion (forcing node merges), and re-insertion (forcing tree regrowth).
Compares Feldera results against PostgreSQL at each phase.

Requires INTEGRATION_TESTS_POSTGRES_URI (skips otherwise).
"""

import random
import unittest
import uuid

from feldera import PipelineBuilder
from feldera.runtime_config import RuntimeConfig, Storage
from feldera.testutils import FELDERA_TEST_NUM_WORKERS, FELDERA_TEST_NUM_HOSTS
from tests import TEST_CLIENT, unique_pipeline_name
from tests.runtime.pg_utils import (
    pg_audit_mode, pg_connect, pg_delete, pg_insert, pg_update,
    send_deletes, send_rows,
)

SEED = 42
NUM_ROWS = 200_000
MIN_STORAGE_BYTES = 1_048_576  # 1 MiB
PERCENTILES = [0.25, 0.50, 0.75, 0.90, 0.99]
AUDIT_SCHEMA = "audit_spill"

# Derived from PERCENTILES — safe to change the list above.
PERCENTILE_KEYS = [f"p{int(p * 100)}" for p in PERCENTILES]

_pct_select = ",\n    ".join(
    f"PERCENTILE_CONT({p}) WITHIN GROUP (ORDER BY val) AS {k}"
    for p, k in zip(PERCENTILES, PERCENTILE_KEYS)
)
SQL = (
    "CREATE TABLE data_table (\n"
    "    id INT NOT NULL PRIMARY KEY,\n"
    "    val DOUBLE NOT NULL\n"
    ");\n\n"
    "CREATE MATERIALIZED VIEW percentile_view AS\n"
    f"SELECT\n    {_pct_select}\n"
    "FROM data_table;\n"
)


class TestPercentileSpill(unittest.TestCase):
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
            self.pg_schema = f"test_{uuid.uuid4().hex[:8]}"
        self._setup_postgres()

        if not self.audit:
            self.pipeline = PipelineBuilder(
                TEST_CLIENT,
                name=unique_pipeline_name("test_percentile_spill"),
                sql=SQL,
                runtime_config=RuntimeConfig(
                    workers=FELDERA_TEST_NUM_WORKERS,
                    hosts=FELDERA_TEST_NUM_HOSTS,
                    storage=Storage(min_storage_bytes=MIN_STORAGE_BYTES),
                ),
            ).create_or_replace()

    def tearDown(self):
        if hasattr(self, "pipeline"):
            self.pipeline.stop(force=True)
            self.pipeline.clear_storage()
        if hasattr(self, "pg_conn") and self.pg_conn is not None:
            try:
                if not getattr(self, "audit", False):
                    with self.pg_conn.cursor() as cur:
                        cur.execute(f"DROP SCHEMA IF EXISTS {self.pg_schema} CASCADE")
            finally:
                self.pg_conn.close()

    def _setup_postgres(self):
        with self.pg_conn.cursor() as cur:
            cur.execute(f"CREATE SCHEMA {self.pg_schema}")
            cur.execute(f"SET search_path TO {self.pg_schema}")
            cur.execute(
                "CREATE TABLE data_table "
                "(id INT PRIMARY KEY, val DOUBLE PRECISION NOT NULL)"
            )
            pct_exprs = ", ".join(
                f"PERCENTILE_CONT({p}) WITHIN GROUP (ORDER BY val) AS {k}"
                for p, k in zip(PERCENTILES, PERCENTILE_KEYS)
            )
            cur.execute(
                f"CREATE VIEW percentile_view AS "
                f"SELECT {pct_exprs} FROM data_table"
            )

        rng = random.Random(SEED)
        self.initial_rows = [
            {"id": i, "val": float(i) + rng.uniform(-0.5, 0.5)}
            for i in range(NUM_ROWS)
        ]
        pg_insert(self.pg_conn, "data_table", self.initial_rows)

    def _pg_query_percentiles(self):
        """Query percentiles from PostgreSQL."""
        with self.pg_conn.cursor() as cur:
            pct_exprs = ", ".join(
                f"PERCENTILE_CONT({p}) WITHIN GROUP (ORDER BY val)"
                for p in PERCENTILES
            )
            cur.execute(f"SELECT {pct_exprs} FROM data_table")
            row = cur.fetchone()
            return dict(zip(PERCENTILE_KEYS, (float(v) for v in row)))

    def _check_percentiles(self, stage_name):
        """Query Feldera and compare against PostgreSQL."""
        rows = list(self.pipeline.query("SELECT * FROM percentile_view"))
        self.assertEqual(len(rows), 1, f"{stage_name}: expected 1 row from percentile_view")
        actual = rows[0]

        pg_result = self._pg_query_percentiles()
        for key in PERCENTILE_KEYS:
            self.assertAlmostEqual(
                float(actual[key]),
                pg_result[key],
                places=6,
                msg=f"{stage_name}: {key} mismatch (Feldera vs PostgreSQL)",
            )

    def test_percentile_cont_with_spill(self):
        if self.audit:
            self.skipTest(
                f"PG_TESTS_AUDIT: {AUDIT_SCHEMA} schema ready — "
                f"data_table ({NUM_ROWS} rows), percentile_view"
            )

        # --- Phase 1: Send initial rows to Feldera (PG already populated in setUp) ---
        self.pipeline.start()
        send_rows(self.pipeline, "data_table", self.initial_rows)
        self.pipeline.wait_for_idle()
        self._check_percentiles("Phase 1 (initial)")

        # --- Phase 2: Modify first third — push above original range ---
        rng2 = random.Random(SEED + 1)
        first_third_end = NUM_ROWS // 3
        updates = []
        for i in range(first_third_end):
            new_val = rng2.uniform(NUM_ROWS + 1, NUM_ROWS * 2)
            updates.append((i, new_val))

        rows_2 = [{"id": i, "val": v} for i, v in updates]
        pg_update(self.pg_conn, "data_table", "id", "val", updates)
        send_rows(self.pipeline, "data_table", rows_2)
        self.pipeline.wait_for_idle()
        self._check_percentiles("Phase 2 (first third modified)")

        # --- Phase 3: Modify last third — push below original range ---
        rng3 = random.Random(SEED + 2)
        last_third_start = NUM_ROWS - (NUM_ROWS // 3)
        updates = []
        for i in range(last_third_start, NUM_ROWS):
            new_val = rng3.uniform(-NUM_ROWS * 2, -(NUM_ROWS + 1))
            updates.append((i, new_val))

        rows_3 = [{"id": i, "val": v} for i, v in updates]
        pg_update(self.pg_conn, "data_table", "id", "val", updates)
        send_rows(self.pipeline, "data_table", rows_3)
        self.pipeline.wait_for_idle()
        self._check_percentiles("Phase 3 (last third modified)")

        # --- Phase 4: Delete every other row ---
        # This halves the B+ tree, forcing significant node merges and
        # eviction of leaves that are no longer needed.
        delete_ids = list(range(0, NUM_ROWS, 2))  # even ids
        pg_delete(self.pg_conn, "data_table", "id", delete_ids)
        send_deletes(self.pipeline, "data_table", delete_ids)
        self.pipeline.wait_for_idle()
        self._check_percentiles("Phase 4 (every other row deleted)")

        # --- Phase 5: Re-insert deleted rows with new extreme values ---
        # Re-inserts rows spanning a wide range, forcing the tree to
        # grow back while previously evicted nodes are being reloaded.
        rng4 = random.Random(SEED + 3)
        reinsert_rows = [
            {"id": id_, "val": rng4.uniform(-NUM_ROWS * 2.5, NUM_ROWS * 2.5)}
            for id_ in delete_ids
        ]
        pg_insert(self.pg_conn, "data_table", reinsert_rows)
        send_rows(self.pipeline, "data_table", reinsert_rows)
        self.pipeline.wait_for_idle()
        self._check_percentiles("Phase 5 (deleted rows re-inserted)")

        # --- Phase 6: Bulk delete first half, then modify second half ---
        # Exercises deletion + modification in the same pipeline step.
        first_half_ids = list(range(0, NUM_ROWS // 2))
        rng5 = random.Random(SEED + 4)
        updates = [
            (i, rng5.uniform(0, NUM_ROWS * 0.005))
            for i in range(NUM_ROWS // 2, NUM_ROWS)
        ]
        rows_6 = [{"id": i, "val": v} for i, v in updates]

        pg_delete(self.pg_conn, "data_table", "id", first_half_ids)
        pg_update(self.pg_conn, "data_table", "id", "val", updates)
        send_deletes(self.pipeline, "data_table", first_half_ids)
        send_rows(self.pipeline, "data_table", rows_6)
        self.pipeline.wait_for_idle()
        self._check_percentiles("Phase 6 (first half deleted, second half modified)")


if __name__ == "__main__":
    unittest.main()
