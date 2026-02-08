"""
Test PERCENTILE_CONT with spill-to-disk via NodeStorage.

Generates 200K rows to exceed the 1 MiB spill threshold, then exercises the
B+ tree through multiple phases: value modification across different regions,
bulk deletion (forcing node merges), and re-insertion (forcing tree regrowth).
Verifies correctness against Python-computed expected values and optionally
against PostgreSQL.

Set PERCENTILE_TEST_POSTGRES_URI to enable PostgreSQL comparison, e.g.:
    PERCENTILE_TEST_POSTGRES_URI="postgresql://user:pass@localhost:5432/db"
"""

import os
import random
import unittest
import uuid

from feldera import PipelineBuilder
from feldera.runtime_config import RuntimeConfig, Storage
from feldera.testutils import FELDERA_TEST_NUM_WORKERS, FELDERA_TEST_NUM_HOSTS
from tests import TEST_CLIENT, unique_pipeline_name

SEED = 42
NUM_ROWS = 200_000
MIN_STORAGE_BYTES = 1_048_576  # 1 MiB
BATCH_SIZE = 5_000
PERCENTILES = [0.25, 0.50, 0.75, 0.90, 0.99]
PERCENTILE_KEYS = ["p25", "p50", "p75", "p90", "p99"]

SQL = """\
CREATE TABLE data_table (
    id INT NOT NULL PRIMARY KEY,
    val DOUBLE NOT NULL
);

CREATE MATERIALIZED VIEW percentile_view AS
SELECT
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY val) AS p25,
    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY val) AS p50,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY val) AS p75,
    PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY val) AS p90,
    PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY val) AS p99
FROM data_table;
"""


def percentile_cont(sorted_data, pct):
    """Compute PERCENTILE_CONT per SQL standard."""
    n = len(sorted_data)
    if n == 0:
        return None
    if n == 1:
        return sorted_data[0]
    pos = pct * (n - 1)
    lower = int(pos)
    frac = pos - lower
    if lower + 1 >= n:
        return sorted_data[lower]
    return sorted_data[lower] + frac * (sorted_data[lower + 1] - sorted_data[lower])


class TestPercentileSpill(unittest.TestCase):
    def setUp(self):
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

        # Optional PostgreSQL connection
        self.pg_conn = None
        self.pg_table = None
        pg_uri = os.environ.get("PERCENTILE_TEST_POSTGRES_URI")
        if pg_uri:
            try:
                import psycopg2

                self.pg_conn = psycopg2.connect(pg_uri)
                self.pg_conn.autocommit = True
                self.pg_table = f"test_percentile_spill_{uuid.uuid4().hex[:8]}"
                with self.pg_conn.cursor() as cur:
                    cur.execute(
                        f"CREATE TABLE {self.pg_table} "
                        f"(id INT PRIMARY KEY, val DOUBLE PRECISION NOT NULL)"
                    )
            except ImportError:
                self.pg_conn = None

    def tearDown(self):
        self.pipeline.stop(force=True)
        self.pipeline.clear_storage()
        if self.pg_conn is not None:
            try:
                with self.pg_conn.cursor() as cur:
                    cur.execute(f"DROP TABLE IF EXISTS {self.pg_table}")
            finally:
                self.pg_conn.close()

    def _send_rows(self, rows):
        """Send insert/upsert rows to Feldera in batches."""
        for i in range(0, len(rows), BATCH_SIZE):
            self.pipeline.input_json("data_table", rows[i : i + BATCH_SIZE])

    def _send_deletes(self, ids):
        """Send delete operations to Feldera in batches."""
        delete_rows = [{"delete": {"id": id_}} for id_ in ids]
        for i in range(0, len(delete_rows), BATCH_SIZE):
            self.pipeline.input_json(
                "data_table",
                delete_rows[i : i + BATCH_SIZE],
                update_format="insert_delete",
            )

    def _pg_insert(self, data):
        """Batch insert into PostgreSQL."""
        if self.pg_conn is None:
            return
        with self.pg_conn.cursor() as cur:
            args = ",".join(
                cur.mogrify("(%s,%s)", (i, v)).decode()
                for i, v in enumerate(data)
            )
            cur.execute(f"INSERT INTO {self.pg_table} (id, val) VALUES {args}")

    def _pg_update(self, id_val_pairs):
        """Batch update PostgreSQL rows."""
        if self.pg_conn is None:
            return
        with self.pg_conn.cursor() as cur:
            for id_, val in id_val_pairs:
                cur.execute(
                    f"UPDATE {self.pg_table} SET val = %s WHERE id = %s",
                    (val, id_),
                )

    def _pg_delete(self, ids):
        """Batch delete PostgreSQL rows by id."""
        if self.pg_conn is None or not ids:
            return
        with self.pg_conn.cursor() as cur:
            id_list = ",".join(str(i) for i in ids)
            cur.execute(f"DELETE FROM {self.pg_table} WHERE id IN ({id_list})")

    def _pg_insert_rows(self, id_val_pairs):
        """Batch insert specific id-val pairs into PostgreSQL."""
        if self.pg_conn is None or not id_val_pairs:
            return
        with self.pg_conn.cursor() as cur:
            args = ",".join(
                cur.mogrify("(%s,%s)", (i, v)).decode() for i, v in id_val_pairs
            )
            cur.execute(f"INSERT INTO {self.pg_table} (id, val) VALUES {args}")

    def _pg_query_percentiles(self):
        """Query percentiles from PostgreSQL."""
        if self.pg_conn is None:
            return None
        with self.pg_conn.cursor() as cur:
            pct_exprs = ", ".join(
                f"PERCENTILE_CONT({p}) WITHIN GROUP (ORDER BY val)"
                for p in PERCENTILES
            )
            cur.execute(f"SELECT {pct_exprs} FROM {self.pg_table}")
            row = cur.fetchone()
            return dict(zip(PERCENTILE_KEYS, (float(v) for v in row)))

    def _check_percentiles(self, data, stage_name):
        """
        Query Feldera and compare against Python-computed expected values
        and optionally PostgreSQL.
        """
        sorted_data = sorted(data)
        expected = {
            key: percentile_cont(sorted_data, pct)
            for key, pct in zip(PERCENTILE_KEYS, PERCENTILES)
        }

        rows = list(self.pipeline.query("SELECT * FROM percentile_view"))
        self.assertEqual(len(rows), 1, f"{stage_name}: expected 1 row from percentile_view")
        actual = rows[0]

        for key in PERCENTILE_KEYS:
            self.assertAlmostEqual(
                float(actual[key]),
                expected[key],
                places=6,
                msg=f"{stage_name}: {key} mismatch (Feldera vs Python)",
            )

        # Optional PostgreSQL comparison
        pg_result = self._pg_query_percentiles()
        if pg_result is not None:
            for key in PERCENTILE_KEYS:
                self.assertAlmostEqual(
                    float(actual[key]),
                    pg_result[key],
                    places=6,
                    msg=f"{stage_name}: {key} mismatch (Feldera vs PostgreSQL)",
                )

    def test_percentile_cont_with_spill(self):
        # --- Generate initial data ---
        rng = random.Random(SEED)
        data = [float(i) + rng.uniform(-0.5, 0.5) for i in range(NUM_ROWS)]
        initial_rows = [{"id": i, "val": data[i]} for i in range(NUM_ROWS)]

        # --- Start pipeline ---
        self.pipeline.start()

        # --- Insert initial data into PostgreSQL ---
        self._pg_insert(data)

        # --- Phase 1: Insert initial 200K rows ---
        self._send_rows(initial_rows)
        self.pipeline.wait_for_idle()
        self._check_percentiles(data, "Phase 1 (initial)")

        # --- Phase 2: Modify first third (ids 0..66666) ---
        rng2 = random.Random(SEED + 1)
        first_third_end = NUM_ROWS // 3
        update_rows_2 = []
        pg_updates_2 = []
        for i in range(first_third_end):
            new_val = rng2.uniform(200001, 400000)
            data[i] = new_val
            update_rows_2.append({"id": i, "val": new_val})
            pg_updates_2.append((i, new_val))

        self._send_rows(update_rows_2)
        self._pg_update(pg_updates_2)
        self.pipeline.wait_for_idle()
        self._check_percentiles(data, "Phase 2 (first third modified)")

        # --- Phase 3: Modify last third (ids 133334..200000) ---
        rng3 = random.Random(SEED + 2)
        last_third_start = NUM_ROWS - (NUM_ROWS // 3)
        update_rows_3 = []
        pg_updates_3 = []
        for i in range(last_third_start, NUM_ROWS):
            new_val = rng3.uniform(-400000, -200001)
            data[i] = new_val
            update_rows_3.append({"id": i, "val": new_val})
            pg_updates_3.append((i, new_val))

        self._send_rows(update_rows_3)
        self._pg_update(pg_updates_3)
        self.pipeline.wait_for_idle()
        self._check_percentiles(data, "Phase 3 (last third modified)")

        # --- Phase 4: Delete every other row (100K deletions) ---
        # This halves the B+ tree, forcing significant node merges and
        # eviction of leaves that are no longer needed.
        delete_ids = list(range(0, NUM_ROWS, 2))  # even ids
        self._send_deletes(delete_ids)
        self._pg_delete(delete_ids)
        remaining_data = [data[i] for i in range(NUM_ROWS) if i % 2 != 0]
        self.pipeline.wait_for_idle()
        self._check_percentiles(remaining_data, "Phase 4 (every other row deleted)")

        # --- Phase 5: Re-insert deleted rows with new extreme values ---
        # Re-inserts 100K rows spanning a wide range, forcing the tree to
        # grow back while previously evicted nodes are being reloaded.
        rng4 = random.Random(SEED + 3)
        reinsert_rows = []
        pg_reinsert = []
        # Rebuild the full data array: odd ids keep their values,
        # even ids get new values.
        full_data = list(data)  # copy current state
        for id_ in delete_ids:
            new_val = rng4.uniform(-500000, 500000)
            full_data[id_] = new_val
            reinsert_rows.append({"id": id_, "val": new_val})
            pg_reinsert.append((id_, new_val))

        self._send_rows(reinsert_rows)
        self._pg_insert_rows(pg_reinsert)
        self.pipeline.wait_for_idle()
        self._check_percentiles(full_data, "Phase 5 (deleted rows re-inserted)")

        # --- Phase 6: Bulk delete first half, then modify second half ---
        # Exercises deletion + modification in the same pipeline step.
        first_half_ids = list(range(0, NUM_ROWS // 2))
        rng5 = random.Random(SEED + 4)
        second_half_updates = []
        pg_updates_6 = []
        for i in range(NUM_ROWS // 2, NUM_ROWS):
            new_val = rng5.uniform(0, 1000)
            full_data[i] = new_val
            second_half_updates.append({"id": i, "val": new_val})
            pg_updates_6.append((i, new_val))

        self._send_deletes(first_half_ids)
        self._send_rows(second_half_updates)
        self._pg_delete(first_half_ids)
        self._pg_update(pg_updates_6)
        remaining_data_6 = [full_data[i] for i in range(NUM_ROWS // 2, NUM_ROWS)]
        self.pipeline.wait_for_idle()
        self._check_percentiles(
            remaining_data_6, "Phase 6 (first half deleted, second half modified)"
        )


if __name__ == "__main__":
    unittest.main()
