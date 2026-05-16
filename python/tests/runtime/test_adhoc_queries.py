import json
import os
import tempfile
from pathlib import Path

from tests import TEST_CLIENT
from tests.shared_test_pipeline import SharedTestPipeline, sql


class TestAdhocQueries(SharedTestPipeline):
    @property
    def pipeline(self):
        return self.p

    def _count(self, sql: str) -> int:
        rows = list(self.pipeline.query(sql))
        if not rows:
            return 0
        row = rows[0]
        for k in ("c", "count", "count(*)"):
            if k in row:
                return row[k]
        # Fallback: first value
        return next(iter(row.values()))

    @sql("""
    CREATE TABLE not_materialized(id bigint not null);

    -- Materialized because of PK constraint.
    CREATE TABLE pk_table_materialized(id bigint not null primary key)
    WITH (
      'connectors' = '[{
        "transport": {
          "name": "datagen",
          "config": {
            "plan": [{
              "limit": 5
            }]
          }
        }
      }]'
    );

    -- Not materialized because of lateness attribute on the primary key column.
    CREATE TABLE lateness_table1_not_materialized(id bigint not null primary key lateness 0)
    WITH (
      'connectors' = '[{
        "transport": {
          "name": "datagen",
          "config": {
            "plan": [{
              "limit": 5
            }]
          }
        }
      }]'
    );

    -- Materialized because of explicit materialized attribute, despite lateness.
    CREATE TABLE lateness_table2_materialized(id bigint not null primary key lateness 0)
    WITH('materialized' = 'true');

    -- Materialized because lateness does not apply to primary key column.
    CREATE TABLE lateness_table3_materialized(id bigint not null primary key, other int not null lateness 0);

    CREATE TABLE t1 (
      id INT NOT NULL,
      dt DATE NOT NULL,
      uid UUID NOT NULL
    ) WITH (
      'materialized' = 'true',
      'connectors' = '[{
        "transport": {
          "name": "datagen",
          "config": {
            "plan": [{
              "limit": 5
            }]
          }
        }
      }]'
    );

    CREATE TABLE t2 (
      id INT NOT NULL,
      st VARCHAR NOT NULL
    ) WITH (
      'materialized' = 'true',
      'connectors' = '[{
        "transport": {
          "name": "datagen",
          "config": {
            "plan": [{
              "limit": 5
            }]
          }
        }
      }]'
    );

    CREATE MATERIALIZED VIEW joined AS (
      SELECT t1.dt AS c1, t2.st AS c2, t1.uid as c3
      FROM t1, t2
      WHERE t1.id = t2.id
    );

    CREATE MATERIALIZED VIEW view_of_not_materialized AS (
      SELECT * FROM not_materialized
    );
    """)
    def test_pipeline_adhoc_query(self):
        self.pipeline.start()

        self.pipeline.execute(
            "INSERT INTO lateness_table2_materialized VALUES (10),(20),(30)"
        )

        # Dropped due to lateness.
        self.pipeline.execute(
            "INSERT INTO lateness_table2_materialized VALUES (1),(2),(3)"
        )

        self.pipeline.execute(
            "INSERT INTO lateness_table3_materialized VALUES (10, 10), (20, 20), (30, 30)"
        )

        # Dropped due to lateness.
        self.pipeline.execute(
            "INSERT INTO lateness_table3_materialized VALUES (1,1),(2,2),(3,3)"
        )

        # Wait for all datagen and ad hoc connectors to finish.
        self.pipeline.wait_for_completion()

        ADHOC_SQL_A = "SELECT * FROM joined"
        ADHOC_SQL_B = "SELECT t1.dt AS c1, t2.st AS c2, t1.uid as c3 FROM t1, t2 WHERE t1.id = t2.id"
        ADHOC_SQL_INSERT = (
            "INSERT INTO t1 VALUES "
            "(99,'2020-01-01','c32d330f-5757-4ada-bcf6-1fac2d54e37f'),"
            "(100,'2020-01-01','00000000-0000-0000-0000-000000000000')"
        )

        # Baseline row count
        assert self._count("SELECT COUNT(*) AS c FROM t1") == 5

        # Compare logically equivalent queries in text + json formats.
        # Text format via TEST_CLIENT (ASCII table)
        text_a = "\n".join(TEST_CLIENT.query_as_text(self.pipeline.name, ADHOC_SQL_A))
        text_b = "\n".join(TEST_CLIENT.query_as_text(self.pipeline.name, ADHOC_SQL_B))

        def _extract_table_lines(txt: str):
            return {
                ln.strip()
                for ln in txt.splitlines()
                if ln.strip() and not ln.startswith("+") and not ln.startswith("| c1")
            }

        assert _extract_table_lines(text_a) == _extract_table_lines(text_b)

        # JSON lines (unordered compare)
        json_a = list(TEST_CLIENT.query_as_json(self.pipeline.name, ADHOC_SQL_A))
        json_b = list(TEST_CLIENT.query_as_json(self.pipeline.name, ADHOC_SQL_B))

        def norm(rows):
            return sorted((json.dumps(r, sort_keys=True) for r in rows))

        assert norm(json_a) == norm(json_b)

        # Parquet comparison with ORDER BY to establish deterministic ordering.
        pf1 = tempfile.NamedTemporaryFile(
            prefix="adhoc_a_", suffix=".parquet", delete=False
        )
        pf2 = tempfile.NamedTemporaryFile(
            prefix="adhoc_b_", suffix=".parquet", delete=False
        )
        pf1.close()
        pf2.close()
        try:
            TEST_CLIENT.query_as_parquet(
                self.pipeline.name, f"{ADHOC_SQL_A} ORDER BY c1,c2,c3", pf1.name[:-8]
            )
            TEST_CLIENT.query_as_parquet(
                self.pipeline.name, f"{ADHOC_SQL_B} ORDER BY c1,c2,c3", pf2.name[:-8]
            )
            b1 = Path(pf1.name).read_bytes()
            b2 = Path(pf2.name).read_bytes()
            assert b1, "Parquet output empty"
            assert b1 == b2, "Parquet outputs differ for equivalent queries"
        finally:
            for p in (
                pf1.name,
                pf2.name,
                pf1.name[:-8] + ".parquet",
                pf2.name[:-8] + ".parquet",
            ):
                try:
                    os.remove(p)
                except OSError:
                    pass

        # Case-sensitive quoted table
        quoted_rows = list(self.pipeline.query('SELECT * FROM "TaBle1"'))
        assert quoted_rows == [], "Expected quoted TaBle1 to be empty initially"

        # Invalid table -> expect error
        invalid_ok = False
        try:
            print(list(self.pipeline.query("SELECT * FROM invalid_table")))
        except Exception:
            invalid_ok = True
        assert invalid_ok, "Expected querying invalid_table to raise"

        # Expression runtime error (division by zero) - expect textual ERROR
        error_text = "\n".join(
            TEST_CLIENT.query_as_text(self.pipeline.name, "SELECT 1/0")
        )
        assert "ERROR" in error_text.upper()

        # Non-materialized table direct access should fail
        res = list(self.pipeline.query("SELECT * FROM not_materialized"))
        assert res and "error" in res[0]

        # INSERT into materialized t1 using JSON query endpoint (to retrieve count row)
        insert_resp = list(
            TEST_CLIENT.query_as_json(self.pipeline.name, ADHOC_SQL_INSERT)
        )
        assert insert_resp and insert_resp[0].get("count") == 2

        prepared_rows = list(
            self.pipeline.query(
                "PREPARE p AS SELECT COUNT(*) AS c FROM t1 WHERE id = $1;"
                " EXECUTE p(99);"
            )
        )
        assert prepared_rows and prepared_rows[0].get("c") == 1
        assert self._count("SELECT COUNT(*) AS c FROM t1") == 7

        # Non-materialized table via its materialized view
        assert self._count("SELECT COUNT(*) AS c FROM view_of_not_materialized") == 0
        ins_nm = list(
            TEST_CLIENT.query_as_json(
                self.pipeline.name, "INSERT INTO not_materialized VALUES (99),(100)"
            )
        )
        assert ins_nm and ins_nm[0].get("count") == 2
        assert self._count("SELECT COUNT(*) AS c FROM view_of_not_materialized") == 2

        assert self._count("SELECT COUNT(*) AS c FROM pk_table_materialized") == 5
        assert (
            self._count("SELECT COUNT(*) AS c FROM lateness_table2_materialized") == 3
        )
        assert (
            self._count("SELECT COUNT(*) AS c FROM lateness_table3_materialized") == 3
        )

        # FIXME: this should raise an exception, but it currently doesn't.
        # https://github.com/feldera/feldera/issues/4973
        result = list(
            self.pipeline.query(
                "SELECT COUNT(*) AS c FROM lateness_table1_not_materialized"
            )
        )
        assert len(result) == 1 and "Execution error" in str(result[0])

    @sql(
        """CREATE TABLE "TaBle1"(id bigint not null) WITH ('materialized' = 'true');"""
    )
    def test_pipeline_adhoc_query_empty(self):
        self.pipeline.start()
        rows = list(self.pipeline.query('SELECT COUNT(*) AS c FROM "TaBle1"'))

        c = rows[0].get("c")
        print(rows, rows[0], c)
        assert c == 0


class TestAdhocQueriesArrow(SharedTestPipeline):
    """Tests for the Arrow IPC adhoc query path.

    Arrow IPC is the recommended default because it preserves SQL type
    fidelity that JSON cannot. See
    https://github.com/feldera/feldera/issues/4219.
    """

    @property
    def pipeline(self):
        return self.p

    @sql("""
    CREATE TABLE t_dup (
      x INT NOT NULL,
      y INT NOT NULL
    ) WITH (
      'materialized' = 'true',
      'connectors' = '[{"transport": {"name": "datagen", "config": {"plan": [{"limit": 1,
        "fields": {"x": {"strategy": "uniform", "range": [10, 11]}, "y": {"strategy": "uniform", "range": [10, 11]}}}]}}}]'
    );

    CREATE TABLE s_dup (
      x INT NOT NULL,
      y INT NOT NULL
    ) WITH (
      'materialized' = 'true',
      'connectors' = '[{"transport": {"name": "datagen", "config": {"plan": [{"limit": 1,
        "fields": {"x": {"strategy": "uniform", "range": [20, 21]}, "y": {"strategy": "uniform", "range": [10, 11]}}}]}}}]'
    );
    """)
    def test_arrow_ipc_preserves_duplicate_column_names(self):
        """https://github.com/feldera/feldera/issues/4218

        ``SELECT T.x, S.x`` produces two output columns that both end up
        with the unqualified name ``x``. The Arrow IPC schema preserves
        both as positional fields, whereas JSON would silently drop one.
        """
        self.pipeline.start()
        self.pipeline.wait_for_completion()

        batches = list(
            self.pipeline.query_arrow(
                "SELECT t_dup.x, s_dup.x FROM t_dup, s_dup WHERE t_dup.y = s_dup.y"
            )
        )
        assert batches, "expected at least one batch"
        total_rows = sum(b.num_rows for b in batches)
        assert total_rows == 1, f"expected exactly one joined row, got {total_rows}"
        first = batches[0]
        assert first.num_columns == 2, (
            f"arrow stream must keep both columns, got {first.num_columns}"
        )
        # The arrow schema preserves both fields, even though their names
        # collide. We don't pin their names because DataFusion may qualify
        # them; what matters is that two distinct integer columns arrive.
        assert first.column(0).to_pylist() != first.column(1).to_pylist()

    @sql(
        """CREATE TABLE all_types (
          i8_col TINYINT NOT NULL,
          i16_col SMALLINT NOT NULL,
          i32_col INT NOT NULL,
          i64_col BIGINT NOT NULL,
          u8_col TINYINT UNSIGNED NOT NULL,
          u16_col SMALLINT UNSIGNED NOT NULL,
          u32_col INT UNSIGNED NOT NULL,
          u64_col BIGINT UNSIGNED NOT NULL,
          real_col REAL NOT NULL,
          double_col DOUBLE NOT NULL,
          dec_col DECIMAL(10, 2) NOT NULL,
          bool_col BOOL NOT NULL,
          str_col VARCHAR NOT NULL,
          char_col CHAR(5) NOT NULL,
          vbin_col VARBINARY NOT NULL,
          dt_col DATE NOT NULL,
          tm_col TIME NOT NULL,
          ts_col TIMESTAMP NOT NULL,
          uuid_col UUID NOT NULL,
          arr_col INT ARRAY NOT NULL,
          map_col MAP<VARCHAR, INT> NOT NULL
        ) WITH ('materialized' = 'true');"""
    )
    def test_arrow_ipc_round_trips_common_sql_types(self):
        """Smoke-test every commonly-used SQL primitive (and ARRAY plus
        MAP with VARCHAR keys) through the Arrow IPC pipe.

        Wide integers (``BIGINT``) and high-precision decimals are the
        cases the deprecated JSON path mishandles; we assert the values
        come back exactly. Types ``ROW``, ``VARIANT``, and ``INTERVAL``
        are exercised separately or not at all here since they are not
        plain column-level scalars in Feldera SQL.
        """
        self.pipeline.start()

        # Use an integer larger than 2**53 (JSON's f64 precision boundary)
        # to demonstrate a value we cannot represent in JSON.
        wide_int = 9007199254740993  # 2**53 + 1
        # `execute` drains the generator so the INSERT actually runs.
        self.pipeline.execute(
            "INSERT INTO all_types VALUES ("
            f"1, 2, 3, {wide_int}, "
            "10, 20, 30, 40, "
            "1.5, 3.141592653589793, "
            "12345.67, "
            "TRUE, 'hello', 'abc', "
            "x'deadbeef', "
            "DATE '2024-05-13', "
            "TIME '12:34:56', "
            "TIMESTAMP '2024-05-13 12:34:56', "
            "'c32d330f-5757-4ada-bcf6-1fac2d54e37f', "
            "ARRAY[10, 20, 30], "
            "MAP {'a': 1, 'b': 2}"
            ")"
        )

        batches = list(
            self.pipeline.query_arrow("SELECT * FROM all_types ORDER BY i32_col")
        )
        assert batches, "expected at least one batch"
        rows = batches[0].to_pylist()
        assert len(rows) == 1
        row = rows[0]

        assert row["i8_col"] == 1
        assert row["i16_col"] == 2
        assert row["i32_col"] == 3
        assert row["i64_col"] == wide_int, (
            f"BIGINT was {row['i64_col']!r}, expected {wide_int}"
        )
        assert row["u8_col"] == 10
        assert row["u16_col"] == 20
        assert row["u32_col"] == 30
        assert row["u64_col"] == 40
        # REAL is f32; expect exact match on a value with an exact binary
        # representation.
        assert row["real_col"] == 1.5
        assert row["double_col"] == 3.141592653589793
        # DECIMAL(10, 2) keeps two fractional digits without loss for a
        # value well within precision.
        from decimal import Decimal

        assert row["dec_col"] == Decimal("12345.67")
        assert row["bool_col"] is True
        assert row["str_col"] == "hello"
        assert row["char_col"] == "abc"
        assert row["vbin_col"] == b"\xde\xad\xbe\xef"
        import datetime

        assert row["dt_col"] == datetime.date(2024, 5, 13)
        assert row["tm_col"] == datetime.time(12, 34, 56)
        assert row["ts_col"] == datetime.datetime(2024, 5, 13, 12, 34, 56)
        # UUIDs come back as their canonical string form.
        assert row["uuid_col"] == "c32d330f-5757-4ada-bcf6-1fac2d54e37f"
        assert row["arr_col"] == [10, 20, 30]
        # MAP<VARCHAR, INT> arrives as a list of (key, value) tuples
        # because Arrow's `MapArray` preserves order.
        assert sorted(row["map_col"]) == [("a", 1), ("b", 2)]

    @sql(
        """CREATE TABLE int_keyed_map (
          m MAP<INT, VARCHAR> NOT NULL
        ) WITH ('materialized' = 'true');"""
    )
    def test_arrow_ipc_map_with_non_string_keys(self):
        """SQL ``MAP`` can have non-string keys; the JSON output format
        fails outright because JSON object keys must be strings. Arrow
        IPC carries the typed key through unchanged.
        """
        self.pipeline.start()

        self.pipeline.execute(
            "INSERT INTO int_keyed_map VALUES (MAP {1: 'one', 2: 'two'})"
        )

        # Arrow IPC: int keys preserved as ints.
        batches = list(self.pipeline.query_arrow("SELECT * FROM int_keyed_map"))
        assert batches, "expected at least one batch"
        rows = batches[0].to_pylist()
        assert len(rows) == 1
        assert sorted(rows[0]["m"]) == [(1, "one"), (2, "two")]

        # JSON: errors because the encoder cannot represent int keys.
        import warnings

        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            json_rows = list(
                TEST_CLIENT.query_as_json(
                    self.pipeline.name, "SELECT * FROM int_keyed_map"
                )
            )
        assert json_rows, "expected JSON to emit at least an error record"
        assert "error" in json_rows[-1], (
            f"expected JSON encoder to surface an error, got {json_rows!r}"
        )

    @sql(
        """CREATE TABLE has_variant (
          v VARIANT NOT NULL
        ) WITH ('materialized' = 'true');"""
    )
    def test_arrow_ipc_reads_variant(self):
        """``VARIANT`` is a dynamically-typed JSON-shaped value. The
        DataFusion ad-hoc layer rejects ``CAST(.. AS VARIANT)`` in
        ``INSERT`` literals, so we populate via the JSON ingress path
        and verify the value comes out over Arrow IPC as the canonical
        JSON string.
        """
        self.pipeline.start()

        self.pipeline.input_json("has_variant", [{"v": {"a": 1, "b": [2, 3]}}])

        batches = list(self.pipeline.query_arrow("SELECT * FROM has_variant"))
        assert batches, "expected at least one batch"
        rows = batches[0].to_pylist()
        assert len(rows) == 1
        # Arrow surfaces VARIANT as a UTF-8 string carrying the JSON
        # representation; whitespace is normalised by the encoder.
        assert rows[0]["v"] == '{"a":1,"b":[2,3]}'

    @sql(
        """CREATE TABLE floats (
          tag VARCHAR NOT NULL,
          d DOUBLE NOT NULL,
          r REAL NOT NULL
        ) WITH ('materialized' = 'true');"""
    )
    def test_arrow_ipc_round_trips_inf_and_nan(self):
        """``+Inf``, ``-Inf`` and ``NaN`` survive the Arrow IPC pipe as
        Python floats.
        """
        import math

        self.pipeline.start()
        # SQL has no `Infinity` / `NaN` literals; produce them via
        # division so the values reach Feldera as DOUBLE / REAL.
        self.pipeline.execute(
            "INSERT INTO floats VALUES "
            "('pos_inf',  1.0/0.0, CAST( 1.0/0.0 AS REAL)),"
            "('neg_inf', -1.0/0.0, CAST(-1.0/0.0 AS REAL)),"
            "('nan',      0.0/0.0, CAST( 0.0/0.0 AS REAL))"
        )

        batches = list(self.pipeline.query_arrow("SELECT * FROM floats ORDER BY tag"))
        assert batches, "expected at least one batch"
        rows = {r["tag"]: r for r in batches[0].to_pylist()}
        assert math.isinf(rows["pos_inf"]["d"]) and rows["pos_inf"]["d"] > 0
        assert math.isinf(rows["pos_inf"]["r"]) and rows["pos_inf"]["r"] > 0
        assert math.isinf(rows["neg_inf"]["d"]) and rows["neg_inf"]["d"] < 0
        assert math.isinf(rows["neg_inf"]["r"]) and rows["neg_inf"]["r"] < 0
        assert math.isnan(rows["nan"]["d"])
        assert math.isnan(rows["nan"]["r"])


class TestAdhocReadAfterWrite(SharedTestPipeline):
    """Regression tests for
    https://github.com/feldera/feldera/issues/6243 .

    A multi-statement adhoc request must see its own intermediate
    INSERTs in the trailing SELECT. The earlier bug was that the SELECT
    ran against the snapshot captured before the request started, so
    inserts in the same request stayed invisible.
    """

    @property
    def pipeline(self):
        return self.p

    @sql(
        """CREATE TABLE example (
          id INT NOT NULL PRIMARY KEY
        ) WITH ('materialized' = 'true');"""
    )
    def test_insert_then_select_in_same_request_sees_inserts(self):
        self.pipeline.start()

        rows = list(
            self.pipeline.query(
                "INSERT INTO example VALUES (2222);"
                " INSERT INTO example VALUES (3333);"
                " SELECT COUNT(*) AS c FROM example"
            )
        )
        assert rows, "trailing SELECT must return a row"
        assert rows[0].get("c") == 2, (
            f"trailing SELECT should see both intermediate inserts, got {rows!r}"
        )

    @sql(
        """CREATE TABLE example2 (
          id INT NOT NULL PRIMARY KEY
        ) WITH ('materialized' = 'true');"""
    )
    def test_multi_statement_query_during_open_transaction(self):
        """While a user transaction is open, `update_snapshot()` is
        skipped, so the trailing SELECT only sees the pre-transaction
        baseline. Pin that shape so we notice if the gating changes.
        """
        self.pipeline.start()

        # Seed one row outside the transaction as the baseline.
        self.pipeline.execute("INSERT INTO example2 VALUES (1)")

        tid = self.pipeline.start_transaction()
        rows_during = list(
            self.pipeline.query(
                "INSERT INTO example2 VALUES (2);"
                " INSERT INTO example2 VALUES (3);"
                " SELECT COUNT(*) AS c FROM example2"
            )
        )
        self.pipeline.commit_transaction(transaction_id=tid, wait=True)

        assert rows_during, "trailing SELECT must return a row"
        count_during = rows_during[0].get("c")
        assert count_during == 1, (
            f"during the open transaction the SELECT must observe only "
            f"the pre-transaction baseline, got {count_during}"
        )

        # After commit, a fresh adhoc query must see all three rows.
        rows_after = list(self.pipeline.query("SELECT COUNT(*) AS c FROM example2"))
        assert rows_after and rows_after[0].get("c") == 3, (
            f"after commit, all three inserts must be visible, got {rows_after!r}"
        )
