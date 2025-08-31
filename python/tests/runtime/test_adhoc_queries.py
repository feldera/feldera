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

    @sql(
        """CREATE TABLE "TaBle1"(id bigint not null) WITH ('materialized' = 'true');"""
    )
    def test_pipeline_adhoc_query_empty(self):
        self.pipeline.start()
        rows = list(self.pipeline.query('SELECT COUNT(*) AS c FROM "TaBle1"'))

        c = rows[0].get("c")
        print(rows, rows[0], c)
        assert c == 0
