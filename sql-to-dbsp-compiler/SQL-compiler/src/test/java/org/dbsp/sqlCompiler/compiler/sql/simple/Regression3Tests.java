package org.dbsp.sqlCompiler.compiler.sql.simple;

import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.circuit.operator.DBSPFlatMapIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPHopOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSourceTableOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPWaterlineOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPWindowOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.TestUtil;
import org.dbsp.sqlCompiler.compiler.sql.tools.SqlIoTest;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.junit.Assert;
import org.junit.Test;

public class Regression3Tests extends SqlIoTest {
    @Test
    public void withSuggestion() {
        this.statementsFailingInCompilation("WITH V AS (SELECT 1) SELECT * FROM V;",
                "Raw 'SELECT' statements are not supported; did you forget to CREATE VIEW?");
    }

    @Test
    public void issue6400() {
        String sql = """
                CREATE TABLE t(b BOOLEAN);
                CREATE VIEW V AS SELECT b = 't' FROM T;""";
        DBSPCompiler compiler = this.chattyCompiler();
        compiler.submitStatementsForCompilation(sql);
        TestUtil.assertMessagesContain(compiler, """
                Suspicious argument: String 't' cannot be interpreted as a BOOLEAN
                    1|CREATE TABLE t(b BOOLEAN);
                    2|CREATE VIEW V AS SELECT b = 't' FROM T;
                                                  ^^^""");
    }

    @Test
    public void issue6342() {
        this.getCC("""
                CREATE TABLE dept_nested (
                  employees ROW(
                      detail ROW(
                          skills ROW(
                              desc VARCHAR
                          ) ARRAY
                      )
                  ) ARRAY
                );
                create view v as select * from dept_nested order by employees[1].detail.skills[2+3].desc""");

        this.getCC("""
                CREATE TABLE T (
                  id INT,
                  col ROW(field1 VARCHAR, field2 INT)
                );

                CREATE VIEW V AS
                SELECT id FROM T t ORDER BY (t.col).field2;""");
    }

    @Test
    public void issue5398() {
        var ccs = this.getCCS("""
                CREATE TABLE T(x INT, y INT, z INT);
                CREATE TABLE S(a INT, b INT);
                CREATE LOCAL VIEW V AS SELECT ROW(T.* EXCLUDE(x), ROW(S.* EXCLUDE(a))) AS R FROM T, S;
                CREATE VIEW W AS SELECT R[1], R[2], R[3][1] FROM V;""");
        ccs.stepWeightOne("""
                INSERT INTO T VALUES(0, 1, 2); INSERT INTO S VALUES(3, 4);""", """
                 y | z | b
                -----------
                 1 | 2 | 4""");
    }

    @Test
    public void issue5806() {
        // Temporal filters and joins can be swapped by Calcite optimizer
        var cc = this.getCC("""
                CREATE TABLE T(x INT, ts TIMESTAMP);
                CREATE TABLE S(x INT);
                CREATE VIEW W AS WITH V AS (SELECT * FROM T JOIN S ON T.x = S.x)
                SELECT * FROM V WHERE ts > NOW() - INTERVAL 1 DAY;""");
        cc.visit(new CircuitVisitor(cc.compiler) {
            boolean waterlineFound = false;
            boolean joinFound = false;

            @Override
            public void postorder(DBSPWaterlineOperator window) {
                // If the optimization works the waterline will be before the join
                Assert.assertFalse(this.joinFound);
                waterlineFound = true;
            }

            @Override
            public void postorder(DBSPStreamJoinOperator window) {
                // If the optimization works the waterline will be before the join
                Assert.assertTrue(waterlineFound);
                joinFound = true;
            }
        });
    }

    @Test
    public void issue5806a() {
        // Temporal filters and joins can be swapped by Calcite optimizer; test for LEFT JOIN
        var cc = this.getCC("""
                CREATE TABLE T(x INT, ts TIMESTAMP);
                CREATE TABLE S(x INT);
                CREATE VIEW W AS WITH V AS (SELECT * FROM T LEFT JOIN S ON T.x = S.x)
                SELECT * FROM V WHERE ts > TIMESTAMP '2020-01-01 10:00:00';""");
        cc.visit(new CircuitVisitor(cc.compiler) {
            boolean waterlineFound = false;
            boolean joinFound = false;

            @Override
            public void postorder(DBSPWaterlineOperator window) {
                // If the optimization works the waterline will be before the join
                Assert.assertFalse(this.joinFound);
                waterlineFound = true;
            }

            @Override
            public void postorder(DBSPStreamJoinOperator window) {
                // If the optimization works the waterline will be before the join
                Assert.assertTrue(waterlineFound);
                joinFound = true;
            }
        });
    }

    @Test
    public void testReplace() {
        var ccs = this.getCCS("""
                CREATE TABLE T(x INT, y INT);
                CREATE VIEW V AS SELECT * REPLACE(x+y AS y) FROM T;""");
        ccs.stepWeightOne("INSERT INTO T VALUES(1, 2)", """
                 x | y
                -------
                 1 | 3""");
    }

    @Test
    public void issue5858() {
        this.getCC("""
                CREATE TABLE purchase (
                ts TIMESTAMP NOT NULL LATENESS INTERVAL 1 HOUR,
                amount BIGINT
                ) WITH (
                'append_only' = 'true'
                );
                CREATE MATERIALIZED VIEW v1
                WITH ('emit_final' = 'a_ts')
                AS
                SELECT
                a.ts AS a_ts,
                a.amount + b.amount AS total
                FROM purchase a
                JOIN purchase b
                ON a.ts = b.ts;

                CREATE MATERIALIZED VIEW v2
                WITH ('emit_final' = 'a_ts')
                AS
                SELECT
                a.ts AS a_ts,
                a.amount + b.amount AS total
                FROM purchase a
                JOIN purchase b
                ON a.ts = b.ts + INTERVAL '0' SECOND;""");
    }

    @Test
    public void issue457_bool() {
        String program = """
                CREATE TABLE T (
                  payment_id INT,
                  customer_id INT,
                  amount INT,
                  seq BOOL
                );

                CREATE VIEW V AS SELECT
                  customer_id,
                  SUM(amount) OVER (PARTITION BY customer_id ORDER BY seq) AS previous
                FROM T;""";
        // Validated on postgres, which is NULLS LAST, so this needs explicit NULLS FIRST
        String data = """
                INSERT INTO T VALUES
                  -- Customer 1: increasing seq
                  (1, 1, 10, FALSE),
                  (2, 1, 20, FALSE),
                  (3, 1, -5, TRUE),
                  -- Customer 2: out‑of‑order seq
                  (4, 2, 7,  TRUE),
                  (5, 2, 0,  FALSE),
                  (6, 2, 3,  FALSE),
                  -- Customer 3: single row partition
                  (7, 3, 100, FALSE),
                  -- Customer 4: duplicate seq values (tests deterministic ordering)
                  (8, 4, 1,  FALSE),
                  (9, 4, 2,  FALSE),
                  (10,4, 3,  TRUE),
                  -- Null seq
                  (11,1, 3, NULL);
                """;
        String expected = """
                 customer_id | previous
                ------------------------
                 1               | 3
                 1               | 33
                 1               | 33
                 1               | 28
                 2               | 3
                 2               | 3
                 2               | 10
                 3               | 100
                 4               | 3
                 4               | 3
                 4               | 6""";

        var ccs = this.getCCS(program);
        ccs.stepWeightOne(data, expected);

        var program1 = program.replace("ORDER BY seq", "ORDER BY seq NULLS FIRST");
        ccs = this.getCCS(program1);
        ccs.stepWeightOne(data, expected);

        var program2 = program.replace("ORDER BY seq", "ORDER BY seq NULLS LAST");
        ccs = this.getCCS(program2);
        String expected2 = """
                 customer_id | previous
                ------------------------
                 1               | 30
                 1               | 30
                 1               | 25
                 1               | 28
                 2               | 3
                 2               | 3
                 2               | 10
                 3               | 100
                 4               | 3
                 4               | 3
                 4               | 6""";
        ccs.stepWeightOne(data, expected2);
    }

    @Test
    public void issue457_interval_short_nullable() {
        String program = """
                CREATE TABLE T (
                  payment_id INT,
                  customer_id INT,
                  amount INT,
                  d1 TIMESTAMP,
                  d2 TIMESTAMP
                );

                CREATE VIEW V AS SELECT
                  customer_id,
                  SUM(amount) OVER (PARTITION BY customer_id ORDER BY (d1 - d2) HOURS) AS previous
                FROM T;""";
        // Validated on postgres, which is NULLS LAST, so this needs explicit NULLS FIRST
        String data = """
                INSERT INTO T VALUES
                  -- Customer 1: clean increasing
                   (1, 1, 10, TIMESTAMP '2020-01-01 10:00:00', TIMESTAMP '2020-01-01 09:00:00'),
                   (2, 1, 20, TIMESTAMP '2020-01-01 12:00:00', TIMESTAMP '2020-01-01 09:00:00'),
                   (3, 1, -5, TIMESTAMP '2020-01-01 16:00:00', TIMESTAMP '2020-01-01 09:00:00'),
                   -- Customer 2: out‑of‑order
                   (4, 2, 7,  TIMESTAMP '2020-01-01 10:00:00', TIMESTAMP '2020-01-01 09:00:00'),
                   (5, 2, 0,  TIMESTAMP '2020-01-01 8:00:00', TIMESTAMP '2020-01-01 09:00:00'),
                   (6, 2, 3,  TIMESTAMP '2020-01-01 3:00:00', TIMESTAMP '2020-01-01 09:00:00'),
                   -- Customer 3: single row partition
                   (7, 3, 100, TIMESTAMP '2020-01-01 10:00:00', TIMESTAMP '2020-01-01 09:00:00'),
                   -- Customer 4: duplicate values
                   (8, 4, 1,  TIMESTAMP '2020-01-01 10:00:00', TIMESTAMP '2020-01-01 09:00:00'),
                   (9, 4, 2,  TIMESTAMP '2020-01-01 10:00:00', TIMESTAMP '2020-01-01 09:00:00'),
                   (10,4, 3,  TIMESTAMP '2020-01-01 10:00:00', TIMESTAMP '2020-01-01 09:00:00'),
                   -- Null seq
                   (11,1, 3, NULL, NULL);
                """;
        String expected = """
                 customer_id | previous
                ------------------------
                 1               | 3
                 1               | 13
                 1               | 33
                 1               | 28
                 2               | 3
                 2               | 3
                 2               | 10
                 3               | 100
                 4               | 6
                 4               | 6
                 4               | 6""";

        var ccs = this.getCCS(program);
        ccs.stepWeightOne(data, expected);

        var program1 = program.replace("HOURS", "HOURS NULLS FIRST");
        ccs = this.getCCS(program1);
        ccs.stepWeightOne(data, expected);

        var program2 = program.replace("HOURS", "HOURS NULLS LAST");
        ccs = this.getCCS(program2);
        String expected2 = """
                 customer_id | previous
                ------------------------
                 1               | 10
                 1               | 30
                 1               | 25
                 1               | 28
                 2               | 3
                 2               | 3
                 2               | 10
                 3               | 100
                 4               | 6
                 4               | 6
                 4               | 6""";
        ccs.stepWeightOne(data, expected2);
    }

    @Test
    public void issue457_interval_long() {
        String program = """
                CREATE TABLE T (
                  payment_id INT,
                  customer_id INT,
                  amount INT,
                  d1 TIMESTAMP,
                  d2 TIMESTAMP
                );

                CREATE VIEW V AS SELECT
                  customer_id,
                  SUM(amount) OVER (PARTITION BY customer_id ORDER BY (d1 - d2) MONTHS) AS previous
                FROM T;""";
        // Validated on postgres, which is NULLS LAST, so this needs explicit NULLS FIRST
        String data = """
                INSERT INTO T VALUES
                  -- Customer 1: clean increasing
                   (1, 1, 10, TIMESTAMP '2021-01-01 10:00:00', TIMESTAMP '2020-01-01 09:00:00'),
                   (2, 1, 20, TIMESTAMP '2023-01-01 12:00:00', TIMESTAMP '2020-01-01 09:00:00'),
                   (3, 1, -5, TIMESTAMP '2027-01-01 16:00:00', TIMESTAMP '2020-01-01 09:00:00'),
                   -- Customer 2: out‑of‑order
                   (4, 2, 7,  TIMESTAMP '2021-01-01 10:00:00', TIMESTAMP '2020-01-01 09:00:00'),
                   (5, 2, 0,  TIMESTAMP '2019-01-01 8:00:00', TIMESTAMP '2020-01-01 09:00:00'),
                   (6, 2, 3,  TIMESTAMP '2013-01-01 3:00:00', TIMESTAMP '2020-01-01 09:00:00'),
                   -- Customer 3: single row partition
                   (7, 3, 100, TIMESTAMP '2021-01-01 10:00:00', TIMESTAMP '2020-01-01 09:00:00'),
                   -- Customer 4: duplicate values
                   (8, 4, 1,  TIMESTAMP '2021-01-01 10:00:00', TIMESTAMP '2020-01-01 09:00:00'),
                   (9, 4, 2,  TIMESTAMP '2021-01-01 10:00:00', TIMESTAMP '2020-01-01 09:00:00'),
                   (10,4, 3,  TIMESTAMP '2021-01-01 10:00:00', TIMESTAMP '2020-01-01 09:00:00'),
                   -- Null seq
                   (11,1, 3, NULL, NULL);
                """;
        String expected = """
                 customer_id | previous
                ------------------------
                 1               | 3
                 1               | 13
                 1               | 33
                 1               | 28
                 2               | 3
                 2               | 3
                 2               | 10
                 3               | 100
                 4               | 6
                 4               | 6
                 4               | 6""";

        var ccs = this.getCCS(program);
        ccs.stepWeightOne(data, expected);

        var program1 = program.replace("MONTHS", "MONTHS NULLS FIRST");
        ccs = this.getCCS(program1);
        ccs.stepWeightOne(data, expected);

        var program2 = program.replace("MONTHS", "MONTHS NULLS LAST");
        ccs = this.getCCS(program2);
        String expected2 = """
                 customer_id | previous
                ------------------------
                 1               | 10
                 1               | 30
                 1               | 25
                 1               | 28
                 2               | 3
                 2               | 3
                 2               | 10
                 3               | 100
                 4               | 6
                 4               | 6
                 4               | 6""";
        ccs.stepWeightOne(data, expected2);
    }

    @Test
    public void issue457_interval_short() {
        // Test that programs sorting on non-nullable intervals and booleans compile
        String root = """
                CREATE TABLE T (
                  payment_id INT,
                  customer_id INT,
                  amount INT,
                  d1 TIMESTAMP NOT NULL,
                  d2 TIMESTAMP NOT NULL
                );

                CREATE VIEW V AS SELECT
                  customer_id,
                """;
        this.getCCS(root + "SUM(amount) OVER (PARTITION BY customer_id ORDER BY (d1 - d2) MONTHS) AS previous FROM T;");
        this.getCCS(root + "SUM(amount) OVER (PARTITION BY customer_id ORDER BY (d1 - d2) SECONDS) AS previous FROM T;");
        this.getCCS("""
                 CREATE TABLE T (
                  payment_id INT,
                  customer_id INT,
                  amount INT,
                  seq BOOL NOT NULL
                );

                CREATE VIEW V AS SELECT
                  customer_id,
                  SUM(amount) OVER (PARTITION BY customer_id ORDER BY seq) AS previous FROM T;""");
    }

    @Test
    public void issue6397() {
        // Validated on Postgres
        var ccs = this.getCCS("""
                CREATE TABLE T(d DOUBLE);
                CREATE VIEW V AS SELECT d, LAG(d) OVER(ORDER BY d) FROM T;""");
        ccs.stepWeightOne("INSERT INTO T VALUES (CAST('-Infinity' AS DOUBLE)), (0), (CAST('Infinity' AS DOUBLE));", """
                 d | lag
                ---------
                 -Infinity | NULL
                 0         | -Infinity
                 Infinity  | 0""");
    }

    @Test
    public void issue457_binary_nullable() {
        String program = """
                CREATE TABLE T (
                  payment_id INT,
                  customer_id INT,
                  amount INT,
                  seq BINARY(1)
                );

                CREATE VIEW V AS SELECT
                  customer_id,
                  SUM(amount) OVER (PARTITION BY customer_id ORDER BY seq) AS previous
                FROM T;""";
        // Validated on postgres, which is NULLS LAST, so this needs explicit NULLS FIRST;
        // replacing BINARY(1) with BYTEA
        String data = """
                INSERT INTO T VALUES
                  -- Customer 1: clean increasing
                   (1, 1, 10, x'01'),
                   (2, 1, 20, x'02'),
                   (3, 1, -5, x'03'),
                   -- Customer 2: out‑of‑order
                   (4, 2, 7,  x'02'),
                   (5, 2, 0,  x'01'),
                   (6, 2, 3,  x'03'),
                   -- Customer 3: single row partition
                   (7, 3, 100, x'03'),
                   -- Customer 4: duplicate values
                   (8, 4, 1,  x'01'),
                   (9, 4, 2,  x'01'),
                   (10,4, 3,  x'02'),
                   -- Null seq
                   (11,1, 3, NULL);
                """;
        String expected = """
                 customer_id | previous
                ------------------------
                 1               | 3
                 1               | 13
                 1               | 33
                 1               | 28
                 2               | 0
                 2               | 7
                 2               | 10
                 3               | 100
                 4               | 3
                 4               | 3
                 4               | 6""";

        var ccs = this.getCCS(program);
        ccs.stepWeightOne(data, expected);

        var program1 = program.replace("ORDER BY seq", "ORDER BY seq NULLS FIRST");
        ccs = this.getCCS(program1);
        ccs.stepWeightOne(data, expected);

        var program2 = program.replace("ORDER BY seq", "ORDER BY seq NULLS LAST");
        ccs = this.getCCS(program2);
        String expected2 = """
                 customer_id | previous
                ------------------------
                 1               | 10
                 1               | 30
                 1               | 25
                 1               | 28
                 2               | 0
                 2               | 7
                 2               | 10
                 3               | 100
                 4               | 3
                 4               | 3
                 4               | 6""";
        ccs.stepWeightOne(data, expected2);
    }

    @Test
    public void issue457_binary() {
        // Like the previous, but BINARY not nullable
        String program = """
                CREATE TABLE T (
                  payment_id INT,
                  customer_id INT,
                  amount INT,
                  seq BINARY(1) NOT NULL
                );

                CREATE VIEW V AS SELECT
                  customer_id,
                  SUM(amount) OVER (PARTITION BY customer_id ORDER BY seq) AS previous
                FROM T;""";
        // Validated on postgres, which is NULLS LAST, so this needs explicit NULLS FIRST;
        // replacing BINARY(1) with BYTEA
        String data = """
                INSERT INTO T VALUES
                  -- Customer 1: clean increasing
                   (1, 1, 10, x'01'),
                   (2, 1, 20, x'02'),
                   (3, 1, -5, x'03'),
                   -- Customer 2: out‑of‑order
                   (4, 2, 7,  x'02'),
                   (5, 2, 0,  x'01'),
                   (6, 2, 3,  x'03'),
                   -- Customer 3: single row partition
                   (7, 3, 100, x'03'),
                   -- Customer 4: duplicate values
                   (8, 4, 1,  x'01'),
                   (9, 4, 2,  x'01'),
                   (10,4, 3,  x'02');
                """;
        String expected = """
                 customer_id | previous
                ------------------------
                 1           | 10
                 1           | 30
                 1           | 25
                 2           | 0
                 2           | 7
                 2           | 10
                 3           | 100
                 4           | 3
                 4           | 3
                 4           | 6""";

        var ccs = this.getCCS(program);
        ccs.stepWeightOne(data, expected);

        var program1 = program.replace("ORDER BY seq", "ORDER BY seq NULLS FIRST");
        ccs = this.getCCS(program1);
        ccs.stepWeightOne(data, expected);

        var program2 = program.replace("ORDER BY seq", "ORDER BY seq NULLS LAST");
        ccs = this.getCCS(program2);
        String expected2 = """
                 customer_id | previous
                ------------------------
                 1            | 10
                 1            | 30
                 1            | 25
                 2            | 0
                 2            | 7
                 2            | 10
                 3            | 100
                 4            | 3
                 4            | 3
                 4            | 6""";
        ccs.stepWeightOne(data, expected2);
    }

    @Test
    public void issue457_unsupported_binary() {
        this.statementsFailingInCompilation("""
                CREATE TABLE T (
                  payment_id INT,
                  customer_id INT,
                  amount INT,
                  seq BINARY(16)
                );

                CREATE VIEW V AS SELECT
                  customer_id,
                  SUM(amount) OVER (PARTITION BY customer_id ORDER BY seq) AS previous
                FROM T;""", "Not yet implemented: OVER currently cannot sort on columns with type 'BINARY(16)'");
        this.statementsFailingInCompilation("""
                CREATE TABLE T (
                  payment_id INT,
                  customer_id INT,
                  amount INT,
                  seq VARBINARY
                );

                CREATE VIEW V AS SELECT
                  customer_id,
                  SUM(amount) OVER (PARTITION BY customer_id ORDER BY seq) AS previous
                FROM T;""", "Not yet implemented: OVER currently cannot sort on columns with type 'VARBINARY'");
    }

    @Test
    public void issue6352() {
        this.qst("""
                 SELECT SAFE_CAST('true' AS BOOL);
                 r
                ---
                 t
                (1 row)

                SELECT SAFE_CAST('false' AS BOOL);
                 r
                ---
                 f
                (1 row)

                SELECT SAFE_CAST('blah' AS BOOL);
                 r
                ---
                NULL
                (1 row)

                SELECT SAFE_CAST('t' AS BOOL);
                 r
                ---
                NULL
                (1 row)""");

        this.qst("""
                 SELECT SAFE_CAST('0.0' AS DOUBLE);
                 r
                ---
                 0
                (1 row)

                SELECT SAFE_CAST('false' AS DOUBLE);
                 r
                ---
                NULL
                (1 row)

                SELECT SAFE_CAST(NULL AS DOUBLE);
                 r
                ---
                NULL
                (1 row)""");

        this.qst("""
                 SELECT SAFE_CAST('0.0' AS REAL);
                 r
                ---
                 0
                (1 row)

                SELECT SAFE_CAST('false' AS REAL);
                 r
                ---
                NULL
                (1 row)

                SELECT SAFE_CAST('Infinity' AS REAL);
                 r
                ---
                 Infinity
                (1 row)

                SELECT SAFE_CAST(NULL AS REAL);
                 r
                ---
                NULL
                (1 row)""");

        this.qf("SELECT CAST('blah' AS DOUBLE)",
                "Parse error during conversion of 'blah' to DOUBLE: invalid float literal");
        this.qf("SELECT CAST('blah' AS BOOLEAN)",
                "Cannot convert string 'blah' to BOOLEAN");
    }

    @Test
    public void issue3636() {
        this.getCC("""
                CREATE TABLE T (
                  id INT,
                  col ROW(field1 VARCHAR, field2 INT)
                );
                CREATE VIEW W AS SELECT col.field2 FROM T;""");

        this.getCC("""
                CREATE TABLE t (
                    id VARCHAR,
                    r ROW(b VARCHAR)
                );
                CREATE VIEW V AS SELECT id, t.r.b, r.b FROM t;""");

        this.getCC("""
                CREATE TABLE T(
                  b VARCHAR,
                  r ROW (b VARCHAR)
                );

                CREATE VIEW Z AS SELECT b, r.b, t.b FROM T;""");
    }

    /** ENFORCE_POSITIVE_INPUTS catches a delete of an absent row in a non-incremental circuit. */
    @Test
    public void testEnforcePositiveInputs() {
        String sql = """
                SET ENFORCE_POSITIVE_INPUTS = TRUE;
                CREATE TABLE T(x INT NOT NULL);
                CREATE VIEW V AS SELECT x FROM T;""";
        DBSPCompiler compiler = this.testCompiler();
        compiler.submitStatementsForCompilation(sql);
        this.runtimeFail(compiler, "REMOVE FROM T VALUES (1);",
                "Table t: negative weight found");
    }

    @Test
    public void issue6453() {
        var ccs = this.getCCS("""
                CREATE TABLE t (id INT, arr ROW(name VARCHAR, score INT) ARRAY);
                CREATE MATERIALIZED VIEW v AS
                SELECT id, TRANSFORM(arr, x -> x.name) AS names FROM t;""");
        String insert = """
                INSERT INTO t VALUES
                  -- Simple single-element array
                  (1, ARRAY[
                        ROW('alice', 10)
                      ]),
                
                  -- Multi-element array
                  (2, ARRAY[
                        ROW('bob', 5),
                        ROW('carol', 7),
                        ROW('dave', 3)
                      ]),
                
                  -- Empty array; direct casts of ARRAY[] do not work
                  (3, ARRAY_REMOVE(ARRAY[NULL], NULL)),
                
                  -- Array with NULL element
                  (4, ARRAY[
                        NULL,
                        ROW('eve', 42)
                      ]),
                
                  -- Repeated names, varied scores
                  (5, ARRAY[
                        ROW('zoe', 1),
                        ROW('zoe', 99),
                        ROW('max', 50)
                      ]),
                
                  -- Stress test: longer array
                  (6, ARRAY[
                        ROW('p1', 10),
                        ROW('p2', 20),
                        ROW('p3', 30),
                        ROW('p4', 40)
                      ]);""";
        String expected = """
                 id | names
                -------------
                  1 | { alice}
                  2 | { bob, carol, dave}
                  3 | {}
                  4 | {NULL, eve}
                  5 | { zoe, zoe, max}
                  6 | { p1, p2, p3, p4}""";
        ccs.stepWeightOne(insert, expected);

        ccs = this.getCCS("""
                CREATE TABLE t (id INT, arr ROW(name VARCHAR, score INT) ARRAY);
                CREATE MATERIALIZED VIEW v AS
                SELECT id, TRANSFORM(arr, x -> (x).name) AS names FROM t;""");
        ccs.stepWeightOne(insert, expected);
    }

    @Test
    public void testMapCast() {
        this.getCCS("""
                CREATE TABLE T(id INT, mapp MAP<VARCHAR, INT>);
                CREATE MATERIALIZED VIEW V AS SELECT
                SAFE_CAST(mapp AS MAP<VARCHAR, VARCHAR>) AS map FROM T;
                --MAP[id, id] as map1 FROM T;""");
    }

    @Test
    public void testMapNullable() {
        var ccs = this.getCCS("""
                CREATE TABLE T(id INT);
                CREATE MATERIALIZED VIEW V AS SELECT
                MAP[id, id] as m FROM T;""");
        ccs.stepWeightOne("INSERT INTO T VALUES(NULL);", """
                 m
                -------------
                 {NULL: NULL}
                """);
    }

    @Test
    public void issue6565() {
        var ccs = this.getCCS("""
                CREATE TABLE employees(dept VARCHAR);
                CREATE VIEW V AS SELECT dept, COUNT(*) AS n
                FROM employees
                GROUP BY dept
                HAVING dept LIKE 'S%';""");
        ccs.visit(new CircuitVisitor(ccs.compiler) {
            boolean filterFound = false;

            @Override
            public void postorder(DBSPFlatMapIndexOperator node) {
                // Source is input
                this.filterFound = true;
                Assert.assertTrue(node.input().node().is(DBSPSourceTableOperator.class));
            }

            @Override
            public void endVisit() {
                Assert.assertTrue(this.filterFound);
            }
        });
    }

    @Test
    public void issue6565b() {
        var ccs = this.getCCS("""
                CREATE TABLE employees(dept VARCHAR);
                CREATE LOCAL VIEW V AS SELECT dept, COUNT(*) AS n
                FROM employees
                GROUP BY dept;
                CREATE VIEW W AS SELECT * FROM V WHERE dept LIKE 'S%';""");
        ccs.visit(new CircuitVisitor(ccs.compiler) {
            boolean filterFound = false;

            @Override
            public void postorder(DBSPFlatMapIndexOperator node) {
                // Source is input
                this.filterFound = true;
                Assert.assertTrue(node.input().node().is(DBSPSourceTableOperator.class));
            }

            @Override
            public void endVisit() {
                Assert.assertTrue(this.filterFound);
            }
        });
    }

    @Test
    public void issue5940() {
        // Without inlining CRT_DATE this generates a very inefficient implementation using JOIN
        // With inlining this generates a Window operator
        var ccs = this.getCCS("""
                CREATE FUNCTION CRT_DATE(T TIMESTAMP) RETURNS DATE AS CAST(T AS DATE);
                CREATE TABLE T(D DATE, X INT);
                CREATE VIEW V AS SELECT * FROM T WHERE T.D < CRT_DATE(NOW())""");
        var visitor = new CircuitVisitor(ccs.compiler) {
            boolean windowFound = false;

            @Override
            public void postorder(DBSPStreamJoinOperator node) {
                Assert.fail("JOIN should not be present");
            }

            @Override
            public void postorder(DBSPWindowOperator node) {
                this.windowFound = true;
            }

            @Override
            public void endVisit() {
                Assert.assertTrue(this.windowFound);
                super.endVisit();
            }
        };
        ccs.visit(visitor);

        /// inline multiple nested functions
        ccs = this.getCCS("""
                CREATE FUNCTION CRT_DATE(T TIMESTAMP) RETURNS DATE AS CAST(T AS DATE);
                CREATE FUNCTION YR(D DATE) RETURNS BIGINT AS EXTRACT(YEAR FROM D);
                CREATE TABLE T(X BIGINT);
                CREATE VIEW V AS SELECT * FROM T WHERE T.X < YR(CRT_DATE(NOW()));""");
        ccs.visit(visitor);
    }

    /** Test case for <a href="https://issues.apache.org/jira/browse/CALCITE-7784">
     * [CALCITE-7784] Parse fails for ROW(CASE ...)</a> */
    @Test
    public void calcite7784() {
        // A CASE expression as a named ROW field
        var ccs = this.getCCS("""
                CREATE TABLE T(given VARCHAR, family VARCHAR, formatted VARCHAR);
                CREATE LOCAL VIEW V AS SELECT ROW(
                    CASE WHEN given IS NULL AND family IS NULL THEN formatted
                         ELSE NULLIF(CONCAT_WS(' ', given, family), '') END AS fullName,
                    given AS givenName) AS r
                FROM T;
                CREATE VIEW W AS SELECT (r).fullName AS fullName, (r).givenName AS givenName FROM V;""")
                .withStringTrim();
        ccs.stepWeightOne("INSERT INTO T VALUES('Ann', 'Lee', 'unused')", """
                 fullName       | givenName
                ----------------------------
                 Ann Lee        | Ann""");
        ccs.stepWeightOne("INSERT INTO T VALUES(NULL, NULL, 'Only Formatted')", """
                 fullName       | givenName
                ----------------------------
                 Only Formatted |NULL""");

        // A CASE without ELSE as the first field of an aggregated ROW, the shape
        // that failed to parse
        ccs = this.getCCS("""
                CREATE TABLE S(id INT, a INT, b INT);
                CREATE LOCAL VIEW V2 AS
                SELECT id, ARRAY_AGG(ROW(CASE WHEN a > 0 THEN a END, b)) AS arr
                FROM S GROUP BY id;
                CREATE VIEW W2 AS SELECT id, TRANSFORM(arr, x -> x[1]) AS first FROM V2;""");
        ccs.stepWeightOne("INSERT INTO S VALUES(1, 2, 3)", """
                 id | first
                ------------
                 1  | { 2 }""");
        ccs.stepWeightOne("INSERT INTO S VALUES(2, -1, 3)", """
                 id | first
                -------------
                 2  | { NULL }""");
    }

    /** Indexing a collection whose element is a ROW with a non-nullable ARRAY field */
    @Test
    public void nestedArrayInNullableRow() {
        var ccs = this.getCCS("""
                CREATE TABLE T(x INT ARRAY);
                CREATE VIEW W AS SELECT ARRAY[ROW(COALESCE(x, ARRAY()) AS b)][1].b[1] AS r FROM T;""");
        ccs.stepWeightOne("INSERT INTO T VALUES(ARRAY[2, 3])", """
                 r
                ---
                 2""");
        ccs.stepWeightOne("INSERT INTO T VALUES(NULL)", """
                 r
                ------
                 NULL""");

        // The same with a MAP
        ccs = this.getCCS("""
                CREATE TABLE T(x INT ARRAY);
                CREATE VIEW W AS SELECT MAP['k', ROW(COALESCE(x, ARRAY()) AS b)]['k'].b[1] AS r FROM T;""");
        ccs.stepWeightOne("INSERT INTO T VALUES(ARRAY[2, 3])", """
                 r
                ---
                 2""");
        ccs.stepWeightOne("INSERT INTO T VALUES(NULL)", """
                 r
                ------
                 NULL""");

        // Two levels of nesting, each one indexing a ROW field that cannot be NULL
        ccs = this.getCCS("""
                CREATE TABLE T(x INT ARRAY);
                CREATE VIEW W AS SELECT
                  ARRAY[ROW(ARRAY[ROW(COALESCE(x, ARRAY()) AS c)] AS b)][1].b[1].c[1] AS r
                FROM T;""");
        ccs.stepWeightOne("INSERT INTO T VALUES(ARRAY[2, 3])", """
                 r
                ---
                 2""");
        ccs.stepWeightOne("INSERT INTO T VALUES(NULL)", """
                 r
                ------
                 NULL""");

        // An index out of bounds and a missing map key produce a NULL ROW result
        ccs = this.getCCS("""
                CREATE TABLE T(x INT ARRAY);
                CREATE VIEW W AS SELECT
                  ARRAY[ROW(COALESCE(x, ARRAY()) AS b)][2].b AS r,
                  ARRAY[ROW(COALESCE(x, ARRAY()) AS b)][2].b[1] AS s,
                  MAP['k', ROW(COALESCE(x, ARRAY()) AS b)]['nope'].b AS t
                FROM T;""");
        ccs.stepWeightOne("INSERT INTO T VALUES(ARRAY[2, 3])", """
                 r    | s    | t
                ---------------------
                 NULL | NULL | NULL""");

        // SAFE_OFFSET 
        ccs = this.getCCS("""
                CREATE TABLE T(x INT ARRAY);
                CREATE VIEW W AS SELECT
                  ARRAY[ROW(COALESCE(x, ARRAY()) AS b)][SAFE_OFFSET(0)].b[1] AS r,
                  ARRAY[ROW(COALESCE(x, ARRAY()) AS b)][SAFE_OFFSET(1)].b AS s
                FROM T;""");
        ccs.stepWeightOne("INSERT INTO T VALUES(ARRAY[2, 3])", """
                 r | s
                ----------
                 2 | NULL""");

        // A DECIMAL element type, which carries a precision and a scale
        ccs = this.getCCS("""
                CREATE TABLE T(x DECIMAL(6, 2) ARRAY);
                CREATE VIEW W AS SELECT ARRAY[ROW(COALESCE(x, ARRAY()) AS b)][1].b[1] AS r FROM T;""");
        ccs.stepWeightOne("INSERT INTO T VALUES(ARRAY[2.50, 3.50])", """
                 r
                ------
                 2.50""");
    }

    /** A NULL ROW constant reaching the monotonicity analysis */
    @Test
    public void nullRowConstantMonotone() {
        this.getCC("""
                CREATE TABLE T(ts TIMESTAMP NOT NULL LATENESS INTERVAL 1 HOUR, x INT);
                CREATE VIEW V AS SELECT ts, CAST(NULL AS ROW(a INT)) AS r FROM T;""");
    }
}
