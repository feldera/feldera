package org.dbsp.sqlCompiler.compiler.sql.simple;

import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPWaterlineOperator;
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
    public void issue457a() {
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
                 1 	         | 3
                 1 	         | 33
                 1 	         | 33
                 1 	         | 28
                 2 	         | 3
                 2 	         | 3
                 2 	         | 10
                 3 	         | 100
                 4 	         | 3
                 4 	         | 3
                 4 	         | 6""";

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
                 1 	         | 30
                 1 	         | 30
                 1 	         | 25
                 1 	         | 28
                 2 	         | 3
                 2 	         | 3
                 2 	         | 10
                 3 	         | 100
                 4 	         | 3
                 4 	         | 3
                 4 	         | 6""";
        ccs.stepWeightOne(data, expected2);
    }

    @Test
    public void issue457b() {
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
                 1 	         | 3
                 1 	         | 13
                 1 	         | 33
                 1 	         | 28
                 2 	         | 3
                 2 	         | 3
                 2 	         | 10
                 3 	         | 100
                 4 	         | 6
                 4 	         | 6
                 4 	         | 6""";

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
                 1 	         | 10
                 1 	         | 30
                 1 	         | 25
                 1 	         | 28
                 2 	         | 3
                 2 	         | 3
                 2 	         | 10
                 3 	         | 100
                 4 	         | 6
                 4 	         | 6
                 4 	         | 6""";
        ccs.stepWeightOne(data, expected2);
    }

    @Test
    public void issue457c() {
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
                 1 	         | 3
                 1 	         | 13
                 1 	         | 33
                 1 	         | 28
                 2 	         | 3
                 2 	         | 3
                 2 	         | 10
                 3 	         | 100
                 4 	         | 6
                 4 	         | 6
                 4 	         | 6""";

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
                 1 	         | 10
                 1 	         | 30
                 1 	         | 25
                 1 	         | 28
                 2 	         | 3
                 2 	         | 3
                 2 	         | 10
                 3 	         | 100
                 4 	         | 6
                 4 	         | 6
                 4 	         | 6""";
        ccs.stepWeightOne(data, expected2);
    }

    @Test
    public void issue457d() {
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
}
