package org.dbsp.sqlCompiler.compiler.sql.simple;

import org.dbsp.sqlCompiler.circuit.operator.DBSPChainAggregateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamAggregateOperator;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.sql.tools.CompilerCircuitStream;
import org.dbsp.sqlCompiler.compiler.sql.tools.SqlIoTest;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.sqlCompiler.ir.aggregate.DBSPMinMax;
import org.junit.Assert;
import org.junit.Test;

public class AggregateTests extends SqlIoTest {
    @Override
    public void prepareInputs(DBSPCompiler compiler) {
        compiler.submitStatementsForCompilation("""
                CREATE TABLE T(
                   B BIGINT,
                   I INTEGER,
                   S SMALLINT,
                   T TINYINT,
                   R REAL,
                   D DOUBLE,
                   E DECIMAL(3,2),
                   V VARCHAR
                );
                INSERT INTO T VALUES
                   (NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
                   (0, 0, 0, 0, 0, 0, 0, '0'),
                   (1, 1, 1, 1, 1, 1, 1, '1'),
                   (2, 2, 2, 2, 2, 2, 2, '2');
                CREATE TABLE warehouse (
                   id INT NOT NULL PRIMARY KEY,
                   parentId INT
                );
                INSERT INTO warehouse VALUES
                   (10, 20),
                   (20, 20),
                   (30, 20),
                   (5,  5),
                   (1,  5),
                   (3,  3);
                CREATE TABLE NN (
                   I INT NOT NULL,
                   J INT,
                   K INT
                ) with ('append_only' = 'true');
                INSERT INTO NN VALUES
                   (0, 0, 0),
                   (1, 1, 1),
                   (2, NULL, 0),
                   (3, NULL, 1);
                """);
    }

    @Test
    public void issue2042() {
        this.qs("""
                SELECT ARG_MAX(I, I), ARG_MAX(I, J), ARG_MAX(J, I), ARG_MAX(J, J)
                FROM NN;
                 ii | ij | ji   | jj
                --------------------
                 3  | 1  | NULL | 1
                (1 row)

                SELECT K, ARG_MAX(I, I), ARG_MAX(I, J), ARG_MAX(J, I), ARG_MAX(J, J)
                FROM NN GROUP BY K;
                 k | ii | ij | ji   | jj
                -------------------------
                 0 | 2  | 0  | NULL | 0
                 1 | 3  | 1  | NULL | 1
                (2 rows)

                SELECT ARG_MIN(I, I), ARG_MIN(I, J), ARG_MIN(J, I), ARG_MIN(J, J)
                FROM NN;
                 ii | ij | ji   | jj
                --------------------
                 0  | 0  | 0    | 0
                (1 row)

                SELECT K, ARG_MIN(I, I), ARG_MIN(I, J), ARG_MIN(J, I), ARG_MIN(J, J)
                FROM NN GROUP BY K;
                 k | ii | ij | ji   | jj
                -------------------------
                 0 | 0  | 0  | 0    | 0
                 1 | 1  | 1  | 1    | 1
                (2 rows)""");
    }

    @Test
    public void testIssue1957() {
        // validated using Postgres
        this.qs("""
                SELECT
                  id,
                  (SELECT ARRAY_AGG(id) FROM (
                    SELECT id FROM warehouse WHERE parentId = warehouse.id
                    ORDER BY id LIMIT 2
                  )) AS first_children
                FROM warehouse;
                 id |  array
                ---------------
                 1  | { 3, 5 }
                 3  | { 3, 5 }
                 5  | { 3, 5 }
                 10 | { 3, 5 }
                 20 | { 3, 5 }
                 30 | { 3, 5 }
                (6 rows)""");
    }

    @Test
    public void issue4777() {
        // validated using Postgres, but second result is not deterministic
        this.qs("""
                SELECT ARRAY_AGG(parentId ORDER BY id)
                FROM warehouse;
                  array
                ----------
                 { 5, 3, 5, 20, 20, 20 }
                (1 row)

                SELECT ARRAY_AGG(id ORDER BY parentId)
                FROM warehouse;
                  array
                ----------
                 { 3, 1, 5, 10, 20, 30 }
                (1 row)

                SELECT ARRAY_AGG(id ORDER BY parentId, id)
                FROM warehouse;
                  array
                ----------
                 { 3, 1, 5, 10, 20, 30 }
                (1 row)

                SELECT ARRAY_AGG(id ORDER BY parentId, id, id DESC)
                FROM warehouse;
                  array
                ----------
                 { 3, 1, 5, 10, 20, 30 }
                (1 row)

                SELECT ARRAY_AGG(id ORDER BY parentId, parentId DESC, id)
                FROM warehouse;
                  array
                ----------
                 { 3, 1, 5, 10, 20, 30 }
                (1 row)

                SELECT ARRAY_AGG(id ORDER BY parentId, id DESC)
                FROM warehouse;
                  array
                ----------
                 { 3, 5, 1, 30, 20, 10 }
                (1 row)""");
    }

    @Test
    public void nullTumble() {
        this.compileRustTestCase("""
                create table PRICE (ts TIMESTAMP);
                create view x AS
                SELECT *
                FROM TABLE(
                  TUMBLE(
                    DATA => TABLE price,
                    TIMECOL => DESCRIPTOR(ts),
                    SIZE => INTERVAL '1' HOUR));""");
    }

    @Test
    public void aggTimestamp() {
        var ccs = this.getCCS("""
                create table data (
                    price DOUBLE,
                    ts TIMESTAMP
                );

                create view v AS
                SELECT
                     window_start,
                     window_end,
                     ARG_MIN(price, ts),
                     MAX(price),
                     MIN(price),
                     ARG_MAX(price, ts)
                FROM TABLE(
                  TUMBLE(
                    DATA => TABLE data,
                    TIMECOL => DESCRIPTOR(ts),
                    SIZE => INTERVAL '1' HOUR))
                GROUP BY
                  window_start, window_end;""");
        ccs.step("""
                INSERT INTO data VALUES(1.0, '2024-01-01 00:00:00');
                INSERT INTO DATA VALUES(2.0, '2024-01-01 00:00:10');
                INSERT INTO DATA VALUES(3.0, '2024-01-01 00:00:20');
                INSERT INTO DATA VALUES(7.0, NULL);
                INSERT INTO DATA VALUES(4.0, '2024-01-01 00:00:30');
                INSERT INTO DATA VALUES(6.0, '2024-01-01 02:00:00');
                INSERT INTO DATA VALUES(5.0, '2024-01-01 02:00:10');""", """
                  ws                 | we                  | f   | max | min | last | weight
                ----------------------------------------------------------------------------
                 2024-01-01 00:00:00 | 2024-01-01 01:00:00 | 1.0 | 4.0 | 1.0 | 4.0  | 1
                 2024-01-01 02:00:00 | 2024-01-01 03:00:00 | 6.0 | 6.0 | 5.0 | 5.0  | 1""");
    }

    @Test
    public void testArrayConstructor() {
        this.qs("""
                SELECT
                  id,
                  (SELECT ARRAY(
                    SELECT id FROM warehouse WHERE parentId = warehouse.id
                    -- ORDER BY id LIMIT 2
                  )) AS first_children
                FROM warehouse;
                 id |  array
                ---------------
                 1  | { 3, 5, 20 }
                 3  | { 3, 5, 20 }
                 5  | { 3, 5, 20 }
                 10 | { 3, 5, 20 }
                 20 | { 3, 5, 20 }
                 30 | { 3, 5, 20 }
                (6 rows)""");

        this.qs("""
                SELECT
                  id,
                  (SELECT ARRAY(
                    SELECT id FROM warehouse WHERE parentId = warehouse.id
                    LIMIT 2
                  )) AS first_children
                FROM warehouse;
                 id |  array
                ---------------
                 1  | { 3, 5 }
                 3  | { 3, 5 }
                 5  | { 3, 5 }
                 10 | { 3, 5 }
                 20 | { 3, 5 }
                 30 | { 3, 5 }
                (6 rows)""");

    }

    @Test
    public void testOneArgMin() {
        this.qs("""
                SELECT ARG_MIN(V, B)
                FROM T;
                 B
                ---
                 0
                (1 row)""");

        var cc = this.getCC("CREATE VIEW V AS SELECT ARG_MIN(V, B) FROM T;");
        CircuitVisitor visitor = new CircuitVisitor(cc.compiler) {
            boolean found = false;

            @Override
            public void postorder(DBSPStreamAggregateOperator operator) {
                // Code generated uses DBSP ArgMinSome aggregator
                Assert.assertSame(DBSPMinMax.Aggregation.ArgMinSome,
                        operator.getFunction().to(DBSPMinMax.class).aggregation);
                found = true;
            }

            @Override
            public void endVisit() {
                Assert.assertTrue(found);
            }
        };
        cc.visit(visitor);

        cc = this.getCC("CREATE VIEW V AS SELECT MIN(B) FROM T;");
        visitor = new CircuitVisitor(cc.compiler) {
            boolean found = false;

            @Override
            public void postorder(DBSPStreamAggregateOperator operator) {
                // Code generated uses MinSome1 DBSP aggregator
                Assert.assertSame(DBSPMinMax.Aggregation.MinSome1,
                        operator.getFunction().to(DBSPMinMax.class).aggregation);
                found = true;
            }

            @Override
            public void endVisit() {
                Assert.assertTrue(found);
            }
        };
        cc.visit(visitor);
    }

    @Test
    public void testArgMin() {
        this.qs("""
                SELECT ARG_MIN(V, B), ARG_MIN(V, I), ARG_MIN(V, S), ARG_MIN(V, T),
                ARG_MIN(V, R), ARG_MIN(V, D), ARG_MIN(V, E)
                FROM T;
                 B | I | S | T | R | D | E
                ---------------------------
                 0| 0| 0| 0| 0| 0| 0
                (1 row)

                SELECT ARG_MAX(V, B), ARG_MAX(V, I), ARG_MAX(V, S), ARG_MAX(V, T),
                ARG_MAX(V, R), ARG_MAX(V, D), ARG_MAX(V, E)
                FROM T;
                 B | I | S | T | R | D | E
                ---------------------------
                 2| 2| 2| 2| 2| 2| 2
                (1 row)""");
    }

    @Test
    public void testAggregates() {
        this.qs("""
                SELECT COUNT(*), COUNT(B), COUNT(I), COUNT(S), COUNT(T), COUNT(R), COUNT(D), COUNT(E) FROM T;
                 * | B | I | S | T | R | D | E
                -------------------------------
                 4 | 3 | 3 | 3 | 3 | 3 | 3 | 3
                (1 row)

                SELECT SUM(B), SUM(I), SUM(S), SUM(T), SUM(R), SUM(D), SUM(E) FROM T;
                 B | I | S | T | R | D | E
                ---------------------------
                 3 | 3 | 3 | 3 | 3 | 3 | 3
                (1 row)

                SELECT AVG(B), AVG(I), AVG(S), AVG(T), AVG(R), AVG(D), AVG(E) FROM T;
                 B | I | S | T | R | D | E
                ---------------------------
                 1 | 1 | 1 | 1 | 1 | 1 | 1
                (1 row)

                SELECT MIN(B), MIN(I), MIN(S), MIN(T), MIN(R), MIN(D), MIN(E) FROM T;
                 B | I | S | T | R | D | E
                ---------------------------
                 0 | 0 | 0 | 0 | 0 | 0 | 0
                (1 row)

                SELECT MAX(B), MAX(I), MAX(S), MAX(T), MAX(R), MAX(D), MAX(E) FROM T;
                 B | I | S | T | R | D | E
                ---------------------------
                 2 | 2 | 2 | 2 | 2 | 2 | 2
                (1 row)

                SELECT STDDEV(B), STDDEV(I), STDDEV(S), STDDEV(T), STDDEV(R), STDDEV(D), STDDEV(E) FROM T;
                 B | I | S | T | R | D | E
                ---------------------------
                 1 | 1 | 1 | 1 | 1 | 1 | 1
                (1 row)

                SELECT STDDEV_POP(B), STDDEV_POP(I), STDDEV_POP(S), STDDEV_POP(T), STDDEV_POP(R), STDDEV_POP(D), STDDEV_POP(E) FROM T;
                 B | I | S | T | R | D | E
                ---------------------------
                 0 | 0 | 0 | 0 | 0.8164966 | 0.816496580927726 | 0.81
                (1 row)

                SELECT STDDEV_SAMP(B), STDDEV_SAMP(I), STDDEV_SAMP(S), STDDEV_SAMP(T), STDDEV_SAMP(R), STDDEV_SAMP(D), STDDEV_SAMP(E) FROM T;
                 B | I | S | T | R | D | E
                ---------------------------
                 1 | 1 | 1 | 1 | 1 | 1 | 1
                (1 row)

                SELECT BIT_AND(B), BIT_AND(I), BIT_AND(S), BIT_AND(T) FROM T;
                 B | I | S | T
                ---------------
                 0 | 0 | 0 | 0
                (1 row)

                SELECT BIT_OR(B), BIT_OR(I), BIT_OR(S), BIT_OR(T) FROM T;
                 B | I | S | T
                ---------------
                 3 | 3 | 3 | 3
                (1 row)

                SELECT BIT_XOR(B), BIT_XOR(I), BIT_XOR(S), BIT_XOR(T) FROM T;
                 B | I | S | T
                ---------------
                 3 | 3 | 3 | 3
                (1 row)""");
    }

    @Test
    public void testDistinctAggregates() {
        this.qs("""
                SELECT COUNT(*), COUNT(DISTINCT B), COUNT(DISTINCT I), COUNT(DISTINCT S), COUNT(DISTINCT T), COUNT(DISTINCT R), COUNT(DISTINCT D), COUNT(DISTINCT E) FROM T;
                 * | B | I | S | T | R | D | E
                -------------------------------
                 4 | 3 | 3 | 3 | 3 | 3 | 3 | 3
                (1 row)

                SELECT SUM(DISTINCT B), SUM(DISTINCT I), SUM(DISTINCT S), SUM(DISTINCT T), SUM(DISTINCT R), SUM(DISTINCT D), SUM(DISTINCT E) FROM T;
                 B | I | S | T | R | D | E
                ---------------------------
                 3 | 3 | 3 | 3 | 3 | 3 | 3
                (1 row)

                SELECT AVG(DISTINCT B), AVG(DISTINCT I), AVG(DISTINCT S), AVG(DISTINCT T), AVG(DISTINCT R), AVG(DISTINCT D), AVG(DISTINCT E) FROM T;
                 B | I | S | T | R | D | E
                ---------------------------
                 1 | 1 | 1 | 1 | 1 | 1 | 1
                (1 row)

                SELECT MIN(DISTINCT B), MIN(DISTINCT I), MIN(DISTINCT S), MIN(DISTINCT T), MIN(DISTINCT R), MIN(DISTINCT D), MIN(DISTINCT E) FROM T;
                 B | I | S | T | R | D | E
                ---------------------------
                 0 | 0 | 0 | 0 | 0 | 0 | 0
                (1 row)

                SELECT MAX(DISTINCT B), MAX(DISTINCT I), MAX(DISTINCT S), MAX(DISTINCT T), MAX(DISTINCT R), MAX(DISTINCT D), MAX(DISTINCT E) FROM T;
                 B | I | S | T | R | D | E
                ---------------------------
                 2 | 2 | 2 | 2 | 2 | 2 | 2
                (1 row)

                SELECT STDDEV(DISTINCT B), STDDEV(DISTINCT I), STDDEV(DISTINCT S), STDDEV(DISTINCT T), STDDEV(DISTINCT R), STDDEV(DISTINCT D), STDDEV(DISTINCT E) FROM T;
                 B | I | S | T | R | D | E
                ---------------------------
                 1 | 1 | 1 | 1 | 1 | 1 | 1
                (1 row)

                SELECT STDDEV_POP(DISTINCT B), STDDEV_POP(DISTINCT I), STDDEV_POP(DISTINCT S), STDDEV_POP(DISTINCT T), STDDEV_POP(DISTINCT R), STDDEV_POP(DISTINCT D), STDDEV_POP(DISTINCT E) FROM T;
                 B | I | S | T | R | D | E
                ---------------------------
                 0 | 0 | 0 | 0 | 0.8164966 | 0.816496580927726 | 0.81
                (1 row)

                SELECT STDDEV_SAMP(DISTINCT B), STDDEV_SAMP(DISTINCT I), STDDEV_SAMP(DISTINCT S), STDDEV_SAMP(DISTINCT T), STDDEV_SAMP(DISTINCT R), STDDEV_SAMP(DISTINCT D), STDDEV_SAMP(DISTINCT E) FROM T;
                 B | I | S | T | R | D | E
                ---------------------------
                 1 | 1 | 1 | 1 | 1 | 1 | 1
                (1 row)

                SELECT BIT_AND(DISTINCT B), BIT_AND(DISTINCT I), BIT_AND(DISTINCT S), BIT_AND(DISTINCT T) FROM T;
                 B | I | S | T
                ---------------
                 0 | 0 | 0 | 0
                (1 row)

                SELECT BIT_OR(DISTINCT B), BIT_OR(DISTINCT I), BIT_OR(DISTINCT S), BIT_OR(DISTINCT T) FROM T;
                 B | I | S | T
                ---------------
                 3 | 3 | 3 | 3
                (1 row)

                SELECT BIT_XOR(DISTINCT B), BIT_XOR(DISTINCT I), BIT_XOR(DISTINCT S), BIT_XOR(DISTINCT T) FROM T;
                 B | I | S | T
                ---------------
                 3 | 3 | 3 | 3
                (1 row)""");
    }

    @Test
    public void testArgMinMin() {
        var cc = this.getCC("CREATE VIEW V AS SELECT ARG_MIN(I, J), MIN(J), MAX(J) FROM NN;");
        CircuitVisitor visitor = new CircuitVisitor(cc.compiler) {
            int chains = 0;

            @Override
            public void postorder(DBSPChainAggregateOperator operator) {
                this.chains++;
            }

            public void postorder(DBSPStreamAggregateOperator operator) {
                Assert.fail("There should be no aggregate operators");
            }

            @Override
            public void endVisit() {
                // MIN, ARG_MIN, MAX are combined in a single operator
                Assert.assertEquals(1, this.chains);
            }
        };
        cc.visit(visitor);

        cc = this.getCC("CREATE VIEW V AS SELECT K, ARG_MIN(I, J), MIN(J), MAX(J) FROM NN GROUP BY k;");
        visitor = new CircuitVisitor(cc.compiler) {
            int chains = 0;

            @Override
            public void postorder(DBSPChainAggregateOperator operator) {
                this.chains++;
            }

            public void postorder(DBSPStreamAggregateOperator operator) {
                Assert.fail("There should be no aggregate operators");
            }

            @Override
            public void endVisit() {
                // MIN, ARG_MIN, MAX are combined in a single operator
                Assert.assertEquals(1, this.chains);
            }
        };
        cc.visit(visitor);
    }

    @Test
    public void testPercentileCont() {
        // Test PERCENTILE_CONT aggregate function
        this.qs("""
                SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY D) AS result
                FROM T;
                 result
                --------
                 1
                (1 row)

                SELECT PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY D) AS result
                FROM T;
                 result
                --------
                 0.5
                (1 row)

                SELECT PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY D) AS result
                FROM T;
                 result
                --------
                 1.5
                (1 row)

                SELECT PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY R) AS result
                FROM T;
                 result
                --------
                 1.8
                (1 row)""");
    }

    @Test
    public void testPercentileDisc() {
        // Test PERCENTILE_DISC aggregate function
        this.qs("""
                SELECT PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY I) AS result
                FROM T;
                 result
                --------
                 1
                (1 row)

                SELECT PERCENTILE_DISC(0.25) WITHIN GROUP (ORDER BY I) AS result
                FROM T;
                 result
                --------
                 0
                (1 row)

                SELECT PERCENTILE_DISC(0.75) WITHIN GROUP (ORDER BY I) AS result
                FROM T;
                 result
                --------
                 2
                (1 row)

                SELECT PERCENTILE_DISC(0.9) WITHIN GROUP (ORDER BY B) AS result
                FROM T;
                 result
                --------
                 2
                (1 row)""");
    }

    @Test
    public void testPercentileDescending() {
        // Test percentile functions with DESC order
        this.qs("""
                SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY D DESC) AS result
                FROM T;
                 result
                --------
                 1
                (1 row)

                SELECT PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY I DESC) AS result
                FROM T;
                 result
                --------
                 1
                (1 row)""");
    }

    @Test
    public void testPercentileGroupBy() {
        // Test PERCENTILE_CONT and PERCENTILE_DISC with GROUP BY
        var ccs = this.getCCS("""
                CREATE TABLE percentile_grp (
                    grp INT,
                    val DOUBLE
                );

                CREATE VIEW v_grouped AS SELECT
                    grp,
                    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY val) AS median_cont,
                    PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY val) AS median_disc
                FROM percentile_grp
                GROUP BY grp;
                """);

        // Test with multiple groups of different sizes
        ccs.step("""
                INSERT INTO percentile_grp VALUES
                    (1, -10.0),
                    (1, 0.0),
                    (1, 10.0),
                    (1, 20.0),
                    (1, 30.0),
                    (2, 100.0),
                    (2, 200.0);
                """, """
                 grp | median_cont | median_disc | weight
                ------------------------------------------
                 1   | 10          | 10          | 1
                 2   | 150         | 100         | 1""");
    }

    @Test
    public void testPercentileSingleValue() {
        // Test PERCENTILE functions with single value
        var ccs = this.getCCS("""
                CREATE TABLE single_val (val DOUBLE);

                CREATE VIEW v_single AS SELECT
                    PERCENTILE_CONT(0.0) WITHIN GROUP (ORDER BY val) AS p0,
                    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY val) AS p50,
                    PERCENTILE_CONT(1.0) WITHIN GROUP (ORDER BY val) AS p100,
                    PERCENTILE_DISC(0.0) WITHIN GROUP (ORDER BY val) AS d0,
                    PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY val) AS d50,
                    PERCENTILE_DISC(1.0) WITHIN GROUP (ORDER BY val) AS d100
                FROM single_val;
                """);

        ccs.step("""
                INSERT INTO single_val VALUES (42.0);
                """, """
                 p0 | p50 | p100 | d0 | d50 | d100 | weight
                --------------------------------------------
                 42 | 42  | 42   | 42 | 42  | 42   | 1""");
    }

    @Test
    public void testPercentileTwoValues() {
        // Test PERCENTILE_CONT interpolation with exactly two values
        var ccs = this.getCCS("""
                CREATE TABLE two_vals (val DOUBLE);

                CREATE VIEW v_two AS SELECT
                    PERCENTILE_CONT(0.0) WITHIN GROUP (ORDER BY val) AS p0,
                    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY val) AS p25,
                    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY val) AS p50,
                    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY val) AS p75,
                    PERCENTILE_CONT(1.0) WITHIN GROUP (ORDER BY val) AS p100
                FROM two_vals;
                """);

        // With values 0.0 and 100.0, percentiles should interpolate linearly
        ccs.step("""
                INSERT INTO two_vals VALUES (0.0), (100.0);
                """, """
                 p0 | p25 | p50 | p75 | p100 | weight
                ---------------------------------------
                 0  | 25  | 50  | 75  | 100  | 1""");
    }

    @Test
    public void testPercentileWithNulls() {
        // Test that NULL values are properly ignored in percentile calculations
        this.qs("""
                SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY D) AS result
                FROM T;
                 result
                --------
                 1
                (1 row)

                SELECT PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY I) AS result
                FROM T;
                 result
                --------
                 1
                (1 row)""");

        // Table T has one NULL row and 3 non-null rows (0, 1, 2)
        // The median of [0, 1, 2] for CONT is 1.0, for DISC is 1
    }

    @Test
    public void testPercentileNearExtremes() {
        // Test percentiles very close to 0 and 1
        var ccs = this.getCCS("""
                CREATE TABLE pct_extreme (val DOUBLE);

                CREATE VIEW v_near_extreme AS SELECT
                    PERCENTILE_CONT(0.01) WITHIN GROUP (ORDER BY val) AS p1,
                    PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY val) AS p99
                FROM pct_extreme;
                """);

        ccs.step("""
                INSERT INTO pct_extreme VALUES (0.0), (25.0), (50.0), (75.0), (100.0);
                """, """
                 p1 | p99 | weight
                -------------------
                 1  | 99  | 1""");
    }

    @Test
    public void testPercentileIntegerTypes() {
        // Test PERCENTILE with different integer types (using existing T table)
        // PERCENTILE_CONT on integer types returns DOUBLE (due to interpolation)
        this.qs("""
                SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY B) AS bigint_result
                FROM T;
                 bigint_result
                ---------------
                 1
                (1 row)

                SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY S) AS smallint_result
                FROM T;
                 smallint_result
                -----------------
                 1
                (1 row)

                SELECT PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY B) AS bigint_result
                FROM T;
                 bigint_result
                ---------------
                 1
                (1 row)

                SELECT PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY S) AS smallint_result
                FROM T;
                 smallint_result
                -----------------
                 1
                (1 row)""");
    }

    @Test
    public void testPercentileDecimalType() {
        // Test PERCENTILE with DECIMAL type (using existing T table with E column)
        this.qs("""
                SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY E) AS decimal_result
                FROM T;
                 decimal_result
                ----------------
                 1
                (1 row)

                SELECT PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY E) AS decimal_result
                FROM T;
                 decimal_result
                ----------------
                 1
                (1 row)

                SELECT PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY E) AS q1
                FROM T;
                 q1
                ----
                 0.5
                (1 row)

                SELECT PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY E) AS q3
                FROM T;
                 q3
                ----
                 1.5
                (1 row)""");
    }

    @Test
    public void testPercentileGroupByWithEmpty() {
        // Test GROUP BY where some groups might have different sizes
        var ccs = this.getCCS("""
                CREATE TABLE grp_test (
                    category VARCHAR,
                    val DOUBLE
                );

                CREATE VIEW v_grp_test AS SELECT
                    category,
                    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY val) AS median
                FROM grp_test
                GROUP BY category;
                """);

        ccs.step("""
                INSERT INTO grp_test VALUES
                    ('A', 10.0),
                    ('A', 20.0),
                    ('A', 30.0),
                    ('B', 100.0),
                    ('C', 1.0),
                    ('C', 2.0),
                    ('C', 3.0),
                    ('C', 4.0),
                    ('C', 5.0);
                """, """
                 category | median | weight
                ----------------------------
                 A| 20     | 1
                 B| 100    | 1
                 C| 3      | 1""");
    }

    @Test
    public void testPercentileAllSameValues() {
        // Test when all values are identical
        var ccs = this.getCCS("""
                CREATE TABLE same_vals (val DOUBLE);

                CREATE VIEW v_same AS SELECT
                    PERCENTILE_CONT(0.0) WITHIN GROUP (ORDER BY val) AS p0,
                    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY val) AS p50,
                    PERCENTILE_CONT(1.0) WITHIN GROUP (ORDER BY val) AS p100,
                    PERCENTILE_DISC(0.0) WITHIN GROUP (ORDER BY val) AS d0,
                    PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY val) AS d50,
                    PERCENTILE_DISC(1.0) WITHIN GROUP (ORDER BY val) AS d100
                FROM same_vals;
                """);

        ccs.step("""
                INSERT INTO same_vals VALUES (7.0), (7.0), (7.0), (7.0), (7.0);
                """, """
                 p0 | p50 | p100 | d0 | d50 | d100 | weight
                --------------------------------------------
                 7  | 7   | 7    | 7  | 7   | 7    | 1""");
    }

    @Test
    public void testPercentileRealType() {
        // Test PERCENTILE with REAL (float) type (using existing T table with R column)
        this.qs("""
                SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY R) AS real_result
                FROM T;
                 real_result
                -------------
                 1
                (1 row)

                SELECT PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY R) AS real_result
                FROM T;
                 real_result
                -------------
                 1
                (1 row)""");
    }

    @Test
    public void testPercentileOnAggregatedSubquery() {
        // Test PERCENTILE_CONT on a subquery that produces aggregated counts
        this.compileRustTestCase("""
                CREATE TABLE contracts (
                    project_id bigint
                );

                CREATE MATERIALIZED VIEW meta_contracts_per_project (
                    p50
                ) AS
                SELECT
                    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY cnt) as p50
                FROM (
                    SELECT project_id, COUNT(*)::double as cnt
                    FROM contracts
                    GROUP BY project_id
                ) t;
                """);
    }
}
