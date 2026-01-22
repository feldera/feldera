package org.dbsp.sqlCompiler.compiler.sql.simple;

import org.dbsp.sqlCompiler.circuit.operator.DBSPAggregateLinearPostprocessOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPLagOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamAggregateOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.sql.tools.Change;
import org.dbsp.sqlCompiler.compiler.sql.tools.CompilerCircuitStream;
import org.dbsp.sqlCompiler.compiler.sql.tools.SqlIoTest;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.sqlCompiler.ir.expression.DBSPCloneExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPZSetExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPStringLiteral;
import org.dbsp.sqlCompiler.ir.statement.DBSPStaticItem;
import org.dbsp.util.Linq;
import org.junit.Assert;
import org.junit.Test;

import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.function.Function;

public class Regression1Tests extends SqlIoTest {
    @Test
    public void issue4093() {
        this.statementsFailingInCompilation("""
                CREATE TABLE T(t0 int, t1 int NOT NULL);
                CREATE VIEW V AS SELECT string(t0) FROM T;""",
                "Cannot apply 'string' to arguments of type 'string(<INTEGER>)'");
    }

    @Test
    public void issue4053() {
        var cc = this.getCC("""
                CREATE TABLE tbl(t0 int, t1 int NOT NULL);
                CREATE VIEW V AS SELECT
                 SUM(t0),
                 SUM(t1),
                 MAX(t0),
                 MAX(t1),
                 MAX(t0 + t1)
                FROM tbl;""");
        CircuitVisitor visitor = new CircuitVisitor(cc.compiler) {
            int streamAggregates = 0;
            int linearAggregates = 0;

            @Override
            public void postorder(DBSPStreamAggregateOperator operator) {
                this.streamAggregates++;
            }

            @Override
            public void postorder(DBSPAggregateLinearPostprocessOperator operator) {
                this.linearAggregates++;
            }

            @Override
            public void endVisit() {
                // 3 separate max
                Assert.assertEquals(3, this.streamAggregates);
                // 1 for the two sums
                Assert.assertEquals(1, this.linearAggregates);
            }
        };
        cc.visit(visitor);
    }

    @Test
    public void mapMapTest() {
        this.statementsFailingInCompilation("CREATE TABLE TBL(x MAP<MAP<INT, INT>, INT>)",
                "MAP key type cannot be MAP");
    }

    @Test
    public void issue3952() {
        var ccs = this.getCCS("""
                CREATE TABLE tbl(t0 TIMESTAMP, t1 TIMESTAMP NOT NULL);
                
                CREATE LOCAL VIEW intervalv AS SELECT
                (t0 - TIMESTAMP '2025-06-21 14:23:44') YEAR AS yr0,
                (TIMESTAMP '2025-06-21 14:23:44' - t1) YEAR AS yr1,
                (t0 - TIMESTAMP '2025-06-21 14:23:44') DAYS AS d0,
                (TIMESTAMP '2025-06-21 14:23:44' - t1) DAYS AS d1
                FROM tbl;
                
                CREATE MATERIALIZED VIEW v AS SELECT
                CAST(ABS(yr0) AS BIGINT), CAST(ABS(yr1) AS BIGINT),
                CAST(ABS(d0) AS BIGINT), CAST(ABS(d1) AS BIGINT)
                FROM intervalv;""");
        ccs.step("INSERT INTO tbl VALUES(TIMESTAMP '2020-01-01 00:00:00', TIMESTAMP '2019-01-01 00:00:00')",
                """
                  v0 | v1 |  v2  |  v3  | weight
                 ----------------------------
                   5 |  6 | 1998 | 2363 | 1""");
    }

    @Test
    public void temporal() {
        var ccs = this.getCCS("""
                CREATE TABLE a (
                    c BIGINT,
                    j TIMESTAMP,
                    ad DOUBLE,
                    av VARCHAR,
                    bg DOUBLE,
                    bk SMALLINT
                );
                
                CREATE VIEW bl AS
                SELECT
                    bm.c AS bn,
                    bm.av AS bo,
                    SUM(CASE WHEN bm.bk = 0 THEN 1 ELSE 0 END) AS bp,
                    SUM(CASE WHEN bm.bk = 1 THEN 1 ELSE 0 END) AS bq,
                    COUNT(*) AS br,
                    SUM(CASE WHEN bm.bk = 0 THEN bm.bg ELSE 0.0 END) AS bs,
                    SUM(CASE WHEN bm.bk = 1 THEN bm.bg ELSE 0.0 END) AS bt,
                    SUM(bm.bg) AS bu
                FROM
                    a bm
                WHERE
                    bm.j >= NOW() - INTERVAL '1' year
                    AND bm.av IS NOT NULL
                    AND bm.bk IS NOT NULL
                    AND bm.bg IS NOT NULL
                    AND bm.ad IS NOT NULL
                    AND bm.bg > 0
                GROUP BY
                    bm.c,
                    bm.av;""");

        LocalDateTime now = LocalDateTime.now();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        String j = now.format(formatter);
        ccs.step("INSERT INTO A VALUES(NULL, '" + j + "', 0, 'a', NULL, 0)", """
                                     bn | bo | bp | bq | br | bs | bt | weight
                                    -------------------------------------------""");
        for (var c : Linq.list("NULL", "0")) {
            for (var ad : Linq.list("NULL", "0")) {
                for (var av : Linq.list("NULL", "'a'")) {
                    for (var bg : Linq.list("NULL", "0", "1")) {
                        for (var bk : Linq.list("NULL", "0")) {
                            String result = "";
                            if (nn(ad) && nn(av) && nn(bg) && nn(bk) && !bg.equals("0")) {
                                result = "\n " + c + " | a| " + (bk.equals("0") ? "1" : "0") + " | 0 | 1 | 1 | 0 | 1 | 1";
                            }
                            ccs.step("INSERT INTO A VALUES(" + c + ", '" + j + "', " + ad + ", " + av + ", " + bg + ", " + bk + ")", """
                                     bn | bo | bp | bq | br | bs | bt | weight
                                    -------------------------------------------""" + result);
                        }
                    }
                }
            }
        }
    }

    static boolean nn(String v) {
        return !v.equals("NULL");
    }

    @Test
    public void issue3972() {
        this.getCC("""
                CREATE TABLE tbl(
                row_row ROW(v2 ROW(v21 VARIANT NULL, v22 VARIANT)));
                CREATE MATERIALIZED VIEW v AS SELECT
                tbl.row_row[1] AS row_row2
                FROM tbl;""");
    }

    @Test
    public void testHsqlDb() throws SQLException, ClassNotFoundException {
        var ccs = this.getCCS("""
                CREATE TABLE t(x int);
                CREATE VIEW V AS SELECT * FROM T;""");
        String program = ccs.compiler.sources.getWholeProgram();
        ccs.compareDB(program, "INSERT INTO T VALUES(0);");
        ccs.compareDB(program, "INSERT INTO T VALUES(1);");
    }

    @Test
    public void decorrelateTest() throws SQLException, ClassNotFoundException {
        // Test for the CalciteOptimizer rule CorrelateUnionSwap
        String tables = """
                CREATE TABLE f1 (
                    arg0 varchar(10) not null,
                    arg1 varchar(10) not null
                );
                CREATE TABLE f2 (
                    arg0 varchar(10) not null,
                    arg1 varchar(10) not null
                );
                CREATE TABLE f3 (
                    arg0 varchar(10) not null,
                    arg1 varchar(10) not null
                );
                CREATE TABLE f4 (
                    arg0 varchar(10) not null
                );
                CREATE TABLE f5 (
                    arg0 varchar(10) not null,
                    arg1 varchar(10) not null
                );""";
        var ccs = this.getCCS(tables + """
                CREATE VIEW e as (
                  SELECT
                    f4.arg0 out0_val,
                    f1.arg0 out1_val
                  FROM
                    f1,
                    f4
                  WHERE
                    NOT EXISTS (
                      SELECT
                        true
                      FROM
                        f3
                      WHERE
                        NOT EXISTS (
                          SELECT
                            true
                          FROM
                            f2
                          WHERE
                            f3.arg1 = f2.arg1
                        )
                        and f4.arg0 = f3.arg0
                    )
                );
                """);
        Function<Integer, String> values =
                v -> "('" + ((v >> 1) % 2) + "', '" + (v % 2) + "');";

        String program = ccs.compiler.sources.getWholeProgram();
        for (int f1 = 0; f1 < 4; f1++) {
            for (int f2 = 0; f2 < 4; f2++) {
                for (int f3 = 0; f3 < 4; f3++) {
                    for (int f4 = 0; f4 < 2; f4++) {
                        for (int f5 = 0; f5 < 4; f5++) {
                            String builder = "INSERT INTO f1 VALUES" + values.apply(f1) +
                                    "INSERT INTO f2 VALUES" + values.apply(f2) +
                                    "INSERT INTO f3 VALUES" + values.apply(f3) +
                                    "INSERT INTO f4 VALUES('" + f4 + "');" +
                                    "INSERT INTO f5 VALUES" + values.apply(f5);
                            ccs.compareDB(program, builder);
                        }
                    }
                }
            }
        }
    }

    @Test
    public void issue4562() {
        this.qs("""
                SELECT TIMESTAMPADD(MINUTE, 2, '2020-06-21 14:23:44.123'::TIMESTAMP);
                 r
                ---
                 2020-06-21 14:25:44.123
                (1 row)""");
    }

    @Test
    public void issue4650() {
        var ccs = this.getCCS("""
                CREATE TABLE t(tmestmp TIMESTAMP);
                
                CREATE MATERIALIZED VIEW v AS SELECT
                TIMESTAMPDIFF(MINUTE, tmestmp, TIMESTAMP '2022-01-22 20:24:44.332') AS min,
                TIMESTAMPDIFF(SECOND, tmestmp, TIMESTAMP '2022-01-22 20:24:44.332') AS sec,
                TIMESTAMPDIFF(MINUTE, TIMESTAMP '2020-06-21 14:23:44.123', TIMESTAMP '2022-01-22 20:24:44.332') AS min1,
                TIMESTAMPDIFF(SECOND, TIMESTAMP '2020-06-21 14:23:44.123', TIMESTAMP '2022-01-22 20:24:44.332') AS sec1
                FROM t;""");
        ccs.step("INSERT INTO T VALUES(TIMESTAMP '2020-06-21 14:23:44.123');", """
                 min | sec | min1 | sec1 | weight
                --------------------------------------------
                 835561 | 50133660 | 835561 | 50133660 | 1""");

        ccs = this.getCCS("""
                CREATE TABLE t(tmestmp TIMESTAMP);

                CREATE MATERIALIZED VIEW v AS SELECT
                TIMESTAMPDIFF(MINUTE, tmestmp, '2022-01-22 20:24:44.332'::TIMESTAMP) AS min,
                TIMESTAMPDIFF(SECOND, tmestmp, '2022-01-22 20:24:44.332'::TIMESTAMP) AS sec,
                TIMESTAMPDIFF(MINUTE, '2020-06-21 14:23:44.123'::TIMESTAMP, '2022-01-22 20:24:44.332'::TIMESTAMP) AS min1,
                TIMESTAMPDIFF(SECOND, '2020-06-21 14:23:44.123'::TIMESTAMP, '2022-01-22 20:24:44.332'::TIMESTAMP) AS sec1
                FROM t;""");
        ccs.step("INSERT INTO T VALUES(TIMESTAMP '2020-06-21 14:23:44.123');", """
                 min | sec | min1 | sec1 | weight
                --------------------------------------------
                 835561 | 50133660 | 835561 | 50133660 | 1""");
    }

    @Test
    public void issue4010() {
        this.getCCS("""
                -- Customers.
                CREATE TABLE customer (
                    id BIGINT NOT NULL,
                    name varchar,
                    state VARCHAR,
                    -- Lateness annotation: customer records cannot arrive more than 7 days out of order.
                    ts TIMESTAMP LATENESS INTERVAL 7 DAYS
                );
                
                -- Credit card transactions.
                CREATE TABLE transaction (
                    -- Lateness annotation: transactions cannot arrive more than 1 day out of order.
                    ts TIMESTAMP LATENESS INTERVAL 1 DAYS,
                    amt DOUBLE,
                    customer_id BIGINT NOT NULL,
                    state VARCHAR
                );
                
                -- Data enrichment:
                -- * Use ASOF JOIN to find the most recent customer record for each transaction.
                -- * Compute 'out_of_state' flag, which indicates that the transaction was performed outside
                --   of the customer's home state.
                CREATE VIEW enriched_transaction AS
                SELECT
                    transaction.*,
                    (transaction.state != customer.state) AS out_of_state
                FROM
                    transaction LEFT ASOF JOIN customer
                    MATCH_CONDITION ( transaction.ts >= customer.ts )
                    ON transaction.customer_id = customer.id;
                
                -- Rolling aggregation: Compute the number of out-of-state transactions in the last 30 days for each transaction.
                CREATE VIEW transaction_with_history AS
                SELECT
                    *,
                    SUM(1) OVER window_30_day as out_of_state_count
                FROM
                    transaction
                WINDOW window_30_day AS (PARTITION BY customer_id ORDER BY ts RANGE BETWEEN INTERVAL 30 DAYS PRECEDING AND CURRENT ROW);""");
    }

    @Test
    public void issue4032() {
        this.getCCS("""
                CREATE VIEW V AS SELECT CASE COUNT ( * ) WHEN - 4 THEN - 75 + - 53 WHEN + 13 / - + 76
                THEN - 84 * ( - NULLIF ( COALESCE ( + + 54, COUNT ( * ) + + MAX ( ALL + 61 ), + - CASE -
                COUNT ( * ) WHEN - - COUNT ( * ) - 96 THEN - - 34 ELSE NULL END - + + 7 + - 77 / + 73 ), + 51 ) ) / + - 59 *
                NULLIF ( + 42, ( 23 ) ) WHEN CASE + 19 WHEN - 96 THEN NULL WHEN - COUNT ( * ) - 38 THEN 9 END THEN NULL END AS col2""");
    }

    @Test
    public void testFpAggNonLinear() {
        // FP aggregates are never linear
        var cc = this.getCC("""
                CREATE TABLE T(x DOUBLE);
                CREATE VIEW V AS SELECT SUM(x), AVG(x) FROM T;""");
        cc.visit(new CircuitVisitor(cc.compiler) {
            int streamAggregate = 0;
            int linearAggregate = 0;

            @Override
            public void postorder(DBSPStreamAggregateOperator operator) {
                this.streamAggregate++;
            }

            @Override
            public void postorder(DBSPAggregateLinearPostprocessOperator operator) {
                this.linearAggregate++;
            }

            @Override
            public void endVisit() {
                Assert.assertEquals(1, this.streamAggregate);
                Assert.assertEquals(0, this.linearAggregate);
            }
        });
    }

    @Test
    public void testExcept() {
        // Tests that the ExceptOptimizerRule is sound.
        // Note: currently this optimization does not exist.
        String sql = """
                CREATE TABLE F(ARG0 VARCHAR, ARG1 INT NOT NULL);
                CREATE TABLE G(ARG1 INT NOT NULL);
                
                CREATE LOCAL VIEW V0 AS SELECT true
                FROM G WHERE NOT EXISTS(
                SELECT true
                FROM F
                WHERE F.arg1 = G.arg1);
                
                CREATE LOCAL VIEW V1 AS SELECT true FROM ((SELECT arg1 FROM G) EXCEPT (SELECT arg1 FROM F));
                
                CREATE VIEW DIFF AS (SELECT * FROM V0 EXCEPT SELECT * FROM V1) UNION ALL (SELECT * FROM V1 EXCEPT SELECT * FROM V0)""";

        for (int i = 0; i < 2; i++) {
            DBSPCompiler compiler = this.testCompiler();
            if (i == 1)
                compiler.options.ioOptions.skipCalciteOptimizations = "Desugar EXCEPT";
            this.prepareInputs(compiler);
            compiler.submitStatementsForCompilation(sql);
            var ccs = new CompilerCircuitStream(compiler, this);
            final String empty = """
                     r | weight
                    ------------""";
            ccs.step("INSERT INTO F VALUES('a', 1);", empty);
            ccs.step("INSERT INTO F VALUES('b', 1);", empty);
            ccs.step("""
                    INSERT INTO F VALUES('a', 1);
                    INSERT INTO G VALUES(1)""", empty);
            ccs.step("""
                    INSERT INTO F VALUES('a', 1);
                    INSERT INTO G VALUES(2)""", empty);
            ccs.step("""
                    INSERT INTO F VALUES('a', 2);
                    INSERT INTO G VALUES(1)""", empty);
            ccs.step("""
                    INSERT INTO F VALUES('a', 1), ('b', 2);
                    INSERT INTO G VALUES(1)""", empty);
            ccs.step("""
                    INSERT INTO F VALUES('a', 1), ('a', 2);
                    INSERT INTO G VALUES(1), (2);""", empty);
            ccs.step("""
                    INSERT INTO F VALUES('a', 1), ('b', 1), ('a', 2);
                    INSERT INTO G VALUES(1), (2);""", empty);
            ccs.step("""
                    INSERT INTO F VALUES('a', 1);
                    INSERT INTO G VALUES(1), (2);""", empty);
        }
    }

    @Test
    public void testRuntimePanic() {
        this.runtimeConstantFail("SELECT 1.0 / 0", "Attempt to divide by zero: 1/0");
    }

    @Test
    public void issue4202() {
        this.statementsFailingInCompilation("""
                CREATE TABLE tbl(bin BINARY);
                CREATE MATERIALIZED VIEW v AS SELECT
                CONCAT(bin, bin) AS bin
                FROM tbl;""",
                "Support for CONCAT() with arguments of type BINARY");
    }

    @Test
    public void issue4203() {
        this.getCCS("""
                CREATE TABLE tbl(bin BINARY);
                CREATE MATERIALIZED VIEW v AS SELECT
                LEFT(bin, 2) AS bin
                FROM tbl;""");
    }

    @Test
    public void calciteIssue7070() {
        String sql = """
                CREATE TABLE tab0(pk INTEGER, col0 INTEGER, col1 DOUBLE, col2 TEXT, col3 INTEGER, col4 DOUBLE, col5 TEXT);
                CREATE VIEW V215 AS (SELECT + 54 FROM tab0 AS cor0
                WHERE NOT CAST ( + col4 AS INTEGER ) NOT BETWEEN ( NULL ) AND 89);""";
        this.getCC(sql);
    }

    @Test
    public void constantFunctionCall() {
        var ccs = this.getCCS("CREATE FUNCTION F(A INT ARRAY) RETURNS INT ARRAY AS A;" +
                "CREATE VIEW V AS SELECT ARRAY_REVERSE(F(ARRAY[2, 3, 4]));");
        final boolean[] staticFound = new boolean[1];
        ccs.getCircuit().accept(new InnerVisitor(ccs.compiler) {
            @Override
            public void postorder(DBSPStaticItem item) {
                staticFound[0] = true;
                // The whole tuple is a static
                Assert.assertTrue(item.expression.is(DBSPTupleExpression.class));
            }

            public void endVisit() {
                Assert.assertTrue(staticFound[0]);
            }
        });
    }

    @Test
    public void issue4255() {
        this.getCCS("""
                CREATE TABLE illegal_tbl(bin BINARY);
                CREATE VIEW v AS SELECT CONCAT_WS('@', bin, 55) AS bin FROM illegal_tbl;""");
    }

    @Test
    public void issue4265() {
        this.statementsFailingInCompilation("""
                CREATE TABLE tbl(bin BINARY);
                CREATE VIEW v AS SELECT
                OVERLAY(bin placing 'i' from 2 for 4) AS bin FROM tbl;""",
                "First and second arguments to 'OVERLAY' must have the same type");
    }

    @Test
    public void issue4263() {
        this.statementsFailingInCompilation("""
                CREATE TABLE tbl(bin BINARY);
                CREATE VIEW v AS SELECT
                REPEAT(bin, 2) AS bin FROM tbl;
                """, "Not yet implemented: 'REPEAT' with a VAR/BINARY argument not yet supported");
    }

    @Test
    public void issue4264() {
        this.qs("""
                SELECT TRIM(trailing '' FROM 'x');
                 result
                --------
                 x
                (1 row)
                
                SELECT TRIM(leading 'abc' FROM 'abacabadaba');
                 result
                --------
                 daba
                (1 row)
                
                SELECT TRIM(both 'abc' FROM 'abacabadaba');
                 result
                --------
                 d
                (1 row)""");
    }

    @Test
    public void castBinaryToString() {
        var ccs = this.getCCS("""
                CREATE TABLE T(x BINARY(2));
                CREATE VIEW V AS SELECT CAST(x AS VARCHAR), CAST(x'AB01' AS VARCHAR) FROM T;""");
        ccs.step("INSERT INTO T VALUES(x'AB01')", """
                    x| y   | weight
                --------------------
                 ab01| ab01| 1""");
    }

    @Test
    public void castDecimalToTimestamp() {
        var ccs = this.getCCS("""
                CREATE TABLE T(x DECIMAL(10, 2), y INT);
                CREATE VIEW V AS SELECT CAST(x AS TIMESTAMP), CAST(10.20 AS TIMESTAMP),
                CAST(10 AS TIMESTAMP), cast(y AS TIMESTAMP) FROM T;""");
        ccs.step("INSERT INTO T VALUES(10.20, 10)", """
                 x                   | decimal             | int                 | y                  | weight
                -----------------------------------------------------------------------------------------------
                 1970-01-01 00:00:00 | 1970-01-01 00:00:00 | 1970-01-01 00:00:00.010 | 1970-01-01 00:00:00 |1""");
    }

    @Test
    public void issue4409() {
        this.statementsFailingInCompilation("""
                CREATE TABLE tbl(str VARCHAR);
                CREATE MATERIALIZED VIEW v AS SELECT
                ARRAY_CONCAT(str, str)  AS str
                FROM tbl;""", "Cannot apply 'ARRAY_CONCAT' to arguments of type");
    }

    @Test
    public void testMapArgMax() {
        this.getCCS("""
                CREATE TABLE map_tbl(
                id INT,
                c1 MAP<VARCHAR, INT> NOT NULL,
                c2 MAP<VARCHAR, INT>);
                
                CREATE VIEW map_arg_max_distinct_gby AS SELECT
                id, ARG_MAX(DISTINCT c1, c2) AS c1
                FROM map_tbl GROUP BY id;""");
    }

    @Test
    public void testAntiJoinIntern() {
        this.getCCS("""
                CREATE TABLE T(x VARCHAR INTERNED, y INT);
                CREATE TABLE S(x VARCHAR);
                CREATE VIEW V AS SELECT T.x, y FROM T LEFT JOIN S ON T.x = S.x;""");
    }

    @Test
    public void issue4335() {
        var ccs = this.getCCS("""
                CREATE TABLE T(id INT, od INT, val DECIMAL(5, 2), ct INT, e BOOLEAN);
                CREATE VIEW V AS SELECT
                    SUM(CASE WHEN od = 0 THEN 1 ELSE 0 END),
                    SUM(CASE WHEN od = 1 THEN 1 ELSE 0 END),
                    COUNT(*),
                    SUM(val),
                    COUNT(DISTINCT CASE WHEN od = 0 THEN id END),
                    COUNT(DISTINCT CASE WHEN od = 1 THEN id END),
                    COUNT(DISTINCT id),
                    SUM(CASE WHEN (od = 0 AND e) THEN 1 ELSE 0 END),
                    SUM(CASE WHEN (od = 1 AND e) THEN 1 ELSE 0 END),
                    SUM(CASE WHEN e THEN 1 ELSE 0 END),
                    SUM(CASE WHEN (od = 0 AND e) THEN ROUND(val, 0) ELSE 0.0 END),
                    COUNT(DISTINCT CASE WHEN (od = 0 AND e) THEN id END),
                    COUNT(DISTINCT CASE WHEN (od = 1 AND e) THEN id END),
                    COUNT(DISTINCT CASE WHEN (e) THEN id END),
                    COUNT(DISTINCT id)
                FROM T""");
        ccs.visit(new CircuitVisitor(ccs.compiler) {
            @Override
            public void postorder(DBSPAggregateLinearPostprocessOperator operator) {
                // Unused input fields for aggregates are removed
                int width = operator.input().getOutputIndexedZSetType().elementType.getToplevelFieldCount();
                Assert.assertTrue(width < 10);
            }
        });
        // Validated on MySQL
        ccs.step("INSERT INTO T VALUES(0, 1, 2, 3, true), (1, 2, 3, 4, false)", """
                 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9 | 10 | 11 | 12 | 13 | 14 | 15 | weight
                --------------------------------------------------------------------------
                 0 | 1 | 2 | 5 | 0 | 1 | 2 | 0 | 1 | 1  |  0 |  0 |  1 |  1 |  2 | 1""");
    }

    @Test
    public void issue4404() {
        var ccs = this.getCCS("""
                CREATE TABLE tbl(str VARCHAR);
                CREATE MATERIALIZED VIEW v AS SELECT
                MOD(str, 3) AS arr
                FROM tbl;""");
        ccs.step("INSERT INTO tbl VALUES('14')", """
                 mod | weight
                --------------
                 2   | 1""");
    }

    @Test
    public void issue3942() {
        var ccs = this.getCCS("""
                CREATE TABLE tbl(
                row_map ROW(v1 MAP<VARIANT, VARIANT> NULL));
                CREATE MATERIALIZED VIEW v AS SELECT
                CAST(row_map[1]['x'] AS INTEGER) AS x
                FROM tbl;""");
        ccs.step("""
                INSERT INTO tbl VALUES(ROW(ROW(MAP['x', 1, 'y', 2])))""", """
                 result | weight
                ------------------
                 1      |1""");
    }

    @Test
    public void variantNullIntern() {
        this.getCC("""
                CREATE TABLE T(X INT, C VARCHAR INTERNED);
                CREATE TABLE S(X INT, Z VARIANT);
                CREATE MATERIALIZED VIEW V AS SELECT T.X, S.Z
                FROM T LEFT JOIN S ON T.X = S.X;""");
    }

    @Test
    public void issue4405() {
        var ccs = this.getCCS("""
                CREATE TABLE tbl(arr VARCHAR ARRAY);
                CREATE MATERIALIZED VIEW v AS SELECT
                arr[2] AS arr,
                arr[SAFE_OFFSET(2)] AS arr2
                FROM tbl;""");
        ccs.step("INSERT INTO tbl VALUES(ARRAY['bye', '14', 'See you!', '-0.52']);", """
                 arr | arr2 | weight
                ---------------------
                 14| See you!| 1""");
    }

    @Test
    public void issue4448() {
        var ccs = this.getCCS("""
                CREATE TABLE T(x CHAR(4), y INT);
                CREATE TABLE S(y INT, z INT);
                CREATE VIEW V AS SELECT T.x, T.y, S.z FROM T, S WHERE T.x in ('a   ', 'ab  ') AND T.y = S.y;""");
        ccs.step("INSERT INTO T VALUES('a', 1), ('b', 2);" +
                "INSERT INTO S VALUES(1, 3);", """
                 x   | y | z | weight
                ----------------------
                 a   | 1 | 3 | 1""");
    }

    @Test
    public void issue4448a() {
        var ccs = this.getCCS("""
                CREATE TABLE T(x CHAR(10));
                CREATE VIEW V AS SELECT * FROM T WHERE x in ('hi        ', 'hey       ');""");
        ccs.step("INSERT INTO T VALUES('hi');", """
                 x         | weight
                --------------------
                 hi        | 1""");

        // Here the constant needs to be padded, but not above.
        ccs = this.getCCS("""
                CREATE TABLE T(x CHAR(10));
                CREATE VIEW V AS SELECT * FROM T WHERE x in ('hi        ');""");
        ccs.step("INSERT INTO T VALUES('hi');", """
                 x         | weight
                --------------------
                 hi        | 1""");
    }

    @Test
    public void issue4446() {
        this.getCC("""
                create table t (d decimal(8,3));
                create view v as select trunc(d) from t;""");
    }

    @Test
    public void issue4456() {
        this.getCCS("""
                CREATE TABLE tbl(arr VARCHAR ARRAY);
                CREATE MATERIALIZED VIEW v AS SELECT
                ARRAY_COMPACT(arr) AS arr
                FROM tbl;""");
    }

    @Test
    public void issue4467() {
        var ccs = this.getCCS("""
                CREATE TABLE tbl(arr1 VARCHAR ARRAY, str VARCHAR);
                CREATE MATERIALIZED VIEW v AS SELECT
                ARRAY_EXCEPT(arr1, ARRAY['hello ']) AS res
                FROM tbl;""");
        ccs.step("INSERT INTO tbl VALUES(ARRAY['bye', '14', 'See you!', '-0.52', NULL, '14', 'hello '], 'hello ');", """
                 res                               | weight
                --------------------------------------------
                 {NULL, -0.52, 14, See you!, bye} | 1""");
    }

    @Test
    public void issue4467a() {
        var ccs = this.getCCS("""
                CREATE TABLE tbl(arr VARCHAR ARRAY, str VARCHAR);
                CREATE MATERIALIZED VIEW v AS SELECT
                ARRAY_INTERSECT(arr, ARRAY['hello ']) AS arr
                FROM tbl;""");
        ccs.step("INSERT INTO tbl VALUES(ARRAY['bye', '14', 'See you!', '-0.52', NULL, '14', 'hello '], 'hello ');", """
                 res       | weight
                --------------------
                 { hello } | 1""");
    }

    @Test
    public void issue4483() {
        this.getCCS("""
                CREATE TABLE row_tbl(
                c1 INT NOT NULL,
                c2 VARCHAR,
                c3 VARCHAR)WITH ('append_only' = 'true');
                CREATE MATERIALIZED VIEW row_max AS SELECT
                MAX(ROW(c1, c2, c3)) AS c1
                FROM row_tbl;""");
    }

    @Test
    public void issue5087() {
        this.qs("""
                SELECT '1
                2' AS r;
                 r
                ---
                 1\\n2
                (1 row)
                
                SELECT U&'hello\\0041';
                 r
                ---
                 helloA
                (1 row)
                
                SELECT U&'hello!0041' UESCAPE '!';
                 r
                ---
                 helloA
                (1 row)""");
    }

    @Test
    public void issue5087a() {
        var ccs = this.getCCS("""
                -- C escape sequences do not have any special meaning
                CREATE VIEW V AS SELECT 'a\\t\\b\\n\\r'""");
        ccs.addPair(new Change(),
                new Change("V",
                new DBSPZSetExpression(new DBSPTupleExpression(
                        new DBSPStringLiteral("a\\t\\b\\n\\r")))));
    }

    @Test
    public void issue4467b() {
        var ccs = this.getCCS("""
                CREATE TABLE tbl(arr VARCHAR ARRAY, str VARCHAR);
                CREATE MATERIALIZED VIEW v AS SELECT
                ARRAYS_OVERLAP(arr, ARRAY['hello ']) AS arr
                FROM tbl;""");
        ccs.step("INSERT INTO tbl VALUES(ARRAY['bye', '14', 'See you!', '-0.52', NULL, '14', 'hello '], 'hello ');", """
                 res  | weight
                ---------------
                 true | 1""");
    }

    @Test
    public void issue4587() {
        this.getCCS("""
                CREATE TABLE t(
                tmestmp TIMESTAMP,
                datee DATE,
                tme TIME);
                
                CREATE MATERIALIZED VIEW v0 AS SELECT
                FLOOR(tmestmp TO YEAR) AS yr,
                FLOOR(datee TO MONTH) AS mth,
                FLOOR(tme TO HOUR) AS hr,
                FLOOR(tmestmp TO MILLISECOND) AS millsec,
                FLOOR(tmestmp TO MICROSECOND) AS microsec,
                FLOOR(tme TO MILLISECOND) AS millsec1,
                FLOOR(tme TO MICROSECOND) AS microsec
                FROM t;
                
                CREATE MATERIALIZED VIEW v1 AS SELECT
                CEIL(tmestmp TO YEAR) AS yr,
                CEIL(datee TO MONTH) AS mth,
                CEIL(tme TO HOUR) AS hr,
                CEIL(tmestmp TO MILLISECOND) AS millsec,
                CEIL(tmestmp TO MICROSECOND) AS microsec,
                CEIL(tme TO MILLISECOND) AS millsec1,
                CEIL(tme TO MICROSECOND) AS microsec
                FROM t;""");
    }

    @Test
    public void issue4614() {
        this.statementsFailingInCompilation("CREATE VIEW V AS SELECT CEIL(DATE '2024-01-01' TO DOW)",
                "Function 'CEIL' not supported with unit 'dow'");
        this.statementsFailingInCompilation("CREATE VIEW V AS SELECT FLOOR(TIME '10:00:00' TO DOY)",
                "Function 'FLOOR' not supported with unit 'doy'");
    }

    @Test
    public void argMin() {
        var ccs = this.getCCS("""
                CREATE TABLE int0_tbl(
                id INT NOT NULL,
                c1 TINYINT,
                c2 TINYINT NOT NULL,
                c3 INT2,
                c4 INT2 NOT NULL,
                c5 INT,
                c6 INT NOT NULL,
                c7 BIGINT,
                c8 BIGINT NOT NULL);
                CREATE MATERIALIZED VIEW int_arg_min_diff AS SELECT
                ARG_MIN(c1, c2) AS c1, ARG_MIN(c2, c1) AS c2, ARG_MIN(c3, c4) AS c3, ARG_MIN(c4, c3) AS c4,
                ARG_MIN(c5, c6) AS c5, ARG_MIN(c6, c5) AS c6, ARG_MIN(c7, c8) AS c7, ARG_MIN(c8, c7) AS c8
                FROM int0_tbl;""");
        ccs.step("""
                INSERT INTO int0_tbl VALUES
                -- id, c1,  c2, c3,  c4, c5, c6, c7, c8
                   (0, 5,    2, NULL, 4, 5, 6, NULL, 8),
                   (1, 4,    3, 4,    6, 2, 3, 4,    2),
                   (0, NULL, 2, 3,    2, 3, 4, 3,    3),
                   (1, NULL, 5, 6,    2, 2, 1, NULL, 5);""", """
                 c1 | c2 | c3 | c4 | c5 | c6 | c7 | c8 | weight
                ------------------------------------------------
                    |  3 |  3 |  2 |  2 | 1  |  4 |  3 | 1""");
    }

    @Test
    public void containsTest() {
        this.getCCS("""
                CREATE TABLE T(x BIGINT, y VARCHAR, z INT ARRAY);
                CREATE VIEW V0 AS SELECT ARRAY_CONTAINS(ARRAY[1], x) FROM T;
                CREATE VIEW V1 AS SELECT ARRAY_CONTAINS(ARRAY['1', '2', NULL], y) FROM T;
                CREATE VIEW V2 AS SELECT ARRAY_CONTAINS(z, x) FROM T;
                CREATE VIEW V3 AS SELECT ARRAY_CONTAINS(z, 0) FROM T;
                CREATE VIEW V4 AS SELECT ARRAY_CONTAINS(z, NULL) FROM T;""");
    }

    @Test
    public void issue4577() {
        var ccs = this.getCCS("""
                CREATE TABLE T(X VARCHAR);
                CREATE VIEW V AS SELECT NULLIF(INITCAP(REPLACE(x, '_', ' ')), '') FROM T;""");
        int[] cloneCount = new int[1];
        InnerVisitor cloneCounter = new InnerVisitor(ccs.compiler) {
            @Override
            public void postorder(DBSPCloneExpression expression) {
                cloneCount[0]++;
            }
        };
        ccs.visit(cloneCounter.getCircuitVisitor(false));
        Assert.assertEquals(7, cloneCount[0]);
    }

    @Test
    public void issue4585() {
        this.getCCS("""
                CREATE TABLE T(x TIME);
                CREATE VIEW V AS SELECT HOUR(x) FROM T;""");
    }

    @Test
    public void sltCrash() {
        this.getCC("""
                CREATE TABLE tab2(col0 INTEGER, col1 INTEGER, col2 INTEGER);
                CREATE VIEW V AS SELECT ( + 34 ) * - ( col0 / + ( col1 / 79 ) ) FROM tab2 WHERE NOT col1 IS NOT NULL;""");
    }

    @Test
    public void issue4677() {
        this.qf("SELECT CAST(1 AS TINYINT UNSIGNED) / CAST(0 AS TINYINT UNSIGNED)",
                "'1 / 0' causes overflow for type TINYINT UNSIGNED");
    }

    @Test
    public void issue4649() {
        var ccs = this.getCCS("""
                CREATE TABLE t(booll BOOL);
                
                CREATE MATERIALIZED VIEW equality_illegal AS SELECT
                booll = 456 AS booll,
                FALSE = 456 AS booll2
                FROM t;""");
        ccs.step("INSERT INTO T VALUES(false);", """
                 booll | booll2 | weight
                -------------------------
                 false | false  | 1""");
    }

    @Test
    public void sltBugTest() {
        var ccs = this.getCCS("""
                CREATE TABLE tab1(col0 INTEGER, col1 INTEGER, col2 INTEGER);
                CREATE VIEW V AS SELECT ALL 77 + col1 * - ( - col2 / + CAST ( NULL AS INTEGER ) ) FROM tab1
                """);
        ccs.step("INSERT INTO tab1 VALUES(0, 1, 2);", """
                 r | weight
                -------------
                NULL | 1""");
    }

    @Test
    public void issue4708() {
        var ccs = this.getCCS("""
                CREATE TABLE tbl(intt INT);
                
                CREATE MATERIALIZED VIEW v AS SELECT
                intt <=> -12 AS intt FROM tbl;""");
        ccs.step("INSERT INTO tbl VALUES(NULL), (-12)", """
                 r | weight
                ------------
                 true | 1
                 false | 1""");
    }

    @Test
    public void issue4729() {
        this.getCCS("CREATE TABLE t (ts BIGINT NOT NULL PRIMARY KEY LATENESS 5);");
    }

    @Test
    public void issue4752() {
        var ccs = this.getCCS("""
                CREATE TABLE tbl(arr VARCHAR ARRAY);
                
                CREATE MATERIALIZED VIEW v1 AS SELECT
                arr BETWEEN ARRAY['bye', '14'] AND ARRAY['bye', '14'] AS arr
                FROM tbl;""");
        ccs.step("INSERT INTO tbl VALUES(ARRAY()), (ARRAY['bye', '14'])", """
                 arr | weight
                --------------
                 false | 1
                 true  | 1""");

        this.getCCS("""
                CREATE TABLE tbl(bin BINARY);
                
                CREATE MATERIALIZED VIEW v2 AS SELECT
                bin BETWEEN X'0B1620' AND X'0B1620' AS bin
                FROM tbl;""");
    }

    @Test
    public void issue4834() {
        this.statementsFailingInCompilation("""
                CREATE TABLE tbl(str VARCHAR, tmestmp TIMESTAMP, datee DATE, tme TIME);
                CREATE MATERIALIZED VIEW v AS SELECT
                LEAST(str, False) AS str FROM tbl;""",
                "Cannot infer return type for LEAST; operand types: [VARCHAR, BOOLEAN]");
    }

    @Test
    public void issue4815() {
        var ccs = this.getCCS("""
                CREATE TABLE tbl(bin BINARY(3));
                
                CREATE VIEW G AS SELECT
                LEAST(bin, X'1F8B0800') AS res,
                LEAST(X'0B1620', X'1F8B0800') AS res1
                FROM tbl;""");
        ccs.step("INSERT INTO tbl VALUES(x'0B1620')", """
                 res | res1 | weight
                ---------------------
                 0B1620 | 0B1620 | 1""");
    }

    @Test
    public void issue4792() {
        this.getCCS("""
                CREATE TABLE T(v VARCHAR, x VARCHAR, z INT);
                CREATE VIEW V0 AS SELECT ARRAY_AGG(v ORDER BY x) FROM T;
                CREATE VIEW V1 AS SELECT ARRAY_AGG(ROW(v, z) ORDER BY x) FROM T;""");
    }

    @Test
    public void issue4797() {
        var ccs = this.getCCS("""
                CREATE TABLE tbl(reall REAL, dbl DOUBLE, str VARCHAR);
                CREATE MATERIALIZED VIEW v1 AS SELECT
                GREATEST(str, '0.12') AS str, LEAST(reall, -0.1234567) as reall, LEAST(dbl, -0.82711234601246) AS dbl
                FROM tbl;""");
        ccs.step("""
                INSERT INTO tbl VALUES(-57681.18, -38.2711234601246, 'hello ');
                """, """
                 str   | reall          | dbl              | weight
                -----------------------------------------------
                 hello | -57681.1796875 | -38.271123460125 | 1""");
    }

    @Test
    public void issue4817() {
        this.getCCS("""
                CREATE TABLE tbl(mapp MAP<VARCHAR, INT>);
                
                CREATE MATERIALIZED VIEW v AS SELECT
                LEAST_IGNORE_NULLS(mapp, MAP['a', 13, 'b', 17]) AS mapp
                FROM tbl;""");
    }

    @Test
    public void issue4814() {
        this.getCCS("""
                CREATE TABLE tbl(mapp MAP<VARCHAR, INT>);
                
                CREATE MATERIALIZED VIEW v AS SELECT
                LEAST(mapp, MAP['a', 13, 'b', 17]) AS mapp
                FROM tbl;""");
    }

    @Test
    public void issue4794() {
        this.getCCS("""
                CREATE TABLE tbl(
                arr VARCHAR ARRAY NULL,
                mapp MAP<VARCHAR, INT> NULL);
                
                CREATE MATERIALIZED VIEW v1 AS SELECT
                COALESCE(NULL, arr, ARRAY ['bye']) AS arr
                FROM tbl;
                
                CREATE MATERIALIZED VIEW v2 AS SELECT
                COALESCE(NULL, mapp, MAP['a', 15, 'b', NULL]) AS mapp
                FROM tbl;""");
    }

    @Test
    public void issue4794_2() {
        this.getCCS("""
                CREATE TABLE tbl(
                arr VARCHAR ARRAY NULL,
                mapp MAP<VARCHAR, INT> NULL);
                
                CREATE MATERIALIZED VIEW v3 AS SELECT
                COALESCE(mapp, MAP['a', 15, 'b', NULL]) AS mapp
                FROM tbl;""");
    }

    @Test
    public void issue4821() {
        this.getCCS("""
                CREATE TABLE t (ids VARIANT NOT NULL);
                CREATE VIEW v AS SELECT *
                FROM t WHERE ARRAY_MAX(CAST(t.ids AS BIGINT ARRAY)) < 10;""");
    }

    @Test
    public void issue4847() {
        // Validated on Postgres
        var ccs = this.getCCS("""
                CREATE TABLE tbl(id INT, tiny_int TINYINT UNSIGNED);
                
                CREATE MATERIALIZED VIEW v AS SELECT
                STDDEV(tiny_int) AS tiny_int
                FROM tbl;""");
        ccs.step("INSERT INTO tbl VALUES (1, 255), (1, 1)", """
                 tiny_int | weight
                -------------------
                 179      | 1""");
    }

    @Test
    public void issue4848() {
        this.runtimeConstantFail(
                "SELECT STDDEV(x) FROM (VALUES (9223372036854775807), (9223372036854775807)) AS tbl(x)",
                "'18446744073709551614 * 18446744073709551614' causes overflow for type BIGINT");
    }

    @Test
    public void ifnullTest() {
        var ccs = this.getCCS("""
            CREATE TABLE T(x INT);
            CREATE VIEW V AS SELECT IFNULL(x, 2) FROM T;""");
        ccs.step("INSERT INTO T VALUES(3), (NULL)", """
                 x | weight
                ------------
                 3 | 1
                 2 | 1""");
    }

    @Test
    public void notDistinctFromTest() {
        this.getCCS("""
                CREATE TABLE illegal_tbl(tmestmp TIMESTAMP);
                
                CREATE MATERIALIZED VIEW equality_null_legal AS SELECT
                tmestmp <=> NULL AS tmestmp FROM illegal_tbl;""");
    }

    @Test
    public void issue4915() {
        this.getCC("""
                CREATE FUNCTION X(d VARCHAR NOT NULL)
                RETURNS TIMESTAMP
                AS ( COALESCE(
                    -- ISO 8601 formats with timezone (Z suffix)
                    PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S Z', d),
                    -- ISO 8601 with timezone offset (+/-HHMM or +/-HH:MM)
                    PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S.%3f%:z', d)));
                
                CREATE materialized VIEW V0 AS SELECT X('2011-12-01 00:00:00');""");
    }

    @Test
    public void issue4888() {
        this.getCC("""
                CREATE TABLE tbl(
                roww ROW(i1 INT, v1 VARCHAR NULL));
                
                CREATE MATERIALIZED VIEW v AS SELECT
                COALESCE(NULL, roww, ROW(4,'cat')) AS roww
                FROM tbl;""");
    }

    @Test
    public void issue4889() {
        this.getCC("""
                CREATE TABLE tbl(
                roww ROW(i1 INT, v1 VARCHAR NULL));
                
                CREATE MATERIALIZED VIEW v1 AS SELECT
                GREATEST_IGNORE_NULLS(NULL, roww, ROW(5, NULL)) AS roww
                FROM tbl;
                
                CREATE MATERIALIZED VIEW v2 AS SELECT
                LEAST_IGNORE_NULLS(NULL, roww, ROW(5, NULL)) AS roww
                FROM tbl;""");
    }

    @Test
    public void issue4932() {
        this.getCC("""
                CREATE TABLE tab0(col0 INTEGER, col1 INTEGER, col2 INTEGER);
                CREATE TABLE tab1(col0 INTEGER, col1 INTEGER, col2 INTEGER);
                CREATE VIEW V53 AS (SELECT DISTINCT * FROM tab1 AS cor0 JOIN tab0 cor1 ON NULL < - - ( + + CAST ( + 47 AS INTEGER ) ) - - - 47);
                """);
    }

    @Test
    public void issue4923() {
        this.getCCS("""
                CREATE TABLE tbl(
                roww ROW(i1 INT, v1 VARCHAR NULL));
                
                CREATE MATERIALIZED VIEW v AS SELECT
                ARRAY(SELECT roww, roww FROM tbl) AS arr;""");
    }

    @Test
    public void mapVariant() {
        var ccs = this.getCCS("""
                create table j(j VARCHAR);
                
                create LOCAL view user_props AS
                SELECT PARSE_JSON(j) AS contacts FROM j;
                
                create view abc as
                WITH ref_profile AS (
                SELECT cast(contacts as MAP<varchar, variant>) contacts
                    FROM user_props
                ) SELECT key, to_json(contact)
                FROM ref_profile profile_0, UNNEST(profile_0.contacts) AS t(key, contact)""");
        ccs.step("""
                INSERT INTO j VALUES('{ "a": "1", "b": 2, "c": [1, 2, 3], "d": null, "e": { "f": 1 } }');""", """
                 key | contact | weight
                ------------------------
                 a| "1"| 1
                 b| 2| 1
                 c| [1,2,3]| 1
                 d| null|1
                 e| {"f":1}|1""");
    }

    @Test
    public void issue4890() {
        this.getCC("""
                CREATE TABLE tbl(
                roww ROW(i1 INT, v1 VARCHAR NULL));
                
                CREATE MATERIALIZED VIEW v1 AS SELECT
                roww IN (ROW(4,'cat')) AS roww
                FROM tbl;
                
                CREATE MATERIALIZED VIEW v2 AS SELECT
                roww IN (ROW(4,'cat')) AS roww
                FROM tbl;""");
    }

    @Test
    public void issue4891() {
        this.getCC("""
                CREATE TABLE tbl(
                roww ROW(i1 INT, v1 VARCHAR NULL));
                
                CREATE MATERIALIZED VIEW v1 AS SELECT
                roww IN (roww) AS roww
                FROM tbl;
                
                CREATE MATERIALIZED VIEW v2 AS SELECT
                roww NOT IN (roww) AS roww
                FROM tbl;""");
    }

    @Test
    public void issue4975() {
        this.getCCS("""
                CREATE TABLE tbl1(id INT, c2 VARCHAR);
                
                CREATE TABLE tbl2(id INT, c2 VARCHAR);
                
                CREATE MATERIALIZED VIEW v AS SELECT
                i.id, i.c2 AS i_c2, v.c2 AS v_c2
                FROM tbl1 i
                LEFT ASOF JOIN tbl2 v
                MATCH_CONDITION ( i.c2 >= v.c2 )
                ON i.id = v.id;""");
    }

    @Test
    public void issue4989() {
        var ccs = this.getCCS("""
                CREATE TABLE T (
                    id INT NOT NULL,
                    step BOOLEAN,
                    en VARCHAR,
                    ss INT,
                    ts BIGINT NOT NULL);
                
                CREATE VIEW s AS
                SELECT
                    *,
                    CASE
                      WHEN LAG(ts) OVER (
                        PARTITION BY id
                        ORDER BY ts
                      ) IS NULL THEN 0
                      ELSE (ts - LAG(ts) OVER (
                        PARTITION BY id
                        ORDER BY ts
                      )) / 10
                    END AS X,
                    LAG(step, 1, FALSE) OVER (
                      PARTITION BY id
                      ORDER BY ts
                    ) as Y,
                    CASE
                      WHEN ss = 10 AND en != LAG(en, 1, 'alpha') OVER (
                        PARTITION BY id
                        ORDER BY ts
                      ) THEN TRUE
                      ELSE FALSE
                    END as Z
                  FROM T;
                """);
        ccs.visit(new CircuitVisitor(ccs.compiler) {
            int lagCount = 0;

            @Override
            public void postorder(DBSPLagOperator node) {
                this.lagCount++;
            }

            @Override
            public void endVisit() {
                Assert.assertEquals(1, this.lagCount);
            }
        });
        // Validated using Postgres
        ccs.step("""
                        INSERT INTO T (id, step, en, ss, ts) VALUES (1, TRUE, 'alpha', 10, 590);
                        INSERT INTO T (id, step, en, ss, ts) VALUES (2, FALSE, 'beta', 10, 593);
                        INSERT INTO T (id, step, en, ss, ts) VALUES (1, TRUE, 'alpha', 10, 597);
                        INSERT INTO T (id, step, en, ss, ts) VALUES (2, FALSE, 'alpha', 25, 600);
                        INSERT INTO T (id, step, en, ss, ts) VALUES (1, TRUE, 'gamma', 10, 604);
                        INSERT INTO T (id, step, en, ss, ts) VALUES (2, FALSE, 'eta', 12, 608);
                        INSERT INTO T (id, step, en, ss, ts) VALUES (1, TRUE, 'beta', 10, 116);
                        INSERT INTO T (id, step, en, ss, ts) VALUES (2, FALSE, 'gamma', 25, 615);
                        INSERT INTO T (id, step, en, ss, ts) VALUES (1, TRUE, 'delta', 20, 618);
                        INSERT INTO T (id, step, en, ss, ts) VALUES (2, FALSE, 'gamma', 25, 622);""",
                """
                 id | step | en   | ss | ts  | x |     y |     z | weight
                ----------------------------- ----------------------------
                 1 | true  | beta|	10 | 116 | 0 | false | true  | 1
                 1 | true  | alpha|	10 | 590 | 47| true  | true  | 1
                 1 | true  | alpha|	10 | 597 | 0 | true  | false | 1
                 1 | true  | gamma|	10 | 604 | 0 | true  | true  | 1
                 1 | true  | delta| 20 | 618 | 1 | true  | false | 1
                 2 | false | beta|	10 | 593 | 0 | false | true  | 1
                 2 | false | alpha|	25 | 600 | 0 | false | false | 1
                 2 | false | eta|	12 | 608 | 0 | false | false | 1
                 2 | false | gamma|	25 | 615 | 0 | false | false | 1
                 2 | false | gamma|	25 | 622 | 0 | false | false | 1""");
    }

    @Test
    public void testSlt() {
        this.getCC("""
                CREATE TABLE tab0(
                pk INTEGER, col0 INTEGER, col1 REAL, col2 TEXT, col3 INTEGER, col4 REAL, col5 TEXT);
                
                CREATE VIEW V AS SELECT pk FROM tab0 WHERE col4 IS NULL AND ((col3 >= 4) OR col1 < 6.94 OR
                 (((col1 BETWEEN 7.86 AND 3.33) AND (col3 > 3 AND col1 <= 3.18 AND col0 > 4 AND col4 < 2.65)
                 AND (col0 IS NULL))) OR col4 BETWEEN 2.56 AND 1.46 AND col3 < 5) OR col1 BETWEEN 0.15 AND 9.37 AND
                 (col3 IS NULL AND ((col4 > 0.44))) OR (col4 > 0.89 AND (col0 <= 6) OR col0 IS NULL OR col4 <= 5.72 AND
                 (col4 <= 0.76 AND col4 > 1.83 OR (col1 >= 0.82) AND (col3 > 1)) AND ((col0 <= 7 AND col4 > 2.76))
                 OR col0 > 0 AND (col4 IS NULL) AND (col3 IN (5)) AND
                 col0 IN (SELECT col3 FROM tab0 WHERE col3 IS NULL)) OR (col0 IS NULL AND col0 = 7 AND col0 IS NULL)""");
    }

    @Test
    public void intervalTest() {
        var ccs = this.getCCS("""
                CREATE TABLE interval_tbl(
                id INT NOT NULL,
                c1 TIMESTAMP,
                c2 TIMESTAMP);
                
                CREATE LOCAL VIEW atbl_interval_months AS SELECT
                id,
                (c1 - c2)MONTH AS c1_minus_c2
                FROM interval_tbl;
                
                CREATE LOCAL VIEW atbl_interval_seconds AS SELECT
                id,
                (c1 - c2)SECOND AS c1_minus_c2
                FROM interval_tbl;
                
                CREATE LOCAL VIEW atbl_interval_seconds_res AS SELECT
                id,
                CAST(c1_minus_c2 AS VARCHAR) AS f_c1
                FROM atbl_interval_seconds;
                
                CREATE LOCAL VIEW atbl_interval_months_res AS SELECT
                id,
                CAST(c1_minus_c2 AS VARCHAR) AS f_c1
                FROM atbl_interval_months;
                
                CREATE VIEW V AS (SELECT * FROM atbl_interval_seconds_res) UNION ALL (SELECT * FROM atbl_interval_months_res);""");
        ccs.step("INSERT INTO interval_tbl VALUES(0, '2014-11-05 08:27:00', '2024-12-05 12:45:00')", """
                 id | f_c1 | weight
                --------------------
                 0  | -121| 1
                 0  | -318226680.000000| 1""");
    }

    @Test
    public void issue5120() {
        var ccs = this.getCCS("""
                CREATE TYPE S AS(i1 INT NOT NULL, i2 INT);
                CREATE TABLE tbl(id INT, c1_arr S ARRAY);
                
                CREATE VIEW v AS SELECT
                id,  i1_val + 1, i2_val + 1,  idx
                FROM tbl,
                UNNEST(c1_arr) WITH ORDINALITY AS t (i1_val, i2_val, idx);""");
        ccs.step("""
                INSERT INTO tbl VALUES (0, ARRAY[S(1, NULL), S(5, 6)]);
                """, """
                 id | i1 | i2 | idx | weight
                -----------------------------
                  0 | 2  |    |   1 | 1
                  0 | 6  |  7 |   2 | 1""");

        ccs = this.getCCS("""
                CREATE TABLE tbl(id INT,
                c1_arr ROW(i1 INT NOT NULL, i2 INT NULL)  ARRAY NOT NULL);
                
                CREATE VIEW v AS SELECT
                id,  i1_val + 1, i2_val + 1,  idx
                FROM tbl,
                UNNEST(c1_arr) WITH ORDINALITY AS t (i1_val, i2_val, idx);""");
        ccs.step("""
                INSERT INTO tbl VALUES (0, ARRAY[ROW(1, NULL), ROW(5, 6)]);
                """, """
                 id | i1 | i2 | idx | weight
                -----------------------------
                  0 | 2  |    |   1 | 1
                  0 | 6  |  7 |   2 | 1""");
    }

    @Test
    public void testBinaryStringCast() {
        this.qs("""
                SELECT bin2utf8(x'404141');
                 r
                ---
                 @AA
                (1 row)
                
                SELECT bin2utf8(NULL);
                 r
                ---
                NULL
                (1 row)
                
                -- FF is invalid
                SELECT bin2utf8(x'FF');
                 r
                ---
                NULL
                (1 row)
                
                SELECT bin2utf8(x'f09f918b');
                 r
                ---
                 👋
                (1 row)
                
                SELECT bin2utf8(CAST('👋' AS VARBINARY));
                 r
                ---
                 👋
                (1 row)""");
    }

    @Test
    public void issue5260() {
        var ccs = this.getCCS("""
                CREATE TABLE tbl(roww ROW(i1 INT, i2 INT) NULL);
                CREATE VIEW V AS SELECT roww <=> NULL FROM tbl;""");
        ccs.step("INSERT INTO tbl VALUES(ROW(ROW(1, 2)))", """
                 r | weight
                ------------
                 false | 1""");
        ccs.step("INSERT INTO tbl VALUES(NULL)", """
                 r | weight
                ------------
                 true | 1""");
    }

    @Test
    public void issue5276() {
        this.getCCS("""
            CREATE TYPE LEVEL_1 AS (
              col VARCHAR
            );
            
            CREATE TYPE LEVEL_0 AS (
              col LEVEL_1
            );
            
            CREATE TABLE T(l LEVEL_0, X INT);
            CREATE TABLE S(X INT);
            
            CREATE VIEW V AS
            (SELECT * FROM T) UNION (SELECT NULL, X FROM S)""");
    }

    @Test
    public void issue5275() {
        this.getCC("""
                CREATE TYPE LEVEL_1 AS (
                  col VARCHAR
                );
                
                CREATE TYPE LEVEL_0 AS (
                  col LEVEL_1
                );
                
                CREATE LOCAL VIEW V AS
                SELECT CAST(NULL AS LEVEL_0)""");
    }

    @Test
    public void issue5285() {
        var ccs = this.getCCS("""
                CREATE TABLE tbl(
                id INT,
                intt INT,
                roww ROW(i1 INT, v1 VARCHAR NULL) NULL);
                
                CREATE MATERIALIZED VIEW v AS SELECT
                AVG(roww[1]) AS roww FROM tbl
                WHERE id = 0;""");
        ccs.step("INSERT INTO tbl VALUES(0, -12, ROW(4, 'cat'))", """
                 avg | weight
                --------------
                 4   | 1""");
    }

    @Test
    public void issue5293() {
        var ccs = this.getCCS("""
                CREATE TABLE T(x VARCHAR);
                CREATE VIEW V AS
                WITH FT as (select 'a' as e union all select 'bc')
                SELECT x, x in (SELECT e from FT)
                FROM T;""");
        ccs.step("INSERT INTO T VALUES('a'), ('b'), ('ab');", """
                 x | in | weight
                -------------
                 a| true | 1
                 b|false | 1
                 ab|false | 1""");
    }

    @Test
    public void issue5299() {
        this.getCC("""
                CREATE LINEAR AGGREGATE u256_sum(value BINARY(32)) RETURNS BINARY(32);
                
                CREATE TABLE A (
                    id VARCHAR(20) NOT NULL PRIMARY KEY,
                    a VARCHAR(64),
                    b VARCHAR(64),
                    sno BIGINT NOT NULL PRIMARY KEY LATENESS 100::BIGINT,
                    small DECIMAL(38, 18),
                    num1 BINARY(32),
                    num2 BINARY(32)
                ) WITH ('append_only' = 'true');
                
                CREATE VIEW b AS
                SELECT u256_sum(num1), u256_sum(num2), SUM(small), MAX(sno)
                FROM A""");
    }

    @Test
    public void issue5307() {
        this.getCCS("""
                CREATE TYPE LEVEL_1 AS (
                  col VARCHAR NOT NULL
                );
                
                CREATE TYPE LEVEL_0 AS (
                  col LEVEL_1
                );
                
                CREATE LOCAL VIEW V AS
                SELECT NULL::LEVEL_0 AS null_col""");
    }

    @Test
    public void issue5311() {
        this.getCCS("""
                CREATE TABLE tbl(intt INT);
                CREATE MATERIALIZED VIEW v AS SELECT
                LAG(intt) OVER ()
                FROM tbl;""");
    }

    @Test
    public void limitDuplicate() {
        var ccs = this.getCCS("""
                CREATE TABLE T(x INT);
                CREATE VIEW V AS SELECT * FROM T ORDER BY x LIMIT 1;""");
        ccs.step("""
                INSERT INTO T VALUES(1);
                INSERT INTO T VALUES(1);
                """, """
                 x | weight
                ------------
                 1 | 1""");
    }

    @Test
    public void issue5345() {
        var ccs = this.getCCS("""
                CREATE MATERIALIZED VIEW t1 AS
                    SELECT DISTINCT
                        t.f1,
                        t.f2,
                        CAST(t.f3 AS TEXT ARRAY) AS f3,
                        t.f4
                    FROM (
                        VALUES
                            ('a', 1, ARRAY['by'], true),
                            ('b', 1, ARRAY(), false)
                    ) AS t (f1, f2, f3, f4);""");
        ccs.step("", """
                 f1 | f2 | f3 | f4    | weight
                -------------------------------
                 a| 1 | { by} | true  | 1
                 b| 1 | {}    | false | 1""");
    }

    @Test
    public void issue5352() {
        var ccs = this.getCCS("""
                CREATE TABLE tbl(str VARCHAR);
                
                CREATE MATERIALIZED VIEW v AS SELECT
                str::BOOLEAN IS FALSE AS arr,
                str::BOOLEAN IS TRUE AS arr1
                FROM tbl;""");
        ccs.step("INSERT INTO TBL values('TRUE')", """
                 arr | arr1 | weight
                ---------------------
                 false | true | 1""");
    }

    @Test
    public void issue5352_a() {
        this.qs("""
             SELECT 'TRUE'::BOOLEAN;
              r
             ---
              true
             (1 row)
             
             SELECT 'true'::BOOLEAN;
              r
             ---
              true
             (1 row)
             
             SELECT 'TrUe'::BOOLEAN;
              r
             ---
              true
             (1 row)
             
             SELECT 't'::BOOLEAN;
              r
             ---
              false
             (1 row)
             
             SELECT '1'::BOOLEAN;
              r
             ---
              false
             (1 row)
             
             SELECT '0'::BOOLEAN;
              r
             ---
              false
             (1 row)
             
             SELECT 'yes'::BOOLEAN;
              r
             ---
              false
             (1 row)
             
             SELECT ''::BOOLEAN;
              r
             ---
              false
             (1 row)
             
             SELECT 'NULL'::BOOLEAN;
              r
             ---
              false
             (1 row)
             
             SELECT NULL::BOOLEAN;
              r
             ---
             NULL
             (1 row)""");
    }

    @Test
    public void issue5215() {
        var ccs = this.getCCS("""
                  CREATE TABLE T(x INT, y INT);
                  CREATE LOCAL VIEW V AS SELECT ROW(*) as R FROM T;
                  CREATE VIEW W AS SELECT R[1], R[2] FROM V;""");
        ccs.step("INSERT INTO T VALUES(10, 20);", """
                 r1 | r2 | weight
                ------------------
                 10 | 20 | 1""");
    }

    @Test
    public void issue5331() {
        this.statementsFailingInCompilation("""
                CREATE TABLE asof_tbl1(intt INT, arr VARCHAR ARRAY);
                CREATE TABLE asof_tbl2(intt INT, arr VARCHAR ARRAY);
                
                CREATE MATERIALIZED VIEW v AS SELECT *
                FROM asof_tbl1 t1
                LEFT ASOF JOIN asof_tbl2 t2
                MATCH_CONDITION (t1.intt >= t2.intt)
                ON t1.arr[2] = t2.arr[2] ;""",
                "ASOF JOIN condition must be a conjunction of equality comparisons of columns from both sides");
    }

    @Test
    public void issue5384() {
        this.statementsFailingInCompilation("""
                CREATE TYPE user_def AS(i1 INT, v1 VARCHAR NULL);
                CREATE TABLE tbl(mapp1 MAP<user_def, ROW(v VARCHAR NULL)>);
                
                CREATE MATERIALIZED VIEW v AS SELECT
                CAST(mapp1 AS MAP<VARCHAR, INT>) AS to_map
                FROM tbl;""", "Cast function cannot convert value of type ");
    }

    @Test
    public void issue5359() {
        this.getCCS("""
                CREATE TABLE source (
                    id VARCHAR NOT NULL,
                    key VARCHAR NOT NULL INTERNED,
                    version BIGINT
                ) WITH ('append_only' = 'true');
                
                CREATE TABLE lookup (
                    key VARCHAR NOT NULL INTERNED PRIMARY KEY,
                    metadata VARCHAR
                );
                
                CREATE LOCAL VIEW v
                AS
                SELECT
                    id,
                    MAX(key) as key
                FROM source
                GROUP BY id;
                
                CREATE VIEW result
                AS
                SELECT
                    a.key,
                    l.metadata
                FROM v a
                LEFT JOIN lookup l ON a.key = l.key;""");
    }

    @Test
    public void issue5386() {
        this.statementsFailingInCompilation("""
                CREATE TABLE T(x INT UNSIGNED);
                CREATE VIEW V AS SELECT -x FROM T;""",
                "Unary minus cannot be applied");
        this.statementsFailingInCompilation(
                "CREATE VIEW V AS SELECT - CAST(1 AS INT UNSIGNED)",
                "Unary minus cannot be applied");
    }

    @Test
    public void incorrectAsofNullabilityTest() {
        var ccs = this.getCCS("""
                CREATE TABLE T(X INT NOT NULL, Y INT);
                CREATE TABLE S(X INT, Y INT);
                CREATE VIEW V AS SELECT T.*, S.x as sx, S.y as sy FROM T LEFT ASOF JOIN S
                MATCH_CONDITION(T.X >= S.X) ON T.Y = S.Y;""");
        ccs.step("""
                INSERT INTO T VALUES(1, NULL);
                INSERT INTO S VALUES(1, 1);""", """
                 X | Y | sx | sy | weight
                --------------------------
                 1 |   |    |    | 1""");
    }

    @Test
    public void issue5375() {
        var ccs = this.getCCS("""
                CREATE TABLE tbl(str VARCHAR );
                CREATE MATERIALIZED VIEW v AS SELECT
                SAFE_CAST(str AS UUID) AS str_uuid
                FROM tbl;""");
        ccs.step("INSERT INTO tbl VALUES('h');", """
                 str_uuid | weight
                -------------------
                NULL      | 1""");
    }

    @Test
    public void issue5378() {
        var ccs = this.getCCS("""
                CREATE TABLE tbl(mapp MAP<VARCHAR, INT>);
                
                CREATE MATERIALIZED VIEW v AS SELECT
                MAP_KEYS(SAFE_CAST(mapp AS MAP<INT, INT>)),
                MAP_VALUES(SAFE_CAST(mapp AS MAP<INT, INT>))
                FROM tbl;""");
        ccs.step("INSERT INTO TBL VALUES(MAP['1', 1])", """
                 keys | values | weight
                ------------------------
                 { 1 } | { 1 } | 1""");
        ccs.step("INSERT INTO TBL VALUES(MAP['a', 2])", """
                 keys | values | weight
                ------------------------
                      |        | 1""");
    }

    @Test
    public void issue5379() {
        this.statementsFailingInCompilation("""
                CREATE TABLE tbl(mapp MAP<VARCHAR, INT>);
                
                CREATE MATERIALIZED VIEW v AS SELECT
                CAST(mapp AS MAP<VARCHAR, ROW(v VARCHAR)>) AS to_map,
                SAFE_CAST(mapp AS MAP<VARCHAR, ROW(v VARCHAR)>) AS to_map1
                FROM tbl;""",
                "Cast function cannot convert value of type (VARCHAR CHARACTER SET \"UTF-8\" NOT NULL, INTEGER) MAP");
    }

    @Test
    public void issue5380() {
        this.statementsFailingInCompilation("""
                CREATE TABLE tbl(mapp MAP<VARCHAR, INT>);
                
                CREATE MATERIALIZED VIEW v AS SELECT
                CAST(mapp AS MAP<VARCHAR, INT ARRAY>) AS to_map
                FROM tbl;""", "Cast function cannot convert value of type (VARCHAR CHARACTER SET \"UTF-8\" NOT NULL, INTEGER) MAP");
    }

    @Test
    public void safeArrayCast() {
        this.qs("""
                SELECT SAFE_CAST(ARRAY['a'] AS INT ARRAY);
                 r
                ---
                NULL
                (1 row)
                
                SELECT SAFE_CAST(ARRAY['1'] AS INT ARRAY);
                 r
                ---
                 { 1 }
                (1 row)""");
    }

    @Test
    public void safeNestedArrayCast() {
        this.qs("""
                SELECT SAFE_CAST(ARRAY[ARRAY['a']] AS INT ARRAY ARRAY);
                 r
                ---
                NULL
                (1 row)
                
                SELECT ELEMENT(SAFE_CAST(ARRAY[ARRAY['1']] AS INT ARRAY ARRAY));
                 r
                ---
                { 1 }
                (1 row)""");
    }

    @Test
    public void issue5389() {
        this.statementsFailingInCompilation("""
                CREATE TABLE tbl(roww ROW(v1 VARCHAR NULL) NULL);
                CREATE MATERIALIZED VIEW v AS SELECT
                SAFE_CAST(roww AS ROW(i1 INT)) AS to_row
                FROM tbl;""", "SAFE_CAST cannot be used to convert ROW(VARCHAR) to ROW(INT)");
    }

    @Test
    public void issue5390() {
        this.getCCS("""
                CREATE TYPE user_def AS(i1 INT, v1 VARCHAR NULL);
                CREATE TYPE user_def_array AS (val VARCHAR ARRAY);
                CREATE TYPE user_def_row AS (val ROW(i1 INT, v1 VARCHAR NULL));
                CREATE TYPE user_def_udt AS (val user_def);
                
                CREATE TABLE tbl(arr VARCHAR ARRAY, mapp MAP<VARCHAR, INT>,
                                 roww ROW(i1 INT, v1 VARCHAR NULL) NULL, udt user_def);
                CREATE MATERIALIZED VIEW v AS SELECT
                    SAFE_CAST(NULL AS user_def_row) AS to_row,
                    SAFE_CAST(NULL AS user_def_udt) AS to_udt
                FROM tbl;""");
    }

    @Test
    public void issue5391() {
        this.getCCS("""
                CREATE TYPE user_def_row AS (val ROW(i1 INT, v1 VARCHAR NULL));
                CREATE MATERIALIZED VIEW v AS SELECT
                SAFE_CAST(NULL AS user_def_row) AS to_roww;""");
    }

    @Test
    public void mapCast() {
        this.getCCS("""
                CREATE TABLE varnt_cmpx_tbl(
                roww ROW(int INT, var VARCHAR));
                
                CREATE MATERIALIZED VIEW cmpx_to_variant AS SELECT
                CAST(roww AS VARIANT) AS roww_varnt
                FROM varnt_cmpx_tbl;
                
                CREATE MATERIALIZED VIEW variant_to_cmpx AS SELECT
                CAST(roww_varnt AS ROW(int INT, var VARCHAR)) AS roww
                FROM cmpx_to_variant;""");
    }

    @Test
    public void issue5448() {
        // Also test for issue 5449
        var ccs = this.getCCS("""
                CREATE TABLE tbl(intt INTEGER UNSIGNED, x INT);
                
                CREATE MATERIALIZED VIEW v AS SELECT
                ABS(intt) AS a, SIGN(x) as s, SIGN(intt) as i FROM tbl;""");
        ccs.step("INSERT INTO tbl VALUES(0, 0), (1, 1), (2, -1)", """
                 a | s | i | weight
                --------------------
                 0 | 0 | 0 | 1
                 1 | 1 | 1 | 1
                 2 | -1 | 1 | 1""");
    }

    @Test
    public void castRow() {
        this.getCCS("""
                CREATE TABLE T (
                  str   VARCHAR,
                  data  INT NOT NULL
                );
                
                CREATE LOCAL VIEW V AS SELECT
                    T.str AS str,
                    ARRAY_AGG(ROW(T.data)) AS e
                  FROM T
                  GROUP BY T.str;
                
                CREATE VIEW W AS SELECT
                    CASE WHEN V.str IS NOT NULL THEN V.e
                    ELSE NULL
                    END
                FROM V;""");
    }
}
