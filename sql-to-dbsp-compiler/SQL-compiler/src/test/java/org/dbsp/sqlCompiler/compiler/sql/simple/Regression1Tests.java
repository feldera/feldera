package org.dbsp.sqlCompiler.compiler.sql.simple;

import org.dbsp.sqlCompiler.circuit.operator.DBSPAggregateLinearPostprocessOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamAggregateOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.sql.tools.CompilerCircuitStream;
import org.dbsp.sqlCompiler.compiler.sql.tools.SqlIoTest;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
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
        int[] streamAggregates = { 0 };
        int[] linearAggregates = { 0 };
        CircuitVisitor visitor = new CircuitVisitor(cc.compiler) {
            @Override
            public void postorder(DBSPStreamAggregateOperator operator) {
                streamAggregates[0]++;
            }

            @Override
            public void postorder(DBSPAggregateLinearPostprocessOperator operator) {
                linearAggregates[0]++;
            }
        };
        cc.visit(visitor);
        // 3 max
        Assert.assertEquals(3, streamAggregates[0]);
        // 1 for the two sums
        Assert.assertEquals(1, linearAggregates[0]);
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
                for (var av: Linq.list("NULL", "'a'")) {
                    for (var bg: Linq.list("NULL", "0", "1")) {
                        for (var bk : Linq.list("NULL", "0")) {
                            String result = "";
                            if (nn(ad) && nn(av) && nn(bg) && nn(bk) && !bg.equals("0")) {
                                result = "\n " + c + " | a | " + (bk.equals("0") ? "1" : "0") + " | 0 | 1 | 1 | 0 | 1 | 1";
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
        String program = ccs.compiler.sources.getWholeProgram();

        Function<Integer, String> values =
                v -> "('" + ((v >> 1) % 2) + "', '" + (v % 2) + "');";

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
                    x | y    | weight
                ----------------------
                 ab01 | ab01 | 1""");
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
                 1970-01-01 00:00:00 | 1970-01-01 00:00:00 | 1970-01-01 00:00:00 | 1970-01-01 00:00:00 |1""");
    }

    @Test
    public void issue4409() {
        this.statementsFailingInCompilation("""
                CREATE TABLE tbl(str VARCHAR);
                CREATE MATERIALIZED VIEW v AS SELECT
                ARRAY_CONCAT(str, str)  AS str
                FROM tbl;""", "Arguments of 'ARRAY_CONCAT' must have ARRAY types");
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
                 14  | See you! | 1""");
    }
}
