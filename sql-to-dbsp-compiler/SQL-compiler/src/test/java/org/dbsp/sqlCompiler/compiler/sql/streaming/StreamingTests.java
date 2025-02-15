package org.dbsp.sqlCompiler.compiler.sql.streaming;

import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.circuit.operator.DBSPAggregateLinearPostprocessRetainKeysOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPChainAggregateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPControlledKeyFilterOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPFlatMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIntegrateTraceRetainKeysOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIntegrateTraceRetainValuesOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPPartitionedRollingAggregateWithWaterlineOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPWaterlineOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPWindowOperator;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.TestUtil;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.sql.OtherTests;
import org.dbsp.sqlCompiler.compiler.sql.StreamingTestBase;
import org.dbsp.sqlCompiler.compiler.sql.tools.Change;
import org.dbsp.sqlCompiler.compiler.sql.tools.CompilerCircuit;
import org.dbsp.sqlCompiler.compiler.sql.tools.CompilerCircuitStream;
import org.dbsp.sqlCompiler.compiler.sql.tools.InputOutputChange;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPDateLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPDoubleLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPStringLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPTimestampLiteral;
import org.dbsp.sqlCompiler.ir.expression.DBSPZSetExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeDate;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeDouble;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeString;
import org.dbsp.util.Linq;
import org.junit.Assert;
import org.junit.Test;

/** Tests that exercise streaming features. */
public class StreamingTests extends StreamingTestBase {
    @Override
    public CompilerOptions testOptions(boolean incremental, boolean optimize) {
        CompilerOptions options = super.testOptions(incremental, optimize);
        // Used by some tests for NOW
        options.ioOptions.nowStream = true;
        return options;
    }

    @Test
    public void issue2846() {
        String sql = """
                CREATE TABLE t1(
                    x INT,
                    ts TIMESTAMP NOT NULL LATENESS INTERVAL 1 HOUR
                );
                
                CREATE TABLE t2(
                    y INT,
                    ts TIMESTAMP NOT NULL LATENESS INTERVAL 1 HOUR
                );
                
                CREATE VIEW v
                WITH ('emit_final' = 'ts')
                AS SELECT t1.ts
                FROM t1 FULL OUTER JOIN t2 on t1.ts = t2.ts;""";
        var ccs = this.getCCS(sql);
        ccs.step("insert into t1 values (1, '2020-01-01 00:00:00');",
                """
                         ts | weight
                        --------------""");
        ccs.step("insert into t2 values (1, '2020-01-01 00:00:00');",
                """
                         ts | weight
                        --------------""");
        ccs.step("""
                        insert into t1 values (1, '2020-01-02 00:00:00');
                        insert into t2 values (1, '2020-01-02 00:00:00');
                        """,
                """
                         ts | weight
                        --------------
                         2020-01-01 00:00:00 | 1""");


        CircuitVisitor visitor = new CircuitVisitor(ccs.compiler) {
            int window = 0;

            @Override
            public void postorder(DBSPWindowOperator operator) {
                this.window++;
            }

            // Should have 1 window for emit_final
            @Override
            public void endVisit() {
                Assert.assertEquals(1, this.window);
            }
        };
        ccs.visit(visitor);
    }

    @Test
    public void issue3465() {
        String sql = """
                CREATE TABLE T(TS INT LATENESS 100, X INT) WITH ('append_only' = 'true');
                CREATE VIEW V AS
                SELECT MAX(TS * 2), MIN(TS - 2)  FROM T;""";
        var ccs = this.getCCS(sql);
        ccs.step("INSERT INTO T VALUES(NULL, 0);",
                """
                         max | min | weight
                        --------------------
                             |     | 1""");
        ccs.step("INSERT INTO T VALUES(10, -10);",
                """
                         max | min | weight
                        --------------------
                             |     | -1
                         20  |   8 | 1""");
        ccs.step("INSERT INTO T VALUES(5, 20);",
                """
                         sum | max | weight
                        --------------------
                         20  |   8 | -1
                         20  |   3 | 1""");
        int[] chains = new int[1];
        CircuitVisitor visitor = new CircuitVisitor(ccs.compiler) {
            @Override
            public void postorder(DBSPChainAggregateOperator operator) {
                chains[0]++;
            }
        };
        ccs.visit(visitor);
        Assert.assertEquals(1, chains[0]);
    }

    @Test
    public void chainAggregateMax() {
        String sql = """
                CREATE TABLE T(TS INT LATENESS 100, X INT) WITH ('append_only' = 'true');
                CREATE VIEW V AS
                SELECT SUM(X), MAX(TS * 2) FROM T;""";
        var ccs = this.getCCS(sql);
        ccs.step("INSERT INTO T VALUES(NULL, 0);",
                """
                         sum | max | weight
                        --------------------
                          0  | NULL| 1""");
        ccs.step("INSERT INTO T VALUES(10, 10);",
                """
                         sum | max | weight
                        --------------------
                          0  | NULL| -1
                         10  | 20  | 1""");
        ccs.step("INSERT INTO T VALUES(5, 20);",
                """
                         sum | max | weight
                        --------------------
                         10  | 20  | -1
                         30  | 20  | 1""");
        ccs.step("INSERT INTO T VALUES(30, 30);",
                """
                         sum | max | weight
                        --------------------
                         30  | 20  | -1
                         60  | 60  | 1""");
    }

    @Test
    public void chainAggregateMin() {
        String sql = """
                CREATE TABLE T(TS INT LATENESS 1000, X INT) WITH ('append_only' = 'true');
                CREATE VIEW V AS
                SELECT SUM(X), MIN(TS * 2) FROM T;""";
        var ccs = this.getCCS(sql);
        ccs.step("INSERT INTO T VALUES(NULL, 0);",
                """
                         sum | min | weight
                        --------------------
                          0  | NULL| 1""");
        ccs.step("INSERT INTO T VALUES(10, 10);",
                """
                         sum | min | weight
                        --------------------
                          0  | NULL| -1
                         10  | 20  | 1""");
        ccs.step("INSERT INTO T VALUES(5, 20);",
                """
                         sum | min | weight
                        --------------------
                         10  | 20  | -1
                         30  | 10  | 1""");
        ccs.step("INSERT INTO T VALUES(30, 30);",
                """
                         sum | min | weight
                        --------------------
                         30  | 10  | -1
                         60  | 10  | 1""");
        ccs.step("INSERT INTO T VALUES(NULL, 0);",
                """
                         sum | min | weight
                        --------------------""");
    }

    @Test
    public void issue2852() {
        String sql = """
                CREATE TABLE t (
                    id int not null primary key,
                    ts TIMESTAMP NOT NULL LATENESS INTERVAL 30 MINUTES
                ) WITH (
                    'append_only' = 'true'
                );
                
                create view v1 AS
                SELECT
                    TIMESTAMP_TRUNC(ts, DAY) as d,
                    MAX(id) m,
                    COUNT(*)
                FROM t
                GROUP BY TIMESTAMP_TRUNC(ts, DAY);""";
        var cc = this.getCC(sql);
        CircuitVisitor visitor = new CircuitVisitor(cc.compiler) {
            int integrate_trace = 0;

            @Override
            public void postorder(DBSPIntegrateTraceRetainKeysOperator operator) {
                this.integrate_trace++;
            }

            @Override
            public void endVisit() {
                Assert.assertEquals(4, this.integrate_trace);
            }
        };
        cc.visit(visitor);
    }

    @Test
    public void chainAggregateGroupBy() {
        String sql = """
                CREATE TABLE T(TS INT, X INT LATENESS 1000) WITH ('append_only' = 'true');
                CREATE VIEW V AS
                SELECT X, MAX(TS * 2) FROM T
                GROUP BY X;""";
        var ccs = this.getCCS(sql);
        ccs.step("INSERT INTO T VALUES(NULL, NULL);",
                """
                         x   | max | weight
                        -------------------
                         NULL| NULL| 1""");
        ccs.step("INSERT INTO T VALUES(10, 10);",
                """
                         x   | max | weight
                        -------------------
                         10  | 20  | 1""");
        ccs.step("INSERT INTO T VALUES(5, 20);",
                """
                         x   | max | weight
                        --------------------
                         20  | 10  | 1""");
        ccs.step("INSERT INTO T VALUES(30, 30);",
                """
                         x   | max | weight
                        --------------------
                         30  | 60  | 1""");
        ccs.step("INSERT INTO T VALUES(20, 10), (0, 20), (30, 30);",
                """
                         x   | max | weight
                        --------------------
                         10  | 20  | -1
                         10  | 40  | 1""");
    }

    @Test
    public void issue2847() {
        String sql = """
                CREATE TABLE t1(
                    x INT,
                    ts TIMESTAMP NOT NULL LATENESS INTERVAL 1 HOUR
                );
                
                CREATE TABLE t2(
                    y INT,
                    ts TIMESTAMP NOT NULL LATENESS INTERVAL 1 HOUR
                );
                
                CREATE VIEW v
                WITH ('emit_final' = 'ts')
                AS SELECT
                    ts, x, LAG(x)
                    OVER (ORDER BY ts)
                FROM t1;""";
        var ccs = this.getCCS(sql);
        CircuitVisitor visitor = new CircuitVisitor(ccs.compiler) {
            int integrate_trace = 0;
            int window = 0;

            @Override
            public void postorder(DBSPWindowOperator operator) {
                this.window++;
            }

            @Override
            public void postorder(DBSPIntegrateTraceRetainKeysOperator operator) {
                this.integrate_trace++;
            }

            @Override
            public void endVisit() {
                // 2 for the lag, one for the window
                Assert.assertEquals(3, this.integrate_trace);
                Assert.assertEquals(1, this.window);
            }
        };
        ccs.visit(visitor);
    }

    @Test
    public void chainAggregateGroupByJoin() {
        String sql = """
                CREATE TABLE T(TS INT, X INT LATENESS 1000) WITH ('append_only' = 'true');
                CREATE VIEW V AS
                SELECT X, SUM(TS / 2), MAX(TS * 2) FROM T
                GROUP BY X;""";
        var ccs = this.getCCS(sql);
        CircuitVisitor visitor = new CircuitVisitor(ccs.compiler) {
            int integrate_trace = 0;

            @Override
            public void postorder(DBSPIntegrateTraceRetainKeysOperator operator) {
                this.integrate_trace++;
            }

            @Override
            public void endVisit() {
                Assert.assertEquals(4, this.integrate_trace);
            }
        };
        ccs.visit(visitor);
    }

    @Test
    public void iceTFTest() {
        String sql = """
                CREATE TABLE T (
                   id INT,
                   ts TIMESTAMP NOT NULL LATENESS INTERVAL 1 HOURS
                ) WITH (
                    'materialized' = 'true',
                    'append_only' = 'true'
                );
                
                CREATE VIEW V
                WITH ('emit_final' = 'ts')
                AS SELECT * FROM T
                WHERE ts >= NOW() - INTERVAL 7 DAYS;""";
        this.getCCS(sql);
    }

    @Test
    public void issue2531() {
        String sql = """
                create table r(
                    id BIGINT NOT NULL,
                    ts timestamp NOT NULL LATENESS INTERVAL 0 days
                );

                create table l (
                    id BIGINT NOT NULL,
                    ts timestamp NOT NULL LATENESS INTERVAL 0 days
                );

                create view v as
                select
                    l.id as id,
                    l.ts as lts,
                    r.ts as rts
                from l join r
                ON
                    l.id = r.id and
                    r.ts = l.ts;

                CREATE VIEW agg1 as
                SELECT MAX(id)
                FROM v
                GROUP BY lts;""";
        CompilerCircuitStream ccs = this.getCCS(sql);
        CircuitVisitor visitor = new CircuitVisitor(ccs.compiler) {
            int integrate_trace = 0;

            @Override
            public void postorder(DBSPIntegrateTraceRetainKeysOperator operator) {
                this.integrate_trace++;
            }

            @Override
            public void endVisit() {
                Assert.assertEquals(4, this.integrate_trace);
            }
        };
        ccs.visit(visitor);
    }

    @Test
    public void issue2532() {
        String sql = """
                create table t (
                    x int,
                    y int,
                    z int,
                    a int,
                    ts timestamp not null lateness interval 1 hours
                );

                create view v as
                select
                    a,
                    AVG(distinct x),
                    AVG(distinct y),
                    AVG(distinct z)
                from
                    t
                group by
                    ts, a;""";
        CompilerCircuitStream ccs = this.getCCS(sql);
        CircuitVisitor visitor = new CircuitVisitor(ccs.compiler) {
            int integrate_trace = 0;

            @Override
            public void postorder(DBSPIntegrateTraceRetainKeysOperator operator) {
                this.integrate_trace++;
            }

            @Override
            public void endVisit() {
                Assert.assertEquals(10, this.integrate_trace);
            }
        };
        ccs.visit(visitor);
    }

    @Test
    public void tpchq14() {
        String sql = """
                CREATE TABLE LINEITEM (
                        L_ORDERKEY    INTEGER NOT NULL,
                        L_PARTKEY     INTEGER NOT NULL,
                        L_SUPPKEY     INTEGER NOT NULL,
                        L_LINENUMBER  INTEGER NOT NULL,
                        L_QUANTITY    DECIMAL(15,2) NOT NULL,
                        L_EXTENDEDPRICE  DECIMAL(15,2) NOT NULL,
                        L_DISCOUNT    DECIMAL(15,2) NOT NULL,
                        L_TAX         DECIMAL(15,2) NOT NULL,
                        L_RETURNFLAG  CHAR(1) NOT NULL,
                        L_LINESTATUS  CHAR(1) NOT NULL,
                        L_SHIPDATE    DATE NOT NULL,
                        L_COMMITDATE  DATE NOT NULL,
                        L_RECEIPTDATE DATE NOT NULL,
                        L_SHIPINSTRUCT CHAR(25) NOT NULL,
                        L_SHIPMODE     CHAR(10) NOT NULL,
                        L_COMMENT      VARCHAR(44) NOT NULL
                );
                CREATE TABLE PART (
                        P_PARTKEY     INTEGER NOT NULL,
                        P_NAME        VARCHAR(55) NOT NULL,
                        P_MFGR        CHAR(25) NOT NULL,
                        P_BRAND       CHAR(10) NOT NULL,
                        P_TYPE        VARCHAR(25) NOT NULL,
                        P_SIZE        INTEGER NOT NULL,
                        P_CONTAINER   CHAR(10) NOT NULL,
                        P_RETAILPRICE DECIMAL(15,2) NOT NULL,
                        P_COMMENT     VARCHAR(23) NOT NULL
                );
                create view q14 (promo_revenue) as
                select
                    100.00 * sum(case
                        when p_type like 'PROMO%'
                            then l_extendedprice * (1 - l_discount)
                        else 0
                    end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue
                from
                    lineitem,
                    part
                where
                    l_partkey = p_partkey
                    and l_shipdate >= date '1994-03-01'
                    and l_shipdate < date '1994-03-01' + interval '1' month;
                """;
        this.compileRustTestCase(sql);
    }

    @Test
    public void q16() {
        // simplified version of q16
        String sql = """
                CREATE TABLE bid (
                   auction  BIGINT,
                   bidder  BIGINT,
                   price  BIGINT,
                   channel  VARCHAR,
                   url  VARCHAR,
                   date_time TIMESTAMP(3) NOT NULL LATENESS INTERVAL 4 minutes,
                   extra  VARCHAR
                ) WITH ('connectors' = '[{"name": "bid"}]');
                CREATE VIEW Q16 AS
                SELECT
                    count(distinct auction) filter (where price >= 10000 and price < 1000000) AS rank2_auctions,
                    count(distinct auction) filter (where price >= 1000000) AS rank3_auctions
                FROM bid
                GROUP BY channel, CAST(date_time AS DATE);""";
        this.compileRustTestCase(sql);
    }

    @Test
    public void issue3344() {
        String sql = """
                CREATE TABLE bid (
                    channel  VARCHAR,
                    date_time TIMESTAMP(3) NOT NULL LATENESS INTERVAL 4 SECONDS
                );

                CREATE VIEW REST AS
                SELECT
                    channel,
                    count(*) AS total_bids
                FROM bid
                GROUP BY channel, CAST(date_time AS DATE);
                """;
        var ccs = this.getCCS(sql);
        CircuitVisitor visitor = new CircuitVisitor(ccs.compiler) {
            int aggregate_retain = 0;

            @Override
            public void postorder(DBSPAggregateLinearPostprocessRetainKeysOperator operator) {
                this.aggregate_retain++;
            }

            @Override
            public void endVisit() {
                Assert.assertEquals(1, this.aggregate_retain);
            }
        };
        ccs.visit(visitor);
    }

    @Test
    public void q16alt() {
        // alternate implementation of q16 from nexmark
        String sql = """
                CREATE TABLE bid (
                    auction  BIGINT,
                    bidder  BIGINT,
                    price  BIGINT,
                    channel  VARCHAR,
                    url  VARCHAR,
                    date_time TIMESTAMP(3) NOT NULL LATENESS INTERVAL 4 SECONDS,
                    extra  VARCHAR
                );

                CREATE LOCAL VIEW low
                AS SELECT * FROM bid WHERE price < 10000;

                CREATE LOCAL VIEW mid
                AS SELECT * FROM bid WHERE price >= 10000 AND price < 1000000;

                CREATE LOCAL VIEW high
                AS SELECT * FROM bid WHERE price >= 1000000;

                CREATE LOCAL VIEW LOW_C AS
                SELECT
                   channel,
                   CAST(date_time AS DATE) as dt,
                   count(*) AS rank1_bids,
                   count(distinct bidder) AS rank1_bidders,
                   count(distinct auction) AS rank1_auctions
                FROM low
                GROUP BY channel, CAST(date_time AS DATE);

                CREATE LOCAL VIEW MID_C AS
                SELECT
                   channel,
                   CAST(date_time AS DATE) as dt,
                   count(*) AS rank2_bids,
                   count(distinct bidder) AS rank2_bidders,
                   count(distinct auction) AS rank2_auctions
                FROM mid
                GROUP BY channel, CAST(date_time AS DATE);

                CREATE LOCAL VIEW HIGH_C AS
                SELECT
                   channel,
                   CAST(date_time AS DATE) as dt,
                   count(*) AS rank3_bids,
                   count(distinct bidder) AS rank3_bidders,
                   count(distinct auction) AS rank3_auctions
                FROM high
                GROUP BY channel, CAST(date_time AS DATE);

                CREATE VIEW REST AS
                SELECT
                    channel,
                    CAST(date_time AS DATE) as dt,
                    format_date('HH:mm', max(date_time)) as 'minute',
                    count(*) AS total_bids,
                    count(distinct bidder) AS total_bidders,
                    count(distinct auction) AS total_auctions
                FROM bid
                GROUP BY channel, CAST(date_time AS DATE);

                CREATE VIEW Q16 AS
                SELECT * FROM REST
                JOIN LOW_C
                  ON REST.channel = LOW_C.channel AND REST.dt = LOW_C.dt
                JOIN MID_C
                  ON REST.channel = MID_C.channel AND REST.dt = MID_C.dt
                JOIN HIGH_C
                  ON REST.channel = HIGH_C.channel AND REST.dt = HIGH_C.dt;
                """;
        this.compileRustTestCase(sql);
    }

    @Test
    public void testAsof() {
        String sql = """
                create table TRANSACTION (
                    id bigint NOT NULL,
                    unix_time BIGINT LATENESS 100
                );

                create table FEEDBACK (
                    id bigint,
                    status int,
                    unix_time bigint NOT NULL LATENESS 100
                );

                CREATE VIEW TRANSACT AS
                    SELECT transaction.*, feedback.status
                    FROM
                    feedback LEFT ASOF JOIN transaction
                    MATCH_CONDITION(transaction.unix_time <= feedback.unix_time)
                    ON transaction.id = feedback.id;
                """;
        CompilerCircuitStream ccs = this.getCCS(sql);
        CircuitVisitor visitor = new CircuitVisitor(ccs.compiler) {
            int integrate_trace = 0;

            @Override
            public void postorder(DBSPIntegrateTraceRetainValuesOperator operator) {
                this.integrate_trace++;
            }

            @Override
            public void endVisit() {
                Assert.assertEquals(1, this.integrate_trace);
            }
        };
        ccs.visit(visitor);
    }

    @Test
    public void issue2004() {
        String sql = """
                CREATE TABLE auction (
                   date_time TIMESTAMP NOT NULL LATENESS INTERVAL 1 MINUTE,
                   expires   TIMESTAMP NOT NULL,
                   id        INT NOT NULL PRIMARY KEY
                );

                CREATE TABLE bid (
                   date_time TIMESTAMP NOT NULL LATENESS INTERVAL 1 MINUTE,
                   price INT,
                   auction INT FOREIGN KEY REFERENCES auction(id)
                );

                CREATE VIEW Q9 AS
                SELECT A.*, B.price, B.date_time AS bid_dateTime
                FROM auction A, bid B
                WHERE A.id = B.auction AND B.date_time BETWEEN A.date_time AND A.expires""";
           CompilerCircuitStream ccs = this.getCCS(sql);
        // Insert an auction. No bids => no output
        ccs.step("""
                INSERT INTO auction VALUES('2024-01-01 00:00:00', '2024-01-01 01:00:00', 0);
                """, """
                 date_time | expires | id | price | bid_dateTime | weight
                ----------------------------------------------------------""");
        // Insert a bid matching auction 0 in the expected time range.  Should produce output.
        ccs.step("""
                INSERT INTO bid VALUES('2024-01-01 00:00:01', 100, 0);
                """, """
                 date_time | expires | id | price | bid_dateTime | weight
                ----------------------------------------------------------
                 2024-01-01 00:00:00 | 2024-01-01 01:00:00 | 0 | 100 | 2024-01-01 00:00:01 | 1""");
        // Insert a second bid matching auction 0 in the expected time range.  Should produce output.
        ccs.step("""
                INSERT INTO bid VALUES('2024-01-01 00:00:10', 200, 0);
                """, """
                 date_time | expires | id | price | bid_dateTime | weight
                ----------------------------------------------------------
                 2024-01-01 00:00:00 | 2024-01-01 01:00:00 | 0 | 200 | 2024-01-01 00:00:10 | 1""");
        // Insert a bid matching auction 1, which doesn't exist yet.  No output.
        ccs.step("""
                INSERT INTO bid VALUES('2024-01-01 00:00:20', 50, 1);
                """, """
                 date_time | expires | id | price | bid_dateTime | weight
                ----------------------------------------------------------""");
        // Insert auction 1, which matches the previous bid.  Should produce output.
        ccs.step("""
                INSERT INTO auction VALUES('2024-01-01 00:00:10', '2024-01-01 01:00:00', 1);
                """, """
                 date_time | expires | id | price | bid_dateTime | weight
                ----------------------------------------------------------
                 2024-01-01 00:00:10 | 2024-01-01 01:00:00 | 1 | 50 | 2024-01-01 00:00:20 | 1""");
        // Insert bid for auction 1 which is out of the auction time range.  No output.
        ccs.step("""
                INSERT INTO bid VALUES('2024-01-01 00:00:00', 50, 1);
                """, """
                 date_time | expires | id | price | bid_dateTime | weight
                ----------------------------------------------------------""");
        // Insert legal bid for auction 1.  Should produce output.
        ccs.step("""
                INSERT INTO bid VALUES('2024-01-01 00:00:30', 80, 1);
                """, """
                 date_time | expires | id | price | bid_dateTime | weight
                ----------------------------------------------------------
                2024-01-01 00:00:10 | 2024-01-01 01:00:00 | 1 | 80 | 2024-01-01 00:00:30 | 1""");
        // Insert auction and before auction LATENESS, no output.
        ccs.step("""
                INSERT INTO auction VALUES('2023-12-12 23:59:59', '2024-01-01 01:00:00', 3);
                INSERT INTO bid VALUES('2024-01-01 00:02:00', 1000, 3);
                """, """
                 date_time | expires | id | price | bid_dateTime | weight
                ----------------------------------------------------------""");
        // Insert legal bid for auction 1 but before bid LATENESS, no output.
        ccs.step("""
                INSERT INTO bid VALUES('2024-01-01 00:00:30', 3000, 1);
                """, """
                 date_time | expires | id | price | bid_dateTime | weight
                ----------------------------------------------------------""");
    }

    @Test
    public void testEmitFail() {
        this.statementsFailingInCompilation("""
                create table t (ts int not null LATENESS 2);
                CREATE VIEW v WITH
                ('emit_final' = 'inexistent') AS
                SELECT ts, COUNT(*)
                FROM t
                GROUP BY ts;""", "Column 'inexistent' not found in 'v'");
        this.statementsFailingInCompilation("""
                create table t (ts int not null LATENESS 2);
                CREATE VIEW v WITH
                ('emit_final' = '2') AS
                SELECT ts, COUNT(*)
                FROM t
                GROUP BY ts;""", "View 'v' does not have a column with number 2");
        this.statementsFailingInCompilation("""
                create table t (ts int);
                CREATE VIEW v WITH
                ('emit_final' = 'ts') AS
                SELECT ts, COUNT(*)
                FROM t
                GROUP BY ts;""", "Could not infer a waterline for column");
    }

    @Test
    public void testViewLateness() {
        String query = """
                LATENESS V.COL1 1;
                -- no view called W
                LATENESS W.COL2 INTERVAL 1 HOUR;
                CREATE VIEW V AS SELECT T.COL1, T.COL2 FROM T;
                CREATE VIEW V1 AS SELECT * FROM V;
                """;
        DBSPCompiler compiler = this.testCompiler();
        compiler.options.ioOptions.quiet = false;  // show warnings
        compiler.submitStatementForCompilation(OtherTests.ddl);
        compiler.submitStatementsForCompilation(query);
        DBSPCircuit circuit = getCircuit(compiler);
        CircuitVisitor visitor = new CircuitVisitor(compiler) {
            boolean found = false;

            @Override
            public VisitDecision preorder(DBSPControlledKeyFilterOperator filter) {
                this.found = true;
                return VisitDecision.CONTINUE;
            }

            @Override
            public void endVisit() {
                Assert.assertTrue(this.found);
            }
        };
        visitor.apply(circuit);
        TestUtil.assertMessagesContain(compiler, "View 'w' used in LATENESS statement not found");
    }

    @Test
    public void testEmitFinal() {
        String sql = """
                create table t (ts int not null LATENESS 2);
                CREATE VIEW v WITH ('emit_final' = 'ts') AS
                SELECT ts, COUNT(*) FROM t
                GROUP BY ts;""";
        CompilerCircuitStream ccs = this.getCCS(sql);
        // waterline is 1
        ccs.step("INSERT INTO T VALUES (0), (1);", """
                 ts | count | weight
                ---------------------""");
        // waterline is 3, but 1 may still be updated, so no output yet
        ccs.step("INSERT INTO T VALUES (1), (2);", """
                 ts | count | weight
                ---------------------""");
        // waterline is 5
        ccs.step("INSERT INTO T VALUES (4), (5);", """
                 ts | count | weight
                ---------------------
                  0 |     1 | 1
                  1 |     2 | 1
                  2 |     1 | 1""");
        // waterline is 5
        ccs.step("", """
                 ts | count | weight
                ---------------------""");
        // There should be 2 retain keys:
        // - one for the aggregate_linear
        // - one for the final window
        CircuitVisitor visitor = new CircuitVisitor(ccs.compiler) {
            int integrate_trace = 0;

            @Override
            public void postorder(DBSPIntegrateTraceRetainKeysOperator operator) {
                this.integrate_trace++;
            }

            @Override
            public void endVisit() {
                Assert.assertEquals(2, this.integrate_trace);
            }
        };
        ccs.visit(visitor);
    }

    @Test
    public void issue1973() {
        String sql = """
                create table t (
                    id bigint not null,
                    ts bigint not null LATENESS 0
                );

                CREATE VIEW v1 AS
                SELECT ts, COUNT(*)
                FROM t
                GROUP BY ts;

                CREATE VIEW v2 as
                select ts, count(*) from v1
                group by ts;""";
           CompilerCircuitStream ccs = this.getCCS(sql);
        CircuitVisitor visitor = new CircuitVisitor(ccs.compiler) {
            int integrate_trace = 0;

            @Override
            public void postorder(DBSPIntegrateTraceRetainKeysOperator operator) {
                this.integrate_trace++;
            }

            @Override
            public void endVisit() {
                Assert.assertEquals(2, this.integrate_trace);
            }
        };
        ccs.visit(visitor);
    }

    @Test
    public void testDelayedOutput() {
        String sql = """
                CREATE TABLE t (
                    ts TIMESTAMP NOT NULL WATERMARK INTERVAL 1 MINUTE LATENESS INTERVAL 1 MINUTE
                );
                CREATE VIEW test as
                SELECT SUM(YEAR(TS)), TIMESTAMP_TRUNC(ts, MINUTE) FROM t
                WHERE TIMESTAMP_TRUNC(TS, MINUTE) < TIMESTAMP_TRUNC(NOW(), MINUTE) - INTERVAL 1 MINUTE
                GROUP BY TIMESTAMP_TRUNC(ts, MINUTE);""";
        var ccs = this.getCCS(sql);
        ccs.step("""
                 INSERT INTO T VALUES ('0001-01-01 00:00:00');
                 INSERT INTO now VALUES ('0001-01-01 00:00:00');""",
                """
                  sum | timestamp | weight
                 --------------------------""");
        ccs.step("""
                 INSERT INTO T VALUES ('0001-01-01 00:00:10');
                 INSERT INTO now VALUES ('0001-01-01 00:00:10');""",
                """
                  sum | timestamp           | weight
                 ------------------------------------""");
        ccs.step("""
                 INSERT INTO T VALUES ('0001-01-01 00:01:00');
                 INSERT INTO now VALUES ('0001-01-01 00:01:00');""",
                """
                  sum | timestamp           | weight
                 ------------------------------------""");
        ccs.step("""
                 INSERT INTO T VALUES ('0001-01-01 00:02:01');
                 INSERT INTO now VALUES ('0001-01-01 00:02:01');""",
                """
                  sum | timestamp           | weight
                 ------------------------------------
                    2 | 0001-01-01 00:00:00 | 1""");
    }

    @Test
    public void testNow() {
        String sql = """
                CREATE VIEW V AS SELECT 1, NOW() < TIMESTAMP '2025-12-12 00:00:00';""";
        CompilerCircuitStream ccs = this.getCCS(sql);
        ccs.step("INSERT INTO now VALUES ('2024-12-12 00:00:00')",
                """
                         c | compare | weight
                        ----------------------
                         1 | true    | 1""");
    }

    @Test
    public void testNow2() {
        String sql = """
                CREATE TABLE T(value INT);
                CREATE VIEW V AS SELECT *, NOW() FROM T;""";
        CompilerCircuitStream ccs = this.getCCS(sql);
        ccs.step("""
                 INSERT INTO T VALUES (2), (3);
                 INSERT INTO now VALUES ('2024-12-12 00:00:00');
                 """,
                """
                 value | now                 | weight
                 -------------------------------------
                  2    | 2024-12-12 00:00:00 | 1
                  3    | 2024-12-12 00:00:00 | 1""");
        ccs.step("INSERT INTO now VALUES ('2024-12-12 00:01:00');",
                """
                value | now                 | weight
                -------------------------------------
                 2 | 2024-12-12 00:00:00 | -1
                 3 | 2024-12-12 00:00:00 | -1
                 2 | 2024-12-12 00:01:00 | 1
                 3 | 2024-12-12 00:01:00 | 1""");
        ccs.step("""
                 INSERT INTO now VALUES ('2024-12-12 00:02:00');
                 INSERT INTO T VALUES (4);""",
                 """
                  value | now                 | weight
                 --------------------------------------
                  2 | 2024-12-12 00:01:00 | -1
                  3 | 2024-12-12 00:01:00 | -1
                  2 | 2024-12-12 00:02:00 | 1
                  3 | 2024-12-12 00:02:00 | 1
                  4 | 2024-12-12 00:02:00 | 1""");
    }

    @Test
    public void testNow3() {
        String sql = """
                CREATE TABLE T(value INT);
                CREATE VIEW V AS SELECT SUM(value) + MINUTE(NOW()) FROM T;""";
        CompilerCircuitStream ccs = this.getCCS(sql);
        ccs.step("""
                        INSERT INTO T VALUES (2), (3);
                        INSERT INTO now VALUES ('2024-12-12 00:00:00');
                        """,
                """
                         value | weight
                        ----------------
                         5     | 1""");
        ccs.step("""
                 INSERT INTO now VALUES ('2024-12-12 00:01:00');
                 """,
                 """
                 value | weight
                 ----------------
                  5     | -1
                  6     | 1""");
        ccs.step("""
                 INSERT INTO T VALUES (1);
                 INSERT INTO now VALUES ('2024-12-12 00:02:00');
                 """, """
                  value | weight
                 ----------------
                  6     | -1
                  8     | 1""");
    }

    @Test
    public void testNow4() {
        // now() used in WHERE
        String sql = """
                CREATE TABLE transactions (
                  id INT NOT NULL PRIMARY KEY,
                  ts TIMESTAMP LATENESS INTERVAL 1 HOUR,
                  users INT,
                  AMOUNT DECIMAL
                );
                CREATE VIEW window_computation AS
                SELECT
                  users,
                  COUNT(*) AS transaction_count_by_user
                FROM transactions
                WHERE ts >= now() - INTERVAL 1 DAY
                GROUP BY users""";
        CompilerCircuitStream ccs = this.getCCS(sql);
        CircuitVisitor visitor = new CircuitVisitor(ccs.compiler) {
            int window = 0;
            int waterline = 0;

            @Override
            public void postorder(DBSPWindowOperator operator) {
                this.window++;
            }

            @Override
            public void postorder(DBSPWaterlineOperator operator) {
                this.waterline++;
            }

            @Override
            public void endVisit() {
                Assert.assertEquals(1, this.window);
                Assert.assertEquals(3, this.waterline);
            }
        };
        ccs.visit(visitor);
    }

    @Test
    public void testNow5() {
        // now() used in WHERE
        String sql = """
                CREATE TABLE transactions (
                  id INT NOT NULL PRIMARY KEY,
                  ts TIMESTAMP
                );
                CREATE VIEW window_computation AS
                SELECT *
                FROM transactions
                WHERE ts BETWEEN now() - INTERVAL 1 DAY AND now() + INTERVAL 1 DAY""";
        CompilerCircuitStream ccs = this.getCCS(sql);
        CircuitVisitor visitor = new CircuitVisitor(ccs.compiler) {
            int window = 0;
            int waterline = 0;

            @Override
            public void postorder(DBSPWindowOperator operator) {
                this.window++;
            }

            @Override
            public void postorder(DBSPWaterlineOperator operator) {
                this.waterline++;
            }

            @Override
            public void endVisit() {
                Assert.assertEquals(1, this.window);
                Assert.assertEquals(2, this.waterline);
            }
        };
        ccs.visit(visitor);
        ccs.step("""
                 INSERT INTO transactions VALUES (1, '2024-01-01 00:00:10');
                 INSERT INTO now VALUES ('2024-01-01 00:00:00');
                 """,
                """
                  id | ts                   | weight
                 ---------------------------------
                  1  | 2024-01-01 00:00:10  | 1""");
        ccs.step("""
                 INSERT INTO now VALUES ('2024-01-01 00:01:20');
                 """,
                """
                value | weight
                ----------------""");
        ccs.step("""
                 INSERT INTO transactions VALUES (2, NULL);
                 INSERT INTO now VALUES ('2024-01-01 00:02:00');
                 """, """
                  value | weight
                 ----------------""");
    }

    @Test
    public void testNow6() {
        // now() used in WHERE with complex monotone function
        String sql = """
                CREATE TABLE transactions (
                  id INT NOT NULL PRIMARY KEY,
                  ts INT
                );
                CREATE VIEW window_computation AS
                SELECT *
                FROM transactions
                WHERE ts >= year(now()) + 10""";
        CompilerCircuit cc = this.getCC(sql);
        CircuitVisitor visitor = new CircuitVisitor(cc.compiler) {
            int window = 0;
            int waterline = 0;

            @Override
            public void postorder(DBSPWindowOperator operator) {
                this.window++;
            }

            @Override
            public void postorder(DBSPWaterlineOperator operator) {
                this.waterline++;
            }

            @Override
            public void endVisit() {
                Assert.assertEquals(1, this.window);
                Assert.assertEquals(2, this.waterline);
            }
        };
        cc.visit(visitor);
    }

    @Test
    public void testNow7() {
        // now() used in WHERE with complex monotone function
        String sql = """
                CREATE TABLE transactions (
                  id INT NOT NULL PRIMARY KEY,
                  ts INT
                );
                CREATE VIEW window_computation AS
                SELECT *
                FROM transactions
                WHERE id + ts/2 - SIN(id) >= year(now()) + 10 AND
                      id + ts/2 - SIN(id) <= EXTRACT(CENTURY FROM now()) * 20;""";
        CompilerCircuit cc = this.getCC(sql);
        CircuitVisitor visitor = new CircuitVisitor(cc.compiler) {
            int window = 0;
            int waterline = 0;

            @Override
            public void postorder(DBSPWindowOperator operator) {
                this.window++;
            }

            @Override
            public void postorder(DBSPWaterlineOperator operator) {
                this.waterline++;
            }

            @Override
            public void endVisit() {
                Assert.assertEquals(1, this.window);
                Assert.assertEquals(2, this.waterline);
            }
        };
        cc.visit(visitor);
    }

    @Test
    public void testNow8() {
        // now() used in WHERE with complex function
        String sql = """
                CREATE TABLE transactions (
                  id INT NOT NULL PRIMARY KEY,
                  ts INT
                );
                CREATE VIEW window_computation AS
                SELECT *
                FROM transactions
                WHERE ts > 4 AND ts < 100 AND
                      id + ts/2 - SIN(id) >= year(now()) + 10 AND
                      id + ts/2 - SIN(id) <= EXTRACT(CENTURY FROM now()) * 20 AND
                      id >= EXTRACT(CENTURY FROM now()) * 20 AND
                      id = 4;""";
        CompilerCircuit cc = this.getCC(sql);
        CircuitVisitor visitor = new CircuitVisitor(cc.compiler) {
            int window = 0;
            int waterline = 0;

            @Override
            public void postorder(DBSPWindowOperator operator) {
                this.window++;
            }

            @Override
            public void postorder(DBSPWaterlineOperator operator) {
                this.waterline++;
            }

            @Override
            public void endVisit() {
                Assert.assertEquals(2, this.window);
                Assert.assertEquals(2, this.waterline);
            }
        };
        cc.visit(visitor);
    }

    @Test
    public void testNow9() {
        // now() used in WHERE with complex function where only some part generates a temporal filter
        String sql = """
                CREATE TABLE transactions (
                  id INT NOT NULL PRIMARY KEY,
                  ts INT
                );
                CREATE VIEW window_computation AS
                SELECT *
                FROM transactions
                WHERE id >= EXTRACT(CENTURY FROM now()) * 20 AND
                      EXTRACT(CENTURY FROM now()) % 10 = 0;""";
        CompilerCircuit cc = this.getCC(sql);
        CircuitVisitor visitor = new CircuitVisitor(cc.compiler) {
            int window = 0;
            int waterline = 0;

            @Override
            public void postorder(DBSPWindowOperator operator) {
                this.window++;
            }

            @Override
            public void postorder(DBSPWaterlineOperator operator) {
                this.waterline++;
            }

            @Override
            public void endVisit() {
                Assert.assertEquals(1, this.window);
                Assert.assertEquals(2, this.waterline);
            }
        };
        cc.visit(visitor);
    }

    @Test
    public void issue2003() {
        String sql = """
                CREATE TABLE event(
                    end   TIMESTAMP,
                    start TIMESTAMP NOT NULL LATENESS INTERVAL '1' HOURS
                );

                -- This is monotone because of the filter
                CREATE VIEW event_duration AS SELECT DISTINCT end
                FROM event
                WHERE end > start;""";
        CompilerCircuitStream ccs = this.getCCS(sql);
        CircuitVisitor visitor = new CircuitVisitor(ccs.compiler) {
            int integrate_trace = 0;

            @Override
            public void postorder(DBSPIntegrateTraceRetainKeysOperator operator) {
                this.integrate_trace++;
            }

            @Override
            public void endVisit() {
                Assert.assertEquals(1, this.integrate_trace);
            }
        };
        ccs.visit(visitor);
    }

    @Test
    public void issue1963() {
        String sql = """
                CREATE TABLE event(
                    id  BIGINT,
                    start   TIMESTAMP NOT NULL LATENESS INTERVAL '1' HOURS
                );

                CREATE VIEW event_duration AS SELECT DISTINCT
                    start,
                    id
                FROM event;

                CREATE VIEW filtered_events AS
                SELECT DISTINCT * FROM event_duration;""";
        this.compileRustTestCase(sql);
    }

    @Test
    public void issue1964() {
        String sql = """
                CREATE TABLE event(start TIMESTAMP NOT NULL LATENESS INTERVAL 1 HOURS);
                LATENESS slotted_events.start 96;
                CREATE VIEW slotted_events AS
                SELECT start
                FROM event;""";
        this.statementsFailingInCompilation(sql, "Cannot apply '-'");
    }

    @Test
    public void issue1965() {
        String sql = """
                CREATE TABLE event(
                    eve_key     VARCHAR,
                    eve_start   TIMESTAMP NOT NULL LATENESS INTERVAL 1 HOURS
                );

                CREATE VIEW filtered_events AS
                SELECT DISTINCT * FROM event
                WHERE eve_key IN ('foo', 'bar');

                CREATE VIEW slotted_events AS
                SELECT eve_start, eve_key
                FROM filtered_events;

                LATENESS slotted_events.eve_start INTERVAL 96 MINUTES;""";
        this.compileRustTestCase(sql);
    }

    @Test
    public void hoppingTest() {
        String sql = """
                CREATE TABLE series (
                    pickup TIMESTAMP NOT NULL
                );
                CREATE VIEW V AS
                SELECT * FROM TABLE(
                  HOP(
                    TABLE series,
                    DESCRIPTOR(pickup),
                    INTERVAL '2' MINUTE,
                    INTERVAL '5' MINUTE));""";
        this.getCCS(sql);
    }

    @Test
    public void testGC() {
        String sql = """
                create table t1(
                    ts bigint not null lateness 100,
                    id bigint
                ) WITH (
                    'connectors' = '[{
                        "transport": {
                            "name": "datagen",
                            "config": {
                                "plan": [{
                                    "fields": {}
                                }]
                            }
                        }
                    }]'
                );

                create table t2(
                    ts bigint not null lateness 100,
                    id bigint
                ) WITH (
                    'connectors' = '[{
                        "transport": {
                            "name": "datagen",
                            "config": {
                                "plan": [{
                                    "fields": {}
                                }]
                            }
                        }
                    }]'
                );

                create view v as
                select t1.* from
                t1 join t2
                on t1.id = t2.id
                where t1.ts  >= t2.ts - 10 and t1.ts <= t2.ts;
                """;
        this.compileRustTestCase(sql);
    }

    @Test
    public void testOver() {
        String sql = """
                CREATE TABLE table_name (
                    id INT NOT NULL PRIMARY KEY,
                    customer_id INT NOT NULL,
                    timestamp_column TIMESTAMP NOT NULL LATENESS INTERVAL 0 DAYS,
                    column_name DECIMAL(10, 2) NOT NULL
                );

                CREATE VIEW V AS SELECT
                    customer_id,
                    timestamp_column,
                    column_name,
                    SUM(column_name) OVER (
                        PARTITION BY customer_id, DATE_TRUNC(timestamp_column, MONTH)
                        ORDER BY timestamp_column
                        RANGE BETWEEN INTERVAL 31 DAYS PRECEDING AND CURRENT ROW
                    ) AS cumulative_sum
                FROM
                    table_name;
                """;
        this.compileRustTestCase(sql);
    }

    @Test
    public void smallTaxiTest() {
        String sql = """
                CREATE TABLE tripdata (
                  t TIMESTAMP NOT NULL LATENESS INTERVAL 1 HOUR,
                  location INT NOT NULL
                );

                CREATE VIEW V AS
                SELECT
                *,
                COUNT(*) OVER(
                   PARTITION BY  location
                   ORDER BY  t
                   RANGE BETWEEN INTERVAL 1 HOUR PRECEDING AND INTERVAL 1 MINUTE PRECEDING ) AS c
                FROM tripdata;""";
        CompilerCircuitStream ccs = this.getCCS(sql);
        CircuitVisitor visitor = new CircuitVisitor(ccs.compiler) {
            int rolling_waterline = 0;
            int integrate_trace = 0;

            @Override
            public void postorder(DBSPPartitionedRollingAggregateWithWaterlineOperator operator) {
                this.rolling_waterline++;
            }

            @Override
            public void postorder(DBSPIntegrateTraceRetainKeysOperator operator) {
                this.integrate_trace++;
            }

            @Override
            public void endVisit() {
                Assert.assertEquals(1, this.rolling_waterline);
                Assert.assertEquals(2, this.integrate_trace);
            }
        };
        ccs.visit(visitor);
    }

    @Test
    public void taxiTest() {
        String sql = """
                CREATE TABLE green_tripdata(
                   lpep_pickup_datetime TIMESTAMP NOT NULL LATENESS INTERVAL '1:00' HOURS TO MINUTES,
                   lpep_dropoff_datetime TIMESTAMP NOT NULL LATENESS INTERVAL '1:00' HOURS TO MINUTES,
                   pickup_location_id BIGINT NOT NULL,
                   dropoff_location_id BIGINT NOT NULL,
                   trip_distance DOUBLE PRECISION,
                   fare_amount DOUBLE PRECISION
                );
                CREATE VIEW V AS SELECT
                *,
                COUNT(*) OVER(
                   PARTITION BY  pickup_location_id
                   ORDER BY  extract (EPOCH from  CAST (lpep_pickup_datetime AS TIMESTAMP) )
                   -- 1 hour is 3600  seconds
                   RANGE BETWEEN 3600  PRECEDING AND 1 PRECEDING ) AS count_trips_window_1h_pickup_zip,
                AVG(fare_amount) OVER(
                   PARTITION BY  pickup_location_id
                   ORDER BY  extract (EPOCH from  CAST (lpep_pickup_datetime AS TIMESTAMP) )
                   -- 1 hour is 3600  seconds
                   RANGE BETWEEN 3600  PRECEDING AND 1 PRECEDING ) AS mean_fare_window_1h_pickup_zip,
                COUNT(*) OVER(
                   PARTITION BY  dropoff_location_id
                   ORDER BY  extract (EPOCH from  CAST (lpep_dropoff_datetime AS TIMESTAMP) )
                   -- 0.5 hour is 1800  seconds
                   RANGE BETWEEN 1800  PRECEDING AND 1 PRECEDING ) AS count_trips_window_30m_dropoff_zip,
                case when extract (ISODOW from  CAST (lpep_dropoff_datetime AS TIMESTAMP))  > 5
                     then 1 else 0 end as dropoff_is_weekend
                FROM green_tripdata""";
        this.compileRustTestCase(sql);
    }

    @Test
    public void unionTest() {
        // Tests the monotone analyzer for the sum and distinct operators
        String sql = """
                CREATE TABLE series (
                    pickup TIMESTAMP NOT NULL LATENESS INTERVAL 1 HOUR
                );
                CREATE VIEW V AS SELECT DISTINCT * FROM
                ((SELECT * FROM series) UNION ALL
                 (SELECT pickup + INTERVAL 5 MINUTES FROM series));""";
        CompilerCircuitStream ccs = this.getCCS(sql);
        CircuitVisitor visitor = new CircuitVisitor(ccs.compiler) {
            int count = 0;

            @Override
            public void postorder(DBSPIntegrateTraceRetainKeysOperator operator) {
                this.count++;
            }

            @Override
            public void endVisit() {
                Assert.assertEquals(1, this.count);
            }
        };
        ccs.visit(visitor);
    }

    @Test
    public void nullableHoppingTest() {
        String sql = """
                CREATE TABLE series (
                    pickup TIMESTAMP
                );
                CREATE VIEW V AS
                SELECT * FROM TABLE(
                  HOP(
                    TABLE series,
                    DESCRIPTOR(pickup),
                    INTERVAL '2' MINUTE,
                    INTERVAL '5' MINUTE));""";
        this.getCCS(sql);
    }

    @Test
    public void longIntervalHoppingTest() {
        String sql = """
                CREATE TABLE series (
                    pickup TIMESTAMP
                );
                CREATE VIEW V AS
                SELECT * FROM TABLE(
                  HOP(
                    TABLE series,
                    DESCRIPTOR(pickup),
                    INTERVAL 1 MONTH,
                    INTERVAL 2 MONTH));""";
        this.statementsFailingInCompilation(sql, "Hopping window intervals must be 'short'");
        sql = """
                CREATE TABLE series (
                    pickup TIMESTAMP
                );
                CREATE VIEW V AS
                SELECT * FROM TABLE(
                  HOP(
                    TABLE series,
                    DESCRIPTOR(pickup),
                    NULL,
                    NULL));""";
        this.statementsFailingInCompilation(sql, "Cannot apply 'HOP'");
        sql = """
                CREATE TABLE series (
                    pickup TIMESTAMP
                );
                CREATE VIEW V AS
                SELECT * FROM TABLE(
                  HOP(
                    TABLE series,
                    DESCRIPTOR(pickup),
                    6,
                    DATE '2020-12-20'));""";
        this.statementsFailingInCompilation(sql, "Cannot apply 'HOP'");
        sql = """
                CREATE TABLE series (
                    pickup TIMESTAMP
                );
                CREATE VIEW V AS
                SELECT * FROM TABLE(
                  HOP(
                    TABLE series,
                    DESCRIPTOR(pickup),
                    DESCRIPTOR(column),
                    INTERVAL 1 HOUR));""";
        this.statementsFailingInCompilation(sql, "Cannot apply 'HOP'");
    }

    @Test
    public void tumblingTestLimits() {
        String sql = """
               CREATE TABLE series (
                   pickup TIMESTAMP NOT NULL LATENESS INTERVAL '1:00' HOURS TO MINUTES
               );
               CREATE VIEW V AS
               SELECT TUMBLE_START(pickup, INTERVAL 30 MINUTES, TIME '00:12:00'),
                      TUMBLE_END(pickup, INTERVAL 30 MINUTES, TIME '00:12:00')
               FROM series
               GROUP BY TUMBLE(pickup, INTERVAL 30 MINUTES, TIME '00:12:00');""";

        CompilerCircuitStream ccs = this.getCCS(sql);
        ccs.step("INSERT INTO series VALUES('2024-02-08 10:00:00')",
                """
                 start               | end                 | weight
                ----------------------------------------------------
                 2024-02-08 09:42:00 | 2024-02-08 10:12:00 | 1""");
        ccs.step("INSERT INTO series VALUES('2024-02-08 10:10:00')",
                """
                start              | end                 | weight
                ---------------------------------------------------"""); // same group
        ccs.step( "INSERT INTO series VALUES('2024-02-08 10:12:00')",
                """
                 start               | end                 | weight
                ----------------------------------------------------
                 2024-02-08 10:12:00 | 2024-02-08 10:42:00 | 1""");
        ccs.step("INSERT INTO series VALUES('2024-02-08 10:30:00')",
                """
                start              | end                 | weight
                ---------------------------------------------------"""); // same group as before
    }

    @Test
    public void tumblingTest() {
        String sql = """
                CREATE TABLE series (
                        distance DOUBLE,
                        pickup TIMESTAMP NOT NULL LATENESS INTERVAL '1:00' HOURS TO MINUTES
                );
                CREATE VIEW V AS
                SELECT AVG(distance), TUMBLE_START(pickup, INTERVAL '1' DAY) FROM series
                GROUP BY TUMBLE(pickup, INTERVAL '1' DAY)""";
        CompilerCircuitStream ccs = this.getCCS(sql);
        ccs.step(
                "INSERT INTO series VALUES(10.0, '2023-12-30 10:00:00');",
                """
                 avg  | start | weight
                ----------------------
                 10.0 | 2023-12-30 00:00:00 | 1""");
        // Insert tuple before waterline, should be dropped
        ccs.step("INSERT INTO series VALUES(10.0, '2023-12-29 10:00:00');",
                """
                avg  | start | weight
                ----------------------""");
        // Insert tuple after waterline, should change average.
        // Waterline is advanced
        ccs.step("INSERT INTO series VALUES(20.0, '2023-12-30 10:10:00');",
                """
                 avg  | start | weight
                ----------------------
                 15.0 | 2023-12-30 00:00:00 | 1
                 10.0 | 2023-12-30 00:00:00 | -1""");
        // Insert tuple before last waterline, should be dropped
        ccs.step("INSERT INTO series VALUES(10.0, '2023-12-29 09:10:00');",
                """
                avg  | start | weight
                ----------------------""");
        // Insert tuple in the past, but before the last waterline
        ccs.step("INSERT INTO series VALUES(10.0, '2023-12-30 10:00:00');",
                """
                avg  | start | weight
                ----------------------
                13.333333333333334 | 2023-12-30 00:00:00 | 1
                15.0               | 2023-12-30 00:00:00 | -1""");
        // Insert tuple in the next tumbling window
        ccs.step("INSERT INTO series VALUES(10.0, '2023-12-31 10:00:00');",
                """
                avg  | start | weight
                ----------------------
                10.0 | 2023-12-31 00:00:00 | 1""");
    }

    @Test
    public void issue3542() {
        // Validated on Postgres
        var ccs = this.getCCS("""
                CREATE TABLE T1(a INT, b INT, c INT);
                CREATE TABLE T2(l INT, m INT, n INT);
                CREATE VIEW V0 AS
                select a, l from t1 full outer join t2 on t1.a = t2.l and t1.b < 5 and t2.m > 0;""");
        int[] filters = new int[1];
        CircuitVisitor visitor = new CircuitVisitor(ccs.compiler) {
            public void postorder(DBSPFlatMapOperator unused) {
                filters[0]++;
            }
        };
        ccs.visit(visitor);
        // Filters combined with maps into flatmaps
        assert filters[0] == 4;
        ccs.step("""
                INSERT INTO T1 VALUES(0, 1, 2);
                INSERT INTO T2 VALUES(0, 1, 2);
                """, """
                 a | l | weight
                ----------------
                 0 | 0 | 1""");
        ccs.step("""
                INSERT INTO T1 VALUES(2, 10, 3);
                """, """
                 a | l | weight
                ----------------
                 2 |   | 1""");
        ccs.step("""
                INSERT INTO T2 VALUES(3, -1, 3);
                """, """
                 a | l | weight
                ----------------
                   | 3 | 1""");
        ccs.step("""
                INSERT INTO T2 VALUES(2, -1, 3);
                """, """
                 a | l | weight
                ----------------
                   | 2 | 1""");
        ccs.step("""
                INSERT INTO T2 VALUES(2, 1, 3);
                """, """
                 a | l | weight
                ----------------
                   | 2 | 1""");
        ccs.step("""
                INSERT INTO T1 VALUES(2, 0, 4);
                """, """
                 a | l | weight
                ----------------
                 2 | 2 | 1
                   | 2 | -1""");
        /* Final result is:
         a 	 |   l
        -----------
         0 	  |  0
         2 	  |  null
         2    |  2
         null |  2
         null |  3 */
    }

    @Test
    public void blogTest() {
        String statements = """
                CREATE TABLE CUSTOMER(name VARCHAR NOT NULL, zipcode INT NOT NULL);
                CREATE VIEW DENSITY AS
                SELECT zipcode, COUNT(name)
                FROM CUSTOMER
                GROUP BY zipcode
                """;
        CompilerCircuitStream ccs = this.getCCS(statements);
        Assert.assertFalse(ccs.compiler.hasErrors());
        ccs.step("",
                """
                 zipcode | count | weight
                --------------------------""");
        ccs.step("""
                 INSERT INTO customer VALUES('Bob', 1000);
                 INSERT INTO customer VALUES('Pam', 2000);
                 INSERT INTO customer VALUES('Sue', 3000);
                 INSERT INTO customer VALUES('Mike', 1000);""",
                """
                 zipcode | count | weight
                --------------------------
                 1000    | 2     | 1
                 2000    | 1     | 1
                 3000    | 1     | 1""");
        ccs.step("""
                REMOVE FROM customer VALUES('Bob', 1000);
                INSERT INTO customer VALUES('Bob', 2000);""",
                """
                 zipcode | count | weight
                --------------------------
                 1000    | 2     | -1
                 2000    | 1     | -1
                 2000    | 2     | 1
                 1000    | 1     | 1""");
    }

    @Test
    public void nullableLatenessTest() {
        // LATENESS used on a nullable column
        String ddl = """
                CREATE TABLE series (
                        distance DOUBLE,
                        pickup TIMESTAMP LATENESS INTERVAL '1:00' HOURS TO MINUTES
                );
                CREATE VIEW V AS
                SELECT AVG(distance), CAST(pickup AS DATE) FROM series GROUP BY CAST(pickup AS DATE);""";
        this.getCCS(ddl);
    }

    @Test
    public void watermarkTest0() {
        // Test for the example in the documentation
        String sql = """
                CREATE TABLE order_pickup (
                   pickup_time TIMESTAMP NOT NULL WATERMARK INTERVAL '1:00' HOURS TO MINUTES,
                   location VARCHAR
                );
                """;
        this.getCCS(sql);
    }

    @Test
    public void watermarkTest() {
        String sql = """
                CREATE TABLE series (
                        distance DOUBLE,
                        pickup TIMESTAMP NOT NULL WATERMARK INTERVAL '1:00' HOURS TO MINUTES
                );
                CREATE VIEW V AS
                SELECT AVG(distance), CAST(pickup AS DATE) FROM series GROUP BY CAST(pickup AS DATE)""";
        CompilerCircuitStream ccs = this.getCCS(sql);
        ccs.step("INSERT INTO series VALUES(10, '2023-12-30 10:00:00');",
                """
                         avg  | date       | weight
                        ---------------------------""");
        // Insert tuple before watermark, should be processed
        ccs.step("INSERT INTO series VALUES(10, '2023-12-29 10:00:00');",
                """
                         avg  | date       | weight
                        ---------------------------
                         10   | 2023-12-29 | 1""");
        // Insert tuple after waterline, but not after watermark
        // Waterline is advanced, no new outputs
        ccs.step("INSERT INTO series VALUES(20, '2023-12-30 10:10:00');",
                """
                         avg  | date        | weight
                        ---------------------------""");
        // Insert tuple before last waterline, should be processed
        // average does not change for 2023-12-19
        ccs.step("INSERT INTO series VALUES(10, '2023-12-29 09:10:00');",
                """
                 avg  | date       | weight
                ---------------------------""");
        // Insert tuple in the past, but before the last waterline
        // no new output
        ccs.step("INSERT INTO series VALUES(10, '2023-12-30 10:00:00');",
                """
                         avg  | date        | weight
                        ---------------------------""");
        // Insert one more tuple that accepts all buffered 3 tuples
        ccs.step("INSERT INTO series VALUES(10, '2023-12-31 10:00:00');",
                """
                         avg  | date        | weight
                        ---------------------------
                         13.333333333333334 | 2023-12-30 | 1""");
    }

    @Test
    public void latenessTest() {
        String sql = """
                CREATE TABLE series (
                        distance DOUBLE,
                        pickup TIMESTAMP NOT NULL LATENESS INTERVAL '1:00' HOURS TO MINUTES
                );
                CREATE VIEW V AS
                SELECT AVG(distance), CAST(pickup AS DATE) FROM series GROUP BY CAST(pickup AS DATE);""";
        CompilerCircuitStream ccs = this.getCCS(sql);
        ccs.step("INSERT INTO series VALUES(10, '2023-12-30 10:00:00');",
                """
                         avg  | date       | weight
                        ---------------------------
                         10.0 | 2023-12-30 | 1""");
        // Insert tuple before waterline, should be dropped
        ccs.step("INSERT INTO series VALUES(10, '2023-12-29 10:00:00');",
                """
                         avg  | date       | weight
                        ---------------------------""");
        // Insert tuple after waterline, should change average.
        // Waterline is advanced
        ccs.step("INSERT INTO series VALUES(20, '2023-12-30 10:10:00');",
                """
                         avg  | date        | weight
                        ---------------------------
                         15.0 | 2023-12-30 | 1
                         10.0 | 2023-12-30 | -1""");
        // Insert tuple before last waterline, should be dropped
        ccs.step("INSERT INTO series VALUES(10, '2023-12-29 09:10:00');",
                        """
                         avg  | date       | weight
                        ---------------------------""");
        // Insert tuple in the past, but before the last waterline
        ccs.step("INSERT INTO series VALUES(10, '2023-12-30 10:00:00');",
                """
                         avg  | date        | weight
                        ---------------------------
                         15.0 | 2023-12-30 | -1
                         13.333333333333334 | 2023-12-30 | 1""");
    }

    @Test
    public void errorStreamTest() {
        // Same as before, but using the error stream
        DBSPType out = new DBSPTypeTuple(
                new DBSPTypeDouble(CalciteObject.EMPTY, true),
                new DBSPTypeDate(CalciteObject.EMPTY, false));
        DBSPType string = DBSPTypeString.varchar(false);
        DBSPType error = new DBSPTypeTuple(string, string,
                // new DBSPTypeVariant(false)
                string);
        /*
        DBSPTypeMap map = new DBSPTypeMap(
                new DBSPTypeVariant(false),
                new DBSPTypeVariant(false), false);
         */
        String sql = """
                CREATE TABLE series (
                        distance DOUBLE,
                        pickup TIMESTAMP NOT NULL LATENESS INTERVAL '1:00' HOURS TO MINUTES
                );
                CREATE VIEW V AS
                SELECT AVG(distance), CAST(pickup AS DATE) FROM series GROUP BY CAST(pickup AS DATE);""";
        CompilerCircuitStream ccs = this.getCCS(sql);
        ccs.addChange(new InputOutputChange(
                new Change(
                        new DBSPZSetExpression(
                                new DBSPTupleExpression(
                                        new DBSPDoubleLiteral(10.0, true),
                                        new DBSPTimestampLiteral("2023-12-30 10:00:00", false)))),
                new Change(
                        new DBSPZSetExpression(
                                new DBSPTupleExpression(
                                        new DBSPDoubleLiteral(10.0, true),
                                        new DBSPDateLiteral("2023-12-30", false))),
                        DBSPZSetExpression.emptyWithElementType(error))));
        // Insert tuple before waterline, should be dropped
        ccs.addChange(new InputOutputChange(
                new Change(
                        new DBSPZSetExpression(
                                new DBSPTupleExpression(
                                        new DBSPDoubleLiteral(10.0, true),
                                        new DBSPTimestampLiteral("2023-12-29 10:00:00", false)))),
                new Change(
                        DBSPZSetExpression.emptyWithElementType(out),
                        new DBSPZSetExpression(
                                new DBSPTupleExpression(
                                        new DBSPStringLiteral("series"),
                                        new DBSPStringLiteral("Late value"),
                                        /*
                                        new DBSPVariantExpression(
                                                new DBSPMapExpression(map,
                                                        Linq.list(
                                                                new DBSPVariantExpression(
                                                                        new DBSPStringLiteral("distance")
                                                                ),
                                                                new DBSPVariantExpression(
                                                                        new DBSPDoubleLiteral(10.0)
                                                                ),
                                                                new DBSPVariantExpression(
                                                                        new DBSPStringLiteral("pickup")
                                                                ),
                                                                new DBSPVariantExpression(
                                                                        new DBSPTimestampLiteral("2023-12-29 10:00:00", false)
                                                                )))))
                                        */
                                        new DBSPStringLiteral("Tup2(Some(10.0), 2023-12-29 10:00:00)")
                        )))));
        // Insert tuple after waterline, should change average.
        // Waterline is advanced
        ccs.addChange(new InputOutputChange(
                new Change(
                        new DBSPZSetExpression(
                                new DBSPTupleExpression(
                                        new DBSPDoubleLiteral(20.0, true),
                                        new DBSPTimestampLiteral("2023-12-30 10:10:00", false)))),
                new Change(
                        new DBSPZSetExpression(
                                new DBSPTupleExpression(
                                        new DBSPDoubleLiteral(15.0, true),
                                        new DBSPDateLiteral("2023-12-30", false)))
                                .add(
                                        new DBSPZSetExpression(
                                                new DBSPTupleExpression(
                                                        new DBSPDoubleLiteral(10.0, true),
                                                        new DBSPDateLiteral("2023-12-30", false))).negate()
                                ),
                        DBSPZSetExpression.emptyWithElementType(error))));
        // Insert tuple before last waterline, should be dropped
        ccs.addChange(new InputOutputChange(
                new Change(
                        new DBSPZSetExpression(
                                new DBSPTupleExpression(
                                        new DBSPDoubleLiteral(10.0, true),
                                        new DBSPTimestampLiteral("2023-12-29 09:10:00", false)))),
                new Change(
                        DBSPZSetExpression.emptyWithElementType(out),
                        new DBSPZSetExpression(
                                new DBSPTupleExpression(
                                        new DBSPStringLiteral("series"),
                                        new DBSPStringLiteral("Late value"),
                                        /*
                                        new DBSPVariantExpression(
                                                new DBSPMapExpression(map,
                                                        Linq.list(
                                                                new DBSPVariantExpression(
                                                                        new DBSPStringLiteral("distance")
                                                                ),
                                                                new DBSPVariantExpression(
                                                                        new DBSPDoubleLiteral(10.0)
                                                                ),
                                                                new DBSPVariantExpression(
                                                                        new DBSPStringLiteral("pickup")
                                                                ),
                                                                new DBSPVariantExpression(
                                                                        new DBSPTimestampLiteral("2023-12-29 09:10:00", false)
                                                                ))))
                                         */
                                        new DBSPStringLiteral("Tup2(Some(10.0), 2023-12-29 09:10:00)")
                                )))));
        // Insert tuple in the past, but before the last waterline
        ccs.addChange(new InputOutputChange(
                new Change(
                        new DBSPZSetExpression(
                                new DBSPTupleExpression(
                                        new DBSPDoubleLiteral(10.0, true),
                                        new DBSPTimestampLiteral("2023-12-30 10:00:00", false)))),
                new Change(
                        new DBSPZSetExpression(
                                new DBSPTupleExpression(
                                        new DBSPDoubleLiteral(13.333333333333334, true),
                                        new DBSPDateLiteral("2023-12-30", false)))
                                .add(
                                        new DBSPZSetExpression(
                                                new DBSPTupleExpression(
                                                        new DBSPDoubleLiteral(15.0, true),
                                                        new DBSPDateLiteral("2023-12-30", false))).negate()
                                ),
                        DBSPZSetExpression.emptyWithElementType(error))));
    }

    @Test
    public void errorStreamQueryTest() {
        // Same as before, but using a query on the error stream.
        // The error stream is the first one
        DBSPType error = new DBSPTypeTuple(
                DBSPTypeString.varchar(false),
                DBSPTypeString.varchar(false),
                //new DBSPTypeVariant(false)
                DBSPTypeString.varchar(false)
        );
        DBSPType e = new DBSPTypeTuple(DBSPTypeString.varchar(false));
        /*
        DBSPTypeMap map = new DBSPTypeMap(
                new DBSPTypeVariant(false),
                new DBSPTypeVariant(false), false);
         */

        String sql = """
                CREATE TABLE series (
                        distance DOUBLE,
                        pickup TIMESTAMP NOT NULL LATENESS INTERVAL '1:00' HOURS TO MINUTES
                );
                CREATE LOCAL VIEW V AS
                SELECT AVG(distance), CAST(pickup AS DATE) FROM series GROUP BY CAST(pickup AS DATE);
                CREATE VIEW E AS SELECT MESSAGE FROM ERROR_VIEW WHERE MESSAGE LIKE '%a%';""";
        CompilerCircuitStream ccs = this.getCCS(sql, Linq.list("series"), Linq.list("e", "error_view"));
        ccs.addChange(new InputOutputChange(
                new Change(
                        new DBSPZSetExpression(
                                new DBSPTupleExpression(
                                        new DBSPDoubleLiteral(10.0, true),
                                        new DBSPTimestampLiteral("2023-12-30 10:00:00", false)))),
                new Change(
                        DBSPZSetExpression.emptyWithElementType(e),
                        DBSPZSetExpression.emptyWithElementType(error))));
        // Insert tuple before waterline, should be dropped
        ccs.addChange(new InputOutputChange(
                new Change(
                        new DBSPZSetExpression(
                                new DBSPTupleExpression(
                                        new DBSPDoubleLiteral(10.0, true),
                                        new DBSPTimestampLiteral("2023-12-29 10:00:00", false)))),
                new Change(
                        new DBSPZSetExpression(
                                new DBSPTupleExpression(new DBSPStringLiteral("Late value"))),
                        new DBSPZSetExpression(
                                new DBSPTupleExpression(
                                        new DBSPStringLiteral("series"),
                                        new DBSPStringLiteral("Late value"),
                                        /*
                                        new DBSPVariantExpression(
                                                new DBSPMapExpression(map,
                                                        Linq.list(
                                                                new DBSPVariantExpression(
                                                                        new DBSPStringLiteral("distance")
                                                                ),
                                                                new DBSPVariantExpression(
                                                                        new DBSPDoubleLiteral(10.0)
                                                                ),
                                                                new DBSPVariantExpression(
                                                                        new DBSPStringLiteral("pickup")
                                                                ),
                                                                new DBSPVariantExpression(
                                                                        new DBSPTimestampLiteral("2023-12-29 10:00:00", false)
                                                                ))))

                                         */
                                        new DBSPStringLiteral("Tup2(Some(10.0), 2023-12-29 10:00:00)")
                                )))));
        // Insert tuple after waterline, should change average.
        // Waterline is advanced
        ccs.addChange(new InputOutputChange(
                new Change(
                        new DBSPZSetExpression(
                                new DBSPTupleExpression(
                                        new DBSPDoubleLiteral(20.0, true),
                                        new DBSPTimestampLiteral("2023-12-30 10:10:00", false)))),
                new Change(
                        DBSPZSetExpression.emptyWithElementType(e),
                        DBSPZSetExpression.emptyWithElementType(error))));
        // Insert tuple before last waterline, should be dropped
        ccs.addChange(new InputOutputChange(
                new Change(
                        new DBSPZSetExpression(
                                new DBSPTupleExpression(
                                        new DBSPDoubleLiteral(10.0, true),
                                        new DBSPTimestampLiteral("2023-12-29 09:10:00", false)))),
                new Change(
                        new DBSPZSetExpression(
                                new DBSPTupleExpression(new DBSPStringLiteral("Late value"))),
                        new DBSPZSetExpression(
                                new DBSPTupleExpression(
                                        new DBSPStringLiteral("series"),
                                        new DBSPStringLiteral("Late value"),
                                        /*
                                        new DBSPVariantExpression(
                                                new DBSPMapExpression(map,
                                                        Linq.list(
                                                                new DBSPVariantExpression(
                                                                        new DBSPStringLiteral("distance")
                                                                ),
                                                                new DBSPVariantExpression(
                                                                        new DBSPDoubleLiteral(10.0)
                                                                ),
                                                                new DBSPVariantExpression(
                                                                        new DBSPStringLiteral("pickup")
                                                                ),
                                                                new DBSPVariantExpression(
                                                                        new DBSPTimestampLiteral("2023-12-29 09:10:00", false)
                                                                ))))
                                         */
                                        new DBSPStringLiteral("Tup2(Some(10.0), 2023-12-29 09:10:00)")
                                )))));
        // Insert tuple in the past, but before the last waterline
        ccs.addChange(new InputOutputChange(
                new Change(
                        new DBSPZSetExpression(
                                new DBSPTupleExpression(
                                        new DBSPDoubleLiteral(10.0, true),
                                        new DBSPTimestampLiteral("2023-12-30 10:00:00", false)))),
                new Change(
                        DBSPZSetExpression.emptyWithElementType(e),
                        DBSPZSetExpression.emptyWithElementType(error))));
    }

    @Test
    public void testJoin() {
        String ddl = """
            CREATE TABLE series (
                    metadata VARCHAR,
                    event_time TIMESTAMP NOT NULL LATENESS INTERVAL '1:00' HOURS TO MINUTES
            );

            CREATE TABLE shift(
                    person VARCHAR,
                    on_call DATE
            );
            CREATE VIEW V AS SELECT metadata, person FROM series
            JOIN shift ON CAST(series.event_time AS DATE) = shift.on_call;""";
        CompilerCircuitStream ccs = this.getCCS(ddl);
        CircuitVisitor visitor = new CircuitVisitor(ccs.compiler) {
            int count = 0;

            @Override
            public void postorder(DBSPIntegrateTraceRetainKeysOperator operator) {
                this.count++;
            }

            @Override
            public void endVisit() {
                Assert.assertEquals(1, this.count);
            }
        };
        ccs.visit(visitor);
    }

    // Test for https://github.com/feldera/feldera/issues/1462
    @Test
    public void testJoinNonMonotoneColumn() {
        String script = """
            CREATE TABLE series (
                    metadata VARCHAR NOT NULL,
                    event_time TIMESTAMP NOT NULL LATENESS INTERVAL '1:00' HOURS TO MINUTES
            );

            CREATE TABLE shift(
                    person VARCHAR NOT NULL,
                    on_call DATE
            );

            CREATE VIEW V AS
            (SELECT * FROM series JOIN shift ON series.metadata = shift.person);
            """;
        CompilerCircuitStream ccs = this.getCCS(script);
        CircuitVisitor visitor = new CircuitVisitor(ccs.compiler) {
            int count = 0;

            @Override
            public void postorder(DBSPIntegrateTraceRetainKeysOperator operator) {
                this.count++;
            }

            @Override
            public void endVisit() {
                Assert.assertEquals(0, this.count);
            }
        };
        ccs.visit(visitor);
    }

    @Test
    public void testJoinTwoColumns() {
        // One joined column is monotone, the other one isn't.
        String sql = """
            CREATE TABLE series (
                    metadata VARCHAR NOT NULL,
                    event_time TIMESTAMP NOT NULL LATENESS INTERVAL '1:00' HOURS TO MINUTES
            );

            CREATE TABLE shift(
                    person VARCHAR NOT NULL,
                    on_call DATE
            );

            CREATE VIEW V AS
            (SELECT * FROM series JOIN shift
             ON series.metadata = shift.person AND CAST(series.event_time AS DATE) = shift.on_call);
            """;
        CompilerCircuitStream ccs = this.getCCS(sql);
        CircuitVisitor visitor = new CircuitVisitor(ccs.compiler) {
            int count = 0;

            @Override
            public void postorder(DBSPIntegrateTraceRetainKeysOperator operator) {
                this.count++;
            }

            @Override
            public void endVisit() {
                Assert.assertEquals(1, this.count);
            }
        };
        ccs.visit(visitor);
    }

    @Test
    public void testJoinFilter() {
        // Join two streams with lateness, and filter based on lateness column
        String script = """
            CREATE TABLE series (
                    metadata VARCHAR NOT NULL,
                    event_date DATE NOT NULL LATENESS INTERVAL 1 DAYS
            );

            CREATE TABLE shift(
                    person VARCHAR NOT NULL,
                    on_call DATE NOT NULL LATENESS INTERVAL 1 DAYS
            );

            CREATE VIEW V AS
            (SELECT metadata, event_date FROM series JOIN shift
             ON series.metadata = shift.person AND event_date > on_call);
            """;
        CompilerCircuitStream ccs = this.getCCS(script);
        CircuitVisitor visitor = new CircuitVisitor(ccs.compiler) {
            int count = 0;

            @Override
            public void postorder(DBSPIntegrateTraceRetainKeysOperator operator) {
                this.count++;
            }

            @Override
            // TODO: should be 1
            public void endVisit() {
                Assert.assertEquals(0, this.count);
            }
        };
        ccs.visit(visitor);
    }

    @Test
    public void testAggregate() {
        String sql = """
                CREATE TABLE event_t (
                    event_type_id BIGINT NOT NULL
                );

                -- running total of event types
                CREATE VIEW event_type_count_v AS
                SELECT count(DISTINCT event_type_id) as event_type_count
                from   event_t
                ;""";
        CompilerCircuitStream ccs = this.getCCS(sql);
        ccs.step("",
                """
                 event_type_count | weight
                ---------------------------
                 0                | 1""");
        ccs.step("",
                """
                 event_type_count | weight
                ---------------------------""");
        ccs.step("INSERT INTO event_t VALUES(1);",
                 """
                 event_type_count | weight
                ---------------------------
                 0                | -1
                 1                | 1""");
        ccs.step("",
                """
                 event_type_count | weight
                ---------------------------""");
        ccs.step("INSERT INTO event_t VALUES(2);",
                """
                 event_type_count | weight
                ---------------------------
                 1                | -1
                 2                | 1""");
    }

    @Test
    public void testHopNotImplemented() {
        // This syntax is not supported, one should use the HOP table functions
        String sql = """
                CREATE TABLE bid (
                    auction  BIGINT FOREIGN KEY REFERENCES auction(id),
                    date_time TIMESTAMP(3) NOT NULL LATENESS INTERVAL 4 SECONDS
                );
                CREATE VIEW V AS SELECT
                  B1.auction,
                  count(*) AS num,
                  HOP_START(B1.date_time, INTERVAL '2' SECOND, INTERVAL '10' SECOND) AS starttime,
                  HOP_END(B1.date_time, INTERVAL '2' SECOND, INTERVAL '10' SECOND) AS endtime
                FROM bid B1
                GROUP BY
                  B1.auction,
                  HOP(B1.date_time, INTERVAL '2' SECOND, INTERVAL '10' SECOND);
                """;
        this.statementsFailingInCompilation(sql, "Please use the TABLE function HOP");
    }

    @Test
    public void issue2529() {
        String sql = """
                CREATE TABLE m(
                   id bigint,
                   ts timestamp not null lateness interval 1 days,
                   data int array
                );
                create local view flattened as
                select id, v, ts
                from m, unnest(data) as v;
                create view agg2 as
                select max(id)
                from flattened
                group by ts;
                """;
        CompilerCircuitStream ccs = this.getCCS(sql);
        CircuitVisitor visitor = new CircuitVisitor(ccs.compiler) {
            int count = 0;

            @Override
            public void postorder(DBSPIntegrateTraceRetainKeysOperator operator) {
                this.count++;
            }

            @Override
            public void endVisit() {
                Assert.assertEquals(2, this.count);
            }
        };
        ccs.visit(visitor);
        ccs.step("INSERT INTO m VALUES(0, '2024-01-03 00:00:00', ARRAY[1, 2, 3])",
                """
                 max | weight
                --------------
                 0   | 1""");
        // insert in the past, ignored
        ccs.step("INSERT INTO m VALUES(3, '2024-01-01 00:00:00', ARRAY[4])",
                """
                 max | weight
                --------------""");
        // empty array: no records after unnest
        ccs.step("INSERT INTO m VALUES(6, '2024-01-04 00:00:00', array_compact(ARRAY[null]))",
                """
                 max | weight
                --------------""");
        // grouped by a different invisible timestamp, so the previous one is not deleted
        ccs.step("INSERT INTO m VALUES(5, '2024-01-04 00:00:00', ARRAY[null])",
                """
                 max | weight
                --------------
                 5   | 1""");
    }

    @Test
    public void testHopWindows() {
        String sql = """
                CREATE TABLE DATA(
                    moment TIMESTAMP NOT NULL LATENESS INTERVAL 1 DAYS,
                    amount DECIMAL(10, 2),
                    cc_num VARCHAR
                );

                CREATE LOCAL VIEW hop AS
                SELECT * FROM TABLE(HOP(TABLE DATA, DESCRIPTOR(moment), INTERVAL 4 HOURS, INTERVAL 1 HOURS));

                CREATE LOCAL VIEW agg AS
                SELECT
                  AVG(amount) AS avg_amt,
                  STDDEV(amount) as stddev_amt,
                  COUNT(cc_num) AS trans,
                  ARRAY_AGG(moment) AS moments
                FROM hop
                GROUP BY cc_num, window_start;

                CREATE VIEW results AS
                SELECT
                  avg_amt,
                  COALESCE(stddev_amt, 0) AS stddev_amt,
                  trans,
                  moment
                FROM agg CROSS JOIN UNNEST(moments) as moment;
                """;
        this.getCCS(sql);
    }

    @Test
    public void calciteMeetupTest() {
        this.getCCS("""
                CREATE TABLE data(ts TIMESTAMP LATENESS INTERVAL 10 MINUTES, price INT, item INT);
                CREATE VIEW V AS
                SELECT DATE_TRUNC(ts, MONTH), MIN(price), item
                FROM data
                GROUP BY DATE_TRUNC(ts, MONTH), item;""");
    }
}
