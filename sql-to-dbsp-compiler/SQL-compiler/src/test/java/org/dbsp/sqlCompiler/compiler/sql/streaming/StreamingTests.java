package org.dbsp.sqlCompiler.compiler.sql.streaming;

import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.circuit.operator.DBSPAggregateLinearPostprocessOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPAggregateLinearPostprocessRetainKeysOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPChainAggregateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPControlledKeyFilterOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPFlatMapIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPInputMapWithWaterlineOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIntegrateTraceRetainKeysOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIntegrateTraceRetainNValuesOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIntegrateTraceRetainValuesOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinBaseOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPPartitionedRollingAggregateWithWaterlineOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPWaterlineOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPWindowOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.TestUtil;
import org.dbsp.sqlCompiler.compiler.frontend.TableData;
import org.dbsp.sqlCompiler.compiler.sql.OtherTests;
import org.dbsp.sqlCompiler.compiler.sql.StreamingTestBase;
import org.dbsp.sqlCompiler.compiler.sql.tools.Change;
import org.dbsp.sqlCompiler.compiler.sql.tools.CompilerCircuit;
import org.dbsp.sqlCompiler.compiler.sql.tools.CompilerCircuitStream;
import org.dbsp.sqlCompiler.compiler.sql.tools.InputOutputChange;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.outer.FindUnboundedState;
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
import org.dbsp.util.NullPrintStream;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nullable;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

/** Tests that exercise streaming features. */
public class StreamingTests extends StreamingTestBase {
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
                         max | min | weight
                        --------------------
                         20  |   8 | -1
                         20  |   3 | 1""");
        CircuitVisitor visitor = new CircuitVisitor(ccs.compiler) {
            int chains = 0;

            @Override
            public void postorder(DBSPChainAggregateOperator operator) {
                chains++;
            }

            @Override
            public void endVisit() {
                Assert.assertEquals(1, chains);
            }
        };
        ccs.visit(visitor);
    }

    @Test
    public void issue4686() {
        String sql = """
                CREATE TABLE T(TS INT, X INT) WITH ('append_only' = 'true');
                CREATE VIEW V AS
                SELECT SUM(TS * 2), COUNT(TS - 2), MAX(TS) FROM T;""";
        var ccs = this.getCCS(sql);
        ccs.step("INSERT INTO T VALUES(NULL, 0);",
                """
                         sum | ct  | max | weight
                        --------------------
                             |   0 |     | 1""");
        ccs.step("INSERT INTO T VALUES(10, -10);",
                """
                         sum | ct  | max | weight
                        --------------------
                             |   0 |     |-1
                         20  |   1 | 10  | 1""");
        ccs.step("INSERT INTO T VALUES(5, 20);",
                """
                         sum | ct  | max | weight
                        --------------------
                         30  |   2 | 10  | 1
                         20  |   1 | 10  | -1""");
        CircuitVisitor visitor = new CircuitVisitor(ccs.compiler) {
            int chains = 0;

            @Override
            public void postorder(DBSPChainAggregateOperator operator) {
                chains++;
            }

            @Override
            public void postorder(DBSPAggregateLinearPostprocessOperator operator) {
                Assert.fail("Should not be present");
            }

            @Override
            public void endVisit() {
                Assert.assertEquals(1, chains);
            }
        };
        ccs.visit(visitor);
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
                Assert.assertEquals(1, this.integrate_trace);
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
                Assert.assertEquals(1, this.integrate_trace);
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
                Assert.assertEquals(1, this.integrate_trace);
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
                    SELECT feedback.*, transaction.*
                    FROM
                    feedback LEFT ASOF JOIN transaction
                    MATCH_CONDITION(transaction.unix_time <= feedback.unix_time)
                    ON transaction.id = feedback.id;
                """;
        CompilerCircuitStream ccs = this.getCCS(sql);
        CircuitVisitor visitor = new CircuitVisitor(ccs.compiler) {
            int integrate_trace = 0;
            int integrate_trace_last = 0;

            @Override
            public void postorder(DBSPIntegrateTraceRetainValuesOperator operator) {
                this.integrate_trace++;
            }

            @Override
            public void postorder(DBSPIntegrateTraceRetainNValuesOperator operator) {
                this.integrate_trace_last++;
            }

            @Override
            public void endVisit() {
                Assert.assertEquals(1, this.integrate_trace);
                Assert.assertEquals(1, this.integrate_trace_last);
            }
        };
        ccs.visit(visitor);

        // waterline for ASOF is -100
        ccs.step("""
                INSERT INTO TRANSACTION VALUES(1, 0), (2, 1);
                INSERT INTO FEEDBACK VALUES(1, 1, 0), (2, 1, 0);
                """, """
                 id | status | time | tid | ttime | weight
                ---------------------------------------------
                  1 |      1 |    0 |   1 |     0 | 1
                  2 |      1 |    0 |     |       | 1""");
        ccs.step("""
                INSERT INTO TRANSACTION VALUES(2, 0)""", """
                 id | status | time | tid | ttime | weight
                ---------------------------------------------
                  2 |      1 |    0 |     |       | -1
                  2 |      1 |    0 |   2 |     0 | 1""");
        ccs.step("""
                INSERT INTO TRANSACTION VALUES(2, 200)""", """
                 id | status | time | tid | ttime | weight
                ---------------------------------------------""");
        // waterline moves to 100 = min(200-100, 300-100)
        ccs.step("""
                INSERT INTO FEEDBACK VALUES(2, 2, 300)""", """
                 id | status | time | tid | ttime | weight
                ---------------------------------------------
                  2 |      2 |  300 |   2 |   200 | 1""");
        ccs.step("""
                INSERT INTO FEEDBACK VALUES(2, 2, 250)""", """
                 id | status | time | tid | ttime | weight
                ---------------------------------------------
                  2 |      2 |  250 |   2 |   200 | 1""");
        // waterline moves to 200 = min(300-100, 400-100)
        ccs.step("""
                INSERT INTO TRANSACTION VALUES(2, 400)""", """
                 id | status | time | tid | ttime | weight
                ---------------------------------------------""");
        // LATE value in FEEDBACK is ignored
        ccs.step("""
                INSERT INTO FEEDBACK VALUES(2, 10, 100)""", """
                 id | status | time | tid | ttime | weight
                ---------------------------------------------""");
        // Remove a value from FEEDBACK, retracts the corresponding output
        ccs.step("""
                REMOVE FROM FEEDBACK VALUES(2, 2, 250)""", """
                 id | status | time | tid | ttime | weight
                ---------------------------------------------
                  2 |      2 |  250 |   2 |   200 | -1""");

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
        CompilerCircuitStream ccs = this.getCCS(sql).compactAfterEachStep();
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

    /** Asserts that the circuit contains exactly {@code keys}
     * integrate_trace_retain_keys operators, and no integrate_trace_retain_values
     * operators, that garbage-collect an input table.  */
    void checkInputGC(CompilerCircuit cc, int keys) {
        cc.visit(new CircuitVisitor(cc.compiler) {
            int retainKeys = 0;
            int retainValues = 0;

            @Override
            public void postorder(DBSPIntegrateTraceRetainKeysOperator operator) {
                if (operator.left().operator.is(DBSPInputMapWithWaterlineOperator.class))
                    this.retainKeys++;
            }

            @Override
            public void postorder(DBSPIntegrateTraceRetainValuesOperator operator) {
                if (operator.left().operator.is(DBSPInputMapWithWaterlineOperator.class))
                    this.retainValues++;
            }

            @Override
            public void endVisit() {
                Assert.assertEquals(keys, this.retainKeys);
                Assert.assertEquals(0, this.retainValues);
            }
        });
    }

    /** Asserts the placement of the trace GC operators around a join.
     * The circuit must contain exactly one join.
     * @param cc          Compiled circuit.
     * @param leftValues  retain_values operators expected on the left join input.
     * @param rightValues retain_values operators expected on the right join input.
     * @param rightLastN  retain_n_values operators expected on the right join input. */
    void checkJoinGC(CompilerCircuit cc, int leftValues, int rightValues, int rightLastN) {
        cc.visit(new CircuitVisitor(cc.compiler) {
            @Nullable DBSPJoinBaseOperator join = null;
            final List<OutputPort> retainValuesData = new ArrayList<>();
            final List<OutputPort> retainNData = new ArrayList<>();

            @Override
            public void postorder(DBSPJoinBaseOperator operator) {
                Assert.assertNull("more than one join", this.join);
                this.join = operator;
            }

            @Override
            public void postorder(DBSPIntegrateTraceRetainValuesOperator operator) {
                this.retainValuesData.add(operator.left());
            }

            @Override
            public void postorder(DBSPIntegrateTraceRetainNValuesOperator operator) {
                this.retainNData.add(operator.left());
            }

            @Override
            public void endVisit() {
                DBSPJoinBaseOperator join = this.join;
                Assert.assertNotNull(join);
                Assert.assertEquals(leftValues,
                        Linq.where(this.retainValuesData, p -> p.equals(join.left())).size());
                Assert.assertEquals(rightValues,
                        Linq.where(this.retainValuesData, p -> p.equals(join.right())).size());
                Assert.assertEquals(rightLastN,
                        Linq.where(this.retainNData, p -> p.equals(join.right())).size());
                // No GC operators anywhere else
                Assert.assertEquals(leftValues + rightValues, this.retainValuesData.size());
                Assert.assertEquals(rightLastN, this.retainNData.size());
            }
        });
    }

    @Test
    public void issue6829() {
        // As-of lookup written as an inequality join plus ARG_MAX.
        // The history row (1, 0, 7) must stay joinable forever
        String sql = """
                CREATE TABLE queries (
                    entity_id BIGINT NOT NULL,
                    query_ts BIGINT NOT NULL LATENESS 0,
                    query_id BIGINT NOT NULL
                ) WITH ('append_only' = 'true');
                CREATE TABLE config_history (
                    entity_id BIGINT NOT NULL,
                    effective_ts BIGINT NOT NULL LATENESS 0,
                    config_value BIGINT NOT NULL
                ) WITH ('append_only' = 'true');
                CREATE VIEW v AS
                SELECT q.entity_id, q.query_ts, q.query_id,
                       ARG_MAX(c.config_value, c.effective_ts) AS config_value
                FROM queries q JOIN config_history c
                  ON q.entity_id = c.entity_id
                 AND c.effective_ts <= q.query_ts
                GROUP BY q.entity_id, q.query_ts, q.query_id;""";
        // GC happens when trace batches merge, and merging is per-shard, so
        // every row uses entity 1, keeping a single worker busy.
        CompilerCircuitStream ccs = this.getCCS(sql).compactAfterEachStep();
        this.checkJoinGC(ccs, 1, 0, 0);
        this.run6829Circuit(ccs);
        // Control: no input row is late, so the program without LATENESS
        // must produce the same outputs; it has no GC operators.
        CompilerCircuitStream control = this.getCCS(
                sql.replace(" LATENESS 0", "")).compactAfterEachStep();
        this.checkJoinGC(control, 0, 0, 0);
        this.run6829Circuit(control);
    }

    /** No input row is ever late: each
     * timestamp column arrives in non-decreasing order.
     * The trace diagrams describe the inequality-join variant with
     * LATENESS: they show the queries join trace and the bound of its
     * integrate_trace_retain_values operator, the config_history waterline
     * delayed by one transaction.  Rows above the ==== line are below the
     * bound and may be dropped when trace batches merge. */
    void run6829Circuit(CompilerCircuitStream ccs) {
        // Before this step: bound = minimum, both tables are empty.
        ccs.step("""
                INSERT INTO config_history VALUES(1, 0, 7);
                INSERT INTO queries VALUES(1, 10, 1);
                """, """
                 entity_id | query_ts | query_id | config_value | weight
                ---------------------------------------------------------
                 1         | 10       | 1        | 7            | 1""");
        // Before this step, queries trace:
        //   entity | query_ts | id
        //  --------+----------+---
        //  ==========================  bound = 0
        //        1 |       10 |  1
        // This row advances the effective_ts waterline to 2000.  Its
        // effective_ts is above the final query's query_ts, so it never
        // changes the lookup result, and it does not join query (1, 10, 1).
        ccs.step("""
                INSERT INTO config_history VALUES(1, 2000, 50);
                """, """
                 entity_id | query_ts | query_id | config_value | weight
                ---------------------------------------------------------""");
        // Before this step, queries trace:
        //   entity | query_ts | id
        //  --------+----------+---
        //        1 |       10 |  1    droppable: future history rows all
        //  ==========================  bound = 2000    exceed its query_ts
        ccs.step("""
                INSERT INTO config_history VALUES(1, 2001, 51);
                """, """
                 entity_id | query_ts | query_id | config_value | weight
                ---------------------------------------------------------""");
        // The merge after the previous step runs with bound 2000; it may
        // drop query (1, 10, 1), whose output stands.  It must retain
        // history row (1, 0, 7): the config_history trace has no GC bound.
        ccs.step("""
                INSERT INTO queries VALUES(1, 1000, 999);
                """, """
                 entity_id | query_ts | query_id | config_value | weight
                ---------------------------------------------------------
                 1         | 1000     | 999      | 7            | 1""");
    }

    @Test
    public void issue6829Windowed() {
        // The windowed-join variant of issue6829, with the streams' waterlines
        // skewed: orders run far ahead of payments.
        String sql = """
                CREATE TABLE orders (
                    order_id BIGINT NOT NULL,
                    order_ts BIGINT NOT NULL LATENESS 0
                ) WITH ('append_only' = 'true');
                CREATE TABLE payments (
                    order_id BIGINT NOT NULL,
                    pay_ts BIGINT NOT NULL LATENESS 0
                ) WITH ('append_only' = 'true');
                CREATE VIEW v AS
                SELECT o.order_id, o.order_ts, p.pay_ts
                FROM orders o JOIN payments p
                  ON o.order_id = p.order_id
                 AND p.pay_ts >= o.order_ts
                 AND p.pay_ts <= o.order_ts + 100;""";
        CompilerCircuitStream ccs = this.getCCS(sql).compactAfterEachStep();
        this.checkJoinGC(ccs, 1, 1, 0);
        this.runCircuit6829Windowed(ccs);
        // Control: no input row is late, so the program without LATENESS
        // must produce the same outputs; it has no GC operators.
        CompilerCircuitStream control = this.getCCS(
                sql.replace(" LATENESS 0", "")).compactAfterEachStep();
        this.checkJoinGC(control, 0, 0, 0);
        this.runCircuit6829Windowed(control);
    }

    /** No input row is ever late:
     * both timestamp columns arrive in non-decreasing order.  The outputs
     * must therefore be identical with and without the LATENESS
     * annotations.
     * The trace diagrams describe the LATENESS variant: they show the
     * orders join trace and the bound of its integrate_trace_retain_values
     * operator, the payments waterline minus the window width 100, delayed
     * by one transaction.  Payments never advance here, so the bound stays
     * at minimum and every order must survive every merge. */
    void runCircuit6829Windowed(CompilerCircuitStream ccs) {
        // Before this step: bound = minimum, both tables are empty.
        ccs.step("""
                INSERT INTO orders VALUES(1, 0);
                """, """
                 order_id | order_ts | pay_ts | weight
                ---------------------------------------""");
        // Before this step, orders trace:
        //   order | order_ts
        //  -------+---------
        //  ===================  bound = minimum
        //       1 |        0
        // This row advances the order_ts waterline to 500.
        ccs.step("""
                INSERT INTO orders VALUES(1, 500);
                """, """
                 order_id | order_ts | pay_ts | weight
                ---------------------------------------""");
        // Before this step, orders trace:
        //   order | order_ts
        //  -------+---------
        //  ===================  bound = minimum
        //       1 |        0    payment window [0, 100] still open
        //       1 |      500
        ccs.step("""
                INSERT INTO orders VALUES(1, 501);
                """, """
                 order_id | order_ts | pay_ts | weight
                ---------------------------------------""");
        // The first payment: on time, because the payments waterline has
        // never advanced.  It falls only inside the window of order (1, 0).
        ccs.step("""
                INSERT INTO payments VALUES(1, 50);
                """, """
                 order_id | order_ts | pay_ts | weight
                ---------------------------------------
                 1        | 0        | 50     | 1""");
    }

    @Test
    public void issue6829Asof() {
        String sql = """
                CREATE TABLE queries (
                    entity_id BIGINT NOT NULL,
                    query_ts BIGINT NOT NULL LATENESS 0,
                    query_id BIGINT NOT NULL
                ) WITH ('append_only' = 'true');
                CREATE TABLE config_history (
                    entity_id BIGINT NOT NULL,
                    effective_ts BIGINT NOT NULL LATENESS 0,
                    config_value BIGINT NOT NULL
                ) WITH ('append_only' = 'true');
                CREATE VIEW v AS
                SELECT q.entity_id, q.query_ts, q.query_id, c.config_value
                FROM queries q
                LEFT ASOF JOIN config_history c
                MATCH_CONDITION(q.query_ts >= c.effective_ts)
                ON q.entity_id = c.entity_id;""";
        CompilerCircuitStream ccs = this.getCCS(sql).compactAfterEachStep();
        this.checkJoinGC(ccs, 1, 0, 1);
        this.run6829Circuit(ccs);
        // Control: no input row is late, so the program without LATENESS
        // must produce the same outputs; it has no GC operators.
        CompilerCircuitStream control = this.getCCS(
                sql.replace(" LATENESS 0", "")).compactAfterEachStep();
        this.checkJoinGC(control, 0, 0, 0);
        this.run6829Circuit(control);
    }

    @Test
    public void sessionGc() {
        // LATENESS on the SESSION timestamp column with SESSION windows.
        // The RetainNValues operator attaches to the JoinIndex of the LAG;
        // the steps below run with compaction to check that this works.
        String sql = """
                CREATE TABLE events(
                    ts  TIMESTAMP NOT NULL LATENESS INTERVAL 1 HOURS,
                    uid VARCHAR
                );
                CREATE VIEW sessions AS
                SELECT uid, COUNT(*) AS cnt, window_start, window_end
                FROM TABLE(SESSION(TABLE events, DESCRIPTOR(ts), DESCRIPTOR(uid), INTERVAL 10 MINUTES))
                GROUP BY uid, window_start, window_end;""";
        CompilerCircuitStream ccs = this.getCCS(sql).compactAfterEachStep().withStringTrim();
        // TODO: window_start cannot have a waterline (a session can be unbounded)
        // but window_end does, but the algorithm we use does not find it.
        ccs.visit(new CircuitVisitor(ccs.compiler) {
            int retainKeys = 0;
            int retainNValues = 0;
            int rollingWithWaterline = 0;

            @Override
            public void postorder(DBSPIntegrateTraceRetainKeysOperator operator) {
                this.retainKeys++;
            }

            @Override
            public void postorder(DBSPIntegrateTraceRetainNValuesOperator operator) {
                this.retainNValues++;
            }

            @Override
            public void postorder(DBSPPartitionedRollingAggregateWithWaterlineOperator operator) {
                this.rollingWithWaterline++;
            }

            @Override
            public void endVisit() {
                Assert.assertEquals(1, this.rollingWithWaterline);
                Assert.assertEquals(2, this.retainKeys);
                Assert.assertEquals(1, this.retainNValues);
            }
        });
        // The waterline is max over all data of (ts - 1 hour); each step is
        // filtered with the waterline computed from the previous steps.
        // Before this step: waterline = minimum, table is empty.
        // Two sessions start: 'a' has two events 5 minutes apart, 'b' one event
        ccs.step("""
                INSERT INTO events VALUES('2020-01-01 10:00:00', 'a'),
                                         ('2020-01-01 10:05:00', 'a'),
                                         ('2020-01-01 10:00:00', 'b');""", """
                 uid | cnt | window_start        | window_end          | weight
                ----------------------------------------------------------------
                 a   |   2 | 2020-01-01 10:00:00 | 2020-01-01 10:15:00 | 1
                 b   |   1 | 2020-01-01 10:00:00 | 2020-01-01 10:10:00 | 1""");
        // Before this step, table contents (rows above the ==== line are
        // below the waterline):
        //   ts    | uid |
        //  -------+-----+
        //  ==============  waterline = 10:05 - 1:00 = 09:05
        //   10:00 | a   |
        //   10:00 | b   |
        //   10:05 | a   |
        // 7 minutes after the last 'a' event: extends the 'a' session
        ccs.step("""
                INSERT INTO events VALUES('2020-01-01 10:12:00', 'a');""", """
                 uid | cnt | window_start        | window_end          | weight
                ----------------------------------------------------------------
                 a   | 2   | 2020-01-01 10:00:00 | 2020-01-01 10:15:00 | -1
                 a   | 3   | 2020-01-01 10:00:00 | 2020-01-01 10:22:00 | 1""");
        // Before this step:
        //   ts    | uid |
        //  -------+-----+
        //  ==============  waterline = 10:12 - 1:00 = 09:12
        //   10:00 | a   |
        //   10:00 | b   |
        //   10:05 | a   |
        //   10:12 | a   |
        // Far from the previous event: a new session
        ccs.step("""
                INSERT INTO events VALUES('2020-01-01 13:00:00', 'a');""", """
                 uid | cnt | window_start        | window_end          | weight
                -----------------------------------------------------------------
                 a   | 1   | 2020-01-01 13:00:00 | 2020-01-01 13:10:00 | 1""");
        // Before this step:
        //   ts    | uid |
        //  -------+-----+
        //   10:00 | a   | frozen
        //   10:00 | b   | frozen
        //   10:05 | a   | frozen
        //   10:12 | a   | frozen
        //  ==============  waterline = 13:00 - 1:00 = 12:00
        //   13:00 | a   |
        // The frozen sessions can no longer change; compaction may collect
        // their state.  This row is below the waterline: late, dropped
        ccs.step("""
                INSERT INTO events VALUES('2020-01-01 11:30:00', 'a');""", """
                 uid | cnt | window_start | window_end | weight
                ------------------------------------------------""");
        // Before this step: same contents and waterline as the previous step.
        // Exactly on the waterline: not late; more than one gap away from
        // both neighbor sessions, so it forms its own
        ccs.step("""
                INSERT INTO events VALUES('2020-01-01 12:00:00', 'a');""", """
                 uid | cnt | window_start        | window_end          | weight
                ----------------------------------------------------------------
                 a   |   1 | 2020-01-01 12:00:00 | 2020-01-01 12:10:00 | 1""");
    }

    @Test
    public void sessionDelete() {
        // Sessions react to deletions: removing a row can split a session in
        // two, or move its start.  No LATENESS.
        String sql = """
                CREATE TABLE events(
                    ts  TIMESTAMP NOT NULL,
                    uid VARCHAR
                );
                CREATE VIEW sessions AS
                SELECT uid, COUNT(*) AS cnt, window_start, window_end
                FROM TABLE(SESSION(TABLE events, DESCRIPTOR(ts), DESCRIPTOR(uid), INTERVAL 10 MINUTES))
                GROUP BY uid, window_start, window_end;""";
        CompilerCircuitStream ccs = this.getCCS(sql).withStringTrim();
        // 10:05 and 10:20 are 15 minutes apart, so there are two sessions
        ccs.step("""
                INSERT INTO events VALUES('2020-01-01 10:00:00', 'a'),
                                         ('2020-01-01 10:05:00', 'a'),
                                         ('2020-01-01 10:20:00', 'a');""", """
                 uid | cnt | window_start        | window_end          | weight
                ----------------------------------------------------------------
                 a   | 2   | 2020-01-01 10:00:00 | 2020-01-01 10:15:00 | 1
                 a   | 1   | 2020-01-01 10:20:00 | 2020-01-01 10:30:00 | 1""");
        // 10:12 is within the gap of both neighbors, so it bridges the two
        // sessions into one
        ccs.step("""
                INSERT INTO events VALUES('2020-01-01 10:12:00', 'a');""", """
                 uid | cnt | window_start        | window_end          | weight
                ----------------------------------------------------------------
                 a   | 2   | 2020-01-01 10:00:00 | 2020-01-01 10:15:00 | -1
                 a   | 1   | 2020-01-01 10:20:00 | 2020-01-01 10:30:00 | -1
                 a   | 4   | 2020-01-01 10:00:00 | 2020-01-01 10:30:00 | 1""");
        // Removing the bridge splits the session again
        ccs.step("""
                REMOVE FROM events VALUES('2020-01-01 10:12:00', 'a');""", """
                 uid | cnt | window_start        | window_end          | weight
                ----------------------------------------------------------------
                 a   |   4 | 2020-01-01 10:00:00 | 2020-01-01 10:30:00 | -1
                 a   |   2 | 2020-01-01 10:00:00 | 2020-01-01 10:15:00 | 1
                 a   |   1 | 2020-01-01 10:20:00 | 2020-01-01 10:30:00 | 1""");
        // Removing the first row of a session moves its window_start
        ccs.step("""
                REMOVE FROM events VALUES('2020-01-01 10:00:00', 'a');""", """
                 uid | cnt | window_start        | window_end          | weight
                ----------------------------------------------------------------
                 a   |   2 | 2020-01-01 10:00:00 | 2020-01-01 10:15:00 | -1
                 a   |   1 | 2020-01-01 10:05:00 | 2020-01-01 10:15:00 | 1""");
        // Removing the last remaining row of a session deletes it
        ccs.step("""
                REMOVE FROM events VALUES('2020-01-01 10:20:00', 'a');""", """
                 uid | cnt | window_start        | window_end          | weight
                ----------------------------------------------------------------
                 a   |   1 | 2020-01-01 10:20:00 | 2020-01-01 10:30:00 | -1""");
    }

    @Test
    public void sessionGcLateMerge() {
        // A session that straddles the waterline can still merge with an
        // on-time row; the merged session's window_start comes from a value
        // below the waterline, so value retention must keep it.
        String sql = """
                CREATE TABLE events(
                    ts  TIMESTAMP NOT NULL LATENESS INTERVAL 1 HOURS,
                    uid VARCHAR
                );
                CREATE VIEW sessions AS
                SELECT uid, COUNT(*) AS cnt, window_start, window_end
                FROM TABLE(SESSION(TABLE events, DESCRIPTOR(ts), DESCRIPTOR(uid), INTERVAL 10 MINUTES))
                GROUP BY uid, window_start, window_end;""";
        CompilerCircuitStream ccs = this.getCCS(sql).compactAfterEachStep().withStringTrim();
        // Before this step: waterline = minimum, table is empty
        ccs.step("""
                INSERT INTO events VALUES('2020-01-01 11:50:00', 'a'),
                                         ('2020-01-01 11:55:00', 'a');""", """
                 uid | cnt | window_start        | window_end          | weight
                ----------------------------------------------------------------
                 a   |   2 | 2020-01-01 11:50:00 | 2020-01-01 12:05:00 | 1""");
        // Before this step:
        //   ts    | uid |
        //  -------+-----+
        //  ==============  waterline = 11:55 - 1:00 = 10:55
        //   11:50 | a   |
        //   11:55 | a   |
        // An unrelated key advances the waterline to 12:00; the 'a' session
        // now straddles it: its rows are below, but a row on the waterline
        // can still merge with it
        ccs.step("""
                INSERT INTO events VALUES('2020-01-01 13:00:00', 'z');""", """
                 uid | cnt | window_start        | window_end          | weight
                ----------------------------------------------------------------
                 z   |   1 | 2020-01-01 13:00:00 | 2020-01-01 13:10:00 | 1""");
        // Before this step:
        //   ts    | uid |
        //  -------+-----+
        //   11:50 | a   | below the waterline, but the session is still live
        //   11:55 | a   | (a row at 12:00..12:05 can merge with it)
        //  ==============  waterline = 13:00 - 1:00 = 12:00
        //   13:00 | z   |
        // On-time row 9 minutes after 11:55: merges; window_start must still
        // be 11:50, which only survives GC if value retention kept it
        ccs.step("""
                INSERT INTO events VALUES('2020-01-01 12:04:00', 'a');""", """
                 uid | cnt | window_start        | window_end          | weight
                ----------------------------------------------------------------
                 a   |   2 | 2020-01-01 11:50:00 | 2020-01-01 12:05:00 | -1
                 a   |   3 | 2020-01-01 11:50:00 | 2020-01-01 12:14:00 | 1""");
    }

    @Test
    public void gcUpsertBoundary() {
        // The LATENESS column is not part of the primary key, so the input
        // trace is never garbage-collected: any key can still be updated.
        // Rows whose timestamp is exactly on the waterline are not late and
        // can be updated and deleted.
        // The waterline is max over all data of (ts - 100); each step is
        // filtered with the waterline computed from the previous steps.
        String sql = """
                CREATE TABLE t (
                    id INT NOT NULL PRIMARY KEY,
                    ts BIGINT NOT NULL LATENESS 100
                );
                CREATE VIEW v AS SELECT * FROM t;""";
        CompilerCircuitStream ccs = this.getCCS(sql).compactAfterEachStep();
        this.checkInputGC(ccs, 0);
        // Before this step: waterline = minimum, table is empty
        ccs.step("""
                INSERT INTO t VALUES(1, 0), (2, 100), (3, 200);
                """, """
                 id | ts  | weight
                --------------------
                  1 |   0 | 1
                  2 | 100 | 1
                  3 | 200 | 1""");
        // Before this step, table contents (rows above the ==== line are
        // below the waterline):
        //   id | ts  |
        //  ----+-----+
        //    1 |   0 | frozen
        //  ================  waterline = 200 - 100
        //    2 | 100 | on the waterline: still updatable
        //    3 | 200 |
        // Updating key 2 retracts the old row.
        ccs.step("""
                INSERT INTO t VALUES(2, 300);
                """, """
                 id | ts  | weight
                --------------------
                  2 | 100 | -1
                  2 | 300 | 1""");
        // Before this step, table contents:
        //   id | ts  |
        //  ----+-----+
        //    1 |   0 | frozen
        //  ================  waterline = 300 - 100
        //    3 | 200 | on the waterline: still deletable
        //    2 | 300 |
        ccs.step("""
                REMOVE FROM t VALUES(3, 200);
                """, """
                 id | ts  | weight
                --------------------
                  3 | 200 | -1""");
        // Before this step, table contents:
        //   id | ts  |
        //  ----+-----+
        //    1 |   0 | frozen
        //  ================  waterline = 300 - 100
        //    2 | 300 |
        ccs.step("""
                INSERT INTO t VALUES(4, 400);
                """, """
                 id | ts  | weight
                --------------------
                  4 | 400 | 1""");
        // Before this step, table contents:
        //   id | ts  |
        //  ----+-----+
        //    1 |   0 | frozen
        //  ================  waterline = 400 - 100
        //    2 | 300 | on the waterline
        //    4 | 400 |
        // A new key with a timestamp below the waterline is late and ignored.
        ccs.step("""
                INSERT INTO t VALUES(5, 100);
                """, """
                 id | ts  | weight
                --------------------""");
    }

    @Test
    public void gcDeleteOldRow() {
        // Deleting a key whose row is below the waterline is ignored: the
        // deletion itself is late.  Deleting a row at or above the waterline works.
        String sql = """
                CREATE TABLE t (
                    id INT NOT NULL PRIMARY KEY,
                    ts BIGINT NOT NULL LATENESS 100
                );
                CREATE VIEW v AS SELECT * FROM t;""";
        CompilerCircuitStream ccs = this.getCCS(sql).compactAfterEachStep();
        this.checkInputGC(ccs, 0);
        // Before this step: waterline = minimum, table is empty
        ccs.step("""
                INSERT INTO t VALUES(1, 0), (2, 200);
                """, """
                 id | ts  | weight
                --------------------
                  1 |   0 | 1
                  2 | 200 | 1""");
        // Before this step, table contents:
        //   id | ts  |
        //  ----+-----+
        //    1 |   0 | frozen
        //  ================  waterline = 200 - 100
        //    2 | 200 |
        ccs.step("""
                INSERT INTO t VALUES(3, 250);
                """, """
                 id | ts  | weight
                --------------------
                  3 | 250 | 1""");
        // Before this step, table contents:
        //   id | ts  |
        //  ----+-----+
        //    1 |   0 | frozen
        //  ================  waterline = 250 - 100
        //    2 | 200 |
        //    3 | 250 |
        // Deleting frozen row (1, 0) is ignored.
        ccs.step("""
                REMOVE FROM t VALUES(1, 0);
                """, """
                 id | ts  | weight
                --------------------""");
        // Before this step: waterline = 250 - 100, table contents unchanged.
        // Row (2, 200) is above the waterline: deleting it works.
        ccs.step("""
                REMOVE FROM t VALUES(2, 200);
                """, """
                 id | ts  | weight
                --------------------
                  2 | 200 | -1""");
    }

    @Test
    public void gcUpsertOldRow() {
        // Updating a key whose row is below the waterline must be ignored:
        // the update would have to retract the old row, which is behind the
        // lateness threshold.  See the warning about primary keys and LATENESS
        // in docs.feldera.com/docs/tutorials/time-series.md: "old" records in
        // such a table can never be updated or deleted.
        String sql = """
                CREATE TABLE t (
                    id INT NOT NULL PRIMARY KEY,
                    ts BIGINT NOT NULL LATENESS 100
                );
                CREATE VIEW v AS SELECT * FROM t;""";
        CompilerCircuitStream ccs = this.getCCS(sql).compactAfterEachStep();
        this.checkInputGC(ccs, 0);
        // Before this step: waterline = minimum, table is empty
        ccs.step("""
                INSERT INTO t VALUES(1, 0), (2, 200);
                """, """
                 id | ts  | weight
                --------------------
                  1 |   0 | 1
                  2 | 200 | 1""");
        // Before this step, table contents:
        //   id | ts  |
        //  ----+-----+
        //    1 |   0 | frozen
        //  ================  waterline = 200 - 100
        //    2 | 200 |
        ccs.step("""
                INSERT INTO t VALUES(3, 250);
                """, """
                 id | ts  | weight
                --------------------
                  3 | 250 | 1""");
        // Before this step, table contents:
        //   id | ts  |
        //  ----+-----+
        //    1 |   0 | frozen
        //  ================  waterline = 250 - 100
        //    2 | 200 |
        //    3 | 250 |
        // The update of key 1 is ignored, even though the new timestamp 300
        // is above the waterline: it would have to retract frozen row (1, 0).
        ccs.step("""
                INSERT INTO t VALUES(1, 300);
                """, """
                 id | ts  | weight
                --------------------""");
    }

    @Test
    public void gcUpsertOldRowLongLag() {
        // Like gcUpsertOldRow, but with several steps between the moment the
        // row falls below the waterline and the attempt to update it, giving
        // compaction many opportunities to run.  The outcome must not depend
        // on compaction timing.
        String sql = """
                CREATE TABLE t (
                    id INT NOT NULL PRIMARY KEY,
                    ts BIGINT NOT NULL LATENESS 100
                );
                CREATE VIEW v AS SELECT * FROM t;""";
        CompilerCircuitStream ccs = this.getCCS(sql).compactAfterEachStep();
        this.checkInputGC(ccs, 0);
        // Before this step: waterline = minimum, table is empty
        ccs.step("""
                INSERT INTO t VALUES(1, 0), (2, 200);
                """, """
                 id | ts  | weight
                --------------------
                  1 |   0 | 1
                  2 | 200 | 1""");
        // Before this step, table contents:
        //   id | ts  |
        //  ----+-----+
        //    1 |   0 | frozen
        //  ================  waterline = 200 - 100
        //    2 | 200 |
        ccs.step("""
                INSERT INTO t VALUES(3, 250);
                """, """
                 id | ts  | weight
                --------------------
                  3 | 250 | 1""");
        // Before this step, table contents:
        //   id | ts  |
        //  ----+-----+
        //    1 |   0 | frozen
        //  ================  waterline = 250 - 100
        //    2 | 200 |
        //    3 | 250 |
        ccs.step("""
                INSERT INTO t VALUES(4, 260);
                """, """
                 id | ts  | weight
                --------------------
                  4 | 260 | 1""");
        // Before this step, table contents:
        //   id | ts  |
        //  ----+-----+
        //    1 |   0 | frozen
        //  ================  waterline = 260 - 100
        //    2 | 200 |
        //    3 | 250 |
        //    4 | 260 |
        ccs.step("""
                INSERT INTO t VALUES(5, 270);
                """, """
                 id | ts  | weight
                --------------------
                  5 | 270 | 1""");
        // Before this step, table contents:
        //   id | ts  |
        //  ----+-----+
        //    1 |   0 | frozen
        //  ================  waterline = 270 - 100
        //    2 | 200 |
        //    3 | 250 |
        //    4 | 260 |
        //    5 | 270 |
        // The update of key 1 is ignored, even though the new timestamp 300
        // is above the waterline: it would have to retract frozen row (1, 0).
        ccs.step("""
                INSERT INTO t VALUES(1, 300);
                """, """
                 id | ts  | weight
                --------------------""");
    }

    @Test
    public void gcTwoLatenessColumns() {
        // Neither LATENESS column is part of the primary key, so the input
        // trace is never garbage-collected.  The waterline is a pair compared
        // pointwise; a row is frozen when it is below the waterline in ANY
        // component.  A row fresh in both components stays updatable.
        String sql = """
                CREATE TABLE t (
                    id INT NOT NULL PRIMARY KEY,
                    ts1 BIGINT NOT NULL LATENESS 100,
                    ts2 BIGINT NOT NULL LATENESS 100
                );
                CREATE VIEW v AS SELECT * FROM t;""";
        CompilerCircuitStream ccs = this.getCCS(sql).compactAfterEachStep();
        this.checkInputGC(ccs, 0);
        // Before this step: waterline = (minimum, minimum), table is empty
        ccs.step("""
                INSERT INTO t VALUES(1, 0, 1000), (2, 1000, 0), (3, 1000, 1000);
                """, """
                 id | ts1  | ts2  | weight
                ----------------------------
                  1 |    0 | 1000 | 1
                  2 | 1000 |    0 | 1
                  3 | 1000 | 1000 | 1""");
        // Before this step: waterline = (1000 - 100, 1000 - 100).
        // The waterline pair is compared pointwise; table contents:
        //   id | ts1  | ts2  |
        //  ----+------+------+-----------------------------
        //    1 |    0 | 1000 | ts1 below waterline: frozen
        //    2 | 1000 |    0 | ts2 below waterline: frozen
        //    3 | 1000 | 1000 |
        // Row 3 is at or above the waterline in both components,
        // so updating key 3 retracts the old row.
        ccs.step("""
                INSERT INTO t VALUES(3, 1100, 1100);
                """, """
                 id | ts1  | ts2  | weight
                ----------------------------
                  3 | 1000 | 1000 | -1
                  3 | 1100 | 1100 | 1""");
        // Before this step: waterline = (1100 - 100, 1100 - 100), table contents:
        //   id | ts1  | ts2  |
        //  ----+------+------+-----------------------------
        //    1 |    0 | 1000 | ts1 below waterline: frozen
        //    2 | 1000 |    0 | ts2 below waterline: frozen
        //    3 | 1100 | 1100 |
        // Row 1 is frozen through ts1 alone: the update is ignored, even
        // though the old ts2 and both new timestamps are fresh.
        ccs.step("""
                INSERT INTO t VALUES(1, 1150, 1150);
                """, """
                 id | ts1  | ts2  | weight
                ----------------------------""");
        // Before this step: waterline = (1100 - 100, 1100 - 100), table contents unchanged.
        // Row 2 is frozen through ts2 alone: the deletion is ignored.
        ccs.step("""
                REMOVE FROM t VALUES(2, 1000, 0);
                """, """
                 id | ts1  | ts2  | weight
                ----------------------------""");
        // Before this step: waterline = (1100 - 100, 1100 - 100), table contents unchanged
        ccs.step("""
                INSERT INTO t VALUES(4, 1200, 1200);
                """, """
                 id | ts1  | ts2  | weight
                ----------------------------
                  4 | 1200 | 1200 | 1""");
    }

    @Test
    public void gcKeyLateness() {
        // The LATENESS column is the primary key, so the compiler
        // garbage-collects the input trace by key.
        String sql = """
                CREATE TABLE t (
                    ts BIGINT NOT NULL PRIMARY KEY LATENESS 100,
                    v INT
                );
                CREATE VIEW vw AS SELECT * FROM t;""";
        CompilerCircuitStream ccs = this.getCCS(sql).compactAfterEachStep();
        this.checkInputGC(ccs, 1);
        // Before this step: waterline = minimum, table is empty
        ccs.step("""
                INSERT INTO t VALUES(0, 1), (100, 1), (200, 1);
                """, """
                 ts  | v | weight
                -------------------
                   0 | 1 | 1
                 100 | 1 | 1
                 200 | 1 | 1""");
        // Before this step, table contents (rows above the ==== line are
        // below the waterline):
        //   ts  | v |
        //  -----+---+
        //    0  | 1 | GC'd
        //  ================  waterline = 200 - 100
        //   100 | 1 | on the waterline: still updatable
        //   200 | 1 |
        // Updating key 100 retracts the old row.
        ccs.step("""
                INSERT INTO t VALUES(100, 2);
                """, """
                 ts  | v | weight
                -------------------
                 100 | 1 | -1
                 100 | 2 | 1""");
        // Before this step, table contents:
        //   ts  | v |
        //  -----+---+
        //    0  | 1 | GC'd
        //  ================  waterline = 200 - 100
        //   100 | 2 |
        //   200 | 1 |
        // Updating GC'd key 0 is rejected: the record itself is late.
        ccs.step("""
                INSERT INTO t VALUES(0, 5);
                """, """
                 ts  | v | weight
                -------------------""");
        // Before this step, table contents:
        //   ts  | v |
        //  -----+---+
        //    0  | 1 | GC'd
        //  ================  waterline = 200 - 100
        //   100 | 2 |
        //   200 | 1 |
        // Deleting GC'd key 0 is a no-op.
        ccs.step("""
                REMOVE FROM t VALUES(0, 1);
                """, """
                 ts  | v | weight
                -------------------""");
        // Before this step, table contents:
        //   ts  | v |
        //  -----+---+
        //    0  | 1 | GC'd
        //  ================  waterline = 200 - 100
        //   100 | 2 |
        //   200 | 1 |
        ccs.step("""
                INSERT INTO t VALUES(300, 1);
                """, """
                 ts  | v | weight
                -------------------
                 300 | 1 | 1""");
        // Before this step, table contents:
        //   ts  | v |
        //  -----+---+
        //    0  | 1 | GC'd
        //   100 | 2 | GC'd
        //  ================  waterline = 300 - 100
        //   200 | 1 | on the waterline: still deletable
        //   300 | 1 |
        ccs.step("""
                REMOVE FROM t VALUES(200, 1);
                """, """
                 ts  | v | weight
                -------------------
                 200 | 1 | -1""");
        // Before this step, table contents:
        //   ts  | v |
        //  -----+---+
        //    0  | 1 | GC'd
        //   100 | 2 | GC'd
        //  ================  waterline = 300 - 100
        //   300 | 1 |
        ccs.step("""
                INSERT INTO t VALUES(300, 9);
                """, """
                 ts  | v | weight
                -------------------
                 300 | 1 | -1
                 300 | 9 | 1""");
    }

    @Test
    public void gcCompositeKeyLateness() {
        // Composite primary key (id, ts) where only ts has LATENESS, plus a
        // LATENESS column outside the key.  The input trace is
        // garbage-collected by key, using only the ts component of the
        // waterline.
        String sql = """
                CREATE TABLE t (
                    id INT NOT NULL,
                    ts BIGINT NOT NULL LATENESS 100,
                    extra BIGINT NOT NULL LATENESS 100,
                    v INT,
                    PRIMARY KEY (id, ts)
                );
                CREATE VIEW vw AS SELECT * FROM t;""";
        CompilerCircuitStream ccs = this.getCCS(sql).compactAfterEachStep();
        this.checkInputGC(ccs, 1);
        // Before this step: waterline = (minimum, minimum), table is empty
        ccs.step("""
                INSERT INTO t VALUES(1, 0, 0, 1), (2, 200, 200, 1);
                """, """
                 id | ts  | extra | v | weight
                --------------------------------
                  1 |   0 |     0 | 1 | 1
                  2 | 200 |   200 | 1 | 1""");
        // Before this step, table ==== line partitions rows by
        // the ts component of the key, the only one used for GC):
        //   id | ts  | extra |
        //  ----+-----+-------+
        //    1 |   0 |     0 | GC'd
        //  ========================  waterline = (200 - 100, 200 - 100)
        //    2 | 200 |   200 |
        // Updating live key (2, 200) retracts the old row.
        ccs.step("""
                INSERT INTO t VALUES(2, 200, 200, 9);
                """, """
                 id | ts  | extra | v | weight
                --------------------------------
                  2 | 200 |   200 | 1 | -1
                  2 | 200 |   200 | 9 | 1""");
        // Before this step:
        //   id | ts  | extra |
        //  ----+-----+-------+
        //    1 |   0 |     0 | GC'd
        //  ========================  waterline = (200 - 100, 200 - 100)
        //    2 | 200 |   200 |
        // Updating GC'd key (1, 0) is rejected: its ts component is late,
        // even though the extra column is fresh.
        ccs.step("""
                INSERT INTO t VALUES(1, 0, 300, 5);
                """, """
                 id | ts  | extra | v | weight
                --------------------------------""");
        // Before this step, table contents:
        //   id | ts  | extra |
        //  ----+-----+-------+
        //    1 |   0 |     0 | GC'd
        //  ========================  waterline = (200 - 100, 200 - 100)
        //    2 | 200 |   200 |
        // Key (1, 300) is new; it does not collide with GC'd key (1, 0).
        ccs.step("""
                INSERT INTO t VALUES(1, 300, 300, 1);
                """, """
                 id | ts  | extra | v | weight
                --------------------------------
                  1 | 300 |   300 | 1 | 1""");
        // Before this step, table contents:
        //   id | ts  | extra |
        //  ----+-----+-------+
        //    1 |   0 |     0 | GC'd
        //  ========================  waterline = (300 - 100, 300 - 100)
        //    2 | 200 |   200 | on the waterline: deletable
        //    1 | 300 |   300 |
        ccs.step("""
                REMOVE FROM t VALUES(2, 200, 200, 9);
                """, """
                 id | ts  | extra | v | weight
                --------------------------------
                  2 | 200 |   200 | 9 | -1""");
    }

    @Test
    public void gcMaterializedNoRetain() {
        // Materialized tables are never garbage-collected, so the compiler
        // must not insert any retain operator for them, even when a primary
        // key column has LATENESS.
        String sql = """
                CREATE TABLE t (
                    ts BIGINT NOT NULL PRIMARY KEY LATENESS 100,
                    v INT
                ) WITH ('materialized' = 'true');
                CREATE VIEW vw AS SELECT * FROM t;""";
        CompilerCircuit cc = this.getCC(sql);
        this.checkInputGC(cc, 0);
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
        PrintStream save = System.err;
        System.setErr(NullPrintStream.INSTANCE);
        var ccs = this.getCCS(compiler);
        System.setErr(save);
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
        ccs.visit(visitor);
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
        CircuitVisitor visitor = new Inspector(ccs.compiler, 1, 1, 1);
        ccs.visit(visitor);
    }

    static class Inspector extends CircuitVisitor {
        final int expectedWindow;
        final int expectedChain;
        final int expectedWaterline;

        public Inspector(DBSPCompiler compiler, int window, int chain, int waterline) {
            super(compiler);
            this.expectedChain = chain;
            this.expectedWaterline = waterline;
            this.expectedWindow = window;
        }

        int window = 0;
        int waterline = 0;
        int chain = 0;

        @Override
        public void postorder(DBSPChainAggregateOperator operator) { this.chain++; }

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
            Assert.assertEquals(this.expectedWindow, this.window);
            Assert.assertEquals(this.expectedWaterline, this.waterline);
            Assert.assertEquals(this.expectedChain, this.chain);
        }
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
        CircuitVisitor visitor = new Inspector(ccs.compiler, 1, 1, 1);
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
    public void twoLowerNowBounds() {
        // https://github.com/feldera/feldera-qa/issues/395
        // Two NOW()-relative lower bounds on the same column become a single window.
        // The window's lower bound must be the tighter of the two.
        String sql = """
                CREATE TABLE t (
                  id INT NOT NULL PRIMARY KEY,
                  ts TIMESTAMP
                );
                CREATE VIEW v AS
                SELECT id FROM t
                WHERE ts >= now() - INTERVAL 10 MINUTES
                  AND ts >= now() - INTERVAL 100 MINUTES""";
        CompilerCircuitStream ccs = this.getCCS(sql);
        // At now() = 12:00 the bounds are ts >= 11:50 and ts >= 10:20; only id 1 satisfies both.
        ccs.step("""
                 INSERT INTO t VALUES (1, '2024-01-01 11:55:00');
                 INSERT INTO t VALUES (2, '2024-01-01 10:30:00');
                 INSERT INTO now VALUES ('2024-01-01 12:00:00');
                 """,
                """
                 id | weight
                -------------
                 1  | 1""");
    }

    @Test
    public void twoUpperNowBounds() {
        // https://github.com/feldera/feldera-qa/issues/395
        // The mirror image of twoLowerNowBounds: the window's upper bound must be
        // the tighter of the two NOW()-relative upper bounds.
        String sql = """
                CREATE TABLE t (
                  id INT NOT NULL PRIMARY KEY,
                  ts TIMESTAMP
                );
                CREATE VIEW v AS
                SELECT id FROM t
                WHERE ts <= now() - INTERVAL 10 MINUTES
                  AND ts <= now() - INTERVAL 100 MINUTES""";
        CompilerCircuitStream ccs = this.getCCS(sql);
        // At now() = 12:00 the bounds are ts <= 11:50 and ts <= 10:20; only id 3 satisfies both.
        ccs.step("""
                 INSERT INTO t VALUES (1, '2024-01-01 11:55:00');
                 INSERT INTO t VALUES (2, '2024-01-01 10:30:00');
                 INSERT INTO t VALUES (3, '2024-01-01 10:00:00');
                 INSERT INTO now VALUES ('2024-01-01 12:00:00');
                 """,
                """
                 id | weight
                -------------
                 3  | 1""");
    }

    @Test
    public void nowBoundsBroughtTogetherByReordering() {
        // https://github.com/feldera/feldera-qa/issues/395
        // The shape that failed in CI: the two NOW()-relative bounds on `ts` are written
        // apart, and the conjunct reordering that prepares windows for sharing brings them
        // together.  The window they merge into must still carry the tighter bound.
        String sql = """
                CREATE TABLE t (
                  id INT NOT NULL PRIMARY KEY,
                  x  INT,
                  ts TIMESTAMP
                );
                CREATE VIEW v AS
                SELECT id FROM t
                WHERE ts >= now() - INTERVAL 10 MINUTES
                  AND x > 5
                  AND ts >= now() - INTERVAL 100 MINUTES""";
        CompilerCircuitStream ccs = this.getCCS(sql);
        // At now() = 12:00 the bounds are ts >= 11:50 and ts >= 10:20; only id 1 satisfies both.
        ccs.step("""
                 INSERT INTO t VALUES (1, 6, '2024-01-01 11:55:00');
                 INSERT INTO t VALUES (2, 6, '2024-01-01 10:30:00');
                 INSERT INTO now VALUES ('2024-01-01 12:00:00');
                 """,
                """
                 id | weight
                -------------
                 1  | 1""");
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
        CircuitVisitor visitor = new Inspector(cc.compiler, 1, 1, 1);
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
        CircuitVisitor visitor = new Inspector(cc.compiler, 1, 1, 1);
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
        CircuitVisitor visitor = new Inspector(cc.compiler, 2, 1, 1);
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
        CircuitVisitor visitor = new Inspector(cc.compiler, 1, 1, 1);
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
    public void rollingInterval() {
        // Test rolling aggregates with INTERVAL windows
        String sql = """
                CREATE TABLE data (
                  t0 TIMESTAMP NOT NULL LATENESS INTERVAL '2' HOURS,
                  location INT NOT NULL
                );
                
                CREATE LOCAL VIEW IT AS SELECT (t0 - TIMESTAMP '2020-01-01 00:00:00') HOURS AS t, location FROM data;
                
                CREATE VIEW V AS
                SELECT
                *,
                COUNT(*) OVER(
                   PARTITION BY location
                   ORDER BY t
                   RANGE BETWEEN INTERVAL '2' DAYS PRECEDING AND INTERVAL '1' DAYS PRECEDING ) AS c
                FROM IT;""";
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
        // Validated on Postgres
        ccs.step("""
                INSERT INTO data
                  VALUES(TIMESTAMP '2020-01-01 10:00:00', 10),
                        (TIMESTAMP '2020-02-01 10:00:00', 10),
                        (TIMESTAMP '2019-12-30 20:00:00', 10);""", """
                 t         | location | c | weight
                -----------------------------------
                 10 hours  | 10       | 1 | 1
                 -28 hours | 10       | 0 | 1
                 754 hours | 10       | 0 | 1""");
    }

    @Test
    public void rollingDecimal() {
        // Test rolling aggregates with DECIMAL windows
        String sql = """
                CREATE TABLE data (
                  t DECIMAL(4, 2) NOT NULL LATENESS 2.0,
                  location INT NOT NULL
                );

                CREATE VIEW V AS
                SELECT
                *,
                COUNT(*) OVER(
                   PARTITION BY location
                   ORDER BY  t
                   RANGE BETWEEN 1.5 PRECEDING AND 0.5 PRECEDING) AS c
                FROM data;""";
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
        // Validated on Postgres
        ccs.visit(visitor);
        ccs.step("INSERT INTO data VALUES(1.0, 1), (2.0, 2), (1.0, 0), (2.0, 3), (3.5, 2), (4.0, 3);", """
                 t | location | c | weight
                ---------------------------
                 1 | 0        | 0 | 1
                 1 | 1        | 0 | 1
                 2 | 2        | 0 | 1
                 3.5 | 2      | 1 | 1
                 2 | 3        | 0 | 1
                 4 | 3        | 0 | 1""");
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
        CircuitVisitor visitor = new CircuitVisitor(ccs.compiler) {
            int fmi = 0;

            @Override
            public void postorder(DBSPFlatMapIndexOperator unused) {
                fmi++;
            }

            @Override
            public void endVisit() {
                Assert.assertEquals(2, fmi);
            }
        };
        ccs.visit(visitor);
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
        DBSPType out = new DBSPTypeTuple(DBSPTypeDouble.NULLABLE_INSTANCE, DBSPTypeDate.INSTANCE);
        DBSPType string = DBSPTypeString.varchar(false);
        DBSPType error = new DBSPTypeTuple(string, string,
                // new DBSPTypeVariant(false)
                string);
        String sql = """
                CREATE TABLE series (
                        distance DOUBLE,
                        pickup TIMESTAMP NOT NULL LATENESS INTERVAL '1:00' HOURS TO MINUTES
                );
                CREATE VIEW V AS
                SELECT AVG(distance), CAST(pickup AS DATE) FROM series GROUP BY CAST(pickup AS DATE);""";
        CompilerCircuitStream ccs = this.getCCS(sql);
        ccs.addChange(new InputOutputChange(
                new Change("series",
                        new DBSPZSetExpression(
                                new DBSPTupleExpression(
                                        new DBSPDoubleLiteral(10.0, true),
                                        new DBSPTimestampLiteral("2023-12-30 10:00:00", false)))),
                new Change(
                        new TableData("V", new DBSPZSetExpression(
                                new DBSPTupleExpression(
                                        new DBSPDoubleLiteral(10.0, true),
                                        new DBSPDateLiteral("2023-12-30", false)))),
                        new TableData(DBSPCompiler.ERROR_TABLE_NAME, DBSPZSetExpression.emptyWithElementType(error)))));
        // Insert tuple before waterline, should be dropped
        ccs.addChange(new InputOutputChange(
                new Change("series",
                        new DBSPZSetExpression(
                                new DBSPTupleExpression(
                                        new DBSPDoubleLiteral(10.0, true),
                                        new DBSPTimestampLiteral("2023-12-29 10:00:00", false)))),
                new Change(
                        new TableData("V", DBSPZSetExpression.emptyWithElementType(out)),
                        new TableData(DBSPCompiler.ERROR_TABLE_NAME, new DBSPZSetExpression(
                                new DBSPTupleExpression(
                                        new DBSPStringLiteral("series"),
                                        new DBSPStringLiteral("Late value"),
                                        new DBSPStringLiteral("(Some(10.0), 2023-12-29 10:00:00)")
                        ))))));
        // Insert tuple after waterline, should change average.
        // Waterline is advanced
        var set = new DBSPZSetExpression(
                new DBSPTupleExpression(
                        new DBSPDoubleLiteral(15.0, true),
                        new DBSPDateLiteral("2023-12-30", false)));
        set.append(new DBSPZSetExpression(
                new DBSPTupleExpression(
                        new DBSPDoubleLiteral(10.0, true),
                        new DBSPDateLiteral("2023-12-30", false))).negate());
        ccs.addChange(new InputOutputChange(
                new Change("series",
                        new DBSPZSetExpression(
                                new DBSPTupleExpression(
                                        new DBSPDoubleLiteral(20.0, true),
                                        new DBSPTimestampLiteral("2023-12-30 10:10:00", false)))),
                new Change(new TableData("V", set),
                        new TableData(DBSPCompiler.ERROR_TABLE_NAME, DBSPZSetExpression.emptyWithElementType(error)))));
        // Insert tuple before last waterline, should be dropped
        ccs.addChange(new InputOutputChange(
                new Change("series",
                        new DBSPZSetExpression(
                                new DBSPTupleExpression(
                                        new DBSPDoubleLiteral(10.0, true),
                                        new DBSPTimestampLiteral("2023-12-29 09:10:00", false)))),
                new Change(
                        new TableData("V", DBSPZSetExpression.emptyWithElementType(out)),
                        new TableData(DBSPCompiler.ERROR_TABLE_NAME, new DBSPZSetExpression(
                                new DBSPTupleExpression(
                                        new DBSPStringLiteral("series"),
                                        new DBSPStringLiteral("Late value"),
                                        new DBSPStringLiteral("(Some(10.0), 2023-12-29 09:10:00)")
                                ))))));
        // Insert tuple in the past, but before the last waterline
        var set1 = new DBSPZSetExpression(
                new DBSPTupleExpression(
                        new DBSPDoubleLiteral(13.333333333333334, true),
                        new DBSPDateLiteral("2023-12-30", false)));
        set1.append(new DBSPZSetExpression(
                new DBSPTupleExpression(
                    new DBSPDoubleLiteral(15.0, true),
                    new DBSPDateLiteral("2023-12-30", false))).negate());
        ccs.addChange(new InputOutputChange(
                new Change("series",
                        new DBSPZSetExpression(
                                new DBSPTupleExpression(
                                        new DBSPDoubleLiteral(10.0, true),
                                        new DBSPTimestampLiteral("2023-12-30 10:00:00", false)))),
                new Change(
                        new TableData("V", set1),
                        new TableData(DBSPCompiler.ERROR_TABLE_NAME, DBSPZSetExpression.emptyWithElementType(error)))));
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
                new Change("series",
                        new DBSPZSetExpression(
                                new DBSPTupleExpression(
                                        new DBSPDoubleLiteral(10.0, true),
                                        new DBSPTimestampLiteral("2023-12-30 10:00:00", false)))),
                new Change(
                        new TableData("E", DBSPZSetExpression.emptyWithElementType(e)),
                        new TableData(DBSPCompiler.ERROR_TABLE_NAME, DBSPZSetExpression.emptyWithElementType(error)))));
        // Insert tuple before waterline, should be dropped
        ccs.addChange(new InputOutputChange(
                new Change("series",
                        new DBSPZSetExpression(
                                new DBSPTupleExpression(
                                        new DBSPDoubleLiteral(10.0, true),
                                        new DBSPTimestampLiteral("2023-12-29 10:00:00", false)))),
                new Change(
                        new TableData("E", new DBSPZSetExpression(
                                new DBSPTupleExpression(new DBSPStringLiteral("Late value")))),
                        new TableData(DBSPCompiler.ERROR_TABLE_NAME, new DBSPZSetExpression(
                                new DBSPTupleExpression(
                                        new DBSPStringLiteral("series"),
                                        new DBSPStringLiteral("Late value"),
                                        new DBSPStringLiteral("(Some(10.0), 2023-12-29 10:00:00)")
                                ))))));
        // Insert tuple after waterline, should change average.
        // Waterline is advanced
        ccs.addChange(new InputOutputChange(
                new Change("series",
                        new DBSPZSetExpression(
                                new DBSPTupleExpression(
                                        new DBSPDoubleLiteral(20.0, true),
                                        new DBSPTimestampLiteral("2023-12-30 10:10:00", false)))),
                new Change(
                        new TableData("E", DBSPZSetExpression.emptyWithElementType(e)),
                        new TableData(DBSPCompiler.ERROR_TABLE_NAME, DBSPZSetExpression.emptyWithElementType(error)))));
        // Insert tuple before last waterline, should be dropped
        ccs.addChange(new InputOutputChange(
                new Change("series",
                        new DBSPZSetExpression(
                                new DBSPTupleExpression(
                                        new DBSPDoubleLiteral(10.0, true),
                                        new DBSPTimestampLiteral("2023-12-29 09:10:00", false)))),
                new Change(
                        new TableData("E", new DBSPZSetExpression(
                                new DBSPTupleExpression(new DBSPStringLiteral("Late value")))),
                        new TableData(DBSPCompiler.ERROR_TABLE_NAME, new DBSPZSetExpression(
                                new DBSPTupleExpression(
                                        new DBSPStringLiteral("series"),
                                        new DBSPStringLiteral("Late value"),
                                        new DBSPStringLiteral("(Some(10.0), 2023-12-29 09:10:00)")
                                ))))));
        // Insert tuple in the past, but before the last waterline
        ccs.addChange(new InputOutputChange(
                new Change("series",
                        new DBSPZSetExpression(
                                new DBSPTupleExpression(
                                        new DBSPDoubleLiteral(10.0, true),
                                        new DBSPTimestampLiteral("2023-12-30 10:00:00", false)))),
                new Change(
                        new TableData("E", DBSPZSetExpression.emptyWithElementType(e)),
                        new TableData(DBSPCompiler.ERROR_TABLE_NAME, DBSPZSetExpression.emptyWithElementType(error)))));
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

    @Test
    public void issue4904() {
        String sql = """
                create table T (
                  x TIMESTAMP,
                  y TIMESTAMP,
                  site_id varchar
                );
                
                create view V
                as select site_id from T
                where ( x >= NOW() + INTERVAL 30 DAYS
                    OR
                    y  >=  NOW() - INTERVAL 30 DAYS);""";
        var ccs = this.getCCS(sql).withStringTrim();
        ccs.step("""
                INSERT INTO NOW VALUES('2019-01-01 00:00:00');
                INSERT INTO T VALUES('2020-01-11 00:00:00', '2020-01-11 00:00:00', 'z');""", """
                 site_id | weight
                ------------------
                 z       | 1""");
        ccs.step("INSERT INTO NOW VALUES('2020-01-01 00:00:00')", """
                 site_id | weight
                ------------------""");
        ccs.step("INSERT INTO NOW VALUES('2020-03-01 00:00:00')", """
                 site_id | weight
                ------------------
                 z       | -1""");
    }

    @Test
    public void issue6655() {
        // Test AggregateNowFilterRule: result is NULL for SUM and 0 for COUNT.
        String sql = """
                CREATE TABLE T(k VARCHAR, tt TIMESTAMP);
                CREATE VIEW V AS
                SELECT k,
                       SUM(CASE WHEN tt >= NOW() - INTERVAL 1 DAY THEN 1 END) AS s,
                       COUNT(CASE WHEN tt >= NOW() - INTERVAL 1 DAY THEN 1 END) AS c,
                       COUNT(*) AS total
                FROM T GROUP BY k;""";
        var ccs = this.getCCS(sql).withStringTrim();
        CircuitVisitor visitor = new CircuitVisitor(ccs.compiler) {
            int window = 0;
            int aggregate = 0;

            @Override
            public void postorder(DBSPWindowOperator operator) {
                this.window++;
            }

            @Override
            public void postorder(DBSPAggregateLinearPostprocessOperator operator) {
                this.aggregate++;
            }

            @Override
            public void endVisit() {
                // SUM and COUNT share one condition, so one window suffices
                Assert.assertEquals(1, this.window);
                // ... and both live in one filtered aggregate; the anchor
                // aggregate computing COUNT(*) is the second one
                Assert.assertEquals(2, this.aggregate);
            }
        };
        ccs.visit(visitor);
        ccs.step("""
                INSERT INTO NOW VALUES('2020-01-01 00:00:00');
                INSERT INTO T VALUES('a', '2020-01-01 00:00:00');
                INSERT INTO T VALUES('b', '2019-12-30 00:00:00');""", """
                 k | s    | c | total | weight
                --------------------------------
                 a | 1    | 1 | 1     | 1
                 b |NULL  | 0 | 1     | 1""");
        // Two days later a's row leaves the window; b is unchanged
        ccs.step("INSERT INTO NOW VALUES('2020-01-03 00:00:00')", """
                 k | s    | c | total | weight
                --------------------------------
                 a | 1    | 1 | 1     | -1
                 a |NULL  | 0 | 1     | 1""");
    }

    @Test
    public void issue6655a() {
        // Aggregates with two different temporal conditions:
        // AggregateNowFilterRule peels one condition per application,
        // so each condition gets its own window operator.
        String sql = """
                CREATE TABLE T(k VARCHAR, tt TIMESTAMP);
                CREATE VIEW V AS
                SELECT k,
                       SUM(CASE WHEN tt >= NOW() - INTERVAL 1 DAY THEN 1 END) AS d,
                       SUM(CASE WHEN tt >= NOW() - INTERVAL 7 DAYS THEN 1 END) AS w,
                       COUNT(*) AS total
                FROM T GROUP BY k;""";
        var ccs = this.getCCS(sql);
        CircuitVisitor visitor = new CircuitVisitor(ccs.compiler) {
            int window = 0;

            @Override
            public void postorder(DBSPWindowOperator operator) {
                this.window++;
            }

            @Override
            public void endVisit() {
                Assert.assertEquals(2, this.window);
            }
        };
        ccs.visit(visitor);
        ccs.withStringTrim();
        ccs.step("""
                INSERT INTO NOW VALUES('2020-01-10 00:00:00');
                INSERT INTO T VALUES('a', '2020-01-10 00:00:00');
                INSERT INTO T VALUES('a', '2020-01-05 00:00:00');""", """
                 k | d    | w    | total | weight
                ----------------------------------
                 a | 1    | 2    | 2     | 1""");
        // Two days later the newest row leaves the 1-day window;
        // both rows are still inside the 7-day window
        ccs.step("INSERT INTO NOW VALUES('2020-01-12 00:00:00')", """
                 k | d    | w    | total | weight
                ----------------------------------
                 a | 1    | 2    | 2     | -1
                 a |NULL  | 2    | 2     | 1""");
        // Ten days after the first step both rows have left both windows
        ccs.step("INSERT INTO NOW VALUES('2020-01-20 00:00:00')", """
                 k | d    | w    | total | weight
                ----------------------------------
                 a |NULL  | 2    | 2     | -1
                 a |NULL  |NULL  | 2     | 1""");
    }

    @Test
    public void issue6655b() {
        // AggregateNowFilterRule without GROUP BY: no join is needed,
        // the aggregate runs directly over the temporal filter.
        String sql = """
                CREATE TABLE T(tt TIMESTAMP);
                CREATE VIEW V AS
                SELECT SUM(CASE WHEN tt >= NOW() - INTERVAL 1 DAY THEN 1 END) AS s
                FROM T;""";
        var ccs = this.getCCS(sql);
        ccs.step("""
                INSERT INTO NOW VALUES('2020-01-01 00:00:00');
                INSERT INTO T VALUES('2020-01-01 00:00:00');""", """
                 s | weight
                -----------
                 1 | 1""");
        ccs.step("INSERT INTO NOW VALUES('2020-01-03 00:00:00')", """
                 s | weight
                -----------
                 1 | -1
                NULL | 1""");
        CircuitVisitor visitor = new CircuitVisitor(ccs.compiler) {
            int window = 0;

            @Override
            public void postorder(DBSPJoinBaseOperator operator) {
                Assert.fail();
            }

            @Override
            public void postorder(DBSPWindowOperator operator) {
                this.window++;
            }

            @Override
            public void endVisit() {
                Assert.assertEquals(1, this.window);
            }
        };
        ccs.visit(visitor);
    }

    @Test
    public void issue6655c() {
        String sql = """
                CREATE TABLE T(
                   id INT,
                   tt TIMESTAMP
                );

                CREATE VIEW V AS
                SELECT
                    id,
                    SUM(CASE WHEN tt >= (NOW() - INTERVAL '24' HOUR) THEN 1 END) AS tc_24h,
                    SUM(CASE WHEN tt >= (NOW() - INTERVAL '1' HOUR) THEN 1 END) AS tc_1h,
                    MAX(tt) AS max_tt
                FROM T
                WHERE tt BETWEEN (NOW() - INTERVAL '25' HOUR) AND NOW()
                  AND id IS NOT NULL
                GROUP BY id;""";
        var ccs = this.getCCS(sql);
        CircuitVisitor visitor = new CircuitVisitor(ccs.compiler) {
            int window = 0;

            @Override
            public void postorder(DBSPWindowOperator operator) {
                this.window++;
            }

            @Override
            public void endVisit() {
                // one window for the WHERE, one per aggregate condition.
                Assert.assertEquals(3, this.window);
            }
        };
        ccs.visit(visitor);
        // id 1: one row in all windows, one row only in the 24h window;
        // id 2: row between 25h and 24h ago, in WHERE but in no window;
        // the NULL id is removed by the WHERE clause
        ccs.step("""
                INSERT INTO NOW VALUES('2020-01-01 12:00:00');
                INSERT INTO T VALUES(1, '2020-01-01 11:30:00');
                INSERT INTO T VALUES(1, '2020-01-01 00:00:00');
                INSERT INTO T VALUES(2, '2019-12-31 11:30:00');
                INSERT INTO T VALUES(NULL, '2020-01-01 11:45:00');""", """
                 id | tc_24h | tc_1h | max_tt              | weight
                -----------------------------------------------------
                 1  | 2      | 1     | 2020-01-01 11:30:00 | 1
                 2  |NULL    |NULL   | 2019-12-31 11:30:00 | 1""");
        // One hour later id 1 has no rows in the 1h window,
        // and id 2's row leaves the WHERE band
        ccs.step("INSERT INTO NOW VALUES('2020-01-01 13:00:00')", """
                 id | tc_24h | tc_1h | max_tt              | weight
                -----------------------------------------------------
                 1  | 2      | 1     | 2020-01-01 11:30:00 | -1
                 1  | 2      |NULL   | 2020-01-01 11:30:00 | 1
                 2  |NULL    |NULL   | 2019-12-31 11:30:00 | -1""");
        // A day later id 1's rows leave the WHERE band as well
        ccs.step("INSERT INTO NOW VALUES('2020-01-02 13:00:00')", """
                 id | tc_24h | tc_1h | max_tt              | weight
                -----------------------------------------------------
                 1  | 2      |NULL   | 2020-01-01 11:30:00 | -1""");
    }

    @Test
    public void issue6655d() {
        // ARRAY_AGG must not be moved by AggregateNowFilterRule (results would be wrong if it was).
        String sql = """
                CREATE TABLE T(k VARCHAR, tt TIMESTAMP, x INT);
                CREATE VIEW V AS
                SELECT k, ARRAY_AGG(x) FILTER (WHERE tt >= NOW() - INTERVAL 1 DAY) AS agg
                FROM T GROUP BY k;""";
        var ccs = this.getCCS(sql).withStringTrim();
        ccs.step("""
                INSERT INTO NOW VALUES('2020-01-01 00:00:00');
                INSERT INTO T VALUES('a', '2020-01-01 00:00:00', 10);""", """
                 k | agg  | weight
                -------------------
                 a |{ 10 } | 1""");
        ccs.step("INSERT INTO NOW VALUES('2020-01-03 00:00:00')", """
                 k | agg  | weight
                -------------------
                 a |{ 10 } | -1
                 a |{}     | 1""");
    }

    static final String issue4909data = """
            INSERT INTO T VALUES ('alpha', TRUE, '2025-10-20 14:23:00', '2025-10-19 09:15:00', '2025-10-18 17:45:00', 'lp1', '2025-10-20 20:00:00');
                INSERT INTO T VALUES ('bravo', FALSE, '2025-10-15 11:00:00', '2025-10-14 08:30:00', '2025-10-13 19:10:00', 'lp2', '2025-10-15 22:00:00');
                INSERT INTO T VALUES ('charlie', TRUE, '2025-10-10 16:45:00', '2025-10-09 10:00:00', '2025-10-08 18:30:00', 'lp3', '2025-10-10 21:00:00');
                INSERT INTO T VALUES ('delta', FALSE, '2025-10-05 13:20:00', '2025-10-04 07:45:00', '2025-10-03 20:15:00', 'lp4', '2025-10-05 23:00:00');
                INSERT INTO T VALUES ('echo', TRUE, '2025-09-30 12:00:00', '2025-09-29 09:00:00', '2025-09-28 16:00:00', 'lp5', '2025-09-30 19:00:00');
                INSERT INTO T VALUES ('foxtrot', FALSE, '2025-09-25 15:30:00', '2025-09-24 10:30:00', '2025-09-23 18:00:00', 'lp6', '2025-09-25 21:30:00');
                INSERT INTO T VALUES ('golf', TRUE, '2025-09-20 14:00:00', '2025-09-19 08:00:00', '2025-09-18 17:00:00', 'lp7', '2025-09-20 22:00:00');
                INSERT INTO T VALUES ('hotel', FALSE, '2025-09-15 11:45:00', '2025-09-14 07:30:00', '2025-09-13 19:30:00', 'lp8', '2025-09-15 20:45:00');
                INSERT INTO T VALUES ('india', TRUE, '2025-09-10 13:15:00', '2025-09-09 09:45:00', '2025-09-08 18:15:00', 'lp9', '2025-09-10 23:15:00');
                INSERT INTO T VALUES ('juliet', FALSE, '2025-09-05 12:30:00', '2025-09-04 08:15:00', '2025-09-03 20:00:00', 'lp10', '2025-09-05 22:30:00');
                INSERT INTO T VALUES ('kilo', TRUE, '2025-08-31 14:45:00', '2025-08-30 10:00:00', '2025-08-29 17:30:00', 'lp11', '2025-08-31 21:45:00');
                INSERT INTO T VALUES ('lima', FALSE, '2025-08-26 13:00:00', '2025-08-25 09:30:00', '2025-08-24 18:45:00', 'lp12', '2025-08-26 20:00:00');
                INSERT INTO T VALUES ('mike', TRUE, '2025-08-21 15:15:00', '2025-08-20 07:45:00', '2025-08-19 19:00:00', 'lp13', '2025-08-21 22:15:00');
                INSERT INTO T VALUES ('november', FALSE, '2025-08-16 12:20:00', '2025-08-15 08:00:00', '2025-08-14 16:30:00', 'lp14', '2025-08-16 23:00:00');
                INSERT INTO T VALUES ('oscar', TRUE, '2025-08-11 14:10:00', '2025-08-10 09:15:00', '2025-08-09 18:00:00', 'lp15', '2025-08-11 20:10:00');
                INSERT INTO T VALUES ('papa', FALSE, '2025-08-06 13:40:00', '2025-08-05 10:30:00', '2025-08-04 17:45:00', 'lp16', '2025-08-06 21:40:00');
                INSERT INTO T VALUES ('quebec', TRUE, '2025-08-01 11:50:00', '2025-07-31 08:45:00', '2025-07-30 19:15:00', 'lp17', '2025-08-01 22:50:00');
                INSERT INTO T VALUES ('romeo', FALSE, '2025-07-27 12:10:00', '2025-07-26 09:00:00', '2025-07-25 18:30:00', 'lp18', '2025-07-27 20:10:00');
                INSERT INTO T VALUES ('sierra', TRUE, '2025-07-22 14:35:00', '2025-07-21 07:30:00', '2025-07-20 17:00:00', 'lp19', '2025-07-22 23:35:00');
                INSERT INTO T VALUES ('tango', FALSE, '2025-07-17 13:25:00', '2025-07-16 10:15:00', '2025-07-15 16:45:00', 'lp20', '2025-07-17 21:25:00');
                INSERT INTO NOW VALUES('2025-10-21 00:00:00');""";

    @Test
    public void issue4909() {
        var ccs = this.getCCS("""
                CREATE TABLE T(
                   s VARCHAR,
                   d BOOL,
                   last TIMESTAMP,
                   clk TIMESTAMP,
                   op TIMESTAMP,
                   lp VARCHAR,
                   lsd TIMESTAMP
                );
                
                create view V
                as SELECT
                    s
                FROM
                    T
                WHERE
                    s LIKE '%i%'
                    AND d = true
                    AND (
                        last >= NOW() - INTERVAL 30 DAYS
                        OR clk >= NOW() - INTERVAL 60 DAYS
                        OR op >= NOW() - INTERVAL 90 DAYS
                    )
                    AND lp IS NOT NULL
                    AND (
                        lsd >= NOW() - INTERVAL 30 DAYS
                        OR op IS NOT NULL
                    );
                """).withStringTrim();
        ccs.visit(new CircuitVisitor(ccs.compiler) {
            @Override
            public void postorder(DBSPJoinBaseOperator join) {
                Assert.fail("Should contain no joins");
            }
        });
        // Validated using Postgres on the right date
        ccs.step(issue4909data, """
                 s        | weight
                -------------------
                 charlie  | 1
                 india    | 1
                 kilo     | 1
                 mike     | 1""");
    }

    @Test
    public void issue4909a() {
        var ccs = this.getCCS("""
                CREATE TABLE T(
                   s VARCHAR,
                   d BOOL,
                   last TIMESTAMP,
                   clk TIMESTAMP,
                   op TIMESTAMP,
                   lp VARCHAR,
                   lsd TIMESTAMP
                );
                
                create view V
                as SELECT
                    s
                FROM
                    T
                WHERE
                    s LIKE '%i%'
                    AND d = true
                    OR (
                        last >= NOW() - INTERVAL 30 DAYS
                        AND clk >= NOW() - INTERVAL 60 DAYS
                        AND op >= NOW() - INTERVAL 90 DAYS
                    )
                    AND lp IS NOT NULL
                    AND (
                        lsd >= NOW() - INTERVAL 30 DAYS
                        OR op IS NOT NULL
                    );
                """).withStringTrim();
        ccs.visit(new CircuitVisitor(ccs.compiler) {
            @Override
            public void postorder(DBSPJoinBaseOperator join) {
                Assert.fail("Should contain no joins");
            }
        });
        // Validated using Postgres on the right date
        ccs.step(issue4909data, """
                 s        | weight
                -------------------
                 alpha    | 1
                 bravo    | 1
                 charlie  | 1
                 delta    | 1
                 echo     | 1
                 foxtrot  | 1
                 india    | 1
                 kilo     | 1
                 mike     | 1
                 sierra   | 1""");
    }

    @Test
    public void issue4909b() {
        var ccs = this.getCCS("""
                CREATE TABLE T(
                   s VARCHAR,
                   d BOOL,
                   last TIMESTAMP,
                   clk TIMESTAMP,
                   op TIMESTAMP,
                   lp VARCHAR,
                   lsd TIMESTAMP
                );
                
                create view V
                as SELECT
                    s
                FROM
                    T
                WHERE
                    s LIKE '%i%'
                    AND d = true
                    OR (
                        (last >= NOW() - INTERVAL 30 DAYS
                         AND clk >= NOW() - INTERVAL 60 DAYS)
                        OR op >= NOW() - INTERVAL 90 DAYS
                    )
                    AND lp IS NOT NULL
                    OR (
                        lsd >= NOW() - INTERVAL 30 DAYS
                        AND op IS NOT NULL
                    );
                """).withStringTrim();
        ccs.visit(new CircuitVisitor(ccs.compiler) {
            @Override
            public void postorder(DBSPJoinBaseOperator join) {
                Assert.fail("Should contain no joins");
            }
        });
        // Validated using Postgres on the right date
        ccs.step(issue4909data, """
                 s        | weight
                -------------------
                 alpha    | 1
                 bravo    | 1
                 charlie  | 1
                 delta    | 1
                 echo     | 1
                 foxtrot  | 1
                 golf     | 1
                 hotel    | 1
                 india    | 1
                 juliet   | 1
                 kilo     | 1
                 lima     | 1
                 mike     | 1
                 november | 1
                 oscar    | 1
                 papa     | 1
                 quebec   | 1
                 romeo    | 1
                 sierra   | 1""");
    }

    @Test
    public void issue4904_alternate() {
        String sql = """
                create table T (
                  properties variant,
                  site_id varchar
                );
                
                create view V
                as (select site_id from T
                    where CAST(properties['x'] AS TIMESTAMP) >= NOW() + INTERVAL 30 DAYS)
                UNION ALL
                (select site_id from T
                 where CAST(properties['y'] AS TIMESTAMP)  >=  NOW() - INTERVAL 30 DAYS);""";
        this.getCCS(sql);
    }

    @Test
    public void leftLateness() {
        // The fact that emit_final is compiled proves that the output of the left_join has a waterline
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
            FROM t1 LEFT JOIN t2 on t1.ts = t2.ts;""";
        this.getCCS(sql);
    }

    @Test
    public void changeLog() {
        // TLOG is a log of insertions and deletions applied to a table with
        // primary key t_key; view T reconstructs the current table contents
        // from the log entries of the last 25 hours.
        String sql = """
                CREATE TABLE TLOG (
                    t_key INT NOT NULL,
                    payload VARCHAR,
                    op VARCHAR NOT NULL,
                    ts TIMESTAMP NOT NULL
                ) WITH ('append_only' = 'true');

                CREATE LOCAL VIEW RECENT AS
                SELECT * FROM TLOG
                WHERE ts >= NOW() - INTERVAL 25 HOURS AND ts <= NOW();

                CREATE VIEW T AS
                SELECT t_key, payload
                FROM (
                    SELECT t_key, payload, op,
                           ROW_NUMBER() OVER (PARTITION BY t_key ORDER BY ts DESC) AS rn
                    FROM RECENT
                ) latest
                WHERE rn = 1 AND op = 'insert';""";
        var ccs = this.getCCS(sql).withStringTrim();
        FindUnboundedState gc = new FindUnboundedState(ccs.compiler);
        ccs.visit(gc);
        // The temporal filter's window operator bounds its own state; its
        // bounded output propagates to the TOP-1 operator, and the NOW-derived
        // window-bound computation is bounded because the NOW table holds one
        // row.  The circuit has no unbounded state.
        Assert.assertTrue(gc.unbounded.toString(), gc.unbounded.isEmpty());
        ccs.step("""
                INSERT INTO NOW VALUES('2020-01-01 01:00:00');
                INSERT INTO TLOG VALUES(1, 'aaa', 'insert', '2020-01-01 00:00:00');
                INSERT INTO TLOG VALUES(2, 'bbb', 'insert', '2020-01-01 00:10:00');""", """
                 t_key | payload | weight
                --------------------------
                 1     | aaa     | 1
                 2     | bbb     | 1""");
        // An update is a newer insertion for an existing key
        ccs.step("""
                INSERT INTO NOW VALUES('2020-01-01 01:10:00');
                INSERT INTO TLOG VALUES(1, 'ccc', 'insert', '2020-01-01 00:20:00');""", """
                 t_key | payload | weight
                --------------------------
                 1     | aaa     | -1
                 1     | ccc     | 1""");
        // The latest entry for key 2 is a deletion, so the key disappears
        ccs.step("""
                INSERT INTO NOW VALUES('2020-01-01 01:20:00');
                INSERT INTO TLOG VALUES(2, NULL, 'delete', '2020-01-01 00:30:00');""", """
                 t_key | payload | weight
                --------------------------
                 2     | bbb     | -1""");
        // Out-of-order entries older than the latest entry for their key
        // leave the view unchanged
        ccs.step("""
                INSERT INTO NOW VALUES('2020-01-01 01:30:00');
                INSERT INTO TLOG VALUES(1, 'xxx', 'insert', '2020-01-01 00:15:00');
                INSERT INTO TLOG VALUES(1, NULL, 'delete', '2020-01-01 00:18:00');""", """
                 t_key | payload | weight
                --------------------------""");
        // A key deleted earlier reappears with a newer insertion
        ccs.step("""
                INSERT INTO NOW VALUES('2020-01-01 01:40:00');
                INSERT INTO TLOG VALUES(2, 'ddd', 'insert', '2020-01-01 00:40:00');""", """
                 t_key | payload | weight
                --------------------------
                 2     | ddd     | 1""");
        // 25 hours later all key-1 entries have left the window, so key 1
        // disappears; key 2 keeps its 00:40 insertion
        ccs.step("INSERT INTO NOW VALUES('2020-01-02 01:30:00');", """
                 t_key | payload | weight
                --------------------------
                 1     | ccc     | -1""");
        // ... and once the 00:40 insertion ages out too, T becomes empty
        ccs.step("INSERT INTO NOW VALUES('2020-01-02 01:45:00');", """
                 t_key | payload | weight
                --------------------------
                 2     | ddd     | -1""");
    }

    @Test
    public void softDeleteLog() {
        // The example from docs/sql/streaming.md: reconstruct the current
        // contents of a soft-deleted stream with bounded state and aggregate
        // over it.
        String sql = """
                CREATE TABLE input_log (
                    id BIGINT,
                    s VARCHAR,
                    ts TIMESTAMP,
                    is_delete BOOLEAN DEFAULT CAST(CONNECTOR_METADATA()['is_delete'] AS BOOLEAN)
                ) WITH (
                    'append_only' = 'true',
                    'connectors' = '[{
                        "name": "changes",
                        "soft_delete": true,
                        "transport": {
                            "name": "kafka_input",
                            "config": {
                                "topic": "changes",
                                "start_from": "earliest",
                                "bootstrap.servers": "example.com:9092",
                                "include_timestamp": true
                            }
                        },
                        "format": {
                            "name": "json",
                            "config": { "update_format": "insert_delete" }
                        }
                    }]'
                );

                CREATE LOCAL VIEW recent AS
                SELECT * FROM input_log
                WHERE ts >= NOW() - INTERVAL 7 DAYS AND ts <= NOW();

                CREATE LOCAL VIEW input AS
                SELECT id, s, ts
                FROM (
                    SELECT *, ROW_NUMBER() OVER (
                        PARTITION BY id ORDER BY ts DESC, is_delete NULLS FIRST
                    ) AS rn
                    FROM recent
                )
                WHERE rn = 1 AND is_delete IS NOT TRUE;

                CREATE VIEW input_stats AS
                SELECT
                    id, s, ts,
                    COUNT(*) OVER minute_window AS rows_last_minute,
                    COUNT(*) OVER hour_window AS rows_last_hour,
                    COUNT(*) OVER day_window AS rows_last_day
                FROM input
                WINDOW
                    minute_window AS (ORDER BY ts RANGE BETWEEN INTERVAL 1 MINUTE PRECEDING AND CURRENT ROW),
                    hour_window AS (ORDER BY ts RANGE BETWEEN INTERVAL 1 HOUR PRECEDING AND CURRENT ROW),
                    day_window AS (ORDER BY ts RANGE BETWEEN INTERVAL 1 DAY PRECEDING AND CURRENT ROW);""";
        var ccs = this.getCCS(sql);
        FindUnboundedState gc = new FindUnboundedState(ccs.compiler);
        ccs.visit(gc);
        Assert.assertTrue(gc.unbounded.toString(), gc.unbounded.isEmpty());
    }
}
