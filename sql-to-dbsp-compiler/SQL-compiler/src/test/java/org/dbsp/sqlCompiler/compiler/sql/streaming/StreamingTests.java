package org.dbsp.sqlCompiler.compiler.sql.streaming;

import org.dbsp.sqlCompiler.CompilerMain;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIntegrateTraceRetainKeysOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIntegrateTraceRetainValuesOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPPartitionedRollingAggregateWithWaterlineOperator;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.circuit.operator.DBSPWaterlineOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPWindowOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.StderrErrorReporter;
import org.dbsp.sqlCompiler.compiler.errors.CompilerMessages;
import org.dbsp.sqlCompiler.compiler.sql.tools.BaseSQLTests;
import org.dbsp.sqlCompiler.compiler.sql.StreamingTestBase;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.outer.OptimizeMaps;
import org.dbsp.util.Linq;
import org.dbsp.util.Utilities;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

/** Tests that exercise streaming features. */
public class StreamingTests extends StreamingTestBase {
    @Override
    public CompilerOptions testOptions(boolean incremental, boolean optimize) {
        CompilerOptions options = super.testOptions(incremental, optimize);
        options.ioOptions.nowStream = true;
        return options;
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
                ) WITH ('connectors' = '{bid}');
                CREATE VIEW Q16 AS
                SELECT
                    count(distinct auction) filter (where price >= 10000 and price < 1000000) AS rank2_auctions,
                    count(distinct auction) filter (where price >= 1000000) AS rank3_auctions
                FROM bid
                GROUP BY channel, CAST(date_time AS DATE);""";
        this.compileRustTestCase(sql);
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
    public void issue2228() throws SQLException, IOException, InterruptedException {
        OptimizeMaps.testIssue2228 = true;
        String sql = """
                CREATE TABLE transaction (
                    id bigint NOT NULL,
                    unix_time BIGINT NOT NULL
                );
                
                CREATE TABLE feedback (
                    id bigint,
                    unix_time bigint LATENESS 3600 * 24
                );
                
                CREATE VIEW TRANSACTIONS AS
                SELECT t.*
                FROM transaction as t JOIN feedback as f
                ON t.id = f.id
                WHERE t.unix_time >= f.unix_time - 3600 * 24 * 7 ;
                """;

        String main = this.createMain("""
                let _ = circuit.step().expect("could not run circuit");
                let _ = circuit.step().expect("could not run circuit");
                """);
        this.measure(sql, main);
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
        CircuitVisitor visitor = new CircuitVisitor(new StderrErrorReporter()) {
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
           CompilerCircuitStream ccs = this.getCCS(sql);
        visitor.apply(ccs.circuit);
        this.addRustTestCase(ccs);
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
        this.addRustTestCase(ccs);
    }

    @Test
    public void profileRetainValues() throws IOException, InterruptedException, SQLException {
        // Based on Q9.  Check whether integrate_trace_retain_values works as expected.
        String sql = """
                CREATE TABLE auction (
                   date_time TIMESTAMP NOT NULL LATENESS INTERVAL 1 MINUTE,
                   expires   TIMESTAMP NOT NULL,
                   id        INT
                );

                CREATE TABLE bid (
                   date_time TIMESTAMP NOT NULL LATENESS INTERVAL 1 MINUTE,
                   price INT,
                   auction INT
                );

                CREATE VIEW Q9 AS
                SELECT A.*, B.price, B.date_time AS bid_dateTime
                FROM auction A, bid B
                WHERE A.id = B.auction AND B.date_time BETWEEN A.date_time AND A.expires;
                """;
        // Rust program which profiles the circuit.
        String main = this.createMain("""
                    // Initial data value for timestamp
                    let mut timestamp = cast_to_Timestamp_s("2024-01-10 10:10:10".to_string());
                    for i in 0..1000000 {
                        let expire = timestamp.add(1000000);
                        timestamp = timestamp.add(20000);
                        let auction = zset!(Tup3::new(timestamp, expire, Some(i)) => 1);
                        append_to_collection_handle(&auction, &streams.0);
                        let bid = zset!(Tup3::new(timestamp.add(100), Some(i), Some(i)) => 1);
                        append_to_collection_handle(&bid, &streams.1);
                        if i % 100 == 0 {
                            let _ = circuit.step().expect("could not run circuit");
                            let _ = &read_output_handle(&streams.2);
                            /*
                            let end = SystemTime::now();
                            let profile = circuit.retrieve_profile().expect("could not get profile");
                            let duration = end.duration_since(start).expect("could not get time");
                            println!("{:?},{:?}", duration.as_millis(), profile.total_used_bytes().unwrap().bytes);
                            */
                        }
                    }""");
        this.profile(sql, main);
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
        this.addRustTestCase(ccs);
        CircuitVisitor visitor = new CircuitVisitor(new StderrErrorReporter()) {
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
        visitor.apply(ccs.circuit);
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
        this.addRustTestCase(ccs);
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
        this.addRustTestCase(ccs);
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
        this.addRustTestCase(ccs);
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
        CircuitVisitor visitor = new CircuitVisitor(new StderrErrorReporter()) {
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
        visitor.apply(ccs.circuit);
        this.addRustTestCase(ccs);
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
        CircuitVisitor visitor = new CircuitVisitor(new StderrErrorReporter()) {
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
        visitor.apply(ccs.circuit);
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
        this.addRustTestCase(ccs);
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
           CompilerCircuitStream ccs = this.getCCS(sql);
        CircuitVisitor visitor = new CircuitVisitor(new StderrErrorReporter()) {
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
        visitor.apply(ccs.circuit);
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
           CompilerCircuitStream ccs = this.getCCS(sql);
        CircuitVisitor visitor = new CircuitVisitor(new StderrErrorReporter()) {
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
        visitor.apply(ccs.circuit);
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
        this.addRustTestCase(ccs);
        CircuitVisitor visitor = new CircuitVisitor(new StderrErrorReporter()) {
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
        visitor.apply(ccs.circuit);
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
                CREATE TABLE event(
                    start   TIMESTAMP NOT NULL LATENESS INTERVAL 1 HOURS
                );

                LATENESS slotted_events.start 96;

                CREATE VIEW slotted_events AS
                SELECT start
                FROM event;""";
        this.statementsFailingInCompilation(sql, "Cannot apply operation '-'");
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
           CompilerCircuitStream ccs = this.getCCS(sql);
        this.addRustTestCase(ccs);
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
        this.addRustTestCase(ccs);
        CircuitVisitor visitor = new CircuitVisitor(new StderrErrorReporter()) {
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
        visitor.apply(ccs.circuit);
    }

    @Test
    public void taxiTest() {
        String sql = """
                CREATE TABLE green_tripdata
                (
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
        this.addRustTestCase(ccs);
        CircuitVisitor visitor = new CircuitVisitor(new StderrErrorReporter()) {
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
        visitor.apply(ccs.circuit);
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
           CompilerCircuitStream ccs = this.getCCS(sql);
        this.addRustTestCase(ccs);
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

        this.addRustTestCase(ccs);
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
        this.addRustTestCase(ccs);
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
        this.addRustTestCase(ccs);
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
        DBSPCompiler compiler = testCompiler();
        compiler.compileStatements(ddl);
        Assert.assertFalse(compiler.hasErrors());
        CompilerCircuitStream ccs = new CompilerCircuitStream(compiler);
        this.addRustTestCase(ccs);
    }

    @Test
    public void watermarkTest() {
        String ddl = """
                CREATE TABLE series (
                        distance DOUBLE,
                        pickup TIMESTAMP NOT NULL WATERMARK INTERVAL '1:00' HOURS TO MINUTES
                );
                CREATE VIEW V AS
                SELECT AVG(distance), CAST(pickup AS DATE) FROM series GROUP BY CAST(pickup AS DATE)""";
        DBSPCompiler compiler = testCompiler();
        compiler.compileStatements(ddl);
        CompilerCircuitStream ccs = new CompilerCircuitStream(compiler);
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
        this.addRustTestCase(ccs);
    }

    @Test
    public void latenessTest() {
        String ddl = """
                CREATE TABLE series (
                        distance DOUBLE,
                        pickup TIMESTAMP NOT NULL LATENESS INTERVAL '1:00' HOURS TO MINUTES
                );
                CREATE VIEW V AS
                SELECT AVG(distance), CAST(pickup AS DATE) FROM series GROUP BY CAST(pickup AS DATE);""";
        DBSPCompiler compiler = testCompiler();
        compiler.compileStatements(ddl);
        CompilerCircuitStream ccs = new CompilerCircuitStream(compiler);
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
        this.addRustTestCase(ccs);
    }

    Long[] measure(String program, String main) throws IOException, InterruptedException, SQLException {
        File script = createInputScript(program);
        CompilerMessages messages = CompilerMain.execute(
                "-o", BaseSQLTests.testFilePath, "--handles", "-i",
                script.getPath());
        System.out.println(messages);
        Assert.assertEquals(0, messages.errorCount());

        String mainFilePath = rustDirectory + "/main.rs";
        File file = new File(mainFilePath);
        try (PrintWriter mainFile = new PrintWriter(file, StandardCharsets.UTF_8)) {
            mainFile.print(main);
        }
        //file.deleteOnExit();
        Utilities.compileAndTestRust(rustDirectory, true, "--release");
        File mainFile = new File(mainFilePath);
        boolean deleted;
        deleted = mainFile.delete();
        Assert.assertTrue(deleted);

        // After executing this Rust program the output is in file "mem.txt"
        // It contains three numbers: time taken (ms), memory used (bytes), and late records.
        String outFile = "mem.txt";
        Path outFilePath = Paths.get(rustDirectory, "..", outFile);
        List<String> strings = Files.readAllLines(outFilePath);
        // System.out.println(strings);
        Assert.assertEquals(1, strings.size());
        String[] split = strings.get(0).split(",");
        Assert.assertEquals(3, split.length);
        deleted = outFilePath.toFile().delete();
        Assert.assertTrue(deleted);
        return Linq.map(split, Long::parseLong, Long.class);
    }

    String createMain(String rustDataGenerator) {
        String preamble = """
                #[allow(unused_imports)]
                use dbsp::{
                    algebra::F64,
                    circuit::{
                        CircuitConfig,
                        metrics::TOTAL_LATE_RECORDS,
                    },
                    utils::{Tup2, Tup3},
                    zset,
                };

                use sqllib::{
                    append_to_collection_handle,
                    read_output_handle,
                    casts::cast_to_Timestamp_s,
                };

                use std::{
                    collections::HashMap,
                    io::Write,
                    ops::Add,
                    fs::File,
                    time::SystemTime,
                };

                use metrics::{Key, SharedString, Unit};
                use metrics_util::{
                    debugging::{DebugValue, DebuggingRecorder},
                    CompositeKey, MetricKind,
                };

                use temp::circuit;
                use dbsp::circuit::Layout;
                use uuid::Uuid;

                type MetricsSnapshot = HashMap<CompositeKey, (Option<Unit>, Option<SharedString>, DebugValue)>;

                fn parse_counter(metrics: &MetricsSnapshot, name: &'static str) -> u64 {
                    if let Some((_, _, DebugValue::Counter(value))) = metrics.get(&CompositeKey::new(
                        MetricKind::Counter,
                        Key::from_static_name(name),
                    )) {
                        *value
                    } else {
                        0
                    }
                }

                #[test]
                // Run the circuit generated by 'circuit' for a while then measure the
                // memory consumption.  Write the time taken and the memory used into
                // a file called "mem.txt".
                pub fn test() {
                    let recorder = DebuggingRecorder::new();
                    let snapshotter = recorder.snapshotter();
                    recorder.install().unwrap();

                    let (mut circuit, streams) = circuit(
                         CircuitConfig {
                             layout: Layout::new_solo(2),
                             storage: None,
                             min_storage_bytes: usize::MAX,
                             init_checkpoint: Uuid::nil(),
                         }).expect("could not build circuit");
                    // uncomment if you want a CPU profile
                    // let _ = circuit.enable_cpu_profiler();
                    let start = SystemTime::now();
                """;
        String postamble = """
                    let metrics = snapshotter.snapshot();
                    let decoded_metrics: MetricsSnapshot = metrics.into_hashmap();
                    let late = parse_counter(&decoded_metrics, TOTAL_LATE_RECORDS);

                    let profile = circuit.retrieve_profile().expect("could not get profile");
                    let end = SystemTime::now();
                    let duration = end.duration_since(start).expect("could not get time");

                    // println!("{:?}", profile);
                    let mut data = String::new();
                    data.push_str(&format!("{},{},{}\\n",
                                           duration.as_millis(),
                                           profile.total_used_bytes().unwrap().bytes,
                                           late));
                    let mut file = File::create("mem.txt").expect("Could not create file");
                    file.write_all(data.as_bytes()).expect("Could not write data");
                    // println!("{:?},{:?},{:?}", duration, profile.total_used_bytes().unwrap(), late);
                }""";
        return preamble + rustDataGenerator + postamble;
    }

    static String stripLateness(String query) {
        String[] lines = query.split("\n");
        for (int i = 0; i < lines.length; i++) {
            String line = lines[i];
            int lateness = line.indexOf("LATENESS");
            if (lateness > 0) {
                boolean comma = line.endsWith(",");
                line = line.substring(0, lateness);
                if (comma) line += ",";
                lines[i] = line;
            }
        }
        return String.join("\n", lines);
    }

    @Test
    public void profileLateness() throws IOException, InterruptedException, SQLException {
        String sql = """
                CREATE TABLE series (
                        distance DOUBLE,
                        pickup TIMESTAMP NOT NULL LATENESS INTERVAL '1:00' HOURS TO MINUTES
                );
                CREATE VIEW V AS
                SELECT MAX(distance), CAST(pickup AS DATE)
                FROM series GROUP BY CAST(pickup AS DATE);
                """;
        // Rust program which profiles the circuit.
        String main = this.createMain("""
                    // Initial data value for timestamp
                    let mut timestamp = cast_to_Timestamp_s("2024-01-10 10:10:10".to_string());
                    for i in 0..1000000 {
                        let value = Some(F64::new(i.into()));
                        timestamp = timestamp.add(20000);
                        let input = zset!(Tup2::new(value, timestamp) => 1);
                        append_to_collection_handle(&input, &streams.0);
                        if i % 1000 == 0 {
                            let _ = circuit.step().expect("could not run circuit");
                            let _ = &read_output_handle(&streams.1);
                            /*
                            let end = SystemTime::now();
                            let profile = circuit.retrieve_profile().expect("could not get profile");
                            let duration = end.duration_since(start).expect("could not get time");
                            println!("{:?},{:?}", duration.as_millis(), profile.total_used_bytes().unwrap().bytes);
                            */
                        }
                    }""");
        this.profile(sql, main);
    }

    void profile(String sql, String main) throws SQLException, IOException, InterruptedException {
        Long[] p0 = this.measure(stripLateness(sql), main);
        Long[] p1 = this.measure(sql, main);
        // Memory consumption of program with lateness is expected to be higher
        if (p0[1] < 1.5 * p1[1]) {
            System.err.println("Profile statistics without and with lateness:");
            System.err.println(Arrays.toString(p0));
            System.err.println(Arrays.toString(p1));
            assert false;
        }
        // No late records
        assert p0[2] == 0 && p1[2] == 0;
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
        this.addRustTestCase(ccs);
        CircuitVisitor visitor = new CircuitVisitor(new StderrErrorReporter()) {
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
        visitor.apply(ccs.circuit);
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
        this.addRustTestCase(ccs);
        CircuitVisitor visitor = new CircuitVisitor(new StderrErrorReporter()) {
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
        visitor.apply(ccs.circuit);
    }

    @Test
    public void testJoinTwoColumns() {
        // One joined column is monotone, the other one isn't.
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
            (SELECT * FROM series JOIN shift
             ON series.metadata = shift.person AND CAST(series.event_time AS DATE) = shift.on_call);
            """;
        DBSPCompiler compiler = testCompiler();
        compiler.compileStatements(script);
        CompilerCircuitStream ccs = new CompilerCircuitStream(compiler);
        this.addRustTestCase(ccs);
        CircuitVisitor visitor = new CircuitVisitor(new StderrErrorReporter()) {
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
        visitor.apply(ccs.circuit);
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
        this.addRustTestCase(ccs);
        CircuitVisitor visitor = new CircuitVisitor(new StderrErrorReporter()) {
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
        visitor.apply(ccs.circuit);
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
        this.addRustTestCase(ccs);
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
                create view agg1 as 
                select max(id)
                from m
                group by ts;
                create view flattened as 
                select id, v, ts
                from m, unnest(data) as v;
                create view agg2 as
                select max(id)
                from flattened
                group by ts;
                """;
        CompilerCircuitStream ccs = this.getCCS(sql);
        this.addRustTestCase(ccs);
        CircuitVisitor visitor = new CircuitVisitor(new StderrErrorReporter()) {
            int count = 0;

            @Override
            public void postorder(DBSPIntegrateTraceRetainKeysOperator operator) {
                this.count++;
            }

            @Override
            public void endVisit() {
                Assert.assertEquals(4, this.count);
            }
        };
        visitor.apply(ccs.circuit);
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
        CompilerCircuitStream ccs = this.getCCS(sql);
        this.addRustTestCase(ccs);
    }
}
