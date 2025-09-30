package org.dbsp.sqlCompiler.compiler.sql.simple;

import org.dbsp.sqlCompiler.circuit.operator.DBSPIntegrateTraceRetainKeysOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIntegrateTraceRetainValuesOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPWindowOperator;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.sql.tools.CompilerCircuit;
import org.dbsp.sqlCompiler.compiler.sql.tools.CompilerCircuitStream;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.sqlCompiler.ir.expression.DBSPApplyExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPCastExpression;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeBool;
import org.dbsp.util.Utilities;
import org.junit.Assert;
import org.dbsp.sqlCompiler.compiler.sql.tools.SqlIoTest;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/** Regression tests that failed in incremental mode using the Catalog API */
public class IncrementalRegressionTests extends SqlIoTest {
    @Override
    public CompilerOptions testOptions() {
        CompilerOptions options = super.testOptions();
        options.languageOptions.incrementalize = true;
        options.languageOptions.optimizationLevel = 2;
        // This causes the use of SourceSet operators
        // options.ioOptions.emitHandles = false;
        // Without the following ORDER BY causes failures
        options.languageOptions.ignoreOrderBy = true;
        return options;
    }

    @Test
    public void issue3941() {
        String sql = """
                create table P(cik integer, pts integer);
                CREATE VIEW V AS SELECT
                  lead(pts) OVER (PARTITION BY cik ORDER BY pts ASC),
                  COUNT(*) OVER (ORDER BY cik RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS sk_companyid,
                  lead(pts) OVER (PARTITION BY cik % 2 ORDER BY pts ASC)
                FROM P;""";
        this.getCCS(sql);
    }

    @Test
    public void internalIssue126() {
        this.statementsFailingInCompilation("""
                create table P(a varchar, ts TIMESTAMP);
                create view win AS
                SELECT ROW_NUMBER() OVER (PARTITION BY a, HOUR(ts) ORDER BY ts DESC)
                FROM P;""", "ROW_NUMBER only supported in a TopK pattern");
    }

    @Test
    public void issue4447() {
        var ccs = this.getCCS("""
            CREATE TABLE TT(id INT, pid INT);
            CREATE VIEW V AS SELECT ARRAY_AGG(id ORDER BY pid) FROM TT;""");
        ccs.step("INSERT INTO TT VALUES(10, 10);", """
                 array  | weight
                ----------------
                 { 10 } | 1""");
        ccs.step("INSERT INTO TT VALUES(5, 5);", """
                 array  | weight
                ----------------
                 { 10 } | -1
                 { 5, 10 } | 1""");
    }

    @Test
    public void internalIssue143() {
        this.compileRustTestCase("""
                CREATE TABLE T(x INT);
                CREATE VIEW V as SELECT x as "timestamp" FROM T;
                LATENESS trades."timestamp" INTERVAL 1 HOUR;""");
    }

    @Test
    public void issue3164() {
        this.getCCS("""
             CREATE TABLE T(x INTEGER LATENESS 10);
             CREATE VIEW V AS SELECT MAP['x', x] FROM T;""");
    }

    @Test
    public void issue3126() {
        this.getCCS("CREATE VIEW e AS SELECT * FROM error_view;");
    }

    @Test
    public void issue3125() {
        this.getCCS("""
                CREATE TABLE purchase0 (
                   customer_id INT,
                   ts TIMESTAMP NOT NULL LATENESS INTERVAL 1 HOURS,
                   amount BIGINT
                );
                
                CREATE TABLE purchase1 (
                   customer_id INT,
                   ts TIMESTAMP NOT NULL,
                   amount BIGINT
                );
                
                CREATE MATERIALIZED VIEW late_records AS (SELECT * FROM purchase1 EXCEPT SELECT * FROM purchase0);""");
    }

    @Test
    public void issue3145() {
        this.compileRustTestCase("""
                CREATE TABLE array_tbl(
                id INT,
                c1 INT ARRAY NOT NULL,
                c2 INT ARRAY,
                c3 MAP<VARCHAR, INT> ARRAY);
                CREATE MATERIALIZED VIEW max_emp AS SELECT
                MAX(c1) AS max FROM array_tbl WHERE FALSE;""");
    }

    @Test
    public void issue3153() {
        this.compileRustTestCase("""
                CREATE TABLE timestamp_tbl(
                c1 TIMESTAMP NOT NULL,
                c2 TIMESTAMP);
                CREATE LOCAL VIEW atbl_long_interval AS SELECT
                (c1 - c2)MONTH AS months
                FROM timestamp_tbl;
                CREATE MATERIALIZED VIEW v AS SELECT
                CAST(months AS VARCHAR) AS res
                FROM atbl_long_interval;""");
    }

    @Test
    public void issue3119() {
        this.compileRustTestCase("""
                CREATE TABLE row_tbl(id INT, c1 INT NOT NULL, c2 VARCHAR, c3 VARCHAR);
                CREATE MATERIALIZED VIEW v_max AS SELECT
                MAX(ROW(c1, c2, c3)) AS c1
                FROM row_tbl;""");
    }

    @Test
    public void wrongLateness() {
        this.statementsFailingInCompilation("""
                create table t (
                    x BIGINT NOT NULL LATENESS INTERVAL 5 MINUTES
                );
                create view v as select * from t;""",
                "Cannot apply '-' to arguments of type '<BIGINT> - <INTERVAL MINUTE>'");
        this.statementsFailingInCompilation("""
                create table t (
                    x VARCHAR NOT NULL LATENESS 5.0
                );
                create view v as select * from t;""",
                "Cannot subtract 5.0 from column 'x' of type VARCHAR");
    }

    @Test
    public void issue2881() {
        String sql = """
                CREATE TABLE transactions (
                    transaction_id INT NOT NULL PRIMARY KEY,
                    transaction_timestamp TIMESTAMP NOT NULL LATENESS INTERVAL 1 WEEK,
                    account_id INT NOT NULL,
                    amount DECIMAL(10, 2) NOT NULL
                );
                
                CREATE VIEW weekly_financial_final
                WITH ('emit_final' = 'week')
                AS SELECT
                    TIMESTAMP_TRUNC(transaction_timestamp, WEEK) as week,
                    account_id,
                    SUM(amount) AS weekly_balance
                FROM
                    transactions
                GROUP BY
                    TIMESTAMP_TRUNC(transaction_timestamp, WEEK), account_id;""";
        this.compileRustTestCase(sql);
    }

    @Test
    public void issue2822() {
        String sql = """
                CREATE TABLE purchase (
                   ts TIMESTAMP NOT NULL LATENESS INTERVAL 1 HOURS,
                   amount BIGINT
                );
                
                CREATE MATERIALIZED VIEW daily_total
                WITH ('emit_final' = 'd') AS SELECT
                    TIMESTAMP_TRUNC(ts, DAY) as d,
                    SUM(amount) AS total
                FROM purchase
                GROUP BY TIMESTAMP_TRUNC(ts, DAY);""";
        var ccs = this.getCCS(sql);
        ccs.step("""
                -- Waterline is now 2020-01-01\s
                INSERT INTO purchase VALUES('2020-01-01 00:00:01', 10);
                INSERT INTO purchase VALUES('2020-01-01 01:00:00', 20);""", """
                  d | sum | weight
                 -----------------""");
        ccs.step("""                                
                -- Waterline still at 2020-01-01 due to lateness
                INSERT INTO purchase VALUES('2020-01-02 00:10:00', 15);""", """ 
                 d | sum | weight
                ------------------""");
        ccs.step("""
                -- Waterline moves to 2020-01-02
                INSERT INTO purchase VALUES('2020-01-02 02:00:00', 65);""", """
                 d | sum | weight
                ------------------
                2020-01-01 00:00:00 | 30 | 1""");
    }

    @Test
    public void issue2584() {
        String sql = """
                create table t (
                    x int,
                    ts timestamp
                );
                
                create view v as
                select *
                from t
                where x = 5 and ts > now();""";
        var ccs = this.getCCS(sql);
        CircuitVisitor visitor = new CircuitVisitor(ccs.compiler) {
            int window = 0;

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
    public void issue2530() {
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
                from
                    l left asof join r
                    MATCH_CONDITION (r.ts <= l.ts)
                ON
                    l.id = r.id;

                CREATE VIEW agg1 as
                SELECT
                    MAX(id)
                FROM
                    v
                GROUP BY lts;""";
        var ccs = this.getCCS(sql);
        CircuitVisitor visitor = new CircuitVisitor(ccs.compiler) {
            int integrateTraceKeys = 0;
            int integrateTraceValues = 0;

            @Override
            public void postorder(DBSPIntegrateTraceRetainKeysOperator operator) {
                this.integrateTraceKeys++;
            }

            @Override
            public void postorder(DBSPIntegrateTraceRetainValuesOperator operator) {
                this.integrateTraceValues++;
            }

            @Override
            public void endVisit() {
                Assert.assertEquals(2, this.integrateTraceKeys);
                Assert.assertEquals(1, this.integrateTraceValues);
            }
        };
        ccs.visit(visitor);
    }

    @Test
    public void issue2502() {
        // Generates DBSPPartitionedRollingAggregateWithWaterlineOperator
        String sql = """
                CREATE TABLE customer (
                    id BIGINT NOT NULL,
                    name varchar,
                    state VARCHAR,
                    ts TIMESTAMP LATENESS INTERVAL 7 DAYS
                );

                -- Credit card transactions.
                CREATE TABLE transaction (
                    ts TIMESTAMP LATENESS INTERVAL 10 MINUTES,
                    amt DOUBLE,
                    customer_id BIGINT NOT NULL,\s
                    cc_num BIGINT NOT NULL,
                    state VARCHAR
                );

                CREATE LOCAL VIEW enriched_transaction AS
                SELECT
                    transaction.*,
                    CASE
                        WHEN transaction.state = customer.state THEN FALSE ELSE TRUE
                    END AS out_of_state
                FROM
                    transaction LEFT ASOF JOIN customer
                    MATCH_CONDITION ( transaction.ts >= customer.ts )
                    ON transaction.customer_id = customer.id;

                CREATE LOCAL VIEW transaction_with_history AS
                SELECT
                    *,
                    COUNT(case when out_of_state then 1 else 0 end) OVER window_30_day as out_of_state_count
                FROM
                    enriched_transaction
                WINDOW window_30_day AS (PARTITION BY customer_id ORDER BY ts RANGE BETWEEN INTERVAL 30 DAYS PRECEDING AND CURRENT ROW);

                CREATE LOCAL VIEW red_transactions AS
                SELECT
                    *
                FROM
                    transaction_with_history
                WHERE
                    out_of_state AND out_of_state_count < 5;

                CREATE LOCAL VIEW green_transactions AS
                SELECT
                    *
                FROM
                    transaction_with_history
                WHERE
                    (NOT out_of_state) OR out_of_state_count >= 5;

                CREATE VIEW user_stats AS
                SELECT
                        customer_id,
                        SUM(red_amt) as red_amt,
                        SUM(green_amt) as green_amt,
                        SUM(amt) as total_amt
                FROM (
                    SELECT customer_id, 0.0 as amt, 0.0 as green_amt, amt as red_amt FROM red_transactions
                    UNION ALL
                    SELECT customer_id, 0.0 as amt, amt as green_amt, 0.0 as red_amt FROM green_transactions
                    UNION ALL
                    SELECT customer_id, amt, 0.0 as green_amt, 0.0 as red_amt FROM transaction
                ) AS combined_tables
                GROUP BY customer_id;""";
        var ccs = this.getCCS(sql);
        ccs.step("""
                INSERT INTO TRANSACTION VALUES ('2024-01-01 00:00:00', 1, 0, 10, 'ca');
                INSERT INTO CUSTOMER VALUES (0, 'a', 'ca', '2024-01-01 00:00:00');""", """
                  customer_id | red_amd | green_amt | total_amt | weight
                 --------------------------------------------------------
                            0 |       0 |         1 |         1 | 1""");
    }

    @Test
    public void issue2514() {
        String sql = """
                CREATE TABLE transaction (
                    ts TIMESTAMP LATENESS INTERVAL 1 DAYS,
                    amt DOUBLE,
                    customer_id BIGINT NOT NULL,
                    state VARCHAR
                );

                CREATE MATERIALIZED VIEW red_transactions AS
                SELECT
                    *
                FROM
                    transaction
                WHERE
                    state = 'CA';""";
        this.compileRustTestCase(sql);
    }

    @Test
    public void issue2517() {
        String sql = """
                CREATE TABLE transaction (
                    ts TIMESTAMP LATENESS INTERVAL 1 DAYS,
                    state VARCHAR
                ) with ('materialized' = 'true');

                CREATE MATERIALIZED VIEW red_transactions AS
                SELECT
                    *
                FROM
                    transaction;""";
        CompilerCircuitStream ccs = this.getCCS(sql);
        ccs.step("insert into transaction values (NULL, 'CA');", """
                 ts | state | weight
                ---------------------
                    | CA| 1""");
        ccs.step("insert into transaction values (NULL, 'WA');", """
                 ts | state | weight
                ---------------------
                    | WA| 1""");
    }

    @Test
    public void testControl() {
        // Test using a control table to save changes from a query
        String query = """
                CREATE TABLE T(COL1 INT, COL2 BIGINT);
                create table control (id int);

                CREATE LOCAL VIEW test  AS
                SELECT
                    COL1,
                    COUNT(*),
                    SUM(COL2)
                FROM T
                GROUP BY T.COL1;

                CREATE VIEW output as
                select * from
                test where exists (select 1 from control);""";
        this.compileRustTestCase(query);
    }

    @Test
    public void issue2243() {
        String sql = """
                CREATE TABLE CUSTOMER (cc_num bigint not null, ts timestamp lateness interval 0 day);
                CREATE TABLE TRANSACTION (cc_num bigint not null, ts timestamp lateness interval 0 day);

                CREATE VIEW V AS
                SELECT t.*
                FROM
                transaction as t JOIN customer as c
                ON
                    t.cc_num = c.cc_num
                WHERE
                    t.ts <= c.ts and t.ts + INTERVAL 7 DAY >= c.ts;

                create view V2 as
                select SUM(5) from v group by ts;""";
        CompilerCircuitStream ccs = this.getCCS(sql);
        CircuitVisitor visitor = new CircuitVisitor(ccs.compiler) {
            int integrateTraceKeys = 0;
            int integrateTraceValues = 0;

            @Override
            public void postorder(DBSPIntegrateTraceRetainKeysOperator operator) {
                this.integrateTraceKeys++;
            }

            @Override
            public void postorder(DBSPIntegrateTraceRetainValuesOperator operator) {
                this.integrateTraceValues++;
            }

            @Override
            public void endVisit() {
                Assert.assertEquals(1, this.integrateTraceKeys);
                Assert.assertEquals(2, this.integrateTraceValues);
            }
        };
        InnerVisitor findBoolCasts = new InnerVisitor(ccs.compiler) {
            int unsafeBoolCasts = 0;

            @Override
            public void postorder(DBSPCastExpression expression) {
                if (!expression.getType().mayBeNull &&
                        expression.getType().is(DBSPTypeBool.class) &&
                        expression.source.getType().mayBeNull)
                    unsafeBoolCasts++;
            }

            @Override
            public void endVisit() {
                Assert.assertEquals(0, this.unsafeBoolCasts);
            }
        };
        ccs.visit(visitor);
        ccs.visit(findBoolCasts.getCircuitVisitor(false));
    }

    @Test
    public void issue2242() {
        String sql = """
                create table TRANSACTION (
                    cc_num bigint,
                    unix_time timestamp,
                    id bigint
                );

                CREATE VIEW TRANSACTION_MONTHLY_AGGREGATE AS
                select cc_num, window_start as month_start, COUNT(*) num_transactions from TABLE(
                  TUMBLE(
                    TABLE transaction,
                    DESCRIPTOR(unix_time),
                    INTERVAL 1 MONTH))
                GROUP BY cc_num, window_start;
                """;
        this.statementsFailingInCompilation(sql, "Tumbling window intervals must be 'short'");
    }

    @Test
    public void issue2039() {
        String sql = """
                CREATE TABLE transactions (
                    id INT NOT NULL PRIMARY KEY,
                    ts TIMESTAMP LATENESS INTERVAL 0 HOURS,
                    user_id INT,
                    AMOUNT DECIMAL
                );""";
        this.compileRustTestCase(sql);
    }

    @Test
    public void issue2043() {
        String sql =
                """
                CREATE TABLE transactions (
                    id INT NOT NULL PRIMARY KEY,
                    ts TIMESTAMP LATENESS INTERVAL 0 SECONDS,
                    user_id INT,
                    AMOUNT DECIMAL
                ) with ('materialized' = 'true');

                CREATE MATERIALIZED VIEW window_computation AS SELECT
                    user_id,
                    COUNT(*) AS transaction_count_by_user
                    FROM transactions
                    WHERE ts > NOW() - INTERVAL 1 DAY and ts <= NOW()
                    GROUP BY user_id;""";
        this.compileRustTestCase(sql);
    }

    @Test
    public void issue2018() {
        String sql = """
                CREATE TABLE customer (
                    c_id INT NOT NULL,
                    c_d_id INT NOT NULL,
                    c_w_id INT NOT NULL,
                    c_first VARCHAR(16),
                    c_middle CHAR(2),
                    c_last VARCHAR(16),
                    c_street_1 VARCHAR(20),
                    c_street_2 VARCHAR(20),
                    c_city VARCHAR(20),
                    c_state CHAR(2),
                    c_zip CHAR(9),
                    c_phone CHAR(16),
                    c_since TIMESTAMP,
                    c_credit CHAR(2),
                    c_credit_lim DECIMAL(12,2),
                    c_discount DECIMAL(4,4),
                    c_balance DECIMAL(12,2),
                    c_ytd_payment DECIMAL(12,2),
                    c_payment_cnt INT,
                    c_delivery_cnt INT,
                    c_data VARCHAR(500),
                    PRIMARY KEY (c_w_id, c_d_id, c_id),
                    FOREIGN KEY (c_w_id, c_d_id) REFERENCES district(d_w_id, d_id)
                );

                CREATE TABLE transaction_parameters (
                    txn_id INT NOT NULL PRIMARY KEY,
                    w_id INT,
                    d_id INT,
                    c_id INT,
                    c_w_id INT,
                    c_d_id INT,
                    c_last VARCHAR(20), -- TODO check
                    h_amount DECIMAL(5,2),
                    h_date TIMESTAMP,
                    datetime_ TIMESTAMP
                );

                -- incremental fails with this query present
                CREATE VIEW cust_enum AS
                SELECT c.c_first, c.c_middle, c.c_id,
                    c.c_street_1, c.c_street_2, c.c_city, c.c_state, c.c_zip,
                    c.c_phone, c.c_credit, c.c_credit_lim,
                    c.c_discount, c.c_balance, c.c_since
                FROM customer AS c,
                     transaction_parameters AS t
                WHERE c.c_last = t.c_last
                  AND c.c_d_id = t.c_d_id
                  AND c.c_w_id = t.c_w_id
                ORDER BY c_first;

                CREATE VIEW cust_agg AS
                SELECT ARRAY_AGG(c_id ORDER BY c_first) AS cust_array
                FROM (SELECT c.c_id, c.c_first
                      FROM customer AS c,
                          transaction_parameters AS t
                      WHERE c.c_last = t.c_last
                        AND c.c_d_id = t.c_d_id
                        AND c.c_w_id = t.c_w_id
                      ORDER BY c_first);

                CREATE VIEW cust_med AS
                SELECT c.c_first, c.c_middle, c.c_id,
                    c.c_street_1, c.c_street_2, c.c_city, c.c_state, c.c_zip,
                    c.c_phone, c.c_credit, c.c_credit_lim,
                    c.c_discount, c.c_balance, c.c_since
                FROM customer as c,
                     cust_agg as a,
                     transaction_parameters as t
                WHERE c.c_id = a.cust_array[(ARRAY_LENGTH(a.cust_array) / 2) + 1];
                """;
        this.compileRustTestCase(sql);
    }

    @Test
    public void issue3621() {
        this.getCCS("""
                DECLARE RECURSIVE VIEW rv1(x VARCHAR NULL);
                CREATE MATERIALIZED VIEW rv1 AS SELECT null as x;
                
                DECLARE RECURSIVE VIEW rv2(x VARCHAR);
                CREATE MATERIALIZED VIEW rv2 AS SELECT null as x;
                
                DECLARE RECURSIVE VIEW rv3(x VARCHAR);
                CREATE MATERIALIZED VIEW rv3 AS SELECT CAST('foo' as varchar) as x;
                
                DECLARE RECURSIVE VIEW rv4(x VARCHAR NULL);
                CREATE MATERIALIZED VIEW rv4 AS SELECT CAST('foo' as varchar) as x;
                
                DECLARE RECURSIVE VIEW rv5(x VARCHAR NOT NULL);
                CREATE MATERIALIZED VIEW rv5 AS SELECT CAST('foo' as varchar) as x;
                
                DECLARE RECURSIVE VIEW rv6(x VARCHAR);
                CREATE MATERIALIZED VIEW rv6 AS SELECT 'foo' as x;
                
                DECLARE RECURSIVE VIEW rv7(type_path VARCHAR ARRAY NOT NULL);
                CREATE MATERIALIZED VIEW rv7
                AS select array[cast('event' as varchar)] as type_path;

                DECLARE RECURSIVE VIEW rv8(type_path VARCHAR ARRAY NOT NULL);
                CREATE MATERIALIZED VIEW rv8
                AS select cast(array['event'] as VARCHAR ARRAY) as type_path;
                
                DECLARE RECURSIVE VIEW rv9(type_path BIGINT);
                CREATE MATERIALIZED VIEW rv9
                AS select 1 as type_path;""");
    }

    @Test
    public void cseTest() {
        var ccs = this.getCCS("""
                CREATE TABLE t(cco DECIMAL(8, 2), er DECIMAL(8, 2), cwr DECIMAL(8, 2), smr DECIMAL(8, 2));
                CREATE VIEW V AS SELECT
                (coalesce(bround(bround(t.cwr / NULLIF(coalesce(
                        1,
                        1
                    ), 0), 2) / NULLIF(coalesce(
                        CASE
                    WHEN PARSE_JSON(t.cco)['u'] IS NULL
                      AND PARSE_JSON(t.cco)['n'] IS NULL THEN NULL
                    ELSE COALESCE(CAST(PARSE_JSON(t.cco)['u'] AS DECIMAL(28,0)), 0) +
                         COALESCE(CAST(PARSE_JSON(t.cco)['n'] AS DECIMAL) / 1000000000, 0)
                END,
                        CASE
                    WHEN PARSE_JSON(t.er)['u'] IS NULL
                      AND PARSE_JSON(t.er)['n'] IS NULL THEN NULL
                    ELSE COALESCE(CAST(PARSE_JSON(t.er)['u'] AS DECIMAL(28,0)), 0) +
                         COALESCE(CAST(PARSE_JSON(t.er)['n'] AS DECIMAL) / 1000000000, 0)
                END,
                        1
                    ), 0), 2), 0)
                    + coalesce(bround(bround(t.smr / NULLIF(coalesce(
                        1,
                        1
                    ), 0), 2) / NULLIF(coalesce(
                        CASE
                    WHEN PARSE_JSON(t.cco)['u'] IS NULL
                      AND PARSE_JSON(t.cco)['n'] IS NULL THEN NULL
                    ELSE COALESCE(CAST(PARSE_JSON(t.cco)['u'] AS DECIMAL(28,0)), 0) +
                         COALESCE(CAST(PARSE_JSON(t.cco)['n'] AS DECIMAL) / 1000000000, 0)
                END,
                        CASE
                    WHEN PARSE_JSON(t.er)['u'] IS NULL
                      AND PARSE_JSON(t.er)['n'] IS NULL THEN NULL
                    ELSE COALESCE(CAST(PARSE_JSON(t.er)['u'] AS DECIMAL(28,0)), 0) +
                         COALESCE(CAST(PARSE_JSON(t.er)['n'] AS DECIMAL) / 1000000000, 0)
                END,
                        1
                    ), 0), 2), 0))::decimal(28,2) as x
                FROM t;""");
        int[] calls = new int[1];
        InnerVisitor visitor = new InnerVisitor(ccs.compiler) {
            @Override
            public void postorder(DBSPApplyExpression expression) {
                if (expression.function.toString().contains("parse_json"))
                    calls[0]++;
            }
        };
        ccs.visit(visitor.getCircuitVisitor(false));
        Assert.assertEquals(2, calls[0]);
    }

    @Test
    public void illegalDecimalTest() {
        this.statementsFailingInCompilation("""
                CREATE TABLE T(c DECIMAL(48, 18));""",
                "Maximum precision supported for DECIMAL");
    }

    @Test
    public void calciteIssue6978() {
        String sql = """
                CREATE TABLE T(x DECIMAL(6, 2), z INT);
                CREATE TABLE S(y INT);
                CREATE VIEW V AS SELECT
                   y,
                   coalesce((select sum(X) from T
                             where y = T.z limit 1), 0) as w
                FROM S;""";
        this.getCCS(sql);
    }

    @Test
    public void issue3918() {
        String sql = """
                CREATE TABLE T(x DECIMAL(6, 2), z INT);
                CREATE TABLE S(y INT);
                CREATE VIEW V AS SELECT
                   y,
                   (select sum(X) from T
                             where y = T.z limit 1) as w
                FROM S;""";
        this.getCCS(sql);
    }

    @Test
    public void aggTree() {
        var ccs = this.getCCS("""
                CREATE TABLE T(id INT, od INT, val DECIMAL(5, 2), ct INT, e BOOLEAN);
                CREATE VIEW V AS SELECT
                    MAX(CASE WHEN od = 0 THEN 1 ELSE 0 END),
                    MAX(CASE WHEN od = 1 THEN 1 ELSE 0 END),
                    COUNT(*),
                    SUM(val),
                    COUNT(DISTINCT id),
                    MIN(CASE WHEN (od = 0 AND e) THEN 1 ELSE 0 END),
                    MIN(CASE WHEN (od = 1 AND e) THEN 1 ELSE 0 END),
                    MAX(CASE WHEN e THEN 1 ELSE 0 END),
                    MIN(CASE WHEN (od = 0 AND e) THEN ROUND(val, 0) ELSE 0.0 END)
                FROM T""");
        Map<DBSPOperator, Integer> depthMap = new HashMap<>();
        ccs.visit(new CircuitVisitor(ccs.compiler) {
            @Override
            public void postorder(DBSPJoinIndexOperator operator) {
                // Check that no long chains of JoinIndex operators are produced
                int depth = 0;
                if (operator.left().operator.is(DBSPJoinIndexOperator.class))
                    depth = depthMap.get(operator.left().operator) + 1;
                if (operator.right().operator.is(DBSPJoinIndexOperator.class))
                    depth = Math.max(depth, depthMap.get(operator.right().operator) + 1);
                depthMap.put(operator, depth);
                Assert.assertTrue(depth <= 3);
            }
        });
    }

    // Tests that are not in the repository; run manually
    @Test @Ignore
    public void extraTests() throws IOException {
        String dir = "../extra";
        File file = new File(dir);
        if (file.exists()) {
            File[] toCompile = file.listFiles();
            if (toCompile == null)
                return;
            Arrays.sort(toCompile);
            for (File c: toCompile) {
                if (!c.getName().contains("no-min.sql")) continue;
                if (c.getName().contains("sql")) {
                    System.out.println("Compiling " + c);
                    String sql = Utilities.readFile(c.getPath());
                    this.compileRustTestCase(sql);
                }
            }
        }
    }

    @Test
    public void issue4503() {
        this.getCC("""
                CREATE TABLE T (
                    id VARCHAR NOT NULL,
                    h VARCHAR NOT NULL,
                    b BIGINT NOT NULL LATENESS 2,
                    r INTEGER
                ) WITH ('append_only'='true');
                
                CREATE VIEW V AS
                SELECT *
                FROM (
                    SELECT
                        *,
                        SUM(r) OVER (
                            PARTITION BY id, h
                            ORDER BY b
                        ) AS rn
                    FROM T
                ) WHERE rn = 1;""");
    }

    @Test
    public void issue4660() {
        var ccs = this.getCCS("""
                CREATE TABLE T(x BIGINT NOT NULL);
                CREATE VIEW V AS SELECT MAX(x) OVER () FROM T;""");
        ccs.step("", """
                 max | weight
                --------------""");
        ccs.step("INSERT INTO T VALUES(1), (2), (-1);", """
                 max | weight
                --------------
                   2 | 3""");
    }

    @Test
    public void issue4694() {
        this.getCCS("""
              CREATE TABLE test_table1 (
                 id UUID NOT NULL PRIMARY KEY,
                 ts TIMESTAMP NOT NULL LATENESS INTERVAL 1 MINUTE,
                 value VARCHAR
              ) WITH ('append_only' = 'true', 'materialized' = 'true');

              CREATE TABLE test_table2 (
                  id UUID NOT NULL PRIMARY KEY,
                  ts TIMESTAMP NOT NULL LATENESS INTERVAL 1 MINUTE,
                  value VARCHAR
              ) WITH ('append_only' = 'true', 'materialized' = 'true');

              CREATE LOCAL VIEW test_union AS
              SELECT id, ts, value, NULL AS fake_field
              FROM test_table1
              UNION ALL
              SELECT id, ts, value, id AS fake_field
              FROM test_table2;

              CREATE LOCAL VIEW test_agg AS
              SELECT id, ARG_MAX (value, ts)
                  FILTER (WHERE value IS NOT NULL) AS value,
                  ARG_MAX (fake_field, ts)
                  FILTER (WHERE fake_field IS NOT NULL) AS fake_field
              FROM test_union
              GROUP BY id;

              CREATE MATERIALIZED VIEW test_mat AS
              SELECT id FROM test_agg;""");
    }

    Set<String> collectHashes(CompilerCircuit cc) {
        HashSet<String> result = new HashSet<>();
        CircuitVisitor vis = new CircuitVisitor(cc.compiler) {
            @Override
            public void postorder(DBSPOperator ignored) {
                result.add(ignored.getNodeName(true));
            }
        };
        cc.getCircuit().accept(vis);
        return result;
    }

    @Test
    public void x() {
        var cc0 = this.getCC("""
                    CREATE TABLE t1(x int) WITH ('materialized'='true');
                    CREATE MATERIALIZED VIEW v1 AS SELECT COUNT(*) AS c FROM t1;""");
        var cc1 = this.getCC("""
                    CREATE TABLE t1(x int) WITH ('materialized'='true');
                    CREATE MATERIALIZED VIEW v1 AS SELECT COUNT(*) AS c FROM t1;
                    CREATE MATERIALIZED VIEW v2 AS SELECT COUNT(*) AS c FROM t1;""");
        // Check that all hashses from cc1 appear unchanged in cc1
        Set<String> hash0 = this.collectHashes(cc0);
        Set<String> hash1 = this.collectHashes(cc1);
        Assert.assertTrue(hash1.containsAll(hash0));
        Assert.assertEquals(hash0.size() + 1, hash1.size());
    }
}
