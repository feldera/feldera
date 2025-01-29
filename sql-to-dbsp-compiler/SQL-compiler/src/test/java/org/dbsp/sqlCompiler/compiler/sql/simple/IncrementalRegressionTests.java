package org.dbsp.sqlCompiler.compiler.sql.simple;

import org.dbsp.sqlCompiler.circuit.operator.DBSPIntegrateTraceRetainKeysOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIntegrateTraceRetainValuesOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPWindowOperator;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.sql.tools.CompilerCircuitStream;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.sqlCompiler.ir.expression.DBSPCastExpression;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeBool;
import org.junit.Assert;
import org.dbsp.sqlCompiler.compiler.sql.tools.SqlIoTest;
import org.junit.Test;

/** Regression tests that failed in incremental mode using the Catalog API */
public class IncrementalRegressionTests extends SqlIoTest {
    @Override
    public DBSPCompiler testCompiler() {
        CompilerOptions options = this.testOptions(true, true);
        // This causes the use of SourceSet operators
        // options.ioOptions.emitHandles = false;
        // Without the following ORDER BY causes failures
        options.languageOptions.ignoreOrderBy = true;
        return new DBSPCompiler(options);
    }

    @Test
    public void internalIssue126() {
        this.statementsFailingInCompilation("""
                create table P(a varchar, ts TIMESTAMP);
                create view win AS
                SELECT ROW_NUMBER() OVER (PARTITION BY a, HOUR(ts) ORDER BY ts DESC)
                FROM P;""", "WINDOW aggregate with ROWS/ROW_NUMBER not yet implemented");
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
        this.addRustTestCase(ccs);
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
        this.addRustTestCase(ccs);
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
        visitor.apply(ccs.circuit);
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
        this.addRustTestCase(ccs);
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
        visitor.apply(ccs.circuit);
    }

    @Test
    public void issue2502() {
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
        this.addRustTestCase(ccs);
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
        this.addRustTestCase(ccs);
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
        this.addRustTestCase(ccs);
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
        visitor.apply(ccs.circuit);
        findBoolCasts.getCircuitVisitor(false).apply(ccs.circuit);
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
    public void issue2043uppercase() {
        // Simulate a different unquotedCasing flag
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
        DBSPCompiler compiler = this.testCompiler();
        compiler.options.languageOptions.throwOnError = false;
        compiler.options.languageOptions.unquotedCasing = "upper";
        compiler.submitStatementsForCompilation(sql);
        getCircuit(compiler);
        Assert.assertEquals(0, compiler.messages.exitCode);
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
}
