package org.dbsp.sqlCompiler.compiler.sql.simple;

import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinFilterMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPPartitionedRollingAggregateWithWaterlineOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.StderrErrorReporter;
import org.dbsp.sqlCompiler.compiler.TestUtil;
import org.dbsp.sqlCompiler.compiler.sql.tools.SqlIoTest;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

public class RegressionTests extends SqlIoTest {
    @Test
    public void issue2261() {
        String sql = """
                CREATE TABLE stddev_groupby(id INT, c2 TINYINT NOT NULL);
                CREATE VIEW stddev_view AS SELECT
                STDDEV_SAMP(c2) AS c2
                FROM stddev_groupby
                GROUP BY id;""";
        this.compileRustTestCase(sql);
    }

    @Test
    public void issue2090() {
        String sql = """
                CREATE TABLE example (
                    id INT
                ) WITH (
                    'connector' = 'value1'
                                  'value2'
                );""";
        this.statementsFailingInCompilation(sql, "Expected a simple string");
    }

    @Test
    public void issue2201() {
        String sql = """
                create table customer_address(
                    ca_zip char(10)
                );
                
                CREATE VIEW V AS SELECT substr(ca_zip,1,5)
                FROM customer_address;""";
        this.compileRustTestCase(sql);
    }

    @Test
    public void issue1189() {
        String sql = """
                create table EVENT_DURATION_V(duration bigint, event_type_id bigint);
                create table EVENTTYPE_T(id bigint, name string);
                
                CREATE VIEW SHORTEST_ALARMS_TYPE_V AS
                SELECT duration
                ,      event_type_id
                ,      ett.name
                FROM   (SELECT duration
                        ,      event_type_id
                        ,      ROW_NUMBER() OVER (PARTITION BY event_type_id
                                                  ORDER BY duration ASC) AS rnum
                        FROM   EVENT_DURATION_V) a
                        JOIN EVENTTYPE_T ett ON a.event_type_id = ett.id
                WHERE   rnum <= 1
                ;""";
        this.compileRustTestCase(sql);
    }

    @Test
    public void issue2017() {
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
                
                CREATE VIEW cust_max AS
                SELECT c.c_first, c.c_middle, c.c_id,
                    c.c_street_1, c.c_street_2, c.c_city, c.c_state, c.c_zip,
                    c.c_phone, c.c_credit, c.c_credit_lim,
                    c.c_discount, c.c_balance, c.c_since
                FROM customer AS c,
                     transaction_parameters AS t
                WHERE c.c_last = t.c_last
                  AND c.c_d_id = t.c_d_id
                  AND c.c_w_id = t.c_w_id
                  AND c_first = (select max(c_first) from customer LIMIT 1)
                LIMIT 1;""";
        this.compileRustTestCase(sql);
    }

    @Test
    public void issue2094() {
        String sql = "CREATE FUNCTION F() RETURNS INT NOT NULL AS 0;";
        this.compileRustTestCase(sql);
    }

    @Test
    public void issue2095() {
        String sql = """
        CREATE FUNCTION F() RETURNS INT NOT NULL AS 0;
        CREATE FUNCTION G() RETURNS INT NOT NULL AS F();
        """;
        this.compileRustTestCase(sql);
    }

    @Test
    public void testErrorPosition() {
        // Test that errors for functions report correct source position
        String sql = """
                CREATE FUNCTION ANCHOR_TIMESTAMP() RETURNS TIMESTAMP NOT NULL
                  AS TIMESTAMP '2024-01-01 00:00:00';
                
                CREATE FUNCTION ROUND_TIMESTAMP(ts TIMESTAMP, billing_interval_days INT) RETURNS TIMESTAMP
                  AS TRUNC(DATEDIFF(DAYS, ts, ANCHOR_TIMESTAMP())) + ANCHOR_TIMESTAMP();
                """;
        this.statementsFailingInCompilation(sql, """
                    5|  AS TRUNC(DATEDIFF(DAYS, ts, ANCHOR_TIMESTAMP())) + ANCHOR_TIMESTAMP();
                           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
                """);
    }

    @Test
    public void issue1868() {
        String sql = """
                CREATE TABLE example_a (
                    id INT NOT NULL
                );
                
                CREATE TABLE example_b (
                    id INT NOT NULL
                );
                
                CREATE VIEW example_c AS (
                    SELECT COALESCE(example_a.id, 0) - COALESCE(example_b.id, 0)
                    FROM example_a
                         FULL JOIN example_b
                         ON example_a.id = example_b.id
                );""";
        this.compileRustTestCase(sql);
    }

    // Test for https://github.com/feldera/feldera/issues/1151
    @Test
    public void issue1151() {
        String sql = "CREATE TABLE event_t ( id BIGINT NOT NULL PRIMARY KEY, local_event_dt DATE )";
        this.compileRustTestCase(sql);
    }

    @Test
    public void testFilterPull() {
        // Example used in a blog post
        String sql = """
                CREATE TABLE transaction (
                   cc_num int
                );
                
                CREATE TABLE users (
                   cc_num int,
                   id bigint,
                   age int
                );
                
                CREATE VIEW transaction_with_user AS
                SELECT
                    transaction.*,
                    users.id as user_id,
                    users.age
                FROM
                    transaction JOIN users
                ON users.cc_num = transaction.cc_num
                WHERE
                    users.age >= 21;""";
        DBSPCompiler compiler = this.testCompiler();
        compiler.compileStatements(sql);
        DBSPCircuit circuit = getCircuit(compiler);
        CircuitVisitor visitor = new CircuitVisitor(new StderrErrorReporter()) {
            int filterJoin = 0;

            @Override
            public void postorder(DBSPJoinFilterMapOperator operator) {
                this.filterJoin++;
            }

            @Override
            public void endVisit() {
                // If the filter for age is not pulled above the join, it
                // will produce a JoinFilterMap operator.
                Assert.assertEquals(0, this.filterJoin);
            }
        };
        visitor.apply(circuit);
    }

    @Test
    public void issue1898() {
        String sql = """
                create table t(
                    id bigint,
                    part bigint
                );
                
                create view v as
                SELECT
                    id,
                    COUNT(DISTINCT id) FILTER (WHERE id > 100) OVER window_100 AS agg
                FROM
                    t
                WINDOW
                    window_100 AS (PARTITION BY part ORDER BY id RANGE BETWEEN 100 PRECEDING AND CURRENT ROW);""";
        DBSPCompiler compiler = this.testCompiler();
        compiler.options.languageOptions.throwOnError = false;
        compiler.compileStatements(sql);
        TestUtil.assertMessagesContain(compiler, "OVER must be applied to aggregate function");
    }

    @Test
    public void issue2027() {
        // validated with Postgres 15
        String sql = """
                CREATE TABLE T (
                   id INT,
                   amt INT,
                   ts TIMESTAMP
                );
                
                CREATE VIEW V AS SELECT
                    id,
                    amt,
                    SUM(amt) OVER window1 AS s1,
                    SUM(amt) OVER window2 AS s2,
                    SUM(amt) OVER window3 AS s3,
                    SUM(amt) OVER window4 AS s4
                FROM T WINDOW
                window1 AS (PARTITION BY id ORDER BY EXTRACT(HOUR FROM ts) RANGE BETWEEN 2 PRECEDING AND 2 FOLLOWING),
                window2 AS (PARTITION BY id ORDER BY ts RANGE BETWEEN INTERVAL 1 HOUR PRECEDING AND INTERVAL 1 MINUTE FOLLOWING),
                window3 AS (PARTITION BY id ORDER BY CAST(ts AS DATE) RANGE BETWEEN INTERVAL 1 HOUR PRECEDING AND INTERVAL 1 MINUTE FOLLOWING),
                window4 AS (PARTITION BY id ORDER BY CAST(ts AS TIME) RANGE BETWEEN INTERVAL 1 HOUR PRECEDING AND INTERVAL 1 MINUTE FOLLOWING);""";
        DBSPCompiler compiler = this.testCompiler();
        compiler.compileStatements(sql);
        CompilerCircuitStream ccs = new CompilerCircuitStream(compiler);
        ccs.step("""
                        INSERT INTO T VALUES(0, 1, '2024-01-01 00:00:00');
                        INSERT INTO T VALUES(1, 2, '2024-01-01 00:00:00');
                        INSERT INTO T VALUES(0, 3, '2024-01-01 00:00:01');
                        INSERT INTO T VALUES(0, 4, '2024-01-01 00:00:01');
                        INSERT INTO T VALUES(0, 5, '2024-01-01 00:10:00');
                        INSERT INTO T VALUES(0, 6, '2024-01-01 00:11:00');
                        INSERT INTO T VALUES(0, 7, '2024-01-01 00:13:00');""",
                """
                        id | amt | s1 | s2 | s3 | s4 | weight
                       ---------------------------------------
                        0  | 1   | 26 | 8  | 26 | 8  | 1
                        0  | 3   | 26 | 8  | 26 | 8  | 1
                        0  | 4   | 26 | 8  | 26 | 8  | 1
                        0  | 5   | 26 | 19 | 26 | 19 | 1
                        0  | 6   | 26 | 19 | 26 | 19 | 1
                        0  | 7   | 26 | 26 | 26 | 26 | 1
                        1  | 2   | 2  | 2  | 2  | 2  | 1"""
        );
        this.addRustTestCase("issue2027", ccs);
    }

    @Test
    public void issue2027negative() {
        this.statementsFailingInCompilation("""
                CREATE TABLE t (
                   id INT,
                   amt INT,
                   ts TIMESTAMP
                );
                
                CREATE VIEW V AS SELECT
                    SUM(amt) OVER window1 AS s1
                FROM t WINDOW
                window1 AS (PARTITION BY id ORDER BY ts RANGE BETWEEN INTERVAL 1 MONTH PRECEDING AND INTERVAL 1 YEAR FOLLOWING);""",
                "Can you rephrase the query using an interval");
    }

    @Test
    public void issue2027negative1() {
        this.statementsFailingInCompilation("""
                CREATE TABLE t (
                   id INT,
                   amt INT,
                   ts TIMESTAMP
                );
                
                CREATE VIEW V AS SELECT
                    SUM(amt) OVER window1 AS s1
                FROM t WINDOW
                window1 AS (PARTITION BY id ORDER BY ts RANGE BETWEEN INTERVAL -1 HOUR PRECEDING AND CURRENT ROW);""",
                "Window bounds must be positive");
    }

    @Test
    public void issue1768() {
        String sql = """
                CREATE TABLE transaction (
                    trans_date_time TIMESTAMP NOT NULL LATENESS INTERVAL 1 DAY,
                    cc_num BIGINT NOT NULL,
                    merchant STRING,
                    category STRING,
                    amt FLOAT64,
                    trans_num STRING,
                    unix_time INTEGER NOT NULL,
                    merch_lat FLOAT64 NOT NULL,
                    merch_long FLOAT64 NOT NULL,
                    is_fraud INTEGER
                );
                
                CREATE TABLE demographics (
                    cc_num BIGINT NOT NULL,
                    first STRING,
                    gender STRING,
                    street STRING,
                    city STRING,
                    state STRING,
                    zip INTEGER,
                    lat FLOAT64,
                    long FLOAT64,
                    city_pop INTEGER,
                    job STRING,
                    dob STRING
                );
            
                CREATE VIEW V AS SELECT
                    transaction.cc_num,
                    CASE
                      WHEN dayofweek(trans_date_time) IN(6, 7) THEN true
                      ELSE false
                    END AS is_weekend,
                    CASE
                      WHEN hour(trans_date_time) <= 6 THEN true
                      ELSE false
                    END AS is_night,
                    category,
                    AVG(amt) OVER window_1_day AS avg_spend_pd,
                    AVG(amt) OVER window_7_day AS avg_spend_pw,
                    AVG(amt) OVER window_30_day AS avg_spend_pm,
                    COUNT(*) OVER window_1_day AS trans_freq_24,
                      amt, state, job, unix_time, city_pop, is_fraud
                  FROM transaction
                  JOIN demographics
                  ON transaction.cc_num = demographics.cc_num
                  WINDOW
                    window_1_day AS (PARTITION BY transaction.cc_num ORDER BY unix_time RANGE BETWEEN 86400 PRECEDING AND CURRENT ROW),
                    window_7_day AS (PARTITION BY transaction.cc_num ORDER BY unix_time RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW),
                    window_30_day AS (PARTITION BY transaction.cc_num ORDER BY unix_time RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW);""";

        DBSPCompiler compiler = this.testCompiler();
        compiler.options.languageOptions.incrementalize = true;
        compiler.compileStatements(sql);
        DBSPCircuit circuit = getCircuit(compiler);
        CircuitVisitor visitor = new CircuitVisitor(new StderrErrorReporter()) {
            int mapIndex = 0;

            @Override
            public void postorder(DBSPMapIndexOperator operator) {
                this.mapIndex++;
            }

            @Override
            public void endVisit() {
                // We expect 9 MapIndex operators instead of 11 if CSE works
                Assert.assertEquals(9, this.mapIndex);
            }
        };
        visitor.apply(circuit);
    }

    @Test
    public void missingCast() {
        String sql = """
                create table TRANSACTION (unix_time BIGINT LATENESS 0);
                """;
        this.compileRustTestCase(sql);
    }

    @Test
    public void testDiv() {
        this.qs("""
            SELEct 95.0/100;
             r
            ----
             0.95
            (1 row)
            
            SELEct 95/100.0;
             r
            ----
             0.95
            (1 row)""");
    }

    @Test @Ignore("Calcite decorrelator fails")
    public void issue1956() {
        String sql = """
                CREATE TABLE auctions (
                  id INT NOT NULL PRIMARY KEY,
                  seller INT,
                  item TEXT
                );
                
                CREATE TABLE bids (
                  id INT NOT NULL PRIMARY KEY,
                  buyer INT,
                  auction_id INT,
                  amount INT
                );
                
                CREATE VIEW V AS SELECT id, (SELECT array_agg(buyer) FROM (
                  SELECT buyer FROM bids WHERE auction_id = auctions.id
                  ORDER BY buyer LIMIT 10
                )) FROM auctions;""";
        this.compileRustTestCase(sql);
    }

    @Test
    public void issue1957() {
        String sql = """
                CREATE TABLE warehouse (
                   id INT NOT NULL PRIMARY KEY,
                   parentId INT
                );
                
                CREATE VIEW V AS SELECT\s
                  id,
                  (SELECT ARRAY_AGG(id) FROM (
                    SELECT id FROM warehouse WHERE parentId = warehouse.id
                    ORDER BY id LIMIT 10
                  )) AS first_children
                FROM warehouse;""";
        this.compileRustTestCase(sql);
    }

    @Test
    public void issue1793() {
        String sql = """
                CREATE TABLE transaction_demographics (
                    trans_date_time TIMESTAMP,
                    cc_num BIGINT NOT NULL,
                    category STRING,
                    amt FLOAT64,
                    unix_time INTEGER NOT NULL LATENESS 86400,
                    first STRING,
                    state STRING,
                    job STRING,
                    city_pop INTEGER,
                    is_fraud BOOLEAN
                );
            
                CREATE VIEW V AS SELECT
                    cc_num,
                    CASE
                      WHEN dayofweek(trans_date_time) IN(6, 7) THEN true
                      ELSE false
                    END AS is_weekend,
                    CASE
                      WHEN hour(trans_date_time) <= 6 THEN true
                      ELSE false
                    END AS is_night,
                    category,
                    AVG(amt) OVER window_1_day AS avg_spend_pd,
                    AVG(amt) OVER window_7_day AS avg_spend_pw,
                    AVG(amt) OVER window_30_day AS avg_spend_pm,
                    COUNT(*) OVER window_1_day AS trans_freq_24,
                      amt, state, job, unix_time, city_pop, is_fraud
                  FROM transaction_demographics
                  WINDOW
                    window_1_day AS (PARTITION BY cc_num ORDER BY unix_time RANGE BETWEEN 86400 PRECEDING AND CURRENT ROW),
                    window_7_day AS (PARTITION BY cc_num ORDER BY unix_time RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW),
                    window_30_day AS (PARTITION BY cc_num ORDER BY unix_time RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW);""";
        DBSPCompiler compiler = this.testCompiler();
        compiler.options.languageOptions.incrementalize = true;
        compiler.compileStatements(sql);
        DBSPCircuit circuit = getCircuit(compiler);
        CircuitVisitor visitor = new CircuitVisitor(new StderrErrorReporter()) {
            int count = 0;

            @Override
            public void postorder(DBSPPartitionedRollingAggregateWithWaterlineOperator operator) {
                this.count++;
            }

            @Override
            public void endVisit() {
                Assert.assertEquals(3, this.count);
            }
        };
        visitor.apply(circuit);
        this.compileRustTestCase(sql);
    }
}
