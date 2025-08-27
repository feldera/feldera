package org.dbsp.sqlCompiler.compiler.sql;

import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.circuit.operator.DBSPPartitionedRollingAggregateWithWaterlineOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.TableData;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.sql.tools.BaseSQLTests;
import org.dbsp.sqlCompiler.compiler.sql.tools.Change;
import org.dbsp.sqlCompiler.compiler.sql.tools.CompilerCircuitStream;
import org.dbsp.sqlCompiler.compiler.sql.tools.InputOutputChange;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPDoubleLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPI32Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPI64Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPStringLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPTimestampLiteral;
import org.dbsp.sqlCompiler.ir.expression.DBSPZSetExpression;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTupleBase;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeDouble;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

public class ComplexQueriesTest extends BaseSQLTests {
    @Test
    public void testMultiJoin() {
        // From SLT/select5
        String sql = """
                CREATE TABLE t29(
                  a29 INTEGER NOT NULL PRIMARY KEY,
                  b29 INTEGER,
                  x29 VARCHAR(40)
                );
                
                CREATE TABLE t31(
                  a31 INTEGER NOT NULL PRIMARY KEY,
                  b31 INTEGER,
                  x31 VARCHAR(40)
                );
                
                CREATE TABLE t51(
                  a51 INTEGER NOT NULL PRIMARY KEY,
                  b51 INTEGER,
                  x51 VARCHAR(40)
                );
                
                CREATE TABLE t55(
                  a55 INTEGER NOT NULL PRIMARY KEY,
                  b55 INTEGER,
                  x55 VARCHAR(40)
                );
                
                CREATE VIEW V AS SELECT x29,x31,x51,x55
                  FROM t51,t29,t31,t55
                 WHERE a51=b31
                   AND a29=6
                   AND a29=b51
                   AND b55=a31;
                """;
        var ccs = this.getCCS(sql);
        ccs.step("""
                INSERT INTO t29 VALUES(1,4,'table t29 row 1');
                INSERT INTO t29 VALUES(2,2,'table t29 row 2');
                INSERT INTO t29 VALUES(3,9,'table t29 row 3');
                INSERT INTO t29 VALUES(4,8,'table t29 row 4');
                INSERT INTO t29 VALUES(5,10,'table t29 row 5');
                INSERT INTO t29 VALUES(6,3,'table t29 row 6');
                INSERT INTO t29 VALUES(7,7,'table t29 row 7');
                INSERT INTO t29 VALUES(8,6,'table t29 row 8');
                INSERT INTO t29 VALUES(9,5,'table t29 row 9');
                INSERT INTO t29 VALUES(10,1,'table t29 row 10');
                
                INSERT INTO t31 VALUES(1,1,'table t31 row 1');
                INSERT INTO t31 VALUES(2,6,'table t31 row 2');
                INSERT INTO t31 VALUES(3,4,'table t31 row 3');
                INSERT INTO t31 VALUES(4,8,'table t31 row 4');
                INSERT INTO t31 VALUES(5,2,'table t31 row 5');
                INSERT INTO t31 VALUES(6,9,'table t31 row 6');
                INSERT INTO t31 VALUES(7,7,'table t31 row 7');
                INSERT INTO t31 VALUES(8,3,'table t31 row 8');
                INSERT INTO t31 VALUES(9,5,'table t31 row 9');
                INSERT INTO t31 VALUES(10,10,'table t31 row 10');
                
                INSERT INTO t51 VALUES(1,5,'table t51 row 1');
                INSERT INTO t51 VALUES(2,3,'table t51 row 2');
                INSERT INTO t51 VALUES(3,10,'table t51 row 3');
                INSERT INTO t51 VALUES(4,7,'table t51 row 4');
                INSERT INTO t51 VALUES(5,6,'table t51 row 5');
                INSERT INTO t51 VALUES(6,2,'table t51 row 6');
                INSERT INTO t51 VALUES(7,9,'table t51 row 7');
                INSERT INTO t51 VALUES(8,4,'table t51 row 8');
                INSERT INTO t51 VALUES(9,8,'table t51 row 9');
                INSERT INTO t51 VALUES(10,1,'table t51 row 10');
                
                INSERT INTO t55 VALUES(1,1,'table t55 row 1');
                INSERT INTO t55 VALUES(2,3,'table t55 row 2');
                INSERT INTO t55 VALUES(3,7,'table t55 row 3');
                INSERT INTO t55 VALUES(4,9,'table t55 row 4');
                INSERT INTO t55 VALUES(5,5,'table t55 row 5');
                INSERT INTO t55 VALUES(6,4,'table t55 row 6');
                INSERT INTO t55 VALUES(7,10,'table t55 row 7');
                INSERT INTO t55 VALUES(8,8,'table t55 row 8');
                INSERT INTO t55 VALUES(9,6,'table t55 row 9');
                INSERT INTO t55 VALUES(10,2,'table t55 row 10');""",
                 """
                 x29 | x31 | x51 | x55 | weight
                ---------------------------------
                 table t29 row 6| table t31 row 9| table t51 row 5| table t55 row 4| 1""");
        InnerVisitor typeWidth = new InnerVisitor(ccs.compiler) {
            @Override
            public void postorder(DBSPTypeTupleBase type) {
                // Without UnusedFields the width of the tuples can be 9
                Assert.assertTrue(type.size() <= 5);
            }
        };
        CircuitVisitor visitor = new CircuitVisitor(ccs.compiler) {
            @Override
            public void postorder(DBSPSimpleOperator operator) {
                operator.outputType().accept(typeWidth);
            }
        };
        ccs.visit(visitor);
    }

    @Test
    public void testDateDiff() {
        String sql = """
                create table PART_ORDER (
                    id bigint,
                    part bigint,
                    customer bigint,
                    target_date date
                );

                create table FULFILLMENT (
                    part_order bigint,
                    fulfillment_date date not null
                );

                create view FLAGGED_ORDER as
                select
                    part_order.customer,
                    AVG(DATEDIFF(day, part_order.target_date, fulfillment.fulfillment_date))
                    OVER (PARTITION BY part_order.customer
                          ORDER BY fulfillment.fulfillment_date
                          RANGE BETWEEN INTERVAL 90 days PRECEDING and CURRENT ROW) as avg_delay
                from
                    part_order
                    join
                    fulfillment
                    on part_order.id = fulfillment.part_order""";
        this.compileRustTestCase(sql);
    }

    @Test
    public void viewLateness() {
        String sql = """
                create table TRANSACTION (
                    trans_date_trans_time TIMESTAMP,
                    cc_num BIGINT,
                    merchant STRING,
                    category STRING,
                    amt DOUBLE,
                    trans_num STRING,
                    unix_time INTEGER LATENESS 0,
                    merch_lat DOUBLE,
                    merch_long DOUBLE,
                    is_fraud BIGINT
                );

                CREATE TABLE DEMOGRAPHICS (
                  cc_num BIGINT,
                  first STRING,
                  last STRING,
                  gender STRING,
                  street STRING,
                  city STRING,
                  state STRING,
                  zip BIGINT,
                  lat DOUBLE,
                  long DOUBLE,
                  city_pop BIGINT,
                  job STRING,
                  dob DATE
                );

                CREATE VIEW TRANSACTION_DEMOGRAPHICS AS
                    SELECT * FROM
                        transaction as t
                        JOIN demographics as d
                        ON t.cc_num = d.cc_num;

                LATENESS TRANSACTION_DEMOGRAPHICS.unix_time 0;

                CREATE VIEW FEATURE AS
                SELECT
                   cc_num,
                   dayofweek(trans_date_trans_time) as d,
                   CASE
                     WHEN dayofweek(trans_date_trans_time) IN(6, 7) THEN true
                     ELSE false
                   END AS is_weekend,
                   hour(trans_date_trans_time) as hour_of_day,
                   CASE
                     WHEN hour(trans_date_trans_time) <= 6 THEN true
                     ELSE false
                   END AS is_night,
                   -- Average spending per day, per week, and per month.
                   AVG(amt) OVER window_1_day AS avg_spend_pd,
                   AVG(amt) OVER window_7_day AS avg_spend_pw,
                   AVG(amt) OVER window_30_day AS avg_spend_pm,
                   -- Average spending over the last three months for the same day of the week.
                   COALESCE(
                    AVG(amt) OVER (
                      PARTITION BY cc_num, EXTRACT(DAY FROM trans_date_trans_time)
                      ORDER BY unix_time
                      RANGE BETWEEN 7776000 PRECEDING and CURRENT ROW
                    ), 0) AS avg_spend_p3m_over_d,
                   -- Number of transactions in the last 24 hours.
                   COUNT(*) OVER window_1_day AS trans_freq_24,
                   amt, unix_time, zip, city_pop, is_fraud
                FROM transaction_demographics
                WINDOW
                  window_1_day AS (PARTITION BY cc_num ORDER BY unix_time RANGE BETWEEN 86400 PRECEDING AND CURRENT ROW),
                  window_7_day AS (PARTITION BY cc_num ORDER BY unix_time RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW),
                  window_30_day AS (PARTITION BY cc_num ORDER BY unix_time RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW);
                      ;""";
        DBSPCompiler compiler = this.testCompiler();
        compiler.options.languageOptions.incrementalize = true;
        compiler.submitStatementsForCompilation(sql);
        DBSPCircuit circuit = getCircuit(compiler);
        CircuitVisitor visitor = new CircuitVisitor(compiler) {
            int count = 0;

            @Override
            public void postorder(DBSPPartitionedRollingAggregateWithWaterlineOperator operator) {
                this.count++;
            }

            @Override
            public void endVisit() {
                Assert.assertEquals(4, this.count);
            }
        };
        visitor.apply(circuit);
        this.compileRustTestCase(sql);
    }

   @Test @Ignore("Cross apply not yet implemented")
    public void testCrossApply() {
        String query = """
                 select d.DocumentID, ds.Status, ds.DateCreated
                 from Documents as d
                 cross apply
                     (select top 1 Status, DateCreated
                      from DocumentStatusLogs
                      where DocumentID = d.DocumentId
                      order by DateCreated desc) as ds
                """;
        this.compileRustTestCase(query);
    }

    @Test
    public void smallTaxiTest() {
        String statements = """
                CREATE TABLE green_tripdata
                (
                  lpep_pickup_datetime TIMESTAMP NOT NULL,
                  lpep_dropoff_datetime TIMESTAMP NOT NULL,
                  pickup_location_id BIGINT NOT NULL,
                  dropoff_location_id BIGINT NOT NULL,
                  trip_distance DOUBLE PRECISION,
                  fare_amount DOUBLE PRECISION
                );

                CREATE VIEW V AS
                SELECT
                *,
                COUNT(*) OVER(
                   PARTITION BY  pickup_location_id
                   ORDER BY  extract (EPOCH from  CAST (lpep_pickup_datetime AS TIMESTAMP) )
                   -- 1 hour is 3600  seconds
                   RANGE BETWEEN 3600  PRECEDING AND 1 PRECEDING ) AS count_trips_window_1h_pickup_zip
                FROM green_tripdata;""";
        this.compileRustTestCase(statements);
    }

    @Test
    public void testProducts() {
        String script = """
                -- create a table
                CREATE TABLE "New Products" (
                    ProductName VARCHAR NOT NULL,
                    Price INT NOT NULL);
                -- statements separated by semicolons
                -- create a view
                CREATE VIEW "Products Above Average Price" AS
                SELECT ProductName, Price
                FROM "New Products"
                WHERE Price > (SELECT AVG(Price) FROM "New Products")
                -- no semicolon at end""";
        this.compileRustTestCase(script);
    }

    @Test
    public void demographicsTest() {
        String script =
                """
            CREATE TABLE demographics (
                cc_num FLOAT64 NOT NULL,
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

            CREATE TABLE transactions (
                trans_date_trans_time TIMESTAMP NOT NULL,
                cc_num FLOAT64 NOT NULL,
                merchant STRING,
                category STRING,
                amt FLOAT64,
                trans_num STRING,
                unix_time INTEGER NOT NULL,
                merch_lat FLOAT64 NOT NULL,
                merch_long FLOAT64 NOT NULL,
                is_fraud INTEGER
            );

            CREATE VIEW features as
                SELECT
                    DAYOFWEEK(trans_date_trans_time) AS d,
                    TIMESTAMPDIFF(YEAR, trans_date_trans_time, CAST(dob as TIMESTAMP)) AS age,
                    ST_DISTANCE(ST_POINT(long,lat), ST_POINT(merch_long,merch_lat)) AS distance,
                    TIMESTAMPDIFF(MINUTE, trans_date_trans_time, last_txn_date) AS trans_diff,
                    AVG(amt) OVER(
                        PARTITION BY   CAST(cc_num AS NUMERIC)
                        ORDER BY unix_time
                        -- 1 week is 604800  seconds
                        RANGE BETWEEN 604800  PRECEDING AND 1 PRECEDING) AS
                    avg_spend_pw,
                    AVG(amt) OVER(
                        PARTITION BY  CAST(cc_num AS NUMERIC)
                        ORDER BY unix_time
                        -- 1 month(30 days) is 2592000 seconds
                        RANGE BETWEEN 2592000 PRECEDING AND 1 PRECEDING) AS
                    avg_spend_pm,
                    COUNT(*) OVER(
                        PARTITION BY  CAST(cc_num AS NUMERIC)
                        ORDER BY unix_time
                        -- 1 day is 86400  seconds
                        RANGE BETWEEN 86400  PRECEDING AND 1 PRECEDING ) AS
                    trans_freq_24,
                    category,
                    amt,
                    state,
                    job,
                    unix_time,
                    city_pop,
                    merchant,
                    is_fraud
                FROM (
                    SELECT t1.*, t2.*, LAG(trans_date_trans_time, 1) OVER
                           (PARTITION BY t1.cc_num  ORDER BY trans_date_trans_time ASC) AS last_txn_date
                    FROM  transactions AS t1
                    LEFT JOIN  demographics AS t2
                    ON t1.cc_num = t2.cc_num);""";
        DBSPCompiler compiler = testCompiler();
        compiler.submitStatementsForCompilation(script);
        TableData demographics = new TableData("demographics",
                new DBSPZSetExpression(new DBSPTupleExpression(
                        new DBSPDoubleLiteral(0.0),
                        new DBSPStringLiteral("First", true),
                        new DBSPStringLiteral("Male", true),
                        new DBSPStringLiteral("Street", true),
                        new DBSPStringLiteral("City", true),
                        new DBSPStringLiteral("State", true),
                        new DBSPI32Literal(94043, true),
                        //new DBSPDoubleLiteral(128.0, true),
                        DBSPLiteral.none(new DBSPTypeDouble(CalciteObject.EMPTY,true)),
                        new DBSPDoubleLiteral(128.0, true),
                        new DBSPI32Literal(100000, true),
                        new DBSPStringLiteral("Job", true),
                        new DBSPStringLiteral("2020-02-20", true)
                        )));
        TableData transactions = new TableData("transactions",
                new DBSPZSetExpression(new DBSPTupleExpression(
                        new DBSPTimestampLiteral("2020-02-20 10:00:00", false),
                        new DBSPDoubleLiteral(0.0, false),
                        new DBSPStringLiteral("Merchant", true),
                        new DBSPStringLiteral("Category", true),
                        new DBSPDoubleLiteral(10.0, true),
                        new DBSPStringLiteral("Transnum", true),
                        new DBSPI32Literal(1000),
                        new DBSPDoubleLiteral(128.0),
                        new DBSPDoubleLiteral(128.0),
                        new DBSPI32Literal(0, true)
                )));
        InputOutputChange ip = new InputOutputChange(new Change(demographics, transactions), new Change());
        CompilerCircuitStream ccs = this.getCCS(compiler);
        ccs.addChange(ip);
    }

    @Test
    public void calcite6020issueTest() {
        String script =
                """
            CREATE TABLE transactions (
                cc_num BIGINT NOT NULL,
                amt FLOAT64,
                unix_time INTEGER NOT NULL
            );

            CREATE VIEW features as
                SELECT
                    AVG(amt) OVER(
                        PARTITION BY cc_num
                        ORDER BY unix_time
                        RANGE BETWEEN 2592000 PRECEDING AND 1 PRECEDING) AS
                    avg_spend_pm,
                    COUNT(*) OVER(
                        PARTITION BY cc_num
                        ORDER BY unix_time
                        RANGE BETWEEN 86400  PRECEDING AND 1 PRECEDING ) AS
                    trans_freq_24
                FROM transactions AS t1;""";
        DBSPCompiler compiler = testCompiler();
        compiler.submitStatementsForCompilation(script);
        TableData transactions = new TableData("transactions",
                new DBSPZSetExpression(new DBSPTupleExpression(
                        new DBSPI64Literal(0, false),
                        new DBSPDoubleLiteral(10.0, true),
                        new DBSPI32Literal(1000)
                )));
        InputOutputChange ip = new InputOutputChange(new Change(transactions), new Change());
        CompilerCircuitStream ccs = this.getCCS(compiler);
        ccs.addChange(ip);
    }

    @Test
    public void fraudDetectionTest() {
        // fraudDetection-352718.cc_data.demo_
        String sql = """
                CREATE TABLE demographics (
                 cc_num FLOAT64 NOT NULL,
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
                 dob DATE
                );
                CREATE TABLE transactions (
                 trans_date_trans_time TIMESTAMP NOT NULL,
                 cc_num FLOAT64,
                 merchant STRING,
                 category STRING,
                 amt FLOAT64,
                 trans_num STRING,
                 unix_time INTEGER NOT NULL,
                 merch_lat FLOAT64,
                 merch_long FLOAT64,
                 is_fraud INTEGER
                );
                CREATE VIEW V AS SELECT
                    DAYOFWEEK(trans_date_trans_time) AS d,
                    TIMESTAMPDIFF(YEAR, trans_date_trans_time, CAST(dob as TIMESTAMP)) AS age,
                    ST_DISTANCE(ST_POINT(long,lat), ST_POINT(merch_long,merch_lat)) AS distance,
                    TIMESTAMPDIFF(MINUTE, trans_date_trans_time, last_txn_date) AS trans_diff,
                    AVG(amt) OVER(
                                PARTITION BY   CAST(cc_num AS NUMERIC)
                                ORDER BY unix_time
                                -- 1 week is 604800  seconds
                                RANGE BETWEEN 604800  PRECEDING AND 1 PRECEDING) AS
                avg_spend_pw,
                      AVG(amt) OVER(
                                PARTITION BY  CAST(cc_num AS NUMERIC)
                                ORDER BY unix_time
                                -- 1 month(30 days) is 2592000 seconds
                                RANGE BETWEEN 2592000 PRECEDING AND 1 PRECEDING) AS
                avg_spend_pm,
                    COUNT(*) OVER(
                                PARTITION BY  CAST(cc_num AS NUMERIC)
                                ORDER BY unix_time
                                -- 1 day is 86400  seconds
                                RANGE BETWEEN 86400  PRECEDING AND 1 PRECEDING ) AS
                trans_freq_24,
                  category,
                    amt,
                    state,
                    job,
                    unix_time,
                    city_pop,
                    merchant,
                    is_fraud
                  FROM (
                          SELECT t1.*, t2.*, LAG(trans_date_trans_time, 1)
                          OVER (PARTITION BY t1.cc_num ORDER BY trans_date_trans_time ASC) AS last_txn_date
                          FROM  transactions AS t1
                          LEFT JOIN  demographics AS t2
                          ON t1.cc_num =t2.cc_num)""";
        this.compileRustTestCase(sql);
    }
}
