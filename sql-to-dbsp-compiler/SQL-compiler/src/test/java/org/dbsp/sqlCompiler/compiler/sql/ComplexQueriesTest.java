/*
 * Copyright 2022 VMware, Inc.
 * SPDX-License-Identifier: MIT
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package org.dbsp.sqlCompiler.compiler.sql;

import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.CalciteObject;
import org.dbsp.sqlCompiler.compiler.sql.simple.InputOutputPair;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPDoubleLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPI32Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPStringLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPTimestampLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPZSetLiteral;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeDouble;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

@SuppressWarnings("SpellCheckingInspection")
public class ComplexQueriesTest extends BaseSQLTests {
    @Test @Ignore("OVER requires only integers")
    public void testDateDiff() {
        String sql = "create table PART_ORDER (\n" +
                "    id bigint,\n" +
                "    part bigint,\n" +
                "    customer bigint,\n" +
                "    target_date date\n" +
                ");\n" +
                "\n" +
                "create table FULFILLMENT (\n" +
                "    part_order bigint,\n" +
                "    fulfillment_date date not null\n" +
                ");\n" +
                "\n" +
                "create view FLAGGED_ORDER as\n" +
                "select\n" +
                "    part_order.customer,\n" +
                "    AVG(DATEDIFF(day, part_order.target_date, fulfillment.fulfillment_date))\n" +
                "    OVER (PARTITION BY part_order.customer\n" +
                "          ORDER BY fulfillment.fulfillment_date\n" +
                "          RANGE BETWEEN INTERVAL 90 days PRECEDING and CURRENT ROW) as avg_delay\n" +
                "from\n" +
                "    part_order\n" +
                "    join\n" +
                "    fulfillment\n" +
                "    on part_order.id = fulfillment.part_order";
        DBSPCompiler compiler = this.testCompiler();
        compiler.compileStatements(sql);
        Assert.assertFalse(compiler.hasErrors());
        this.addRustTestCase("ComplexQueriesTest.testDateDiff", compiler, getCircuit(compiler));
    }

    /*
    @Test @Ignore("Not yet tested")
    public void testCrossApply() {
        String query = " select d.DocumentID, ds.Status, ds.DateCreated \n" +
                " from Documents as d \n" +
                " cross apply \n" +
                "     (select top 1 Status, DateCreated\n" +
                "      from DocumentStatusLogs \n" +
                "      where DocumentID = d.DocumentId\n" +
                "      order by DateCreated desc) as ds";
    }
     */

    @Test
    public void smallTaxiTest() {
        String ddl = "CREATE TABLE green_tripdata\n" +
                "(\n" +
                "  lpep_pickup_datetime TIMESTAMP NOT NULL,\n" +
                "  lpep_dropoff_datetime TIMESTAMP NOT NULL,\n" +
                "  pickup_location_id BIGINT NOT NULL,\n" +
                "  dropoff_location_id BIGINT NOT NULL,\n" +
                "  trip_distance DOUBLE PRECISION,\n" +
                "  fare_amount DOUBLE PRECISION \n" +
                ")";
        String query =
                "SELECT\n" +
                        "*,\n" +
                        "COUNT(*) OVER(\n" +
                        "   PARTITION BY  pickup_location_id\n" +
                        "   ORDER BY  extract (EPOCH from  CAST (lpep_pickup_datetime AS TIMESTAMP) ) \n" +
                        "   -- 1 hour is 3600  seconds\n" +
                        "   RANGE BETWEEN 3600  PRECEDING AND 1 PRECEDING ) AS count_trips_window_1h_pickup_zip\n" +
                        "FROM green_tripdata";
        DBSPCompiler compiler = this.testCompiler();
        query = "CREATE VIEW V AS (" + query + ")";
        compiler.compileStatement(ddl);
        compiler.compileStatement(query);
        Assert.assertFalse(compiler.hasErrors());
        this.addRustTestCase("ComplexQueriesTest.smallTaxiTest", compiler, getCircuit(compiler));
    }

    @Test
    public void testProducts() {
        String script = "-- create a table\n" +
                "CREATE TABLE Products (\n" +
                "    ProductName VARCHAR NOT NULL,\n" +
                "    Price INT NOT NULL);\n" +
                "-- statements separated by semicolons\n" +
                "-- create a view\n" +
                "CREATE VIEW \"Products Above Average Price\" AS\n" +
                "SELECT ProductName, Price\n" +
                "FROM Products\n" +
                "WHERE Price > (SELECT AVG(Price) FROM Products)\n" +
                "-- no semicolon at end ";
        DBSPCompiler compiler = testCompiler();
        compiler.compileStatements(script);
    }

    @Test
    public void demographicsTest() {
        // TODO: LAG is disabled
        String script =
                "CREATE TABLE demographics (\n" +
                "    cc_num FLOAT64 NOT NULL,\n" +
                "    first STRING,\n" +
                "    gender STRING,\n" +
                "    street STRING,\n" +
                "    city STRING,\n" +
                "    state STRING,\n" +
                "    zip INTEGER,\n" +
                "    lat FLOAT64,\n" +
                "    long FLOAT64,\n" +
                "    city_pop INTEGER,\n" +
                "    job STRING,\n" +
                "    dob STRING\n" +
                ");\n" +
                "\n" +
                "CREATE TABLE transactions (\n" +
                "    trans_date_trans_time TIMESTAMP NOT NULL,\n" +
                "    cc_num FLOAT64 NOT NULL,\n" +
                "    merchant STRING,\n" +
                "    category STRING,\n" +
                "    amt FLOAT64,\n" +
                "    trans_num STRING,\n" +
                "    unix_time INTEGER NOT NULL,\n" +
                "    merch_lat FLOAT64 NOT NULL,\n" +
                "    merch_long FLOAT64 NOT NULL,\n" +
                "    is_fraud INTEGER\n" +
                ");\n" +
                "\n" +
                "CREATE VIEW features as\n" +
                "    SELECT\n" +
                "        DAYOFWEEK(trans_date_trans_time) AS d,\n" +
                "        TIMESTAMPDIFF(YEAR, trans_date_trans_time, CAST(dob as TIMESTAMP)) AS age,\n" +
                "        ST_DISTANCE(ST_POINT(long,lat), ST_POINT(merch_long,merch_lat)) AS distance,\n" +
                "        -- TIMESTAMPDIFF(MINUTE, trans_date_trans_time, last_txn_date) AS trans_diff,\n" +
                "        AVG(amt) OVER(\n" +
                "            PARTITION BY   CAST(cc_num AS NUMERIC)\n" +
                "            ORDER BY unix_time\n" +
                "            -- 1 week is 604800  seconds\n" +
                "            RANGE BETWEEN 604800  PRECEDING AND 1 PRECEDING) AS\n" +
                "        avg_spend_pw,\n" +
                "        AVG(amt) OVER(\n" +
                "            PARTITION BY  CAST(cc_num AS NUMERIC)\n" +
                "            ORDER BY unix_time\n" +
                "            -- 1 month(30 days) is 2592000 seconds\n" +
                "            RANGE BETWEEN 2592000 PRECEDING AND 1 PRECEDING) AS\n" +
                "        avg_spend_pm,\n" +
                "        COUNT(*) OVER(\n" +
                "            PARTITION BY  CAST(cc_num AS NUMERIC)\n" +
                "            ORDER BY unix_time\n" +
                "            -- 1 day is 86400  seconds\n" +
                "            RANGE BETWEEN 86400  PRECEDING AND 1 PRECEDING ) AS\n" +
                "        trans_freq_24,\n" +
                "        category,\n" +
                "        amt,\n" +
                "        state,\n" +
                "        job,\n" +
                "        unix_time,\n" +
                "        city_pop,\n" +
                "        merchant,\n" +
                "        is_fraud\n" +
                "    FROM (\n" +
                "        SELECT t1.*, t2.*\n" +
                "               -- , LAG(trans_date_trans_time, 1) OVER " +
                "               -- (PARTITION BY t1.cc_num  ORDER BY trans_date_trans_time ASC) AS last_txn_date\n" +
                "        FROM  transactions AS t1\n" +
                "        LEFT JOIN  demographics AS t2\n" +
                "        ON t1.cc_num = t2.cc_num);";
        DBSPCompiler compiler = testCompiler();
        compiler.compileStatements(script);
        DBSPZSetLiteral.Contents[] inputs = new DBSPZSetLiteral.Contents[] {
                new DBSPZSetLiteral.Contents(new DBSPTupleExpression(
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
                        )),
                new DBSPZSetLiteral.Contents(new DBSPTupleExpression(
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
                ))
        };
        DBSPZSetLiteral.Contents[] outputs = new DBSPZSetLiteral.Contents[] {};
        InputOutputPair ip = new InputOutputPair(inputs, outputs);
        this.addRustTestCase("ComplexQueriesTest.demographicsTest", compiler, getCircuit(compiler), ip);
    }

    // Test for https://github.com/feldera/feldera/issues/1151
    @Test
    public void primaryKeyTest() {
        String sql = "CREATE TABLE event_t ( id BIGINT NOT NULL PRIMARY KEY, local_event_dt DATE )";
        DBSPCompiler compiler = testCompiler();
        compiler.compileStatements(sql);
        DBSPCircuit circuit = getCircuit(compiler);
        this.addRustTestCase(sql, compiler, circuit);
    }

    @Test
    public void taxiTest() {
        String ddl = "CREATE TABLE green_tripdata\n" +
                "(\n" +
                "        lpep_pickup_datetime TIMESTAMP NOT NULL LATENESS INTERVAL '1:00' HOURS TO MINUTES,\n" +
                "        lpep_dropoff_datetime TIMESTAMP NOT NULL LATENESS INTERVAL '1:00' HOURS TO MINUTES,\n" +
                "        pickup_location_id BIGINT NOT NULL,\n" +
                "        dropoff_location_id BIGINT NOT NULL,\n" +
                "        trip_distance DOUBLE PRECISION,\n" +
                "        fare_amount DOUBLE PRECISION \n" +
                ")";
        String query =
                "SELECT\n" +
                        "*,\n" +
                        "COUNT(*) OVER(\n" +
                        "   PARTITION BY  pickup_location_id\n" +
                        "   ORDER BY  extract (EPOCH from  CAST (lpep_pickup_datetime AS TIMESTAMP) ) \n" +
                        "   -- 1 hour is 3600  seconds\n" +
                        "   RANGE BETWEEN 3600  PRECEDING AND 1 PRECEDING ) AS count_trips_window_1h_pickup_zip,\n" +
                        "AVG(fare_amount) OVER(\n" +
                        "   PARTITION BY  pickup_location_id\n" +
                        "   ORDER BY  extract (EPOCH from  CAST (lpep_pickup_datetime AS TIMESTAMP) ) \n" +
                        "   -- 1 hour is 3600  seconds\n" +
                        "   RANGE BETWEEN 3600  PRECEDING AND 1 PRECEDING ) AS mean_fare_window_1h_pickup_zip,\n" +
                        "COUNT(*) OVER(\n" +
                        "   PARTITION BY  dropoff_location_id\n" +
                        "   ORDER BY  extract (EPOCH from  CAST (lpep_dropoff_datetime AS TIMESTAMP) ) \n" +
                        "   -- 0.5 hour is 1800  seconds\n" +
                        "   RANGE BETWEEN 1800  PRECEDING AND 1 PRECEDING ) AS count_trips_window_30m_dropoff_zip,\n" +
                        "case when extract (ISODOW from  CAST (lpep_dropoff_datetime AS TIMESTAMP))  > 5 " +
                        "     then 1 else 0 end as dropoff_is_weekend\n" +
                        "FROM green_tripdata";
        DBSPCompiler compiler = testCompiler();
        query = "CREATE VIEW V AS (" + query + ")";
        compiler.compileStatement(ddl);
        compiler.compileStatement(query);
        DBSPCircuit circuit = getCircuit(compiler);
        this.addRustTestCase("ComplexQueriesTest.taxiTest", compiler, circuit);
    }

    @Test
    public void fraudDetectionTest() {
        // fraudDetection-352718.cc_data.demo_
        String ddl0 = "CREATE TABLE demographics (\n" +
                " cc_num FLOAT64 NOT NULL,\n" +
                " first STRING,\n" +
                " gender STRING,\n" +
                " street STRING,\n" +
                " city STRING,\n" +
                " state STRING,\n" +
                " zip INTEGER,\n" +
                " lat FLOAT64,\n" +
                " long FLOAT64,\n" +
                " city_pop INTEGER,\n" +
                " job STRING,\n" +
                " dob DATE\n" +
                ")";
        String ddl1 = "CREATE TABLE transactions (\n" +
                " trans_date_trans_time TIMESTAMP NOT NULL,\n" +
                " cc_num FLOAT64,\n" +
                " merchant STRING,\n" +
                " category STRING,\n" +
                " amt FLOAT64,\n" +
                " trans_num STRING,\n" +
                " unix_time INTEGER NOT NULL,\n" +
                " merch_lat FLOAT64,\n" +
                " merch_long FLOAT64,\n" +
                " is_fraud INTEGER\n" +
                ")";
        String query = "SELECT\n" +
                "    DAYOFWEEK(trans_date_trans_time) AS d,\n" +
                "    TIMESTAMPDIFF(YEAR, trans_date_trans_time, CAST(dob as TIMESTAMP)) AS age,\n" +
                "    ST_DISTANCE(ST_POINT(long,lat), ST_POINT(merch_long,merch_lat)) AS distance,\n" +
                "    -- TIMESTAMPDIFF(MINUTE, trans_date_trans_time, last_txn_date) AS trans_diff,\n" +
                "    AVG(amt) OVER(\n" +
                "                PARTITION BY   CAST(cc_num AS NUMERIC)\n" +
                "                ORDER BY unix_time\n" +
                "                -- 1 week is 604800  seconds\n" +
                "                RANGE BETWEEN 604800  PRECEDING AND 1 PRECEDING) AS\n" +
                "avg_spend_pw,\n" +
                "      AVG(amt) OVER(\n" +
                "                PARTITION BY  CAST(cc_num AS NUMERIC)\n" +
                "                ORDER BY unix_time\n" +
                "                -- 1 month(30 days) is 2592000 seconds\n" +
                "                RANGE BETWEEN 2592000 PRECEDING AND 1 PRECEDING) AS\n" +
                "avg_spend_pm,\n" +
                "    COUNT(*) OVER(\n" +
                "                PARTITION BY  CAST(cc_num AS NUMERIC)\n" +
                "                ORDER BY unix_time\n" +
                "                -- 1 day is 86400  seconds\n" +
                "                RANGE BETWEEN 86400  PRECEDING AND 1 PRECEDING ) AS\n" +
                "trans_freq_24,\n" +
                "  category,\n" +
                "    amt,\n" +
                "    state,\n" +
                "    job,\n" +
                "    unix_time,\n" +
                "    city_pop,\n" +
                "    merchant,\n" +
                "    is_fraud\n" +
                "  FROM (\n" +
                "          SELECT t1.*, t2.*\n" +
                "          --,    LAG(trans_date_trans_time, 1) OVER (PARTITION BY t1.cc_num\n" +
                "          -- ORDER BY trans_date_trans_time ASC) AS last_txn_date\n" +
                "          FROM  transactions AS t1\n" +
                "          LEFT JOIN  demographics AS t2\n" +
                "          ON t1.cc_num =t2.cc_num)";
        DBSPCompiler compiler = testCompiler();
        query = "CREATE VIEW V AS (" + query + ")";
        compiler.compileStatement(ddl0);
        compiler.compileStatement(ddl1);
        compiler.compileStatement(query);
        this.addRustTestCase("ComplexQueriesTest.fraudDetectionTest", compiler, getCircuit(compiler));
    }
}
