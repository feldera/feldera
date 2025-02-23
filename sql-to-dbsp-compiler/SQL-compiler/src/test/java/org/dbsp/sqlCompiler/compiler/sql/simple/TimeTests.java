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

package org.dbsp.sqlCompiler.compiler.sql.simple;

import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.sql.tools.BaseSQLTests;
import org.dbsp.sqlCompiler.compiler.sql.tools.Change;
import org.dbsp.sqlCompiler.compiler.sql.tools.CompilerCircuitStream;
import org.dbsp.sqlCompiler.compiler.sql.tools.InputOutputChange;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPI32Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPI64Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPIntervalMillisLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPIntervalMonthsLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPStringLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPTimestampLiteral;
import org.dbsp.sqlCompiler.ir.expression.DBSPZSetExpression;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeMillisInterval;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeMonthsInterval;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeTimestamp;
import org.junit.Test;

public class TimeTests extends BaseSQLTests {
    public DBSPCompiler compileQuery(String query) {
        DBSPCompiler compiler = this.testCompiler();
        String ddl = "CREATE TABLE T (\n" +
                "COL1 TIMESTAMP NOT NULL" +
                ")";
        compiler.submitStatementForCompilation(ddl);
        compiler.submitStatementForCompilation(query);
        return compiler;
    }

    public void testQuery(String query, DBSPExpression... fields) {
        // T contains a date with timestamp '100'.
        query = "CREATE VIEW V AS " + query;
        DBSPCompiler compiler = this.compileQuery(query);
        CompilerCircuitStream ccs = this.getCCS(compiler);
        DBSPZSetExpression expectedOutput = new DBSPZSetExpression(new DBSPTupleExpression(fields));
        InputOutputChange change = new InputOutputChange(this.createInput(), new Change(expectedOutput));
        ccs.addChange(change);
    }

    public Change createInput() {
        return new Change(new DBSPZSetExpression(new DBSPTupleExpression(new DBSPTimestampLiteral(100))));
    }

    @Test
    public void issue1843() {
        String sql = """
                create table credit_card_transactions(transaction_time timestamp);
                create table my_timer(t timestamp);
                create view recent_transactions as
                SELECT * FROM credit_card_transactions
                WHERE transaction_time >= (SELECT t FROM my_timer) - INTERVAL 1 DAY;""";
        this.compileRustTestCase(sql);
    }

    @Test
    public void issue1844() {
        String sql = """
                create table credit_card_transactions(transaction_time timestamp);
                create table my_timer(id int NOT NULL primary key, t timestamp);

                create view recent_transactions as
                SELECT * FROM credit_card_transactions
                WHERE transaction_time >= (SELECT DATE_SUB(t, INTERVAL 1 DAY) FROM my_timer);""";
        this.compileRustTestCase(sql);
    }

    @Test
    public void testInterval() {
        this.testQuery("SELECT INTERVAL '20' YEAR",
                new DBSPIntervalMonthsLiteral(DBSPTypeMonthsInterval.Units.YEARS, 240));
        this.testQuery("SELECT INTERVAL '20-7' YEAR TO MONTH",
                new DBSPIntervalMonthsLiteral(DBSPTypeMonthsInterval.Units.YEARS_TO_MONTHS, 247));
        this.testQuery("SELECT INTERVAL '10' MONTH",
                new DBSPIntervalMonthsLiteral(DBSPTypeMonthsInterval.Units.MONTHS, 10));
        this.testQuery("SELECT INTERVAL '10' DAY",
                new DBSPIntervalMillisLiteral(DBSPTypeMillisInterval.Units.DAYS, 10L * 86400 * 1000, false));
        this.testQuery("SELECT INTERVAL '10 10' DAY TO HOUR",
                new DBSPIntervalMillisLiteral(DBSPTypeMillisInterval.Units.DAYS_TO_HOURS, 10L * 86400 * 1000 + 10 * 3600 * 1000, false));
        this.testQuery("SELECT INTERVAL '10 10:30' DAY TO MINUTE",
                new DBSPIntervalMillisLiteral(DBSPTypeMillisInterval.Units.DAYS_TO_MINUTES, 10L * 86400 * 1000 + 10 * 3600 * 1000 + 30 * 60 * 1000, false));
        this.testQuery("SELECT INTERVAL '10 10:30:40.999' DAY TO SECOND",
                new DBSPIntervalMillisLiteral(DBSPTypeMillisInterval.Units.DAYS_TO_SECONDS,
                        10L * 86400 * 1000 + 10 * 3600 * 1000 + 30 * 60 * 1000 + 40999, false));
        this.testQuery("SELECT INTERVAL '12' HOUR",
                new DBSPIntervalMillisLiteral(DBSPTypeMillisInterval.Units.HOURS,
                        12L * 3600 * 1000, false));
        this.testQuery("SELECT INTERVAL '12:10' HOUR TO MINUTE",
                new DBSPIntervalMillisLiteral(DBSPTypeMillisInterval.Units.HOURS_TO_MINUTES,
                        12L * 3600 * 1000 + 10 * 60 * 1000, false));
        this.testQuery("SELECT INTERVAL '12:10:59' HOUR TO SECOND",
                new DBSPIntervalMillisLiteral(DBSPTypeMillisInterval.Units.HOURS_TO_SECONDS,
                        12L * 3600 * 1000 + 10 * 60 * 1000 + 59000, false));
        this.testQuery("SELECT INTERVAL '10' MINUTE",
                new DBSPIntervalMillisLiteral(
                        DBSPTypeMillisInterval.Units.MINUTES, 10L * 60 * 1000, false));
        this.testQuery("SELECT INTERVAL '80:01.001' MINUTE TO SECOND",
                new DBSPIntervalMillisLiteral(
                        DBSPTypeMillisInterval.Units.MINUTES_TO_SECONDS, 80L * 60 * 1000 + 1001, false));
        this.testQuery("SELECT INTERVAL '80.001' SECOND",
                new DBSPIntervalMillisLiteral(
                        DBSPTypeMillisInterval.Units.SECONDS, 80001, false));
        this.testQuery("SELECT INTERVAL '100' HOUR(3)",
                new DBSPIntervalMillisLiteral(
                        DBSPTypeMillisInterval.Units.HOURS, 100L * 3600 * 1000, false));
        this.testQuery("SELECT INTERVAL '-1 2:03:04' DAYS TO SECONDS",
                new DBSPIntervalMillisLiteral(
                        DBSPTypeMillisInterval.Units.DAYS_TO_SECONDS, -(86400L * 1000 + 2 * 3600 * 1000 + 3 * 60 * 1000 + 4000), false));
    }

    @Test
    public void maxTest() {
        String query = """
                    SELECT MAX(COL1)
                    FROM T
                """;
        this.testQuery(query, new DBSPTimestampLiteral(
                CalciteObject.EMPTY, DBSPTypeTimestamp.NULLABLE_INSTANCE, 100L));
    }

    @Test
    public void timestampTableTest() {
        String query = "SELECT COL1 FROM T";
        this.testQuery(query, new DBSPTimestampLiteral(100));
    }

    @Test
    public void castTimestampToString() {
        String query = "SELECT CAST(T.COL1 AS STRING) FROM T";
        this.testQuery(query, new DBSPStringLiteral("1970-01-01 00:00:00"));
    }

    @Test
    public void castTimestampToStringToTimestamp() {
        String query = "SELECT CAST(CAST(T.COL1 AS STRING) AS Timestamp) FROM T";
        this.testQuery(query, new DBSPTimestampLiteral(0));
    }

    @Test
    public void secondTest() {
        String query = "SELECT SECOND(T.COL1) FROM T";
        this.testQuery(query, new DBSPI64Literal(0));
    }

    @Test
    public void minuteTest() {
        String query = "SELECT MINUTE(T.COL1) FROM T";
        this.testQuery(query, new DBSPI64Literal(0));
    }

    @Test
    public void hourTest() {
        String query = "SELECT HOUR(T.COL1) FROM T";
        this.testQuery(query, new DBSPI64Literal(0));
    }

    @Test
    public void dayTest() {
        String query = "SELECT DAYOFMONTH(T.COL1) FROM T";
        this.testQuery(query, new DBSPI64Literal(1));
    }

    @Test
    public void dayOfWeekTest() {
        String query = "SELECT DAYOFWEEK(T.COL1) FROM T";
        this.testQuery(query, new DBSPI64Literal(5));
    }

    @Test
    public void monthTest() {
        String query = "SELECT MONTH(T.COL1) FROM T";
        this.testQuery(query, new DBSPI64Literal(1));
    }

    @Test
    public void yearTest() {
        String query = "SELECT YEAR(T.COL1) FROM T";
        this.testQuery(query, new DBSPI64Literal(1970));
    }

    @Test
    public void dowTest() {
        String query = "SELECT extract (ISODOW from COL1) FROM T";
        this.testQuery(query, new DBSPI64Literal(4));
        // We know that the next date was a Thursday
        query = "SELECT extract (ISODOW from TIMESTAMP '2022-12-15')";
        this.testQuery(query, new DBSPI64Literal(4));
    }

    @Test
    public void timestampAddTableTest() {
        String query =
                "SELECT TIMESTAMPADD(SECOND, 10, COL1), " +
                " TIMESTAMPADD(HOUR, 1, COL1), " +
                " TIMESTAMPADD(MINUTE, 10, COL1) FROM T";
        this.testQuery(query,
                        new DBSPTimestampLiteral(10100),
                        new DBSPTimestampLiteral(3600100),
                        new DBSPTimestampLiteral(600100)
                        );
    }

    @Test
    public void timestampParse() {
        String query = "SELECT TIMESTAMP '2020-04-30 12:25:13.45'";
        this.testQuery(query, new DBSPTimestampLiteral(1588249513450L));
    }

    @Test
    public void timestampParseIllegal() {
        String query = "SELECT DATE '1997-02-29'";
        this.queryFailingInCompilation(query, "Illegal DATE literal");
    }

    @Test
    public void timestampDiffTest() {
        String query =
                "SELECT timestampdiff(MONTH, TIMESTAMP'2021-02-28 12:00:00', TIMESTAMP'2021-03-28 11:59:59'), " +
                "timestampdiff(MONTH, TIMESTAMP'2021-02-28 12:00:00', TIMESTAMP'2021-03-28 12:00:00'), " +
                "timestampdiff(YEAR, DATE'2021-01-01', DATE'1900-03-28')";
        this.testQuery(query,
                new DBSPI32Literal(0), new DBSPI32Literal(1), new DBSPI32Literal(-120)
                );
    }
}
