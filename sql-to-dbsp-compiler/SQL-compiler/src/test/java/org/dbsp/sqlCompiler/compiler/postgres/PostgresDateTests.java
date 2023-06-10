/*
 * Copyright 2023 VMware, Inc.
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

package org.dbsp.sqlCompiler.compiler.postgres;

import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.compiler.BaseSQLTests;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.frontend.CalciteToDBSPCompiler;
import org.dbsp.sqlCompiler.compiler.backend.DBSPCompiler;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.*;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeDate;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeInteger;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeMillisInterval;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeTimestamp;
import org.dbsp.util.Linq;
import org.junit.Ignore;
import org.junit.Test;

import javax.annotation.Nullable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Tests manually adapted from
 * https://github.com/postgres/postgres/blob/master/src/test/regress/expected/date.out
 */
@SuppressWarnings("JavadocLinkAsPlainText")
public class PostgresDateTests extends BaseSQLTests {
    static final DBSPType nullableDate = DBSPTypeDate.NULLABLE_INSTANCE;
    
    // CREATE TABLE DATE_TBL (f1 date);
    //INSERT INTO DATE_TBL VALUES ('1957-04-09');
    //INSERT INTO DATE_TBL VALUES ('1957-06-13');
    //INSERT INTO DATE_TBL VALUES ('1996-02-28');
    //INSERT INTO DATE_TBL VALUES ('1996-02-29');
    //INSERT INTO DATE_TBL VALUES ('1996-03-01');
    //INSERT INTO DATE_TBL VALUES ('1996-03-02');
    //INSERT INTO DATE_TBL VALUES ('1997-02-28');
    //INSERT INTO DATE_TBL VALUES ('1997-02-29');
    //ERROR:  date/time field value out of range: "1997-02-29"
    //LINE 1: INSERT INTO DATE_TBL VALUES ('1997-02-29');
    //                                     ^
    //INSERT INTO DATE_TBL VALUES ('1997-03-01');
    //INSERT INTO DATE_TBL VALUES ('1997-03-02');
    //INSERT INTO DATE_TBL VALUES ('2000-04-01');
    //INSERT INTO DATE_TBL VALUES ('2000-04-02');
    //INSERT INTO DATE_TBL VALUES ('2000-04-03');
    //INSERT INTO DATE_TBL VALUES ('2038-04-08');
    //INSERT INTO DATE_TBL VALUES ('2039-04-09');
    //INSERT INTO DATE_TBL VALUES ('2040-04-10');
    //INSERT INTO DATE_TBL VALUES ('2040-04-10 BC');

    public DBSPCompiler compileQuery(String query, int optimizationLevel) {
        String data = "CREATE TABLE DATE_TBL (f1 date);\n" +
                "INSERT INTO DATE_TBL VALUES ('1957-04-09');\n" +
                "INSERT INTO DATE_TBL VALUES ('1957-06-13');\n" +
                "INSERT INTO DATE_TBL VALUES ('1996-02-28');\n" +
                "INSERT INTO DATE_TBL VALUES ('1996-02-29');\n" +
                "INSERT INTO DATE_TBL VALUES ('1996-03-01');\n" +
                "INSERT INTO DATE_TBL VALUES ('1996-03-02');\n" +
                "INSERT INTO DATE_TBL VALUES ('1997-02-28');\n" +
                // illegal date; this fails in Postgres, but inserts a NULL in Calcite.
                "INSERT INTO DATE_TBL VALUES ('1997-02-29');\n" +
                "INSERT INTO DATE_TBL VALUES ('1997-03-01');\n" +
                "INSERT INTO DATE_TBL VALUES ('1997-03-02');\n" +
                "INSERT INTO DATE_TBL VALUES ('2000-04-01');\n" +
                "INSERT INTO DATE_TBL VALUES ('2000-04-02');\n" +
                "INSERT INTO DATE_TBL VALUES ('2000-04-03');\n" +
                "INSERT INTO DATE_TBL VALUES ('2038-04-08');\n" +
                "INSERT INTO DATE_TBL VALUES ('2039-04-09');\n" +
                "INSERT INTO DATE_TBL VALUES ('2040-04-10');\n";
                // Calcite does not seem to support dates BC
                //"INSERT INTO DATE_TBL VALUES ('2040-04-10 BC');";
        CompilerOptions options = new CompilerOptions();
        options.optimizerOptions.optimizationLevel = optimizationLevel;
        options.optimizerOptions.generateInputForEveryTable = true;
        DBSPCompiler compiler = new DBSPCompiler(options);
        compiler.compileStatements(data);
        compiler.compileStatement(query);
        return compiler;
    }

    void testQuery(String query, DBSPZSetLiteral.Contents expectedOutput, boolean optimize) {
        query = "CREATE VIEW V AS " + query;
        DBSPCompiler compiler = this.compileQuery(query, optimize ? 2 : 0);
        compiler.throwIfErrorsOccurred();
        DBSPCircuit circuit = getCircuit(compiler);
        DBSPZSetLiteral.Contents input = compiler.getTableContents().getTableContents("DATE_TBL");
        InputOutputPair streams = new InputOutputPair(input, expectedOutput);
        this.addRustTestCase(compiler, circuit, streams);
    }

    void testQueryTwice(String query, long expectedValue) {
        // by running with optimizations disabled we test runtime evaluation
        DBSPZSetLiteral.Contents zset = new DBSPZSetLiteral.Contents(
                new DBSPTupleExpression(new DBSPI64Literal(expectedValue)));
        this.testQuery(query, zset, false);
        this.testQuery(query, zset, true);
    }

    static final SimpleDateFormat inputFormat = new SimpleDateFormat("MM-dd-yyyy");
    static final SimpleDateFormat outputFormats = new SimpleDateFormat("yyyy-MM-dd");

    /**
     * Convert a date from the MM-DD-YYYY format (which is used in the Postgres output)
     * to YYYY-MM-DD
     */
    static DBSPExpression reformatDate(@Nullable String date) {
        if (date == null)
            return DBSPLiteral.none(DBSPTypeTimestamp.NULLABLE_INSTANCE);
        try {
            Date converted = inputFormat.parse(date);
            String out = outputFormats.format(converted);
            return new DBSPDateLiteral(out, true);
        } catch (ParseException ex) {
            throw new RuntimeException("Could not parse " + date);
        }
    }

    static DBSPZSetLiteral.Contents makeSet(String[] data) {
        DBSPType type = nullableDate;
        DBSPZSetLiteral.Contents result = DBSPZSetLiteral.Contents.emptyWithElementType(
                new DBSPTypeTuple(type));
        for (String s: data) {
            DBSPExpression value;
            if (s == null)
                value = DBSPLiteral.none(type);
            else
                value = reformatDate(s);
            result.add(new DBSPTupleExpression(value));
        }
        return result;
    }
    
    @Test
    public void testSelect() {
        // SELECT f1 FROM DATE_TBL;
        String query = "SELECT f1 FROM DATE_TBL";
        String[] results = {
                "04-09-1957",
                "06-13-1957",
                "02-28-1996",
                "02-29-1996",
                "03-01-1996",
                "03-02-1996",
                "02-28-1997",
                "03-01-1997",
                "03-02-1997",
                "04-01-2000",
                "04-02-2000",
                "04-03-2000",
                "04-08-2038",
                "04-09-2039",
                "04-10-2040",
                //"04-10-2040 BC",
                null
        };
        DBSPZSetLiteral.Contents result = makeSet(results);
        this.testQuery(query, result, true);
    }

    @Test
    public void testLt() {
        // SELECT f1 FROM DATE_TBL WHERE f1 < '2000-01-01';
        String query = "SELECT f1 FROM DATE_TBL WHERE f1 < '2000-01-01'";
        String[] results = {
                "04-09-1957",
                "06-13-1957",
                "02-28-1996",
                "02-29-1996",
                "03-01-1996",
                "03-02-1996",
                "02-28-1997",
                "03-01-1997",
                "03-02-1997",
                //" 04-10-2040 BC",
        };
        DBSPZSetLiteral.Contents result = makeSet(results);
        this.testQuery(query, result, true);
    }

    @Test
    public void testBetween() {
        // SELECT f1 FROM DATE_TBL
        //  WHERE f1 BETWEEN '2000-01-01' AND '2001-01-01';
        String query = "SELECT f1 FROM DATE_TBL\n" +
                "  WHERE f1 BETWEEN '2000-01-01' AND '2001-01-01'";
        String[] results = {
                "04-01-2000",
                "04-02-2000",
                "04-03-2000"
        };
        DBSPZSetLiteral.Contents result = makeSet(results);
        this.testQuery(query, result, true);
    }

    // SELECT date '1999-01-08';
    //    date
    //------------
    // 1999-01-08
    //(1 row)
    //
    //SELECT date '1999-01-18';
    //    date
    //------------
    // 1999-01-18
    //(1 row)
    //
    //SELECT date '1/8/1999';
    //ERROR:  date/time field value out of range: "1/8/1999"
    //LINE 1: SELECT date '1/8/1999';
    //                    ^
    //HINT:  Perhaps you need a different "datestyle" setting.
    //SELECT date '1/18/1999';
    //ERROR:  date/time field value out of range: "1/18/1999"
    //LINE 1: SELECT date '1/18/1999';
    //                    ^
    //HINT:  Perhaps you need a different "datestyle" setting.
    //SELECT date '18/1/1999';
    //ERROR:  date/time field value out of range: "18/1/1999"
    //LINE 1: SELECT date '18/1/1999';
    //                    ^
    //HINT:  Perhaps you need a different "datestyle" setting.
    //SELECT date '01/02/03';
    //    date
    //------------
    // 2001-02-03
    //(1 row)
    //
    //SELECT date '19990108';
    //    date
    //------------
    // 1999-01-08
    //(1 row)
    //
    //SELECT date '990108';
    //    date
    //------------
    // 1999-01-08
    //(1 row)
    //
    //SELECT date '1999.008';
    //    date
    //------------
    // 1999-01-08
    //(1 row)
    //
    //SELECT date 'J2451187';
    //    date
    //------------
    // 1999-01-08
    //(1 row)
    //
    //SELECT date 'January 8, 99 BC';
    //ERROR:  date/time field value out of range: "January 8, 99 BC"
    //LINE 1: SELECT date 'January 8, 99 BC';
    //                    ^
    //HINT:  Perhaps you need a different "datestyle" setting.
    //SELECT date '99-Jan-08';
    //    date
    //------------
    // 1999-01-08
    //(1 row)
    //
    //SELECT date '1999-Jan-08';
    //    date
    //------------
    // 1999-01-08
    //(1 row)
    //
    //SELECT date '08-Jan-99';
    //ERROR:  date/time field value out of range: "08-Jan-99"
    //LINE 1: SELECT date '08-Jan-99';
    //                    ^
    //HINT:  Perhaps you need a different "datestyle" setting.
    //SELECT date '08-Jan-1999';
    //    date
    //------------
    // 1999-01-08
    //(1 row)
    //
    //SELECT date 'Jan-08-99';
    //ERROR:  date/time field value out of range: "Jan-08-99"
    //LINE 1: SELECT date 'Jan-08-99';
    //                    ^
    //HINT:  Perhaps you need a different "datestyle" setting.
    //SELECT date 'Jan-08-1999';
    //    date
    //------------
    // 1999-01-08
    //(1 row)
    //
    //SELECT date '99-08-Jan';
    //ERROR:  invalid input syntax for type date: "99-08-Jan"
    //LINE 1: SELECT date '99-08-Jan';
    //                    ^
    //SELECT date '1999-08-Jan';
    //ERROR:  invalid input syntax for type date: "1999-08-Jan"
    //LINE 1: SELECT date '1999-08-Jan';
    //                    ^
    //SELECT date '99 Jan 08';
    //    date
    //------------
    // 1999-01-08
    //(1 row)
    //
    //SELECT date '1999 Jan 08';
    //    date
    //------------
    // 1999-01-08
    //(1 row)
    //
    //SELECT date '08 Jan 99';
    //ERROR:  date/time field value out of range: "08 Jan 99"
    //LINE 1: SELECT date '08 Jan 99';
    //                    ^
    //HINT:  Perhaps you need a different "datestyle" setting.
    //SELECT date '08 Jan 1999';
    //    date
    //------------
    // 1999-01-08
    //(1 row)
    //
    //SELECT date 'Jan 08 99';
    //ERROR:  date/time field value out of range: "Jan 08 99"
    //LINE 1: SELECT date 'Jan 08 99';
    //                    ^
    //HINT:  Perhaps you need a different "datestyle" setting.
    //SELECT date 'Jan 08 1999';
    //    date
    //------------
    // 1999-01-08
    //(1 row)
    //
    //SELECT date '99 08 Jan';
    //    date
    //------------
    // 1999-01-08
    //(1 row)
    //
    //SELECT date '1999 08 Jan';
    //    date
    //------------
    // 1999-01-08
    //(1 row)
    //
    //SELECT date '99-01-08';
    //    date
    //------------
    // 1999-01-08
    //(1 row)
    //
    //SELECT date '1999-01-08';
    //    date
    //------------
    // 1999-01-08
    //(1 row)
    //
    //SELECT date '08-01-99';
    //ERROR:  date/time field value out of range: "08-01-99"
    //LINE 1: SELECT date '08-01-99';
    //                    ^
    //HINT:  Perhaps you need a different "datestyle" setting.
    //SELECT date '08-01-1999';
    //ERROR:  date/time field value out of range: "08-01-1999"
    //LINE 1: SELECT date '08-01-1999';
    //                    ^
    //HINT:  Perhaps you need a different "datestyle" setting.
    //SELECT date '01-08-99';
    //ERROR:  date/time field value out of range: "01-08-99"
    //LINE 1: SELECT date '01-08-99';
    //                    ^
    //HINT:  Perhaps you need a different "datestyle" setting.
    //SELECT date '01-08-1999';
    //ERROR:  date/time field value out of range: "01-08-1999"
    //LINE 1: SELECT date '01-08-1999';
    //                    ^
    //HINT:  Perhaps you need a different "datestyle" setting.
    //SELECT date '99-08-01';
    //    date
    //------------
    // 1999-08-01
    //(1 row)
    //
    //SELECT date '1999-08-01';
    //    date
    //------------
    // 1999-08-01
    //(1 row)
    //
    //SELECT date '99 01 08';
    //    date
    //------------
    // 1999-01-08
    //(1 row)
    //
    //SELECT date '1999 01 08';
    //    date
    //------------
    // 1999-01-08
    //(1 row)
    //
    //SELECT date '08 01 99';
    //ERROR:  date/time field value out of range: "08 01 99"
    //LINE 1: SELECT date '08 01 99';
    //                    ^
    //HINT:  Perhaps you need a different "datestyle" setting.
    //SELECT date '08 01 1999';
    //ERROR:  date/time field value out of range: "08 01 1999"
    //LINE 1: SELECT date '08 01 1999';
    //                    ^
    //HINT:  Perhaps you need a different "datestyle" setting.
    //SELECT date '01 08 99';
    //ERROR:  date/time field value out of range: "01 08 99"
    //LINE 1: SELECT date '01 08 99';
    //                    ^
    //HINT:  Perhaps you need a different "datestyle" setting.
    //SELECT date '01 08 1999';
    //ERROR:  date/time field value out of range: "01 08 1999"
    //LINE 1: SELECT date '01 08 1999';
    //                    ^
    //HINT:  Perhaps you need a different "datestyle" setting.
    //SELECT date '99 08 01';
    //    date
    //------------
    // 1999-08-01
    //(1 row)
    //
    //SELECT date '1999 08 01';
    //    date
    //------------
    // 1999-08-01
    //(1 row)
    //
    //SET datestyle TO dmy;
    //SELECT date 'January 8, 1999';
    //    date
    //------------
    // 1999-01-08
    //(1 row)
    //
    //SELECT date '1999-01-08';
    //    date
    //------------
    // 1999-01-08
    //(1 row)
    //
    //SELECT date '1999-01-18';
    //    date
    //------------
    // 1999-01-18
    //(1 row)
    //
    //SELECT date '1/8/1999';
    //    date
    //------------
    // 1999-08-01
    //(1 row)
    //
    //SELECT date '1/18/1999';
    //ERROR:  date/time field value out of range: "1/18/1999"
    //LINE 1: SELECT date '1/18/1999';
    //                    ^
    //HINT:  Perhaps you need a different "datestyle" setting.
    //SELECT date '18/1/1999';
    //    date
    //------------
    // 1999-01-18
    //(1 row)
    //
    //SELECT date '01/02/03';
    //    date
    //------------
    // 2003-02-01
    //(1 row)
    //
    //SELECT date '19990108';
    //    date
    //------------
    // 1999-01-08
    //(1 row)
    //
    //SELECT date '990108';
    //    date
    //------------
    // 1999-01-08
    //(1 row)
    //
    //SELECT date '1999.008';
    //    date
    //------------
    // 1999-01-08
    //(1 row)
    //
    //SELECT date 'J2451187';
    //    date
    //------------
    // 1999-01-08
    //(1 row)
    //
    //SELECT date 'January 8, 99 BC';
    //     date
    //---------------
    // 0099-01-08 BC
    //(1 row)
    //
    //SELECT date '99-Jan-08';
    //ERROR:  date/time field value out of range: "99-Jan-08"
    //LINE 1: SELECT date '99-Jan-08';
    //                    ^
    //HINT:  Perhaps you need a different "datestyle" setting.
    //SELECT date '1999-Jan-08';
    //    date
    //------------
    // 1999-01-08
    //(1 row)
    //
    //SELECT date '08-Jan-99';
    //    date
    //------------
    // 1999-01-08
    //(1 row)
    //
    //SELECT date '08-Jan-1999';
    //    date
    //------------
    // 1999-01-08
    //(1 row)
    //
    //SELECT date 'Jan-08-99';
    //    date
    //------------
    // 1999-01-08
    //(1 row)
    //
    //SELECT date 'Jan-08-1999';
    //    date
    //------------
    // 1999-01-08
    //(1 row)
    //
    //SELECT date '99-08-Jan';
    //ERROR:  invalid input syntax for type date: "99-08-Jan"
    //LINE 1: SELECT date '99-08-Jan';
    //                    ^
    //SELECT date '1999-08-Jan';
    //ERROR:  invalid input syntax for type date: "1999-08-Jan"
    //LINE 1: SELECT date '1999-08-Jan';
    //                    ^
    //SELECT date '99 Jan 08';
    //ERROR:  date/time field value out of range: "99 Jan 08"
    //LINE 1: SELECT date '99 Jan 08';
    //                    ^
    //HINT:  Perhaps you need a different "datestyle" setting.
    //SELECT date '1999 Jan 08';
    //    date
    //------------
    // 1999-01-08
    //(1 row)
    //
    //SELECT date '08 Jan 99';
    //    date
    //------------
    // 1999-01-08
    //(1 row)
    //
    //SELECT date '08 Jan 1999';
    //    date
    //------------
    // 1999-01-08
    //(1 row)
    //
    //SELECT date 'Jan 08 99';
    //    date
    //------------
    // 1999-01-08
    //(1 row)
    //
    //SELECT date 'Jan 08 1999';
    //    date
    //------------
    // 1999-01-08
    //(1 row)
    //
    //SELECT date '99 08 Jan';
    //ERROR:  invalid input syntax for type date: "99 08 Jan"
    //LINE 1: SELECT date '99 08 Jan';
    //                    ^
    //SELECT date '1999 08 Jan';
    //    date
    //------------
    // 1999-01-08
    //(1 row)
    //
    //SELECT date '99-01-08';
    //ERROR:  date/time field value out of range: "99-01-08"
    //LINE 1: SELECT date '99-01-08';
    //                    ^
    //HINT:  Perhaps you need a different "datestyle" setting.
    //SELECT date '1999-01-08';
    //    date
    //------------
    // 1999-01-08
    //(1 row)
    //
    //SELECT date '08-01-99';
    //    date
    //------------
    // 1999-01-08
    //(1 row)
    //
    //SELECT date '08-01-1999';
    //    date
    //------------
    // 1999-01-08
    //(1 row)
    //
    //SELECT date '01-08-99';
    //    date
    //------------
    // 1999-08-01
    //(1 row)
    //
    //SELECT date '01-08-1999';
    //    date
    //------------
    // 1999-08-01
    //(1 row)
    //
    //SELECT date '99-08-01';
    //ERROR:  date/time field value out of range: "99-08-01"
    //LINE 1: SELECT date '99-08-01';
    //                    ^
    //HINT:  Perhaps you need a different "datestyle" setting.
    //SELECT date '1999-08-01';
    //    date
    //------------
    // 1999-08-01
    //(1 row)
    //
    //SELECT date '99 01 08';
    //ERROR:  date/time field value out of range: "99 01 08"
    //LINE 1: SELECT date '99 01 08';
    //                    ^
    //HINT:  Perhaps you need a different "datestyle" setting.
    //SELECT date '1999 01 08';
    //    date
    //------------
    // 1999-01-08
    //(1 row)
    //
    //SELECT date '08 01 99';
    //    date
    //------------
    // 1999-01-08
    //(1 row)
    //
    //SELECT date '08 01 1999';
    //    date
    //------------
    // 1999-01-08
    //(1 row)
    //
    //SELECT date '01 08 99';
    //    date
    //------------
    // 1999-08-01
    //(1 row)
    //
    //SELECT date '01 08 1999';
    //    date
    //------------
    // 1999-08-01
    //(1 row)
    //
    //SELECT date '99 08 01';
    //ERROR:  date/time field value out of range: "99 08 01"
    //LINE 1: SELECT date '99 08 01';
    //                    ^
    //HINT:  Perhaps you need a different "datestyle" setting.
    //SELECT date '1999 08 01';
    //    date
    //------------
    // 1999-08-01
    //(1 row)
    //
    //SET datestyle TO mdy;
    //SELECT date 'January 8, 1999';
    //    date
    //------------
    // 1999-01-08
    //(1 row)
    //
    //SELECT date '1999-01-08';
    //    date
    //------------
    // 1999-01-08
    //(1 row)
    //
    //SELECT date '1999-01-18';
    //    date
    //------------
    // 1999-01-18
    //(1 row)
    //
    //SELECT date '1/8/1999';
    //    date
    //------------
    // 1999-01-08
    //(1 row)
    //
    //SELECT date '1/18/1999';
    //    date
    //------------
    // 1999-01-18
    //(1 row)
    //
    //SELECT date '18/1/1999';
    //ERROR:  date/time field value out of range: "18/1/1999"
    //LINE 1: SELECT date '18/1/1999';
    //                    ^
    //HINT:  Perhaps you need a different "datestyle" setting.
    //SELECT date '01/02/03';
    //    date
    //------------
    // 2003-01-02
    //(1 row)
    //
    //SELECT date '19990108';
    //    date
    //------------
    // 1999-01-08
    //(1 row)
    //
    //SELECT date '990108';
    //    date
    //------------
    // 1999-01-08
    //(1 row)
    //
    //SELECT date '1999.008';
    //    date
    //------------
    // 1999-01-08
    //(1 row)
    //
    //SELECT date 'J2451187';
    //    date
    //------------
    // 1999-01-08
    //(1 row)
    //
    //SELECT date 'January 8, 99 BC';
    //     date
    //---------------
    // 0099-01-08 BC
    //(1 row)
    //
    //SELECT date '99-Jan-08';
    //ERROR:  date/time field value out of range: "99-Jan-08"
    //LINE 1: SELECT date '99-Jan-08';
    //                    ^
    //HINT:  Perhaps you need a different "datestyle" setting.
    //SELECT date '1999-Jan-08';
    //    date
    //------------
    // 1999-01-08
    //(1 row)
    //
    //SELECT date '08-Jan-99';
    //    date
    //------------
    // 1999-01-08
    //(1 row)
    //
    //SELECT date '08-Jan-1999';
    //    date
    //------------
    // 1999-01-08
    //(1 row)
    //
    //SELECT date 'Jan-08-99';
    //    date
    //------------
    // 1999-01-08
    //(1 row)
    //
    //SELECT date 'Jan-08-1999';
    //    date
    //------------
    // 1999-01-08
    //(1 row)
    //
    //SELECT date '99-08-Jan';
    //ERROR:  invalid input syntax for type date: "99-08-Jan"
    //LINE 1: SELECT date '99-08-Jan';
    //                    ^
    //SELECT date '1999-08-Jan';
    //ERROR:  invalid input syntax for type date: "1999-08-Jan"
    //LINE 1: SELECT date '1999-08-Jan';
    //                    ^
    //SELECT date '99 Jan 08';
    //ERROR:  invalid input syntax for type date: "99 Jan 08"
    //LINE 1: SELECT date '99 Jan 08';
    //                    ^
    //SELECT date '1999 Jan 08';
    //    date
    //------------
    // 1999-01-08
    //(1 row)
    //
    //SELECT date '08 Jan 99';
    //    date
    //------------
    // 1999-01-08
    //(1 row)
    //
    //SELECT date '08 Jan 1999';
    //    date
    //------------
    // 1999-01-08
    //(1 row)
    //
    //SELECT date 'Jan 08 99';
    //    date
    //------------
    // 1999-01-08
    //(1 row)
    //
    //SELECT date 'Jan 08 1999';
    //    date
    //------------
    // 1999-01-08
    //(1 row)
    //
    //SELECT date '99 08 Jan';
    //ERROR:  invalid input syntax for type date: "99 08 Jan"
    //LINE 1: SELECT date '99 08 Jan';
    //                    ^
    //SELECT date '1999 08 Jan';
    //    date
    //------------
    // 1999-01-08
    //(1 row)
    //
    //SELECT date '99-01-08';
    //ERROR:  date/time field value out of range: "99-01-08"
    //LINE 1: SELECT date '99-01-08';
    //                    ^
    //HINT:  Perhaps you need a different "datestyle" setting.
    //SELECT date '1999-01-08';
    //    date
    //------------
    // 1999-01-08
    //(1 row)
    //
    //SELECT date '08-01-99';
    //    date
    //------------
    // 1999-08-01
    //(1 row)
    //
    //SELECT date '08-01-1999';
    //    date
    //------------
    // 1999-08-01
    //(1 row)
    //
    //SELECT date '01-08-99';
    //    date
    //------------
    // 1999-01-08
    //(1 row)
    //
    //SELECT date '01-08-1999';
    //    date
    //------------
    // 1999-01-08
    //(1 row)
    //
    //SELECT date '99-08-01';
    //ERROR:  date/time field value out of range: "99-08-01"
    //LINE 1: SELECT date '99-08-01';
    //                    ^
    //HINT:  Perhaps you need a different "datestyle" setting.
    //SELECT date '1999-08-01';
    //    date
    //------------
    // 1999-08-01
    //(1 row)
    //
    //SELECT date '99 01 08';
    //ERROR:  date/time field value out of range: "99 01 08"
    //LINE 1: SELECT date '99 01 08';
    //                    ^
    //HINT:  Perhaps you need a different "datestyle" setting.
    //SELECT date '1999 01 08';
    //    date
    //------------
    // 1999-01-08
    //(1 row)
    //
    //SELECT date '08 01 99';
    //    date
    //------------
    // 1999-08-01
    //(1 row)
    //
    //SELECT date '08 01 1999';
    //    date
    //------------
    // 1999-08-01
    //(1 row)
    //
    //SELECT date '01 08 99';
    //    date
    //------------
    // 1999-01-08
    //(1 row)
    //
    //SELECT date '01 08 1999';
    //    date
    //------------
    // 1999-01-08
    //(1 row)
    //
    //SELECT date '99 08 01';
    //ERROR:  date/time field value out of range: "99 08 01"
    //LINE 1: SELECT date '99 08 01';
    //                    ^
    //HINT:  Perhaps you need a different "datestyle" setting.
    //SELECT date '1999 08 01';
    //    date
    //------------
    // 1999-08-01
    //(1 row)
    //
    //-- Check upper and lower limits of date range
    //SELECT date '4714-11-24 BC';
    //     date
    //---------------
    // 4714-11-24 BC
    //(1 row)
    //
    //SELECT date '4714-11-23 BC';  -- out of range
    //ERROR:  date out of range: "4714-11-23 BC"
    //LINE 1: SELECT date '4714-11-23 BC';
    //                    ^
    //SELECT date '5874897-12-31';
    //     date
    //---------------
    // 5874897-12-31
    //(1 row)
    //
    //SELECT date '5874898-01-01';  -- out of range
    //ERROR:  date out of range: "5874898-01-01"
    //LINE 1: SELECT date '5874898-01-01';
    //                    ^
    //-- Test non-error-throwing API
    //SELECT pg_input_is_valid('now', 'date');
    // pg_input_is_valid
    //-------------------
    // t
    //(1 row)
    //
    //SELECT pg_input_is_valid('garbage', 'date');
    // pg_input_is_valid
    //-------------------
    // f
    //(1 row)
    //
    //SELECT pg_input_is_valid('6874898-01-01', 'date');
    // pg_input_is_valid
    //-------------------
    // f
    //(1 row)
    //
    //SELECT pg_input_error_message('garbage', 'date');
    //            pg_input_error_message
    //-----------------------------------------------
    // invalid input syntax for type date: "garbage"
    //(1 row)
    //
    //SELECT pg_input_error_message('6874898-01-01', 'date');
    //       pg_input_error_message
    //------------------------------------
    // date out of range: "6874898-01-01"
    //(1 row)

    @Test
    public void testDiff() {
        // SELECT f1 - date '2000-01-01' AS "Days From 2K" FROM DATE_TBL;
        String query = "SELECT (f1 - date '2000-01-01') day AS \"Days From 2K\" FROM DATE_TBL";
        Long[] results = {
                -15607L,
                -15542L,
                -1403L,
                -1402L,
                -1401L,
                -1400L,
                -1037L,
                -1036L,
                -1035L,
                91L,
                92L,
                93L,
                13977L,
                14343L,
                14710L,
                //-1475115L
        };
        DBSPZSetLiteral.Contents result =
                new DBSPZSetLiteral.Contents(Linq.map(results,
                        l -> new DBSPTupleExpression(new DBSPIntervalMillisLiteral(
                                l * 86400 * 1000, true)), DBSPExpression.class));
        result.add(new DBSPTupleExpression(DBSPLiteral.none(
                DBSPTypeMillisInterval.NULLABLE_INSTANCE)));
        this.testQuery(query, result, true);
    }

    // There is no 'epoch' date in Calcite
    // SELECT f1 - date 'epoch' AS "Days From Epoch" FROM DATE_TBL;

    int dow(int value) {
        // This is an adjustment from the Postgres values
        return value + CalciteToDBSPCompiler.firstDOW;
    }

    // SELECT date 'yesterday' - date 'today' AS "One day";
    // One day
    //---------
    //      -1
    //(1 row)
    //
    //SELECT date 'today' - date 'tomorrow' AS "One day";
    // One day
    //---------
    //      -1
    //(1 row)
    //
    //SELECT date 'yesterday' - date 'tomorrow' AS "Two days";
    // Two days
    //----------
    //       -2
    //(1 row)
    //
    //SELECT date 'tomorrow' - date 'today' AS "One day";
    // One day
    //---------
    //       1
    //(1 row)
    //
    //SELECT date 'today' - date 'yesterday' AS "One day";
    // One day
    //---------
    //       1
    //(1 row)
    //
    //SELECT date 'tomorrow' - date 'yesterday' AS "Two days";
    // Two days
    //----------
    //        2
    //(1 row)

    @Test
    public void testParts() {
        // SELECT f1 as "date",
        //    date_part('year', f1) AS year,
        //    date_part('month', f1) AS month,
        //    date_part('day', f1) AS day,
        //    date_part('quarter', f1) AS quarter,
        //    date_part('decade', f1) AS decade,
        //    date_part('century', f1) AS century,
        //    date_part('millennium', f1) AS millennium,
        //    date_part('isoyear', f1) AS isoyear,
        //    date_part('week', f1) AS week,
        //    date_part('dow', f1) AS dow,
        //    date_part('isodow', f1) AS isodow,
        //    date_part('doy', f1) AS doy,
        //    date_part('julian', f1) AS julian,
        //    date_part('epoch', f1) AS epoch
        //    FROM date_tbl

        // 04-09-1957    |  1957 |     4 |   9 |       2 |    195 |      20 |          2 |    1957 |   15 |   2 |      2 |  99 | 2435938 |    -401760000
        // 06-13-1957    |  1957 |     6 |  13 |       2 |    195 |      20 |          2 |    1957 |   24 |   4 |      4 | 164 | 2436003 |    -396144000
        // 02-28-1996    |  1996 |     2 |  28 |       1 |    199 |      20 |          2 |    1996 |    9 |   3 |      3 |  59 | 2450142 |     825465600
        // 02-29-1996    |  1996 |     2 |  29 |       1 |    199 |      20 |          2 |    1996 |    9 |   4 |      4 |  60 | 2450143 |     825552000
        // 03-01-1996    |  1996 |     3 |   1 |       1 |    199 |      20 |          2 |    1996 |    9 |   5 |      5 |  61 | 2450144 |     825638400
        // 03-02-1996    |  1996 |     3 |   2 |       1 |    199 |      20 |          2 |    1996 |    9 |   6 |      6 |  62 | 2450145 |     825724800
        // 02-28-1997    |  1997 |     2 |  28 |       1 |    199 |      20 |          2 |    1997 |    9 |   5 |      5 |  59 | 2450508 |     857088000
        // 03-01-1997    |  1997 |     3 |   1 |       1 |    199 |      20 |          2 |    1997 |    9 |   6 |      6 |  60 | 2450509 |     857174400
        // 03-02-1997    |  1997 |     3 |   2 |       1 |    199 |      20 |          2 |    1997 |    9 |   0 |      7 |  61 | 2450510 |     857260800
        // 04-01-2000    |  2000 |     4 |   1 |       2 |    200 |      20 |          2 |    2000 |   13 |   6 |      6 |  92 | 2451636 |     954547200
        // 04-02-2000    |  2000 |     4 |   2 |       2 |    200 |      20 |          2 |    2000 |   13 |   0 |      7 |  93 | 2451637 |     954633600
        // 04-03-2000    |  2000 |     4 |   3 |       2 |    200 |      20 |          2 |    2000 |   14 |   1 |      1 |  94 | 2451638 |     954720000
        // 04-08-2038    |  2038 |     4 |   8 |       2 |    203 |      21 |          3 |    2038 |   14 |   4 |      4 |  98 | 2465522 |    2154297600
        // 04-09-2039    |  2039 |     4 |   9 |       2 |    203 |      21 |          3 |    2039 |   14 |   6 |      6 |  99 | 2465888 |    2185920000
        // 04-10-2040    |  2040 |     4 |  10 |       2 |    204 |      21 |          3 |    2040 |   15 |   2 |      2 | 101 | 2466255 |    2217628800
        // 04-10-2040 BC | -2040 |     4 |  10 |       2 |   -204 |     -21 |         -3 |   -2040 |   15 |   1 |      1 | 100 |  976430 | -126503251200

        Object[][] data = {
                { "04-09-1957"   ,  1957,     4,   9,       2,    195,      20,          2,    1957,   15,   dow(2),      2,  99,    -401760000L },
                { "06-13-1957"   ,  1957,     6,  13,       2,    195,      20,          2,    1957,   24,   dow(4),      4, 164,    -396144000L },
                { "02-28-1996"   ,  1996,     2,  28,       1,    199,      20,          2,    1996,    9,   dow(3),      3,  59,     825465600L },
                { "02-29-1996"   ,  1996,     2,  29,       1,    199,      20,          2,    1996,    9,   dow(4),      4,  60,     825552000L },
                { "03-01-1996"   ,  1996,     3,   1,       1,    199,      20,          2,    1996,    9,   dow(5),      5,  61,     825638400L },
                { "03-02-1996"   ,  1996,     3,   2,       1,    199,      20,          2,    1996,    9,   dow(6),      6,  62,     825724800L },
                { "02-28-1997"   ,  1997,     2,  28,       1,    199,      20,          2,    1997,    9,   dow(5),      5,  59,     857088000L },
                { "03-01-1997"   ,  1997,     3,   1,       1,    199,      20,          2,    1997,    9,   dow(6),      6,  60,     857174400L },
                { "03-02-1997"   ,  1997,     3,   2,       1,    199,      20,          2,    1997,    9,   dow(0),      7,  61,     857260800L },
                { "04-01-2000"   ,  2000,     4,   1,       2,    200,      20,          2,    2000,   13,   dow(6),      6,  92,     954547200L },
                { "04-02-2000"   ,  2000,     4,   2,       2,    200,      20,          2,    2000,   13,   dow(0),      7,  93,     954633600L },
                { "04-03-2000"   ,  2000,     4,   3,       2,    200,      20,          2,    2000,   14,   dow(1),      1,  94,     954720000L },
                { "04-08-2038"   ,  2038,     4,   8,       2,    203,      21,          3,    2038,   14,   dow(4),      4,  98,    2154297600L },
                { "04-09-2039"   ,  2039,     4,   9,       2,    203,      21,          3,    2039,   14,   dow(6),      6,  99,    2185920000L },
                { "04-10-2040"   ,  2040,     4,  10,       2,    204,      21,          3,    2040,   15,   dow(2),      2, 101,    2217628800L },
              //{ "04-10-2040 BC", -2040,     4,  10,       2,   -204,     -21,         -3,   -2040,   15,   dow(1),      1, 100, -126503251200L }
        };
        String query = "SELECT f1 as \"date\",\n" +
                "    EXTRACT(YEAR from f1) AS 'year',\n" +
                "    EXTRACT(month from f1) AS 'month',\n" +
                "    EXTRACT(day from f1) AS 'day',\n" +
                "    EXTRACT(quarter from f1) AS 'quarter',\n" +
                "    EXTRACT(decade from f1) AS 'decade',\n" +
                "    EXTRACT(century from f1) AS 'century',\n" +
                "    EXTRACT(millennium from f1) AS 'millennium',\n" +
                "    EXTRACT(isoyear from f1) AS 'isoyear',\n" +
                "    EXTRACT(week from f1) AS 'week',\n" +
                "    EXTRACT(dow from f1) AS 'dow',\n" +
                "    EXTRACT(isodow from f1) AS 'isodow',\n" +
                "    EXTRACT(doy from f1) AS 'doy',\n" +
                "    EXTRACT(epoch from f1) AS 'epoch'\n" +
                "    FROM DATE_TBL";
        DBSPExpression[] exprs = new DBSPExpression[data.length + 1];
        for (int i = 0; i < data.length; i++) {
            Object[] row = data[i];
            DBSPTupleExpression tuple = new DBSPTupleExpression(
                    reformatDate((String)row[0]),
                    new DBSPI64Literal((Integer)row[1], true),
                    new DBSPI64Literal((Integer)row[2], true),
                    new DBSPI64Literal((Integer)row[3], true),
                    new DBSPI64Literal((Integer)row[4], true),
                    new DBSPI64Literal((Integer)row[5], true),
                    new DBSPI64Literal((Integer)row[6], true),
                    new DBSPI64Literal((Integer)row[7], true),
                    new DBSPI64Literal((Integer)row[8], true),
                    new DBSPI64Literal((Integer)row[9], true),
                    new DBSPI64Literal((Integer)row[10], true),
                    new DBSPI64Literal((Integer)row[11], true),
                    new DBSPI64Literal((Integer)row[12], true),
                    new DBSPI64Literal((Long)row[13], true)
            );
            exprs[i] = tuple;
        }
        DBSPExpression ni = DBSPLiteral.none(DBSPTypeInteger.SIGNED_64.setMayBeNull(true));
        exprs[data.length] = new DBSPTupleExpression(DBSPLiteral.none(nullableDate), ni, ni, ni, ni, ni, ni, ni, ni, ni, ni, ni, ni, ni);
        this.testQuery(query, new DBSPZSetLiteral.Contents(exprs), true);
    }

    @Test
    public void testEpoch() {
        // SELECT EXTRACT(EPOCH FROM DATE        '1970-01-01');
        String query = "SELECT EXTRACT(EPOCH FROM DATE '1970-01-01')";
        this.testQueryTwice(query, 0);
    }

    @Test
    public void testCentury() {
        // SELECT EXTRACT(CENTURY FROM DATE '0001-01-01');
        String query = "SELECT EXTRACT(CENTURY FROM DATE '0001-01-01')";
        this.testQueryTwice(query, 1);
    }

    @Test
    public void testCentury1() {
        String query = "SELECT EXTRACT(CENTURY FROM DATE '1900-12-31')";
        this.testQueryTwice(query, 19);
    }

    @Test
    public void testCentury2() {
        String query = "SELECT EXTRACT(CENTURY FROM DATE '1901-01-01')";
        this.testQueryTwice(query, 20);
    }

    @Test
    public void testCentury3() {
        String query = "SELECT EXTRACT(CENTURY FROM DATE '2000-12-31')";
        this.testQueryTwice(query, 20);
    }

    @Test
    public void testCentury4() {
        String query = "SELECT EXTRACT(CENTURY FROM DATE '2001-01-01')";
        this.testQueryTwice(query, 21);
    }

    @Test
    public void testMillennium() {
        String query = "SELECT EXTRACT(MILLENNIUM FROM DATE '0001-01-01')";
        this.testQueryTwice(query, 1);
    }

    @Test
    public void testMillennium1() {
        String query = "SELECT EXTRACT(MILLENNIUM FROM DATE '1000-12-31')";
        this.testQueryTwice(query, 1);
    }

    @Test
    public void testMillennium2() {
        String query = "SELECT EXTRACT(MILLENNIUM FROM DATE '2000-12-31')";
        this.testQueryTwice(query, 2);
    }

    // CURRENT_DATE not supported
    // SELECT EXTRACT(CENTURY FROM CURRENT_DATE)>=21 AS True;     -- true

    @Test
    public void testMillennium3() {
        String query = "SELECT EXTRACT(MILLENNIUM FROM DATE '2001-01-01')";
        this.testQueryTwice(query, 3);
    }

    @Test
    public void testDecade() {
        String query = "SELECT EXTRACT(DECADE FROM DATE '1994-12-25')";
        this.testQueryTwice(query, 199);
    }

    @Test
    public void testDecade1() {
        String query = "SELECT EXTRACT(DECADE FROM DATE '0010-01-01')";
        this.testQueryTwice(query, 1);
    }

    @Test
    public void testDecade2() {
        String query = "SELECT EXTRACT(DECADE FROM DATE '0009-12-31')";
        this.testQueryTwice(query, 0);
    }

    @Test
    public void testMicroseconds() {
        String query = "SELECT EXTRACT(MICROSECOND  FROM DATE '2020-08-11')";
        this.testQueryTwice(query, 0);
    }

    // These fail in Postgres but work in Calcite
    // SELECT EXTRACT(MICROSECONDS  FROM DATE '2020-08-11');
    //ERROR:  unit "microseconds" not supported for type date
    //SELECT EXTRACT(MILLISECONDS  FROM DATE '2020-08-11');
    //ERROR:  unit "milliseconds" not supported for type date
    //SELECT EXTRACT(SECOND        FROM DATE '2020-08-11');
    //ERROR:  unit "second" not supported for type date
    //SELECT EXTRACT(MINUTE        FROM DATE '2020-08-11');
    //ERROR:  unit "minute" not supported for type date
    //SELECT EXTRACT(HOUR          FROM DATE '2020-08-11');
    //ERROR:  unit "hour" not supported for type date

    @Test
    public void testMilliseconds() {
        String query = "SELECT EXTRACT(MILLISECOND  FROM DATE '2020-08-11')";
        this.testQueryTwice(query, 0);
    }

    @Test
    public void testSeconds() {
        String query = "SELECT EXTRACT(SECOND        FROM DATE '2020-08-11')";
        this.testQueryTwice(query, 0);
    }

    @Test
    public void testSeconds0() {
        String query = "SELECT SECOND(DATE '2020-08-11')";
        this.testQueryTwice(query, 0);
    }

    @Test
    public void testMinutes() {
        String query = "SELECT EXTRACT(MINUTE FROM DATE '2020-08-11')";
        this.testQueryTwice(query, 0);
    }

    @Test
    public void testMinutes1() {
        String query = "SELECT MINUTE(DATE '2020-08-11')";
        this.testQueryTwice(query, 0);
    }

    @Test
    public void testHour() {
        String query = "SELECT EXTRACT(HOUR          FROM DATE '2020-08-11')";
        this.testQueryTwice(query, 0);
    }

    @Test
    public void testHour1() {
        String query = "SELECT HOUR(DATE '2020-08-11')";
        this.testQueryTwice(query, 0);
    }

    @Test
    public void testDay() {
        String query = "SELECT EXTRACT(DAY           FROM DATE '2020-08-11')";
        this.testQueryTwice(query, 11);
    }

    @Test
    public void testDay1() {
        String query = "SELECT DAYOFMONTH(DATE '2020-08-11')";
        this.testQueryTwice(query, 11);
    }

    @Test
    public void testMonth() {
        String query = "SELECT EXTRACT(MONTH         FROM DATE '2020-08-11')";
        this.testQueryTwice(query, 8);
    }

    @Test
    public void testMonth1() {
        String query = "SELECT MONTH(DATE '2020-08-11')";
        this.testQueryTwice(query, 8);
    }

    @Test
    public void testYear() {
        String query = "SELECT EXTRACT(YEAR          FROM DATE '2020-08-11')";
        this.testQueryTwice(query, 2020);
    }

    @Test
    public void testYear1() {
        String query = "SELECT YEAR(DATE '2020-08-11')";
        this.testQueryTwice(query, 2020);
    }

    @Test
    public void testDecade5() {
        String query = "SELECT EXTRACT(DECADE        FROM DATE '2020-08-11')";
        this.testQueryTwice(query,202);
    }

    @Test
    public void testCentury5() {
        String query = "SELECT EXTRACT(CENTURY       FROM DATE '2020-08-11')";
        this.testQueryTwice(query, 21);
    }

    @Test
    public void testMillennium5() {
        String query = "SELECT EXTRACT(MILLENNIUM    FROM DATE '2020-08-11')";
        this.testQueryTwice(query, 3);
    }

    @Test
    public void testIsoYear() {
        String query = "SELECT EXTRACT(ISOYEAR       FROM DATE '2020-08-11')";
        this.testQueryTwice(query, 2020);
    }

    @Test
    public void testQuarter() {
        String query = "SELECT EXTRACT(QUARTER       FROM DATE '2020-08-11')";
        this.testQueryTwice(query, 3);
    }

    @Test
    public void testWeek() {
        String query = "SELECT EXTRACT(WEEK          FROM DATE '2020-08-11')";
        this.testQueryTwice(query, 33);
    }

    @Test
    public void testDow() {
        String query = "SELECT EXTRACT(DOW           FROM DATE '2020-08-11')";
        this.testQueryTwice(query, dow(2));
    }

    @Test
    public void testDow2() {
        String query = "SELECT DAYOFWEEK(DATE '2020-08-11')";
        this.testQueryTwice(query, dow(2));
    }

    /*
     * TODO Postgres actually returns 0 for this query, but the Calcite optimizer
     * folds this to 1. Same for previous test.
     */
    @Test
    public void testDow1() {
        // Sunday
        String query = "SELECT EXTRACT(DOW FROM DATE '2020-08-16')";
        this.testQueryTwice(query, dow(0));
    }

    @Test
    public void testIsoDow() {
        String query = "SELECT EXTRACT(ISODOW FROM DATE '2020-08-16')";
        this.testQueryTwice(query, 7);
    }

    @Test
    public void testDoy() {
        String query = "SELECT EXTRACT(DOY           FROM DATE '2020-08-11')";
        this.testQueryTwice(query, 224);
    }

    // SELECT EXTRACT(TIMEZONE      FROM DATE '2020-08-11');
    //ERROR:  unit "timezone" not supported for type date
    //SELECT EXTRACT(TIMEZONE_M    FROM DATE '2020-08-11');
    //ERROR:  unit "timezone_m" not supported for type date
    //SELECT EXTRACT(TIMEZONE_H    FROM DATE '2020-08-11');
    //ERROR:  unit "timezone_h" not supported for type date

    @Test @Ignore("There are two bugs in Calcite; " +
            "now waiting for https://issues.apache.org/jira/projects/CALCITE/issues/CALCITE-5760")
    public void testDateTrunc() {
        // In the BigQuery library there is a DATE_TRUNC, but arguments are swapped
        String query = "SELECT DATE_TRUNC(DATE '1970-03-20', MILLENNIUM)";
        DBSPZSetLiteral.Contents zset = new DBSPZSetLiteral.Contents(
                new DBSPTupleExpression(new DBSPDateLiteral("1001-01-01")));
        this.testQuery(query, zset, false);
        this.testQuery(query, zset, true);
    }

    // SELECT DATE_TRUNC('MILLENNIUM', DATE '1970-03-20'); -- 1001-01-01
    //          date_trunc
    //------------------------------
    // Thu Jan 01 00:00:00 1001 PST
    //(1 row)
    //
    //SELECT DATE_TRUNC('CENTURY', TIMESTAMP '1970-03-20 04:30:00.00000'); -- 1901
    //        date_trunc
    //--------------------------
    // Tue Jan 01 00:00:00 1901
    //(1 row)
    //
    //SELECT DATE_TRUNC('CENTURY', DATE '1970-03-20'); -- 1901
    //          date_trunc
    //------------------------------
    // Tue Jan 01 00:00:00 1901 PST
    //(1 row)
    //
    //SELECT DATE_TRUNC('CENTURY', DATE '2004-08-10'); -- 2001-01-01
    //          date_trunc
    //------------------------------
    // Mon Jan 01 00:00:00 2001 PST
    //(1 row)
    //
    //SELECT DATE_TRUNC('CENTURY', DATE '0002-02-04'); -- 0001-01-01
    //          date_trunc
    //------------------------------
    // Mon Jan 01 00:00:00 0001 PST
    //(1 row)
    //
    //SELECT DATE_TRUNC('CENTURY', DATE '0055-08-10 BC'); -- 0100-01-01 BC
    //           date_trunc
    //---------------------------------
    // Tue Jan 01 00:00:00 0100 PST BC
    //(1 row)
    //
    //SELECT DATE_TRUNC('DECADE', DATE '1993-12-25'); -- 1990-01-01
    //          date_trunc
    //------------------------------
    // Mon Jan 01 00:00:00 1990 PST
    //(1 row)
    //
    //SELECT DATE_TRUNC('DECADE', DATE '0004-12-25'); -- 0001-01-01 BC
    //           date_trunc
    //---------------------------------
    // Sat Jan 01 00:00:00 0001 PST BC
    //(1 row)
    //
    //SELECT DATE_TRUNC('DECADE', DATE '0002-12-31 BC'); -- 0011-01-01 BC
    //           date_trunc
    //---------------------------------
    // Mon Jan 01 00:00:00 0011 PST BC

    // select 'infinity'::date > 'today'::date as t;
    // t
    //---
    // t
    //(1 row)
    //
    //select '-infinity'::date < 'today'::date as t;
    // t
    //---
    // t
    //(1 row)
    //
    //select isfinite('infinity'::date), isfinite('-infinity'::date), isfinite('today'::date);
    // isfinite | isfinite | isfinite
    //----------+----------+----------
    // f        | f        | t
    //(1 row)
    //
    //select 'infinity'::date = '+infinity'::date as t;
    // t
    //---
    // t

    @Test(expected = RuntimeException.class)
    // Calcite does not seem to support infinity dates
    public void testInfinity() {
        String query = "SELECT DATE 'infinity'";
        this.testQueryTwice(query, 0);
    }

    // Calcite does not support make_date
    // select make_date(0, 7, 15);
    //ERROR:  date field value out of range: 0-07-15
    //select make_date(2013, 2, 30);
    //ERROR:  date field value out of range: 2013-02-30
    //select make_date(2013, 13, 1);
    //ERROR:  date field value out of range: 2013-13-01
    //select make_date(2013, 11, -1);
    //ERROR:  date field value out of range: 2013-11--1
    //select make_time(10, 55, 100.1);
    //ERROR:  time field value out of range: 10:55:100.1
    //select make_time(24, 0, 2.1);
    //ERROR:  time field value out of range: 24:00:2.1
}
