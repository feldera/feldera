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

import org.apache.calcite.config.Lex;
import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.compiler.BaseSQLTests;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.frontend.CalciteToDBSPCompiler;
import org.dbsp.sqlCompiler.compiler.backend.DBSPCompiler;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.*;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeInteger;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeTimestamp;
import org.dbsp.util.Linq;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nullable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Tests manually adapted from
 * https://github.com/postgres/postgres/blob/master/src/test/regress/expected/timestamp.out
 */
@SuppressWarnings("JavadocLinkAsPlainText")
public class PostgresTimestampTests extends BaseSQLTests {
    // Cannot use non-deterministic values:
    // INSERT INTO TIMESTAMP_TBL VALUES ('today');
    // INSERT INTO TIMESTAMP_TBL VALUES ('yesterday');
    // INSERT INTO TIMESTAMP_TBL VALUES ('tomorrow');
    // -- time zone should be ignored by this data type
    // INSERT INTO TIMESTAMP_TBL VALUES ('tomorrow EST');
    // INSERT INTO TIMESTAMP_TBL VALUES ('tomorrow zulu');
    // INSERT INTO TIMESTAMP_TBL VALUES ('now');
    // Infinity values not supported by Calcite
    // INSERT INTO TIMESTAMP_TBL VALUES ('-infinity');
    // INSERT INTO TIMESTAMP_TBL VALUES ('infinity');

    // Calcite is not very flexible regarding timestamp formats
    public DBSPCompiler compileQuery(String query, boolean optimize) {
        String data =
                "CREATE TABLE TIMESTAMP_TBL (d1 timestamp(2) without time zone)\n;" +
                                                                                         // -- Postgres v6.0 standard output format
                "INSERT INTO TIMESTAMP_TBL VALUES ('1970-01-01 00:00:00');\n" +          // INSERT INTO TIMESTAMP_TBL VALUES ('epoch');
                "INSERT INTO TIMESTAMP_TBL VALUES ('1997-02-10 17:32:01');\n" +          // INSERT INTO TIMESTAMP_TBL VALUES ('Mon Feb 10 17:32:01 1997 PST');
                                                                                         // -- Variations on Postgres v6.1 standard output format
                "INSERT INTO TIMESTAMP_TBL VALUES ('1997-02-10 17:32:01.000001');\n" +   // INSERT INTO TIMESTAMP_TBL VALUES ('Mon Feb 10 17:32:01.000001 1997 PST');
                "INSERT INTO TIMESTAMP_TBL VALUES ('1997-02-10 17:32:01.999999');\n" +   // INSERT INTO TIMESTAMP_TBL VALUES ('Mon Feb 10 17:32:01.999999 1997 PST');
                "INSERT INTO TIMESTAMP_TBL VALUES ('1997-02-10 17:32:01.4');\n" +        // INSERT INTO TIMESTAMP_TBL VALUES ('Mon Feb 10 17:32:01.4 1997 PST');
                "INSERT INTO TIMESTAMP_TBL VALUES ('1997-02-10 17:32:01.5');\n" +        // INSERT INTO TIMESTAMP_TBL VALUES ('Mon Feb 10 17:32:01.5 1997 PST');
                "INSERT INTO TIMESTAMP_TBL VALUES ('1997-02-10 17:32:01.6');\n" +        // INSERT INTO TIMESTAMP_TBL VALUES ('Mon Feb 10 17:32:01.6 1997 PST');
                                                                                         // -- ISO 8601 format
                "INSERT INTO TIMESTAMP_TBL VALUES ('1997-01-02');\n" +                   // INSERT INTO TIMESTAMP_TBL VALUES ('1997-01-02');
                "INSERT INTO TIMESTAMP_TBL VALUES ('1997-01-02 03:04:05');\n" +          // INSERT INTO TIMESTAMP_TBL VALUES ('1997-01-02 03:04:05');
                "INSERT INTO TIMESTAMP_TBL VALUES ('1997-02-10 17:32:01');\n" +          // INSERT INTO TIMESTAMP_TBL VALUES ('1997-02-10 17:32:01-08');
                "INSERT INTO TIMESTAMP_TBL VALUES ('1997-02-10 17:32:01');\n" +          // INSERT INTO TIMESTAMP_TBL VALUES ('1997-02-10 17:32:01-0800');
                "INSERT INTO TIMESTAMP_TBL VALUES ('1997-02-10 17:32:01');\n" +          // INSERT INTO TIMESTAMP_TBL VALUES ('1997-02-10 17:32:01 -08:00');
                "INSERT INTO TIMESTAMP_TBL VALUES ('1997-02-10 17:32:01');\n" +          // INSERT INTO TIMESTAMP_TBL VALUES ('19970210 173201 -0800');
                "INSERT INTO TIMESTAMP_TBL VALUES ('1997-06-10 17:32:01');\n" +          // INSERT INTO TIMESTAMP_TBL VALUES ('1997-06-10 17:32:01 -07:00');
                "INSERT INTO TIMESTAMP_TBL VALUES ('2001-09-22 18:19:20');\n" +          // INSERT INTO TIMESTAMP_TBL VALUES ('2001-09-22T18:19:20');
                                                                                         // POSIX format (note that the timezone abbrev is just decoration here)
                "INSERT INTO TIMESTAMP_TBL VALUES ('2000-03-15 08:14:01');\n" +          // INSERT INTO TIMESTAMP_TBL VALUES ('2000-03-15 08:14:01 GMT+8');
                "INSERT INTO TIMESTAMP_TBL VALUES ('2000-03-15 13:14:02');\n" +          // INSERT INTO TIMESTAMP_TBL VALUES ('2000-03-15 13:14:02 GMT-1');
                "INSERT INTO TIMESTAMP_TBL VALUES ('2000-03-15 12:14:03');\n" +          // INSERT INTO TIMESTAMP_TBL VALUES ('2000-03-15 12:14:03 GMT-2');
                "INSERT INTO TIMESTAMP_TBL VALUES ('2000-03-15 03:14:04');\n" +          // INSERT INTO TIMESTAMP_TBL VALUES ('2000-03-15 03:14:04 PST+8');
                "INSERT INTO TIMESTAMP_TBL VALUES ('2000-03-15 02:14:05');\n" +          // INSERT INTO TIMESTAMP_TBL VALUES ('2000-03-15 02:14:05 MST+7:00');
                                                                                         // -- Variations for acceptable input formats
                "INSERT INTO TIMESTAMP_TBL VALUES ('1997-02-10 17:32:01');\n" +          // INSERT INTO TIMESTAMP_TBL VALUES ('Feb 10 17:32:01 1997 -0800');
                "INSERT INTO TIMESTAMP_TBL VALUES ('1997-02-10 17:32:01');\n" +          // INSERT INTO TIMESTAMP_TBL VALUES ('Feb 10 17:32:01 1997');
                "INSERT INTO TIMESTAMP_TBL VALUES ('1997-02-10 17:32:00');\n" +          // INSERT INTO TIMESTAMP_TBL VALUES ('Feb 10 5:32PM 1997');
                "INSERT INTO TIMESTAMP_TBL VALUES ('1997-02-10 17:32:01');\n" +          // INSERT INTO TIMESTAMP_TBL VALUES ('1997/02/10 17:32:01-0800');
                "INSERT INTO TIMESTAMP_TBL VALUES ('1997-02-10 17:32:01');\n" +          // INSERT INTO TIMESTAMP_TBL VALUES ('1997-02-10 17:32:01 PST');
                "INSERT INTO TIMESTAMP_TBL VALUES ('1997-02-10 17:32:01');\n" +          // INSERT INTO TIMESTAMP_TBL VALUES ('Feb-10-1997 17:32:01 PST');
                "INSERT INTO TIMESTAMP_TBL VALUES ('1997-02-10 17:32:01');\n" +          // INSERT INTO TIMESTAMP_TBL VALUES ('02-10-1997 17:32:01 PST');
                "INSERT INTO TIMESTAMP_TBL VALUES ('1997-02-10 17:32:01');\n" +          // INSERT INTO TIMESTAMP_TBL VALUES ('19970210 173201 PST');
                                                                                         // set datestyle to ymd;
                "INSERT INTO TIMESTAMP_TBL VALUES ('1997-02-10 17:32:01');\n" +          // INSERT INTO TIMESTAMP_TBL VALUES ('97FEB10 5:32:01PM UTC');
                "INSERT INTO TIMESTAMP_TBL VALUES ('1997-02-10 17:32:01');\n" +          // INSERT INTO TIMESTAMP_TBL VALUES ('97/02/10 17:32:01 UTC');
                                                                                         // reset datestyle
                "INSERT INTO TIMESTAMP_TBL VALUES ('1997-02-10 17:32:01');\n" +          // INSERT INTO TIMESTAMP_TBL VALUES ('1997.041 17:32:01 UTC'); // 41st day
                "INSERT INTO TIMESTAMP_TBL VALUES ('1997-02-10 17:32:01');\n" +          // INSERT INTO TIMESTAMP_TBL VALUES ('19970210 173201 America/New_York');
                                                                                         // -- this fails (even though TZ is a no-op, we still look it up)
                                                                                         // INSERT INTO TIMESTAMP_TBL VALUES ('19970710 173201 America/Does_not_exist');
                                                                                         // ERROR:  time zone "america/does_not_exist" not recognized
                                                                                         // LINE 1: INSERT INTO TIMESTAMP_TBL VALUES ('19970710 173201 America/D...
                                                                                         // -- Check date conversion and date arithmetic
                "INSERT INTO TIMESTAMP_TBL VALUES ('1997-06-10 18:32:01');\n" +          // INSERT INTO TIMESTAMP_TBL VALUES ('1997-06-10 18:32:01 PDT');
                "INSERT INTO TIMESTAMP_TBL VALUES ('1997-02-10 17:32:01');\n" +          // INSERT INTO TIMESTAMP_TBL VALUES ('Feb 10 17:32:01 1997');
                "INSERT INTO TIMESTAMP_TBL VALUES ('1997-02-11 17:32:01');\n" +          // INSERT INTO TIMESTAMP_TBL VALUES ('Feb 11 17:32:01 1997');
                "INSERT INTO TIMESTAMP_TBL VALUES ('1997-02-12 17:32:01');\n" +          // INSERT INTO TIMESTAMP_TBL VALUES ('Feb 12 17:32:01 1997');
                "INSERT INTO TIMESTAMP_TBL VALUES ('1997-02-13 17:32:01');\n" +          // INSERT INTO TIMESTAMP_TBL VALUES ('Feb 13 17:32:01 1997');
                "INSERT INTO TIMESTAMP_TBL VALUES ('1997-02-14 17:32:01');\n" +          // INSERT INTO TIMESTAMP_TBL VALUES ('Feb 14 17:32:01 1997');
                "INSERT INTO TIMESTAMP_TBL VALUES ('1997-02-15 17:32:01');\n" +          // INSERT INTO TIMESTAMP_TBL VALUES ('Feb 15 17:32:01 1997');
                "INSERT INTO TIMESTAMP_TBL VALUES ('1997-02-16 17:32:01');\n" +          // INSERT INTO TIMESTAMP_TBL VALUES ('Feb 16 17:32:01 1997');
                //"INSERT INTO TIMESTAMP_TBL VALUES ('-0097-02-16 17:32:01');\n" +         // INSERT INTO TIMESTAMP_TBL VALUES ('Feb 16 17:32:01 0097 BC');
                "INSERT INTO TIMESTAMP_TBL VALUES ('0097-02-16 17:32:01');\n" +          // INSERT INTO TIMESTAMP_TBL VALUES ('Feb 16 17:32:01 0097');
                "INSERT INTO TIMESTAMP_TBL VALUES ('0597-02-16 17:32:01');\n" +          // INSERT INTO TIMESTAMP_TBL VALUES ('Feb 16 17:32:01 0597');
                "INSERT INTO TIMESTAMP_TBL VALUES ('1097-02-16 17:32:01');\n" +          // INSERT INTO TIMESTAMP_TBL VALUES ('Feb 16 17:32:01 1097');
                "INSERT INTO TIMESTAMP_TBL VALUES ('1697-02-16 17:32:01');\n" +          // INSERT INTO TIMESTAMP_TBL VALUES ('Feb 16 17:32:01 1697');
                "INSERT INTO TIMESTAMP_TBL VALUES ('1797-02-16 17:32:01');\n" +          // INSERT INTO TIMESTAMP_TBL VALUES ('Feb 16 17:32:01 1797');
                "INSERT INTO TIMESTAMP_TBL VALUES ('1897-02-16 17:32:01');\n" +          // INSERT INTO TIMESTAMP_TBL VALUES ('Feb 16 17:32:01 1897');
                "INSERT INTO TIMESTAMP_TBL VALUES ('1997-02-16 17:32:01');\n" +          // INSERT INTO TIMESTAMP_TBL VALUES ('Feb 16 17:32:01 1997');
                "INSERT INTO TIMESTAMP_TBL VALUES ('2097-02-16 17:32:01');\n" +          // INSERT INTO TIMESTAMP_TBL VALUES ('Feb 16 17:32:01 2097');
                "INSERT INTO TIMESTAMP_TBL VALUES ('1996-02-28 17:32:01');\n" +          // INSERT INTO TIMESTAMP_TBL VALUES ('Feb 28 17:32:01 1996');
                "INSERT INTO TIMESTAMP_TBL VALUES ('1996-02-29 17:32:01');\n" +          // INSERT INTO TIMESTAMP_TBL VALUES ('Feb 29 17:32:01 1996');
                "INSERT INTO TIMESTAMP_TBL VALUES ('1996-03-01 17:32:01');\n" +          // INSERT INTO TIMESTAMP_TBL VALUES ('Mar 01 17:32:01 1996');
                "INSERT INTO TIMESTAMP_TBL VALUES ('1996-12-30 17:32:01');\n" +          // INSERT INTO TIMESTAMP_TBL VALUES ('Dec 30 17:32:01 1996');
                "INSERT INTO TIMESTAMP_TBL VALUES ('1996-12-31 17:32:01');\n" +          // INSERT INTO TIMESTAMP_TBL VALUES ('Dec 31 17:32:01 1996');
                "INSERT INTO TIMESTAMP_TBL VALUES ('1997-01-01 17:32:01');\n" +          // INSERT INTO TIMESTAMP_TBL VALUES ('Jan 01 17:32:01 1997');
                "INSERT INTO TIMESTAMP_TBL VALUES ('1997-02-28 17:32:01');\n" +          // INSERT INTO TIMESTAMP_TBL VALUES ('Feb 28 17:32:01 1997');
                        // The following is not a legal date.  In calcite this inserts a NULL, in Postgres this insertion fails altogether.
                        // That's why the next few queries will find a NULL in this table.
                "INSERT INTO TIMESTAMP_TBL VALUES ('1997-02-29 17:32:01');\n" +          // INSERT INTO TIMESTAMP_TBL VALUES ('Feb 29 17:32:01 1997');
                                                                                         // ERROR:  date/time field value out of range: "Feb 29 17:32:01 1997"
                "INSERT INTO TIMESTAMP_TBL VALUES ('1997-03-01 17:32:01');\n" +          // INSERT INTO TIMESTAMP_TBL VALUES ('Mar 01 17:32:01 1997');
                "INSERT INTO TIMESTAMP_TBL VALUES ('1997-12-30 17:32:01');\n" +          // INSERT INTO TIMESTAMP_TBL VALUES ('Dec 30 17:32:01 1997');
                "INSERT INTO TIMESTAMP_TBL VALUES ('1997-12-31 17:32:01');\n" +          // INSERT INTO TIMESTAMP_TBL VALUES ('Dec 31 17:32:01 1997');
                "INSERT INTO TIMESTAMP_TBL VALUES ('1999-12-31 17:32:01');\n" +          // INSERT INTO TIMESTAMP_TBL VALUES ('Dec 31 17:32:01 1999');
                "INSERT INTO TIMESTAMP_TBL VALUES ('2000-01-01 17:32:01');\n" +          // INSERT INTO TIMESTAMP_TBL VALUES ('Jan 01 17:32:01 2000');
                "INSERT INTO TIMESTAMP_TBL VALUES ('2000-12-31 17:32:01');\n" +          // INSERT INTO TIMESTAMP_TBL VALUES ('Dec 31 17:32:01 2000');
                "INSERT INTO TIMESTAMP_TBL VALUES ('2001-01-01 17:32:01')\n";            // INSERT INTO TIMESTAMP_TBL VALUES ('Jan 01 17:32:01 2001');
        CompilerOptions options = new CompilerOptions();
        options.ioOptions.lexicalRules = Lex.ORACLE;
        options.optimizerOptions.optimizationLevel = optimize ? 2 : 1;
        options.optimizerOptions.generateInputForEveryTable = true;
        DBSPCompiler compiler = new DBSPCompiler(options);
        compiler.compileStatements(data);
        compiler.compileStatement(query);
        return compiler;
    }

    void testQuery(String query, DBSPZSetLiteral.Contents expectedOutput, boolean optimize) {
        query = "CREATE VIEW V AS " + query;
        DBSPCompiler compiler = this.compileQuery(query, optimize);
        DBSPCircuit circuit = getCircuit(compiler);
        DBSPZSetLiteral.Contents input = compiler.getTableContents().getTableContents("TIMESTAMP_TBL");
        InputOutputPair streams = new InputOutputPair(input, expectedOutput);
        this.addRustTestCase(query, compiler, circuit, streams);
    }

    static final SimpleDateFormat[] inputFormats = {
            new SimpleDateFormat("EEE MMM d HH:mm:ss.SSS yyyy"),
            new SimpleDateFormat("EEE MMM d HH:mm:ss yyyy G"),
            new SimpleDateFormat("EEE MMM d HH:mm:ss yyyy")
    };
    static final SimpleDateFormat outputFormats = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S");

    /**
     * Convert a date from a format like Sat Feb 16 17:32:01 1996 to
     * a format like 1996-02-16 17:32:01
     */
    static DBSPExpression convertDate(@Nullable String date) {
        if (date == null)
            return DBSPLiteral.none(DBSPTypeTimestamp.NULLABLE_INSTANCE);
        for (SimpleDateFormat input: inputFormats) {
            String out;
            try {
                // Calcite problems: does not support negative years, or fractional seconds ending in 0
                Date zero = new SimpleDateFormat("yyyy-MM-dd").parse("0000-01-01");
                Date converted = input.parse(date);
                out = outputFormats.format(converted);
                if (converted.before(zero))
                    out = "-" + out;
            } catch (ParseException ignored) {
                continue;
            }
            return new DBSPTimestampLiteral(out, true);
        }
        throw new RuntimeException("Could not parse " + date);
    }

    @Test
    public void testTS() {
        String[] data = {
                //"Tue Feb 16 17:32:01 0097 BC",
                "Thu Jan 01 00:00:00 1970",
                "Mon Feb 10 17:32:01 1997",
                "Mon Feb 10 17:32:01 1997",
                "Mon Feb 10 17:32:02 1997",
                "Mon Feb 10 17:32:01.4 1997",
                "Mon Feb 10 17:32:01.5 1997",
                "Mon Feb 10 17:32:01.6 1997",
                "Thu Jan 02 00:00:00 1997",
                "Thu Jan 02 03:04:05 1997",
                "Mon Feb 10 17:32:01 1997",
                "Mon Feb 10 17:32:01 1997",
                "Mon Feb 10 17:32:01 1997",
                "Mon Feb 10 17:32:01 1997",
                "Tue Jun 10 17:32:01 1997",
                "Sat Sep 22 18:19:20 2001",
                "Wed Mar 15 08:14:01 2000",
                "Wed Mar 15 13:14:02 2000",
                "Wed Mar 15 12:14:03 2000",
                "Wed Mar 15 03:14:04 2000",
                "Wed Mar 15 02:14:05 2000",
                "Mon Feb 10 17:32:01 1997",
                "Mon Feb 10 17:32:01 1997",
                "Mon Feb 10 17:32:00 1997",
                "Mon Feb 10 17:32:01 1997",
                "Mon Feb 10 17:32:01 1997",
                "Mon Feb 10 17:32:01 1997",
                "Mon Feb 10 17:32:01 1997",
                "Mon Feb 10 17:32:01 1997",
                "Mon Feb 10 17:32:01 1997",
                "Mon Feb 10 17:32:01 1997",
                "Mon Feb 10 17:32:01 1997",
                "Mon Feb 10 17:32:01 1997",
                "Tue Jun 10 18:32:01 1997",
                "Mon Feb 10 17:32:01 1997",
                "Tue Feb 11 17:32:01 1997",
                "Wed Feb 12 17:32:01 1997",
                "Thu Feb 13 17:32:01 1997",
                "Fri Feb 14 17:32:01 1997",
                "Sat Feb 15 17:32:01 1997",
                "Sun Feb 16 17:32:01 1997",
                "Sat Feb 16 17:32:01 0097",
                "Thu Feb 16 17:32:01 0597",
                "Tue Feb 16 17:32:01 1097",
                "Sat Feb 16 17:32:01 1697",
                "Thu Feb 16 17:32:01 1797",
                "Tue Feb 16 17:32:01 1897",
                "Sun Feb 16 17:32:01 1997",
                "Sat Feb 16 17:32:01 2097",
                "Wed Feb 28 17:32:01 1996",
                "Thu Feb 29 17:32:01 1996",
                "Fri Mar 01 17:32:01 1996",
                "Mon Dec 30 17:32:01 1996",
                "Tue Dec 31 17:32:01 1996",
                "Wed Jan 01 17:32:01 1997",
                "Fri Feb 28 17:32:01 1997",
                "Sat Mar 01 17:32:01 1997",
                "Tue Dec 30 17:32:01 1997",
                "Wed Dec 31 17:32:01 1997",
                "Fri Dec 31 17:32:01 1999",
                "Sat Jan 01 17:32:01 2000",
                "Sun Dec 31 17:32:01 2000",
                "Mon Jan 01 17:32:01 2001",
                null
        };
        String query = "SELECT d1 FROM TIMESTAMP_TBL";
        DBSPExpression[] results = Linq.map(data, d ->
                new DBSPTupleExpression(convertDate(d)), DBSPExpression.class);
        this.testQuery(query, new DBSPZSetLiteral.Contents(results), true);
    }

    // BC: not supported by Calcite
    // -- Check behavior at the boundaries of the timestamp range
    //SELECT '4714-11-24 00:00:00 BC'::timestamp;
    //          timestamp
    //-----------------------------
    // Mon Nov 24 00:00:00 4714 BC
    //(1 row)
    //
    //SELECT '4714-11-23 23:59:59 BC'::timestamp;  -- out of range
    //ERROR:  timestamp out of range: "4714-11-23 23:59:59 BC"
    //LINE 1: SELECT '4714-11-23 23:59:59 BC'::timestamp;
    //               ^
    // Calcite only supports 4 digit years.
    //SELECT '294276-12-31 23:59:59'::timestamp;
    //         timestamp
    //----------------------------
    // Sun Dec 31 23:59:59 294276

    @Test
    public void testGt() {
        // Calcite does not support 'without time zone'.
        // SELECT d1 FROM TIMESTAMP_TBL
        //   WHERE d1 > timestamp without time zone '1997-01-02';
        String[] data = {
                "Mon Feb 10 17:32:01 1997",
                "Mon Feb 10 17:32:01 1997",
                "Mon Feb 10 17:32:02 1997",
                "Mon Feb 10 17:32:01.4 1997",
                "Mon Feb 10 17:32:01.5 1997",
                "Mon Feb 10 17:32:01.6 1997",
                "Thu Jan 02 03:04:05 1997",
                "Mon Feb 10 17:32:01 1997",
                "Mon Feb 10 17:32:01 1997",
                "Mon Feb 10 17:32:01 1997",
                "Mon Feb 10 17:32:01 1997",
                "Tue Jun 10 17:32:01 1997",
                "Sat Sep 22 18:19:20 2001",
                "Wed Mar 15 08:14:01 2000",
                "Wed Mar 15 13:14:02 2000",
                "Wed Mar 15 12:14:03 2000",
                "Wed Mar 15 03:14:04 2000",
                "Wed Mar 15 02:14:05 2000",
                "Mon Feb 10 17:32:01 1997",
                "Mon Feb 10 17:32:01 1997",
                "Mon Feb 10 17:32:00 1997",
                "Mon Feb 10 17:32:01 1997",
                "Mon Feb 10 17:32:01 1997",
                "Mon Feb 10 17:32:01 1997",
                "Mon Feb 10 17:32:01 1997",
                "Mon Feb 10 17:32:01 1997",
                "Mon Feb 10 17:32:01 1997",
                "Mon Feb 10 17:32:01 1997",
                "Mon Feb 10 17:32:01 1997",
                "Mon Feb 10 17:32:01 1997",
                "Tue Jun 10 18:32:01 1997",
                "Mon Feb 10 17:32:01 1997",
                "Tue Feb 11 17:32:01 1997",
                "Wed Feb 12 17:32:01 1997",
                "Thu Feb 13 17:32:01 1997",
                "Fri Feb 14 17:32:01 1997",
                "Sat Feb 15 17:32:01 1997",
                "Sun Feb 16 17:32:01 1997",
                "Sun Feb 16 17:32:01 1997",
                "Sat Feb 16 17:32:01 2097",
                "Fri Feb 28 17:32:01 1997",
                "Sat Mar 01 17:32:01 1997",
                "Tue Dec 30 17:32:01 1997",
                "Wed Dec 31 17:32:01 1997",
                "Fri Dec 31 17:32:01 1999",
                "Sat Jan 01 17:32:01 2000",
                "Sun Dec 31 17:32:01 2000",
                "Mon Jan 01 17:32:01 2001"
        };
        DBSPExpression[] results = Linq.map(data, d ->
                new DBSPTupleExpression(convertDate(d)), DBSPExpression.class);
        String query = "SELECT d1 FROM TIMESTAMP_TBL\n" +
                "   WHERE d1 > timestamp '1997-01-02'";
        this.testQuery(query, new DBSPZSetLiteral.Contents(results), true);
    }
    
    @Test
    public void testLt() {
        // SELECT d1 FROM TIMESTAMP_TBL
        //   WHERE d1 < timestamp without time zone '1997-01-02';
        String[] data = {
                "Thu Jan 01 00:00:00 1970",
                //"Tue Feb 16 17:32:01 0097 BC",
                "Sat Feb 16 17:32:01 0097",
                "Thu Feb 16 17:32:01 0597",
                "Tue Feb 16 17:32:01 1097",
                "Sat Feb 16 17:32:01 1697",
                "Thu Feb 16 17:32:01 1797",
                "Tue Feb 16 17:32:01 1897",
                "Wed Feb 28 17:32:01 1996",
                "Thu Feb 29 17:32:01 1996",
                "Fri Mar 01 17:32:01 1996",
                "Mon Dec 30 17:32:01 1996",
                "Tue Dec 31 17:32:01 1996",
                "Wed Jan 01 17:32:01 1997"
        };
        DBSPExpression[] results = Linq.map(data, d ->
                new DBSPTupleExpression(convertDate(d)), DBSPExpression.class);
        String query = "SELECT d1 FROM TIMESTAMP_TBL\n" +
                "   WHERE d1 < timestamp '1997-01-02'";
        this.testQuery(query, new DBSPZSetLiteral.Contents(results), true);
    }

    @Test
    public void testEq() {
        // SELECT d1 FROM TIMESTAMP_TBL
        //   WHERE d1 = timestamp without time zone '1997-01-02';
        String[] data = {
                "Thu Jan 02 00:00:00 1997"
        };
        DBSPExpression[] results = Linq.map(data, d ->
                new DBSPTupleExpression(convertDate(d)), DBSPExpression.class);
        String query = "SELECT d1 FROM TIMESTAMP_TBL\n" +
                "   WHERE d1 = timestamp '1997-01-02'";
        this.testQuery(query, new DBSPZSetLiteral.Contents(results), true);
    }

    @Test
    public void testNe() {
        // SELECT d1 FROM TIMESTAMP_TBL
        //   WHERE d1 != timestamp without time zone '1997-01-02';
        String[] data = {
                "Thu Jan 01 00:00:00 1970",
                "Mon Feb 10 17:32:01 1997",
                "Mon Feb 10 17:32:01 1997",
                "Mon Feb 10 17:32:02 1997",
                "Mon Feb 10 17:32:01.4 1997",
                "Mon Feb 10 17:32:01.5 1997",
                "Mon Feb 10 17:32:01.6 1997",
                "Thu Jan 02 03:04:05 1997",
                "Mon Feb 10 17:32:01 1997",
                "Mon Feb 10 17:32:01 1997",
                "Mon Feb 10 17:32:01 1997",
                "Mon Feb 10 17:32:01 1997",
                "Tue Jun 10 17:32:01 1997",
                "Sat Sep 22 18:19:20 2001",
                "Wed Mar 15 08:14:01 2000",
                "Wed Mar 15 13:14:02 2000",
                "Wed Mar 15 12:14:03 2000",
                "Wed Mar 15 03:14:04 2000",
                "Wed Mar 15 02:14:05 2000",
                "Mon Feb 10 17:32:01 1997",
                "Mon Feb 10 17:32:01 1997",
                "Mon Feb 10 17:32:00 1997",
                "Mon Feb 10 17:32:01 1997",
                "Mon Feb 10 17:32:01 1997",
                "Mon Feb 10 17:32:01 1997",
                "Mon Feb 10 17:32:01 1997",
                "Mon Feb 10 17:32:01 1997",
                "Mon Feb 10 17:32:01 1997",
                "Mon Feb 10 17:32:01 1997",
                "Mon Feb 10 17:32:01 1997",
                "Mon Feb 10 17:32:01 1997",
                "Tue Jun 10 18:32:01 1997",
                "Mon Feb 10 17:32:01 1997",
                "Tue Feb 11 17:32:01 1997",
                "Wed Feb 12 17:32:01 1997",
                "Thu Feb 13 17:32:01 1997",
                "Fri Feb 14 17:32:01 1997",
                "Sat Feb 15 17:32:01 1997",
                "Sun Feb 16 17:32:01 1997",
                //"Tue Feb 16 17:32:01 0097 BC",
                "Sat Feb 16 17:32:01 0097",
                "Thu Feb 16 17:32:01 0597",
                "Tue Feb 16 17:32:01 1097",
                "Sat Feb 16 17:32:01 1697",
                "Thu Feb 16 17:32:01 1797",
                "Tue Feb 16 17:32:01 1897",
                "Sun Feb 16 17:32:01 1997",
                "Sat Feb 16 17:32:01 2097",
                "Wed Feb 28 17:32:01 1996",
                "Thu Feb 29 17:32:01 1996",
                "Fri Mar 01 17:32:01 1996",
                "Mon Dec 30 17:32:01 1996",
                "Tue Dec 31 17:32:01 1996",
                "Wed Jan 01 17:32:01 1997",
                "Fri Feb 28 17:32:01 1997",
                "Sat Mar 01 17:32:01 1997",
                "Tue Dec 30 17:32:01 1997",
                "Wed Dec 31 17:32:01 1997",
                "Fri Dec 31 17:32:01 1999",
                "Sat Jan 01 17:32:01 2000",
                "Sun Dec 31 17:32:01 2000",
                "Mon Jan 01 17:32:01 2001",
        };
        DBSPExpression[] results = Linq.map(data, d ->
                new DBSPTupleExpression(convertDate(d)), DBSPExpression.class);
        String query = "SELECT d1 FROM TIMESTAMP_TBL\n" +
                "   WHERE d1 != timestamp '1997-01-02'";
        this.testQuery(query, new DBSPZSetLiteral.Contents(results), true);
    }

    @Test
    public void testLeq() {
        // SELECT d1 FROM TIMESTAMP_TBL
        //   WHERE d1 <= timestamp without time zone '1997-01-02';
        String[] data = {
                "Thu Jan 01 00:00:00 1970",
                "Thu Jan 02 00:00:00 1997",
                //"Tue Feb 16 17:32:01 0097 BC",
                "Sat Feb 16 17:32:01 0097",
                "Thu Feb 16 17:32:01 0597",
                "Tue Feb 16 17:32:01 1097",
                "Sat Feb 16 17:32:01 1697",
                "Thu Feb 16 17:32:01 1797",
                "Tue Feb 16 17:32:01 1897",
                "Wed Feb 28 17:32:01 1996",
                "Thu Feb 29 17:32:01 1996",
                "Fri Mar 01 17:32:01 1996",
                "Mon Dec 30 17:32:01 1996",
                "Tue Dec 31 17:32:01 1996",
                "Wed Jan 01 17:32:01 1997"
        };
        DBSPExpression[] results = Linq.map(data, d ->
                new DBSPTupleExpression(convertDate(d)), DBSPExpression.class);
        String query = "SELECT d1 FROM TIMESTAMP_TBL\n" +
                "   WHERE d1 <= timestamp '1997-01-02'";
        this.testQuery(query, new DBSPZSetLiteral.Contents(results), true);
    }

    @Test
    public void testGte() {
        // SELECT d1 FROM TIMESTAMP_TBL
        //   WHERE d1 >= timestamp without time zone '1997-01-02';
        String[] data = {
                "Mon Feb 10 17:32:01 1997",
                "Mon Feb 10 17:32:01 1997",
                "Mon Feb 10 17:32:02 1997",
                "Mon Feb 10 17:32:01.4 1997",
                "Mon Feb 10 17:32:01.5 1997",
                "Mon Feb 10 17:32:01.6 1997",
                "Thu Jan 02 00:00:00 1997",
                "Thu Jan 02 03:04:05 1997",
                "Mon Feb 10 17:32:01 1997",
                "Mon Feb 10 17:32:01 1997",
                "Mon Feb 10 17:32:01 1997",
                "Mon Feb 10 17:32:01 1997",
                "Tue Jun 10 17:32:01 1997",
                "Sat Sep 22 18:19:20 2001",
                "Wed Mar 15 08:14:01 2000",
                "Wed Mar 15 13:14:02 2000",
                "Wed Mar 15 12:14:03 2000",
                "Wed Mar 15 03:14:04 2000",
                "Wed Mar 15 02:14:05 2000",
                "Mon Feb 10 17:32:01 1997",
                "Mon Feb 10 17:32:01 1997",
                "Mon Feb 10 17:32:00 1997",
                "Mon Feb 10 17:32:01 1997",
                "Mon Feb 10 17:32:01 1997",
                "Mon Feb 10 17:32:01 1997",
                "Mon Feb 10 17:32:01 1997",
                "Mon Feb 10 17:32:01 1997",
                "Mon Feb 10 17:32:01 1997",
                "Mon Feb 10 17:32:01 1997",
                "Mon Feb 10 17:32:01 1997",
                "Mon Feb 10 17:32:01 1997",
                "Tue Jun 10 18:32:01 1997",
                "Mon Feb 10 17:32:01 1997",
                "Tue Feb 11 17:32:01 1997",
                "Wed Feb 12 17:32:01 1997",
                "Thu Feb 13 17:32:01 1997",
                "Fri Feb 14 17:32:01 1997",
                "Sat Feb 15 17:32:01 1997",
                "Sun Feb 16 17:32:01 1997",
                "Sun Feb 16 17:32:01 1997",
                "Sat Feb 16 17:32:01 2097",
                "Fri Feb 28 17:32:01 1997",
                "Sat Mar 01 17:32:01 1997",
                "Tue Dec 30 17:32:01 1997",
                "Wed Dec 31 17:32:01 1997",
                "Fri Dec 31 17:32:01 1999",
                "Sat Jan 01 17:32:01 2000",
                "Sun Dec 31 17:32:01 2000",
                "Mon Jan 01 17:32:01 2001"
        };
        DBSPExpression[] results = Linq.map(data, d ->
                new DBSPTupleExpression(convertDate(d)), DBSPExpression.class);
        String query = "SELECT d1 FROM TIMESTAMP_TBL\n" +
                "   WHERE d1 >= timestamp '1997-01-02'";
        this.testQuery(query, new DBSPZSetLiteral.Contents(results), true);
    }

    static int intervalToSeconds(String interval) {
        String orig = interval;
        Pattern days = Pattern.compile("^(\\d+) days?(.*)");
        Pattern hours = Pattern.compile("^\\s*(\\d+) hours?(.*)");
        Pattern minutes = Pattern.compile("\\s*(\\d+) mins?(.*)");
        Pattern seconds = Pattern.compile("\\s*(\\d+)([.](\\d+))? secs?(.*)");
        Pattern ago = Pattern.compile("\\s*ago(.*)");

        int result = 0;
        if (interval.equals("0")) {
            interval = "";
        } else {
            Matcher m = days.matcher(interval);
            if (m.matches()) {
                int d = Integer.parseInt(m.group(1));
                result += d * 86400;
                interval = m.group(2);
            }

            m = hours.matcher(interval);
            if (m.matches()) {
                int h = Integer.parseInt(m.group(1));
                result += h * 3600;
                interval = m.group(2);
            }

            m = minutes.matcher(interval);
            if (m.matches()) {
                int mm = Integer.parseInt(m.group(1));
                result += mm * 60;
                interval = m.group(2);
            }

            m = seconds.matcher(interval);
            if (m.matches()) {
                int s = Integer.parseInt(m.group(1));
                result += s;
                interval = m.group(4);
            }

            m = ago.matcher(interval);
            if (m.matches()) {
                interval = m.group(1);
                result = -result;
            }
        }
        //System.out.println(orig + "->" + result + ": " + interval);
        if (!interval.isEmpty())
            throw new RuntimeException("Could not parse interval " + orig);
        return result;
    }

    @Test
    public void diff() {
        // Calcite does not support timestamp difference
        // SELECT d1 - timestamp without time zone '1997-01-02' AS diff
        //   FROM TIMESTAMP_TBL WHERE d1 BETWEEN '1902-01-01' AND '2038-01-01';
        String[] data = {
                "9863 days ago",
                "39 days 17 hours 32 mins 1 sec",
                "39 days 17 hours 32 mins 1 sec",
                "39 days 17 hours 32 mins 2 secs",
                "39 days 17 hours 32 mins 1.4 secs",
                "39 days 17 hours 32 mins 1.5 secs",
                "39 days 17 hours 32 mins 1.6 secs",
                "0",
                "3 hours 4 mins 5 secs",
                "39 days 17 hours 32 mins 1 sec",
                "39 days 17 hours 32 mins 1 sec",
                "39 days 17 hours 32 mins 1 sec",
                "39 days 17 hours 32 mins 1 sec",
                "159 days 17 hours 32 mins 1 sec",
                "1724 days 18 hours 19 mins 20 secs",
                "1168 days 8 hours 14 mins 1 sec",
                "1168 days 13 hours 14 mins 2 secs",
                "1168 days 12 hours 14 mins 3 secs",
                "1168 days 3 hours 14 mins 4 secs",
                "1168 days 2 hours 14 mins 5 secs",
                "39 days 17 hours 32 mins 1 sec",
                "39 days 17 hours 32 mins 1 sec",
                "39 days 17 hours 32 mins",
                "39 days 17 hours 32 mins 1 sec",
                "39 days 17 hours 32 mins 1 sec",
                "39 days 17 hours 32 mins 1 sec",
                "39 days 17 hours 32 mins 1 sec",
                "39 days 17 hours 32 mins 1 sec",
                "39 days 17 hours 32 mins 1 sec",
                "39 days 17 hours 32 mins 1 sec",
                "39 days 17 hours 32 mins 1 sec",
                "39 days 17 hours 32 mins 1 sec",
                "159 days 18 hours 32 mins 1 sec",
                "39 days 17 hours 32 mins 1 sec",
                "40 days 17 hours 32 mins 1 sec",
                "41 days 17 hours 32 mins 1 sec",
                "42 days 17 hours 32 mins 1 sec",
                "43 days 17 hours 32 mins 1 sec",
                "44 days 17 hours 32 mins 1 sec",
                "45 days 17 hours 32 mins 1 sec",
                "45 days 17 hours 32 mins 1 sec",
                "308 days 6 hours 27 mins 59 secs ago",
                "307 days 6 hours 27 mins 59 secs ago",
                "306 days 6 hours 27 mins 59 secs ago",
                "2 days 6 hours 27 mins 59 secs ago",
                "1 day 6 hours 27 mins 59 secs ago",
                "6 hours 27 mins 59 secs ago",
                "57 days 17 hours 32 mins 1 sec",
                "58 days 17 hours 32 mins 1 sec",
                "362 days 17 hours 32 mins 1 sec",
                "363 days 17 hours 32 mins 1 sec",
                "1093 days 17 hours 32 mins 1 sec",
                "1094 days 17 hours 32 mins 1 sec",
                "1459 days 17 hours 32 mins 1 sec",
                "1460 days 17 hours 32 mins 1 sec"
        };

        DBSPExpression[] results = Linq.map(data, d ->
                new DBSPTupleExpression(d == null ? DBSPLiteral.none(DBSPTypeInteger.NULLABLE_SIGNED_32) :
                        new DBSPI32Literal(-intervalToSeconds(d) / 60, true)), DBSPExpression.class);
        String query = "SELECT TIMESTAMPDIFF(MINUTE, d1, timestamp '1997-01-02') AS diff\n" +
                "   FROM TIMESTAMP_TBL WHERE d1 BETWEEN '1902-01-01' AND '2038-01-01'";
        this.testQuery(query, new DBSPZSetLiteral.Contents(results), true);
    }

    /**
     * Run query with and without optimizations.
     * By running with optimizations disabled we test runtime evaluation of constant expressions,
     * which should give the same result as the Calcite evaluation.
     */
    void testQueryTwice(String query, DBSPTimestampLiteral expectedValue) {
        DBSPZSetLiteral.Contents zset = new DBSPZSetLiteral.Contents(new DBSPTupleExpression(expectedValue));
        this.testQuery(query, zset, false);
        this.testQuery(query, zset, true);
    }

    @Test
    public void testWeek() {
        // SELECT date_trunc( 'week', timestamp '2004-02-29 15:44:17.71393' ) AS week_trunc;
        // Calcite DDL does not support date_trunc
        // TODO: Postgres dives a different result from Calcite!
        // This day was a Sunday.  Postgres returns 2004-02-23, the previous Monday.
        String query = "SELECT FLOOR(timestamp '2004-02-29 15:44:17.71393' TO WEEK) AS week_trunc";
        this.testQueryTwice(query, new DBSPTimestampLiteral("2004-02-29 00:00:00", false));
    }

    // DATE_BIN not supported by Calcite.
    // SELECT
    //  str,
    //  interval,
    //  date_trunc(str, ts) = date_bin(interval::interval, ts, timestamp '2001-01-01') AS equal
    //FROM (
    //  VALUES
    //  ('week', '7 d'),
    //  ('day', '1 d'),
    //  ('hour', '1 h'),
    //  ('minute', '1 m'),
    //  ('second', '1 s'),
    //  ('millisecond', '1 ms'),
    //  ('microsecond', '1 us')
    //) intervals (str, interval),
    //(VALUES (timestamp '2020-02-29 15:44:17.71393')) ts (ts);

    @Test
    public void testCastBetween() {
        // SELECT d1 - timestamp without time zone '1997-01-02' AS diff
        //  FROM TIMESTAMP_TBL
        //  WHERE d1 BETWEEN timestamp without time zone '1902-01-01'
        //   AND timestamp without time zone '2038-01-01';
        String query = "SELECT TIMESTAMPDIFF(SECOND, d1, timestamp '1997-01-02') AS diff\n" +
                "  FROM TIMESTAMP_TBL\n" +
                "  WHERE d1 BETWEEN timestamp '1902-01-01'\n" +
                "   AND timestamp '2038-01-01'";
        String[] data = {
            "9863 days ago",
            "39 days 17 hours 32 mins 1 sec",
            "39 days 17 hours 32 mins 1 sec",
            "39 days 17 hours 32 mins 2 secs",
            "39 days 17 hours 32 mins 1.4 secs",
            "39 days 17 hours 32 mins 1.5 secs",
            "39 days 17 hours 32 mins 1.6 secs",
            "0",
            "3 hours 4 mins 5 secs",
            "39 days 17 hours 32 mins 1 sec",
            "39 days 17 hours 32 mins 1 sec",
            "39 days 17 hours 32 mins 1 sec",
            "39 days 17 hours 32 mins 1 sec",
            "159 days 17 hours 32 mins 1 sec",
            "1724 days 18 hours 19 mins 20 secs",
            "1168 days 8 hours 14 mins 1 sec",
            "1168 days 13 hours 14 mins 2 secs",
            "1168 days 12 hours 14 mins 3 secs",
            "1168 days 3 hours 14 mins 4 secs",
            "1168 days 2 hours 14 mins 5 secs",
            "39 days 17 hours 32 mins 1 sec",
            "39 days 17 hours 32 mins 1 sec",
            "39 days 17 hours 32 mins",
            "39 days 17 hours 32 mins 1 sec",
            "39 days 17 hours 32 mins 1 sec",
            "39 days 17 hours 32 mins 1 sec",
            "39 days 17 hours 32 mins 1 sec",
            "39 days 17 hours 32 mins 1 sec",
            "39 days 17 hours 32 mins 1 sec",
            "39 days 17 hours 32 mins 1 sec",
            "39 days 17 hours 32 mins 1 sec",
            "39 days 17 hours 32 mins 1 sec",
            "159 days 18 hours 32 mins 1 sec",
            "39 days 17 hours 32 mins 1 sec",
            "40 days 17 hours 32 mins 1 sec",
            "41 days 17 hours 32 mins 1 sec",
            "42 days 17 hours 32 mins 1 sec",
            "43 days 17 hours 32 mins 1 sec",
            "44 days 17 hours 32 mins 1 sec",
            "45 days 17 hours 32 mins 1 sec",
            "45 days 17 hours 32 mins 1 sec",
            "308 days 6 hours 27 mins 59 secs ago",
            "307 days 6 hours 27 mins 59 secs ago",
            "306 days 6 hours 27 mins 59 secs ago",
            "2 days 6 hours 27 mins 59 secs ago",
            "1 day 6 hours 27 mins 59 secs ago",
            "6 hours 27 mins 59 secs ago",
            "57 days 17 hours 32 mins 1 sec",
            "58 days 17 hours 32 mins 1 sec",
            "362 days 17 hours 32 mins 1 sec",
            "363 days 17 hours 32 mins 1 sec",
            "1093 days 17 hours 32 mins 1 sec",
            "1094 days 17 hours 32 mins 1 sec",
            "1459 days 17 hours 32 mins 1 sec",
            "1460 days 17 hours 32 mins 1 sec"
        };
        DBSPExpression[] results = Linq.map(data, d ->
                new DBSPTupleExpression(d == null ? DBSPLiteral.none(DBSPTypeInteger.NULLABLE_SIGNED_32) :
                        new DBSPI32Literal(-intervalToSeconds(d), true)), DBSPExpression.class);
        this.testQuery(query, new DBSPZSetLiteral.Contents(results), true);
    }

    @Test
    public void testDatePart() {
        // SELECT d1 as "timestamp",
        //   date_part( 'year', d1) AS year, date_part( 'month', d1) AS month,
        //   date_part( 'day', d1) AS day, date_part( 'hour', d1) AS hour,
        //   date_part( 'minute', d1) AS minute, date_part( 'second', d1) AS second
        //   FROM TIMESTAMP_TBL;
        // Postgres EXTRACT returns floats for seconds...
        String query = "SELECT d1 as \"timestamp\",\n" +
                "EXTRACT(YEAR FROM d1) AS 'year', EXTRACT(month FROM d1) AS 'month',\n" +
                "EXTRACT(day FROM d1) AS 'day', EXTRACT(hour FROM d1) AS 'hour',\n" +
                "EXTRACT(minute FROM d1) AS 'minute', EXTRACT(second FROM d1) AS 'second'\n" +
                "FROM TIMESTAMP_TBL";
        String[] data = {
            "Thu Jan 01 00:00:00 1970    |      1970 |     1 |   1 |    0 |      0 |      0",
            "Mon Feb 10 17:32:01 1997    |      1997 |     2 |  10 |   17 |     32 |      1",
            "Mon Feb 10 17:32:01 1997    |      1997 |     2 |  10 |   17 |     32 |      1",
            "Mon Feb 10 17:32:02 1997    |      1997 |     2 |  10 |   17 |     32 |      2",
            "Mon Feb 10 17:32:01.4 1997  |      1997 |     2 |  10 |   17 |     32 |    1.4",
            "Mon Feb 10 17:32:01.5 1997  |      1997 |     2 |  10 |   17 |     32 |    1.5",
            "Mon Feb 10 17:32:01.6 1997  |      1997 |     2 |  10 |   17 |     32 |    1.6",
            "Thu Jan 02 00:00:00 1997    |      1997 |     1 |   2 |    0 |      0 |      0",
            "Thu Jan 02 03:04:05 1997    |      1997 |     1 |   2 |    3 |      4 |      5",
            "Mon Feb 10 17:32:01 1997    |      1997 |     2 |  10 |   17 |     32 |      1",
            "Mon Feb 10 17:32:01 1997    |      1997 |     2 |  10 |   17 |     32 |      1",
            "Mon Feb 10 17:32:01 1997    |      1997 |     2 |  10 |   17 |     32 |      1",
            "Mon Feb 10 17:32:01 1997    |      1997 |     2 |  10 |   17 |     32 |      1",
            "Tue Jun 10 17:32:01 1997    |      1997 |     6 |  10 |   17 |     32 |      1",
            "Sat Sep 22 18:19:20 2001    |      2001 |     9 |  22 |   18 |     19 |     20",
            "Wed Mar 15 08:14:01 2000    |      2000 |     3 |  15 |    8 |     14 |      1",
            "Wed Mar 15 13:14:02 2000    |      2000 |     3 |  15 |   13 |     14 |      2",
            "Wed Mar 15 12:14:03 2000    |      2000 |     3 |  15 |   12 |     14 |      3",
            "Wed Mar 15 03:14:04 2000    |      2000 |     3 |  15 |    3 |     14 |      4",
            "Wed Mar 15 02:14:05 2000    |      2000 |     3 |  15 |    2 |     14 |      5",
            "Mon Feb 10 17:32:01 1997    |      1997 |     2 |  10 |   17 |     32 |      1",
            "Mon Feb 10 17:32:01 1997    |      1997 |     2 |  10 |   17 |     32 |      1",
            "Mon Feb 10 17:32:00 1997    |      1997 |     2 |  10 |   17 |     32 |      0",
            "Mon Feb 10 17:32:01 1997    |      1997 |     2 |  10 |   17 |     32 |      1",
            "Mon Feb 10 17:32:01 1997    |      1997 |     2 |  10 |   17 |     32 |      1",
            "Mon Feb 10 17:32:01 1997    |      1997 |     2 |  10 |   17 |     32 |      1",
            "Mon Feb 10 17:32:01 1997    |      1997 |     2 |  10 |   17 |     32 |      1",
            "Mon Feb 10 17:32:01 1997    |      1997 |     2 |  10 |   17 |     32 |      1",
            "Mon Feb 10 17:32:01 1997    |      1997 |     2 |  10 |   17 |     32 |      1",
            "Mon Feb 10 17:32:01 1997    |      1997 |     2 |  10 |   17 |     32 |      1",
            "Mon Feb 10 17:32:01 1997    |      1997 |     2 |  10 |   17 |     32 |      1",
            "Mon Feb 10 17:32:01 1997    |      1997 |     2 |  10 |   17 |     32 |      1",
            "Tue Jun 10 18:32:01 1997    |      1997 |     6 |  10 |   18 |     32 |      1",
            "Mon Feb 10 17:32:01 1997    |      1997 |     2 |  10 |   17 |     32 |      1",
            "Tue Feb 11 17:32:01 1997    |      1997 |     2 |  11 |   17 |     32 |      1",
            "Wed Feb 12 17:32:01 1997    |      1997 |     2 |  12 |   17 |     32 |      1",
            "Thu Feb 13 17:32:01 1997    |      1997 |     2 |  13 |   17 |     32 |      1",
            "Fri Feb 14 17:32:01 1997    |      1997 |     2 |  14 |   17 |     32 |      1",
            "Sat Feb 15 17:32:01 1997    |      1997 |     2 |  15 |   17 |     32 |      1",
            "Sun Feb 16 17:32:01 1997    |      1997 |     2 |  16 |   17 |     32 |      1",
            "Sat Feb 16 17:32:01 0097    |        97 |     2 |  16 |   17 |     32 |      1",
            "Thu Feb 16 17:32:01 0597    |       597 |     2 |  16 |   17 |     32 |      1",
            "Tue Feb 16 17:32:01 1097    |      1097 |     2 |  16 |   17 |     32 |      1",
            "Sat Feb 16 17:32:01 1697    |      1697 |     2 |  16 |   17 |     32 |      1",
            "Thu Feb 16 17:32:01 1797    |      1797 |     2 |  16 |   17 |     32 |      1",
            "Tue Feb 16 17:32:01 1897    |      1897 |     2 |  16 |   17 |     32 |      1",
            "Sun Feb 16 17:32:01 1997    |      1997 |     2 |  16 |   17 |     32 |      1",
            "Sat Feb 16 17:32:01 2097    |      2097 |     2 |  16 |   17 |     32 |      1",
            "Wed Feb 28 17:32:01 1996    |      1996 |     2 |  28 |   17 |     32 |      1",
            "Thu Feb 29 17:32:01 1996    |      1996 |     2 |  29 |   17 |     32 |      1",
            "Fri Mar 01 17:32:01 1996    |      1996 |     3 |   1 |   17 |     32 |      1",
            "Mon Dec 30 17:32:01 1996    |      1996 |    12 |  30 |   17 |     32 |      1",
            "Tue Dec 31 17:32:01 1996    |      1996 |    12 |  31 |   17 |     32 |      1",
            "Wed Jan 01 17:32:01 1997    |      1997 |     1 |   1 |   17 |     32 |      1",
            "Fri Feb 28 17:32:01 1997    |      1997 |     2 |  28 |   17 |     32 |      1",
            "Sat Mar 01 17:32:01 1997    |      1997 |     3 |   1 |   17 |     32 |      1",
            "Tue Dec 30 17:32:01 1997    |      1997 |    12 |  30 |   17 |     32 |      1",
            "Wed Dec 31 17:32:01 1997    |      1997 |    12 |  31 |   17 |     32 |      1",
            "Fri Dec 31 17:32:01 1999    |      1999 |    12 |  31 |   17 |     32 |      1",
            "Sat Jan 01 17:32:01 2000    |      2000 |     1 |   1 |   17 |     32 |      1",
            "Sun Dec 31 17:32:01 2000    |      2000 |    12 |  31 |   17 |     32 |      1",
            "Mon Jan 01 17:32:01 2001    |      2001 |     1 |   1 |   17 |     32 |      1"      
        };
        int columns = 7;
        DBSPExpression[] tuples = new DBSPExpression[data.length+1]; // last one with nulls.
        for (int j = 0; j < data.length; j++) {
            String d = data[j];
            String[] fields = d.split("[|]");
            Assert.assertEquals(columns, fields.length);
            DBSPExpression[] expressions = new DBSPExpression[columns];
            expressions[0] = convertDate(fields[0].trim());
            for (int i = 1; i < columns - 1; i++)
                expressions[i] = new DBSPI64Literal(Long.parseLong(fields[i].trim()), true);
            expressions[columns - 1] = new DBSPI64Literal((long)Double.parseDouble(fields[6].trim()), true);
            tuples[j] = new DBSPTupleExpression(expressions);
        }
        DBSPExpression none = DBSPLiteral.none(DBSPTypeInteger.SIGNED_64.setMayBeNull(true));
        tuples[data.length] = new DBSPTupleExpression(convertDate(null), none, none, none, none, none, none);
        this.testQuery(query, new DBSPZSetLiteral.Contents(tuples), true);
    }
    
    @Test
    public void testQuarter() {
        // SELECT d1 as "timestamp",
        //   date_part( 'quarter', d1) AS quarter, date_part( 'msec', d1) AS msec,
        //   date_part( 'usec', d1) AS usec
        //   FROM TIMESTAMP_TBL;
        String query = "SELECT d1 as \"timestamp\",\n" +
                "   EXTRACT(quarter FROM d1) AS 'quarter', EXTRACT(MILLISECOND FROM d1) AS 'msec',\n" +
                "   EXTRACT(MICROSECOND FROM d1) AS 'usec'\n" +
                "   FROM TIMESTAMP_TBL";
        String[] data = {
                "Thu Jan 01 00:00:00 1970    |       1 |     0 |        0",
                "Mon Feb 10 17:32:01 1997    |       1 |  1000 |  1000000",
                "Mon Feb 10 17:32:01 1997    |       1 |  1000 |  1000000",
                "Mon Feb 10 17:32:02 1997    |       1 |  2000 |  2000000",
                "Mon Feb 10 17:32:01.4 1997  |       1 |  1400 |  1400000",
                "Mon Feb 10 17:32:01.5 1997  |       1 |  1500 |  1500000",
                "Mon Feb 10 17:32:01.6 1997  |       1 |  1600 |  1600000",
                "Thu Jan 02 00:00:00 1997    |       1 |     0 |        0",
                "Thu Jan 02 03:04:05 1997    |       1 |  5000 |  5000000",
                "Mon Feb 10 17:32:01 1997    |       1 |  1000 |  1000000",
                "Mon Feb 10 17:32:01 1997    |       1 |  1000 |  1000000",
                "Mon Feb 10 17:32:01 1997    |       1 |  1000 |  1000000",
                "Mon Feb 10 17:32:01 1997    |       1 |  1000 |  1000000",
                "Tue Jun 10 17:32:01 1997    |       2 |  1000 |  1000000",
                "Sat Sep 22 18:19:20 2001    |       3 | 20000 | 20000000",
                "Wed Mar 15 08:14:01 2000    |       1 |  1000 |  1000000",
                "Wed Mar 15 13:14:02 2000    |       1 |  2000 |  2000000",
                "Wed Mar 15 12:14:03 2000    |       1 |  3000 |  3000000",
                "Wed Mar 15 03:14:04 2000    |       1 |  4000 |  4000000",
                "Wed Mar 15 02:14:05 2000    |       1 |  5000 |  5000000",
                "Mon Feb 10 17:32:01 1997    |       1 |  1000 |  1000000",
                "Mon Feb 10 17:32:01 1997    |       1 |  1000 |  1000000",
                "Mon Feb 10 17:32:00 1997    |       1 |     0 |        0",
                "Mon Feb 10 17:32:01 1997    |       1 |  1000 |  1000000",
                "Mon Feb 10 17:32:01 1997    |       1 |  1000 |  1000000",
                "Mon Feb 10 17:32:01 1997    |       1 |  1000 |  1000000",
                "Mon Feb 10 17:32:01 1997    |       1 |  1000 |  1000000",
                "Mon Feb 10 17:32:01 1997    |       1 |  1000 |  1000000",
                "Mon Feb 10 17:32:01 1997    |       1 |  1000 |  1000000",
                "Mon Feb 10 17:32:01 1997    |       1 |  1000 |  1000000",
                "Mon Feb 10 17:32:01 1997    |       1 |  1000 |  1000000",
                "Mon Feb 10 17:32:01 1997    |       1 |  1000 |  1000000",
                "Tue Jun 10 18:32:01 1997    |       2 |  1000 |  1000000",
                "Mon Feb 10 17:32:01 1997    |       1 |  1000 |  1000000",
                "Tue Feb 11 17:32:01 1997    |       1 |  1000 |  1000000",
                "Wed Feb 12 17:32:01 1997    |       1 |  1000 |  1000000",
                "Thu Feb 13 17:32:01 1997    |       1 |  1000 |  1000000",
                "Fri Feb 14 17:32:01 1997    |       1 |  1000 |  1000000",
                "Sat Feb 15 17:32:01 1997    |       1 |  1000 |  1000000",
                "Sun Feb 16 17:32:01 1997    |       1 |  1000 |  1000000",
                "Sat Feb 16 17:32:01 0097    |       1 |  1000 |  1000000",
                "Thu Feb 16 17:32:01 0597    |       1 |  1000 |  1000000",
                "Tue Feb 16 17:32:01 1097    |       1 |  1000 |  1000000",
                "Sat Feb 16 17:32:01 1697    |       1 |  1000 |  1000000",
                "Thu Feb 16 17:32:01 1797    |       1 |  1000 |  1000000",
                "Tue Feb 16 17:32:01 1897    |       1 |  1000 |  1000000",
                "Sun Feb 16 17:32:01 1997    |       1 |  1000 |  1000000",
                "Sat Feb 16 17:32:01 2097    |       1 |  1000 |  1000000",
                "Wed Feb 28 17:32:01 1996    |       1 |  1000 |  1000000",
                "Thu Feb 29 17:32:01 1996    |       1 |  1000 |  1000000",
                "Fri Mar 01 17:32:01 1996    |       1 |  1000 |  1000000",
                "Mon Dec 30 17:32:01 1996    |       4 |  1000 |  1000000",
                "Tue Dec 31 17:32:01 1996    |       4 |  1000 |  1000000",
                "Wed Jan 01 17:32:01 1997    |       1 |  1000 |  1000000",
                "Fri Feb 28 17:32:01 1997    |       1 |  1000 |  1000000",
                "Sat Mar 01 17:32:01 1997    |       1 |  1000 |  1000000",
                "Tue Dec 30 17:32:01 1997    |       4 |  1000 |  1000000",
                "Wed Dec 31 17:32:01 1997    |       4 |  1000 |  1000000",
                "Fri Dec 31 17:32:01 1999    |       4 |  1000 |  1000000",
                "Sat Jan 01 17:32:01 2000    |       1 |  1000 |  1000000",
                "Sun Dec 31 17:32:01 2000    |       4 |  1000 |  1000000",
                "Mon Jan 01 17:32:01 2001    |       1 |  1000 |  1000000"
        };
        int columns = 4;
        DBSPExpression[] tuples = new DBSPExpression[data.length+1]; // last one with nulls.
        for (int j = 0; j < data.length; j++) {
            String d = data[j];
            String[] fields = d.split("[|]");
            Assert.assertEquals(columns, fields.length);
            DBSPExpression[] expressions = new DBSPExpression[columns];
            expressions[0] = convertDate(fields[0].trim());
            for (int i = 1; i < columns; i++)
                expressions[i] = new DBSPI64Literal(Long.parseLong(fields[i].trim()), true);
            tuples[j] = new DBSPTupleExpression(expressions);
        }
        DBSPExpression none = DBSPLiteral.none(DBSPTypeInteger.SIGNED_64.setMayBeNull(true));
        tuples[data.length] = new DBSPTupleExpression(convertDate(null), none, none, none);
        this.testQuery(query, new DBSPZSetLiteral.Contents(tuples), true);
    }
    
    @Test
    public void testDay() {
        // SELECT d1 as "timestamp",
        //   date_part( 'isoyear', d1) AS isoyear, date_part( 'week', d1) AS week,
        //   date_part( 'isodow', d1) AS isodow, date_part( 'dow', d1) AS dow,
        //   date_part( 'doy', d1) AS doy
        //   FROM TIMESTAMP_TBL;
        String query = "SELECT d1 as \"timestamp\",\n" +
                "   extract(isoyear FROM d1) AS 'isoyear', extract(week FROM d1) AS 'week',\n" +
                "   extract(isodow FROM d1) AS 'isodow', extract(dow FROM d1) AS 'dow',\n" +
                "   extract(doy FROM d1) AS 'doy'\n" +
                "   FROM TIMESTAMP_TBL";
        String[] data = {
            "Thu Jan 01 00:00:00 1970    |      1970 |    1 |      4 |   4 |   1",
            "Mon Feb 10 17:32:01 1997    |      1997 |    7 |      1 |   1 |  41",
            "Mon Feb 10 17:32:01 1997    |      1997 |    7 |      1 |   1 |  41",
            "Mon Feb 10 17:32:02 1997    |      1997 |    7 |      1 |   1 |  41",
            "Mon Feb 10 17:32:01.4 1997  |      1997 |    7 |      1 |   1 |  41",
            "Mon Feb 10 17:32:01.5 1997  |      1997 |    7 |      1 |   1 |  41",
            "Mon Feb 10 17:32:01.6 1997  |      1997 |    7 |      1 |   1 |  41",
            "Thu Jan 02 00:00:00 1997    |      1997 |    1 |      4 |   4 |   2",
            "Thu Jan 02 03:04:05 1997    |      1997 |    1 |      4 |   4 |   2",
            "Mon Feb 10 17:32:01 1997    |      1997 |    7 |      1 |   1 |  41",
            "Mon Feb 10 17:32:01 1997    |      1997 |    7 |      1 |   1 |  41",
            "Mon Feb 10 17:32:01 1997    |      1997 |    7 |      1 |   1 |  41",
            "Mon Feb 10 17:32:01 1997    |      1997 |    7 |      1 |   1 |  41",
            "Tue Jun 10 17:32:01 1997    |      1997 |   24 |      2 |   2 | 161",
            "Sat Sep 22 18:19:20 2001    |      2001 |   38 |      6 |   6 | 265",
            "Wed Mar 15 08:14:01 2000    |      2000 |   11 |      3 |   3 |  75",
            "Wed Mar 15 13:14:02 2000    |      2000 |   11 |      3 |   3 |  75",
            "Wed Mar 15 12:14:03 2000    |      2000 |   11 |      3 |   3 |  75",
            "Wed Mar 15 03:14:04 2000    |      2000 |   11 |      3 |   3 |  75",
            "Wed Mar 15 02:14:05 2000    |      2000 |   11 |      3 |   3 |  75",
            "Mon Feb 10 17:32:01 1997    |      1997 |    7 |      1 |   1 |  41",
            "Mon Feb 10 17:32:01 1997    |      1997 |    7 |      1 |   1 |  41",
            "Mon Feb 10 17:32:00 1997    |      1997 |    7 |      1 |   1 |  41",
            "Mon Feb 10 17:32:01 1997    |      1997 |    7 |      1 |   1 |  41",
            "Mon Feb 10 17:32:01 1997    |      1997 |    7 |      1 |   1 |  41",
            "Mon Feb 10 17:32:01 1997    |      1997 |    7 |      1 |   1 |  41",
            "Mon Feb 10 17:32:01 1997    |      1997 |    7 |      1 |   1 |  41",
            "Mon Feb 10 17:32:01 1997    |      1997 |    7 |      1 |   1 |  41",
            "Mon Feb 10 17:32:01 1997    |      1997 |    7 |      1 |   1 |  41",
            "Mon Feb 10 17:32:01 1997    |      1997 |    7 |      1 |   1 |  41",
            "Mon Feb 10 17:32:01 1997    |      1997 |    7 |      1 |   1 |  41",
            "Mon Feb 10 17:32:01 1997    |      1997 |    7 |      1 |   1 |  41",
            "Tue Jun 10 18:32:01 1997    |      1997 |   24 |      2 |   2 | 161",
            "Mon Feb 10 17:32:01 1997    |      1997 |    7 |      1 |   1 |  41",
            "Tue Feb 11 17:32:01 1997    |      1997 |    7 |      2 |   2 |  42",
            "Wed Feb 12 17:32:01 1997    |      1997 |    7 |      3 |   3 |  43",
            "Thu Feb 13 17:32:01 1997    |      1997 |    7 |      4 |   4 |  44",
            "Fri Feb 14 17:32:01 1997    |      1997 |    7 |      5 |   5 |  45",
            "Sat Feb 15 17:32:01 1997    |      1997 |    7 |      6 |   6 |  46",
            "Sun Feb 16 17:32:01 1997    |      1997 |    7 |      7 |   0 |  47",
            //"Tue Feb 16 17:32:01 0097 BC |       -97 |    7 |      2 |   2 |  47",
            "Sat Feb 16 17:32:01 0097    |        97 |    7 |      6 |   6 |  47",
            "Thu Feb 16 17:32:01 0597    |       597 |    7 |      4 |   4 |  47",
            "Tue Feb 16 17:32:01 1097    |      1097 |    7 |      2 |   2 |  47",
            "Sat Feb 16 17:32:01 1697    |      1697 |    7 |      6 |   6 |  47",
            "Thu Feb 16 17:32:01 1797    |      1797 |    7 |      4 |   4 |  47",
            "Tue Feb 16 17:32:01 1897    |      1897 |    7 |      2 |   2 |  47",
            "Sun Feb 16 17:32:01 1997    |      1997 |    7 |      7 |   0 |  47",
            "Sat Feb 16 17:32:01 2097    |      2097 |    7 |      6 |   6 |  47",
            "Wed Feb 28 17:32:01 1996    |      1996 |    9 |      3 |   3 |  59",
            "Thu Feb 29 17:32:01 1996    |      1996 |    9 |      4 |   4 |  60",
            "Fri Mar 01 17:32:01 1996    |      1996 |    9 |      5 |   5 |  61",
            "Mon Dec 30 17:32:01 1996    |      1997 |    1 |      1 |   1 | 365",
            "Tue Dec 31 17:32:01 1996    |      1997 |    1 |      2 |   2 | 366",
            "Wed Jan 01 17:32:01 1997    |      1997 |    1 |      3 |   3 |   1",
            "Fri Feb 28 17:32:01 1997    |      1997 |    9 |      5 |   5 |  59",
            "Sat Mar 01 17:32:01 1997    |      1997 |    9 |      6 |   6 |  60",
            "Tue Dec 30 17:32:01 1997    |      1998 |    1 |      2 |   2 | 364",
            "Wed Dec 31 17:32:01 1997    |      1998 |    1 |      3 |   3 | 365",
            "Fri Dec 31 17:32:01 1999    |      1999 |   52 |      5 |   5 | 365",
            "Sat Jan 01 17:32:01 2000    |      1999 |   52 |      6 |   6 |   1",
            "Sun Dec 31 17:32:01 2000    |      2000 |   52 |      7 |   0 | 366",
            "Mon Jan 01 17:32:01 2001    |      2001 |    1 |      1 |   1 |   1"
        };
        int columns = 6;
        DBSPExpression[] tuples = new DBSPExpression[data.length+1]; // last one with nulls.
        for (int j = 0; j < data.length; j++) {
            String d = data[j];
            String[] fields = d.split("[|]");
            Assert.assertEquals(columns, fields.length);
            DBSPExpression[] expressions = new DBSPExpression[columns];
            expressions[0] = convertDate(fields[0].trim());
            for (int i = 1; i < columns; i++) {
                long adjust = (i == 4) ? CalciteToDBSPCompiler.firstDOW : 0;
                expressions[i] = new DBSPI64Literal(Long.parseLong(fields[i].trim()) + adjust, true);
            }
            tuples[j] = new DBSPTupleExpression(expressions);
        }
        DBSPExpression none = DBSPLiteral.none(DBSPTypeInteger.SIGNED_64.setMayBeNull(true));
        tuples[data.length] = new DBSPTupleExpression(convertDate(null), none, none, none, none, none);
        this.testQuery(query, new DBSPZSetLiteral.Contents(tuples), true);
    }
    
    @Test
    public void testCenturies() {
        // SELECT d1 as "timestamp",
        //   date_part( 'decade', d1) AS decade,
        //   date_part( 'century', d1) AS century,
        //   date_part( 'millennium', d1) AS millennium,
        //   round(date_part( 'julian', d1)) AS julian,
        //   date_part( 'epoch', d1) AS epoch
        //   FROM TIMESTAMP_TBL;
        String query = "SELECT d1 as \"timestamp\",\n" +
                "   extract(decade FROM d1) AS 'decade',\n" +
                "   extract(century FROM d1) AS 'century',\n" +
                "   extract(millennium FROM d1) AS 'millennium',\n" +
                "--   round(extract(julian FROM d1)) AS 'julian',\n" + // Julian is not supported
                "   extract(epoch FROM d1) AS 'epoch'\n" +
                "   FROM TIMESTAMP_TBL";
        String[] data = {
                "Thu Jan 01 00:00:00 1970    |       197 |        20 |          2 |   2440588 |            0",
                "Mon Feb 10 17:32:01 1997    |       199 |        20 |          2 |   2450491 |    855595921",
                "Mon Feb 10 17:32:01 1997    |       199 |        20 |          2 |   2450491 |    855595921",
                "Mon Feb 10 17:32:02 1997    |       199 |        20 |          2 |   2450491 |    855595922",
                "Mon Feb 10 17:32:01.4 1997  |       199 |        20 |          2 |   2450491 |  855595921.4",
                "Mon Feb 10 17:32:01.5 1997  |       199 |        20 |          2 |   2450491 |  855595921.5",
                "Mon Feb 10 17:32:01.6 1997  |       199 |        20 |          2 |   2450491 |  855595921.6",
                "Thu Jan 02 00:00:00 1997    |       199 |        20 |          2 |   2450451 |    852163200",
                "Thu Jan 02 03:04:05 1997    |       199 |        20 |          2 |   2450451 |    852174245",
                "Mon Feb 10 17:32:01 1997    |       199 |        20 |          2 |   2450491 |    855595921",
                "Mon Feb 10 17:32:01 1997    |       199 |        20 |          2 |   2450491 |    855595921",
                "Mon Feb 10 17:32:01 1997    |       199 |        20 |          2 |   2450491 |    855595921",
                "Mon Feb 10 17:32:01 1997    |       199 |        20 |          2 |   2450491 |    855595921",
                "Tue Jun 10 17:32:01 1997    |       199 |        20 |          2 |   2450611 |    865963921",
                "Sat Sep 22 18:19:20 2001    |       200 |        21 |          3 |   2452176 |   1001182760",
                "Wed Mar 15 08:14:01 2000    |       200 |        20 |          2 |   2451619 |    953108041",
                "Wed Mar 15 13:14:02 2000    |       200 |        20 |          2 |   2451620 |    953126042",
                "Wed Mar 15 12:14:03 2000    |       200 |        20 |          2 |   2451620 |    953122443",
                "Wed Mar 15 03:14:04 2000    |       200 |        20 |          2 |   2451619 |    953090044",
                "Wed Mar 15 02:14:05 2000    |       200 |        20 |          2 |   2451619 |    953086445",
                "Mon Feb 10 17:32:01 1997    |       199 |        20 |          2 |   2450491 |    855595921",
                "Mon Feb 10 17:32:01 1997    |       199 |        20 |          2 |   2450491 |    855595921",
                "Mon Feb 10 17:32:00 1997    |       199 |        20 |          2 |   2450491 |    855595920",
                "Mon Feb 10 17:32:01 1997    |       199 |        20 |          2 |   2450491 |    855595921",
                "Mon Feb 10 17:32:01 1997    |       199 |        20 |          2 |   2450491 |    855595921",
                "Mon Feb 10 17:32:01 1997    |       199 |        20 |          2 |   2450491 |    855595921",
                "Mon Feb 10 17:32:01 1997    |       199 |        20 |          2 |   2450491 |    855595921",
                "Mon Feb 10 17:32:01 1997    |       199 |        20 |          2 |   2450491 |    855595921",
                "Mon Feb 10 17:32:01 1997    |       199 |        20 |          2 |   2450491 |    855595921",
                "Mon Feb 10 17:32:01 1997    |       199 |        20 |          2 |   2450491 |    855595921",
                "Mon Feb 10 17:32:01 1997    |       199 |        20 |          2 |   2450491 |    855595921",
                "Mon Feb 10 17:32:01 1997    |       199 |        20 |          2 |   2450491 |    855595921",
                "Tue Jun 10 18:32:01 1997    |       199 |        20 |          2 |   2450611 |    865967521",
                "Mon Feb 10 17:32:01 1997    |       199 |        20 |          2 |   2450491 |    855595921",
                "Tue Feb 11 17:32:01 1997    |       199 |        20 |          2 |   2450492 |    855682321",
                "Wed Feb 12 17:32:01 1997    |       199 |        20 |          2 |   2450493 |    855768721",
                "Thu Feb 13 17:32:01 1997    |       199 |        20 |          2 |   2450494 |    855855121",
                "Fri Feb 14 17:32:01 1997    |       199 |        20 |          2 |   2450495 |    855941521",
                "Sat Feb 15 17:32:01 1997    |       199 |        20 |          2 |   2450496 |    856027921",
                "Sun Feb 16 17:32:01 1997    |       199 |        20 |          2 |   2450497 |    856114321",
                //"Tue Feb 16 17:32:01 0097 BC |       -10 |        -1 |         -1 |   1686043 | -65192711279",
                "Sat Feb 16 17:32:01 0097    |         9 |         1 |          1 |   1756537 | -59102029679",
                "Thu Feb 16 17:32:01 0597    |        59 |         6 |          1 |   1939158 | -43323575279",
                "Tue Feb 16 17:32:01 1097    |       109 |        11 |          2 |   2121779 | -27545120879",
                "Sat Feb 16 17:32:01 1697    |       169 |        17 |          2 |   2340925 |  -8610906479",
                "Thu Feb 16 17:32:01 1797    |       179 |        18 |          2 |   2377449 |  -5455232879",
                "Tue Feb 16 17:32:01 1897    |       189 |        19 |          2 |   2413973 |  -2299559279",
                "Sun Feb 16 17:32:01 1997    |       199 |        20 |          2 |   2450497 |    856114321",
                "Sat Feb 16 17:32:01 2097    |       209 |        21 |          3 |   2487022 |   4011874321",
                "Wed Feb 28 17:32:01 1996    |       199 |        20 |          2 |   2450143 |    825528721",
                "Thu Feb 29 17:32:01 1996    |       199 |        20 |          2 |   2450144 |    825615121",
                "Fri Mar 01 17:32:01 1996    |       199 |        20 |          2 |   2450145 |    825701521",
                "Mon Dec 30 17:32:01 1996    |       199 |        20 |          2 |   2450449 |    851967121",
                "Tue Dec 31 17:32:01 1996    |       199 |        20 |          2 |   2450450 |    852053521",
                "Wed Jan 01 17:32:01 1997    |       199 |        20 |          2 |   2450451 |    852139921",
                "Fri Feb 28 17:32:01 1997    |       199 |        20 |          2 |   2450509 |    857151121",
                "Sat Mar 01 17:32:01 1997    |       199 |        20 |          2 |   2450510 |    857237521",
                "Tue Dec 30 17:32:01 1997    |       199 |        20 |          2 |   2450814 |    883503121",
                "Wed Dec 31 17:32:01 1997    |       199 |        20 |          2 |   2450815 |    883589521",
                "Fri Dec 31 17:32:01 1999    |       199 |        20 |          2 |   2451545 |    946661521",
                "Sat Jan 01 17:32:01 2000    |       200 |        20 |          2 |   2451546 |    946747921",
                "Sun Dec 31 17:32:01 2000    |       200 |        20 |          2 |   2451911 |    978283921",
                "Mon Jan 01 17:32:01 2001    |       200 |        21 |          3 |   2451912 |    978370321"
        };
        int columns = 6;
        DBSPExpression[] tuples = new DBSPExpression[data.length+1]; // last one with nulls.
        for (int j = 0; j < data.length; j++) {
            String d = data[j];
            String[] fields = d.split("[|]");
            Assert.assertEquals(columns, fields.length);
            DBSPExpression[] expressions = new DBSPExpression[columns - 1]; // Skip the Julian unsupported column
            expressions[0] = convertDate(fields[0].trim());
            for (int i = 1; i < columns - 2; i++)
                expressions[i] = new DBSPI64Literal(Long.parseLong(fields[i].trim()), true);
            // Postgres gives a float for epoch
            expressions[4] = new DBSPI64Literal((long)Double.parseDouble(fields[5].trim()), true);
            tuples[j] = new DBSPTupleExpression(expressions);
        }
        DBSPExpression none = DBSPLiteral.none(DBSPTypeInteger.SIGNED_64.setMayBeNull(true));
        tuples[data.length] = new DBSPTupleExpression(convertDate(null), none, none, none, none);
        this.testQuery(query, new DBSPZSetLiteral.Contents(tuples), true);
    }

    @Test
    public void testMicroseconds() {
        // SELECT d1 as "timestamp",
        //   extract(microseconds from d1) AS microseconds,
        //   extract(milliseconds from d1) AS milliseconds,
        //   extract(seconds from d1) AS seconds,
        //   round(extract(julian from d1)) AS julian,
        //   extract(epoch from d1) AS epoch
        //   FROM TIMESTAMP_TBL;
        // microsecond, second, epoch are all floats in Postgres
        String query = "SELECT d1 as \"timestamp\",\n" +
                "   extract(microsecond from d1) AS 'microseconds',\n" +
                "   extract(millisecond from d1) AS 'milliseconds',\n" +
                "   extract(second from d1) AS 'seconds',\n" +
                "   -- round(extract(julian from d1)) AS julian,\n" +
                "   extract(epoch from d1) AS 'epoch'\n" +
                "   FROM TIMESTAMP_TBL";
        String[] data = {
            "Thu Jan 01 00:00:00 1970    |            0 |        0.000 |  0.000000 |   2440588 |            0.000000",
            "Mon Feb 10 17:32:01 1997    |      1000000 |     1000.000 |  1.000000 |   2450491 |    855595921.000000",
            "Mon Feb 10 17:32:01 1997    |      1000000 |     1000.000 |  1.000000 |   2450491 |    855595921.000000",
            "Mon Feb 10 17:32:02 1997    |      2000000 |     2000.000 |  2.000000 |   2450491 |    855595922.000000",
            "Mon Feb 10 17:32:01.4 1997  |      1400000 |     1400.000 |  1.400000 |   2450491 |    855595921.400000",
            "Mon Feb 10 17:32:01.5 1997  |      1500000 |     1500.000 |  1.500000 |   2450491 |    855595921.500000",
            "Mon Feb 10 17:32:01.6 1997  |      1600000 |     1600.000 |  1.600000 |   2450491 |    855595921.600000",
            "Thu Jan 02 00:00:00 1997    |            0 |        0.000 |  0.000000 |   2450451 |    852163200.000000",
            "Thu Jan 02 03:04:05 1997    |      5000000 |     5000.000 |  5.000000 |   2450451 |    852174245.000000",
            "Mon Feb 10 17:32:01 1997    |      1000000 |     1000.000 |  1.000000 |   2450491 |    855595921.000000",
            "Mon Feb 10 17:32:01 1997    |      1000000 |     1000.000 |  1.000000 |   2450491 |    855595921.000000",
            "Mon Feb 10 17:32:01 1997    |      1000000 |     1000.000 |  1.000000 |   2450491 |    855595921.000000",
            "Mon Feb 10 17:32:01 1997    |      1000000 |     1000.000 |  1.000000 |   2450491 |    855595921.000000",
            "Tue Jun 10 17:32:01 1997    |      1000000 |     1000.000 |  1.000000 |   2450611 |    865963921.000000",
            "Sat Sep 22 18:19:20 2001    |     20000000 |    20000.000 | 20.000000 |   2452176 |   1001182760.000000",
            "Wed Mar 15 08:14:01 2000    |      1000000 |     1000.000 |  1.000000 |   2451619 |    953108041.000000",
            "Wed Mar 15 13:14:02 2000    |      2000000 |     2000.000 |  2.000000 |   2451620 |    953126042.000000",
            "Wed Mar 15 12:14:03 2000    |      3000000 |     3000.000 |  3.000000 |   2451620 |    953122443.000000",
            "Wed Mar 15 03:14:04 2000    |      4000000 |     4000.000 |  4.000000 |   2451619 |    953090044.000000",
            "Wed Mar 15 02:14:05 2000    |      5000000 |     5000.000 |  5.000000 |   2451619 |    953086445.000000",
            "Mon Feb 10 17:32:01 1997    |      1000000 |     1000.000 |  1.000000 |   2450491 |    855595921.000000",
            "Mon Feb 10 17:32:01 1997    |      1000000 |     1000.000 |  1.000000 |   2450491 |    855595921.000000",
            "Mon Feb 10 17:32:00 1997    |            0 |        0.000 |  0.000000 |   2450491 |    855595920.000000",
            "Mon Feb 10 17:32:01 1997    |      1000000 |     1000.000 |  1.000000 |   2450491 |    855595921.000000",
            "Mon Feb 10 17:32:01 1997    |      1000000 |     1000.000 |  1.000000 |   2450491 |    855595921.000000",
            "Mon Feb 10 17:32:01 1997    |      1000000 |     1000.000 |  1.000000 |   2450491 |    855595921.000000",
            "Mon Feb 10 17:32:01 1997    |      1000000 |     1000.000 |  1.000000 |   2450491 |    855595921.000000",
            "Mon Feb 10 17:32:01 1997    |      1000000 |     1000.000 |  1.000000 |   2450491 |    855595921.000000",
            "Mon Feb 10 17:32:01 1997    |      1000000 |     1000.000 |  1.000000 |   2450491 |    855595921.000000",
            "Mon Feb 10 17:32:01 1997    |      1000000 |     1000.000 |  1.000000 |   2450491 |    855595921.000000",
            "Mon Feb 10 17:32:01 1997    |      1000000 |     1000.000 |  1.000000 |   2450491 |    855595921.000000",
            "Mon Feb 10 17:32:01 1997    |      1000000 |     1000.000 |  1.000000 |   2450491 |    855595921.000000",
            "Tue Jun 10 18:32:01 1997    |      1000000 |     1000.000 |  1.000000 |   2450611 |    865967521.000000",
            "Mon Feb 10 17:32:01 1997    |      1000000 |     1000.000 |  1.000000 |   2450491 |    855595921.000000",
            "Tue Feb 11 17:32:01 1997    |      1000000 |     1000.000 |  1.000000 |   2450492 |    855682321.000000",
            "Wed Feb 12 17:32:01 1997    |      1000000 |     1000.000 |  1.000000 |   2450493 |    855768721.000000",
            "Thu Feb 13 17:32:01 1997    |      1000000 |     1000.000 |  1.000000 |   2450494 |    855855121.000000",
            "Fri Feb 14 17:32:01 1997    |      1000000 |     1000.000 |  1.000000 |   2450495 |    855941521.000000",
            "Sat Feb 15 17:32:01 1997    |      1000000 |     1000.000 |  1.000000 |   2450496 |    856027921.000000",
            "Sun Feb 16 17:32:01 1997    |      1000000 |     1000.000 |  1.000000 |   2450497 |    856114321.000000",
            //"Tue Feb 16 17:32:01 0097 BC |      1000000 |     1000.000 |  1.000000 |   1686043 | -65192711279.000000",
            "Sat Feb 16 17:32:01 0097    |      1000000 |     1000.000 |  1.000000 |   1756537 | -59102029679.000000",
            "Thu Feb 16 17:32:01 0597    |      1000000 |     1000.000 |  1.000000 |   1939158 | -43323575279.000000",
            "Tue Feb 16 17:32:01 1097    |      1000000 |     1000.000 |  1.000000 |   2121779 | -27545120879.000000",
            "Sat Feb 16 17:32:01 1697    |      1000000 |     1000.000 |  1.000000 |   2340925 |  -8610906479.000000",
            "Thu Feb 16 17:32:01 1797    |      1000000 |     1000.000 |  1.000000 |   2377449 |  -5455232879.000000",
            "Tue Feb 16 17:32:01 1897    |      1000000 |     1000.000 |  1.000000 |   2413973 |  -2299559279.000000",
            "Sun Feb 16 17:32:01 1997    |      1000000 |     1000.000 |  1.000000 |   2450497 |    856114321.000000",
            "Sat Feb 16 17:32:01 2097    |      1000000 |     1000.000 |  1.000000 |   2487022 |   4011874321.000000",
            "Wed Feb 28 17:32:01 1996    |      1000000 |     1000.000 |  1.000000 |   2450143 |    825528721.000000",
            "Thu Feb 29 17:32:01 1996    |      1000000 |     1000.000 |  1.000000 |   2450144 |    825615121.000000",
            "Fri Mar 01 17:32:01 1996    |      1000000 |     1000.000 |  1.000000 |   2450145 |    825701521.000000",
            "Mon Dec 30 17:32:01 1996    |      1000000 |     1000.000 |  1.000000 |   2450449 |    851967121.000000",
            "Tue Dec 31 17:32:01 1996    |      1000000 |     1000.000 |  1.000000 |   2450450 |    852053521.000000",
            "Wed Jan 01 17:32:01 1997    |      1000000 |     1000.000 |  1.000000 |   2450451 |    852139921.000000",
            "Fri Feb 28 17:32:01 1997    |      1000000 |     1000.000 |  1.000000 |   2450509 |    857151121.000000",
            "Sat Mar 01 17:32:01 1997    |      1000000 |     1000.000 |  1.000000 |   2450510 |    857237521.000000",
            "Tue Dec 30 17:32:01 1997    |      1000000 |     1000.000 |  1.000000 |   2450814 |    883503121.000000",
            "Wed Dec 31 17:32:01 1997    |      1000000 |     1000.000 |  1.000000 |   2450815 |    883589521.000000",
            "Fri Dec 31 17:32:01 1999    |      1000000 |     1000.000 |  1.000000 |   2451545 |    946661521.000000",
            "Sat Jan 01 17:32:01 2000    |      1000000 |     1000.000 |  1.000000 |   2451546 |    946747921.000000",
            "Sun Dec 31 17:32:01 2000    |      1000000 |     1000.000 |  1.000000 |   2451911 |    978283921.000000",
            "Mon Jan 01 17:32:01 2001    |      1000000 |     1000.000 |  1.000000 |   2451912 |    978370321.000000"
        };
        int columns = 6;
        DBSPExpression[] tuples = new DBSPExpression[data.length+1]; // last one with nulls.
        for (int j = 0; j < data.length; j++) {
            String d = data[j];
            String[] fields = d.split("[|]");
            Assert.assertEquals(columns, fields.length);
            DBSPExpression[] expressions = new DBSPExpression[columns - 1]; // Skip the Julian unsupported column
            expressions[0] = convertDate(fields[0].trim());
            for (int i = 1; i < columns - 2; i++)
                expressions[i] = new DBSPI64Literal((long)Double.parseDouble(fields[i].trim()), true);
            // Postgres gives a float for epoch
            expressions[4] = new DBSPI64Literal((long)Double.parseDouble(fields[5].trim()), true);
            tuples[j] = new DBSPTupleExpression(expressions);
        }
        DBSPExpression none = DBSPLiteral.none(DBSPTypeInteger.SIGNED_64.setMayBeNull(true));
        tuples[data.length] = new DBSPTupleExpression(convertDate(null), none, none, none, none);
        this.testQuery(query, new DBSPZSetLiteral.Contents(tuples), true);
    }

    // Postgres supports the ::timestamp syntax for casts
    // Calcite only supports 4 digit year for timestamps.
    // SELECT date_part('epoch', '294270-01-01 00:00:00'::timestamp);

    @Test
    public void testLargeYear() {
        // Timestamp out of range to be represented using milliseconds
        String query = "SELECT extract(epoch from TIMESTAMP '5000-01-01 00:00:00')\n";
        this.testQuery(query, new DBSPZSetLiteral.Contents(new DBSPTupleExpression(new DBSPI64Literal(95617584000L))), true);
    }

    //-- TO_CHAR()
    // Calcite does not support to_char
    //SELECT to_char(d1, 'DAY Day day DY Dy dy MONTH Month month RM MON Mon mon')
    //   FROM TIMESTAMP_TBL;

    // select make_timestamp(0, 7, 15, 12, 30, 15);
    // Calcite does not support make_timestamp

    // select * from generate_series('2020-01-01 00:00'::timestamp,
    //                              '2020-01-02 03:00'::timestamp,
    //                              '1 hour'::interval);
    // Calcite does not support generate_series
}
