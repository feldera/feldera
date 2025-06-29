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

package org.dbsp.sqlCompiler.compiler.sql.postgres;

import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.sql.tools.CompilerCircuitStream;
import org.dbsp.sqlCompiler.compiler.sql.tools.SqlIoTest;
import org.dbsp.sqlCompiler.compiler.sql.tools.TableParser;
import org.dbsp.sqlCompiler.compiler.sql.tools.Change;
import org.dbsp.sqlCompiler.compiler.sql.tools.InputOutputChange;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPI32Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPLiteral;
import org.dbsp.sqlCompiler.ir.expression.DBSPZSetExpression;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeInteger;
import org.dbsp.util.Linq;
import org.junit.Test;

/* Tests manually adapted from
 * https://github.com/postgres/postgres/blob/master/src/test/regress/expected/timestamp.out */
public class PostgresTimestampTests extends SqlIoTest {
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

    @Override
    public void prepareInputs(DBSPCompiler compiler) {
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
                "INSERT INTO TIMESTAMP_TBL VALUES ('1997-01-02 00:00:00');\n" +          // INSERT INTO TIMESTAMP_TBL VALUES ('1997-01-02');
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
                // "INSERT INTO TIMESTAMP_TBL VALUES ('1997-02-29 17:32:01');\n" +          // INSERT INTO TIMESTAMP_TBL VALUES ('Feb 29 17:32:01 1997');
                // ERROR:  date/time field value out of range: "Feb 29 17:32:01 1997"
                "INSERT INTO TIMESTAMP_TBL VALUES ('1997-03-01 17:32:01');\n" +          // INSERT INTO TIMESTAMP_TBL VALUES ('Mar 01 17:32:01 1997');
                "INSERT INTO TIMESTAMP_TBL VALUES ('1997-12-30 17:32:01');\n" +          // INSERT INTO TIMESTAMP_TBL VALUES ('Dec 30 17:32:01 1997');
                "INSERT INTO TIMESTAMP_TBL VALUES ('1997-12-31 17:32:01');\n" +          // INSERT INTO TIMESTAMP_TBL VALUES ('Dec 31 17:32:01 1997');
                "INSERT INTO TIMESTAMP_TBL VALUES ('1999-12-31 17:32:01');\n" +          // INSERT INTO TIMESTAMP_TBL VALUES ('Dec 31 17:32:01 1999');
                "INSERT INTO TIMESTAMP_TBL VALUES ('2000-01-01 17:32:01');\n" +          // INSERT INTO TIMESTAMP_TBL VALUES ('Jan 01 17:32:01 2000');
                "INSERT INTO TIMESTAMP_TBL VALUES ('2000-12-31 17:32:01');\n" +          // INSERT INTO TIMESTAMP_TBL VALUES ('Dec 31 17:32:01 2000');
                "INSERT INTO TIMESTAMP_TBL VALUES ('2001-01-01 17:32:01')\n";            // INSERT INTO TIMESTAMP_TBL VALUES ('Jan 01 17:32:01 2001');
        compiler.submitStatementsForCompilation(data);
    }

    void testQuery(String query, Change expectedOutput) {
        query = "CREATE VIEW V AS " + query;
        DBSPCompiler compiler = this.compileQuery(query, true);
        CompilerCircuitStream ccs = this.getCCS(compiler);
        DBSPZSetExpression input = compiler.getTableContents().getTableContents(
                compiler.canonicalName("TIMESTAMP_TBL", false));
        InputOutputChange change = new InputOutputChange(new Change(input), expectedOutput);
        ccs.addChange(change);
    }

    @Test
    public void testTS() {
        //" Tue Feb 16 17:32:01 0097 BC\n" +
        this.q("""
                SELECT d1 FROM TIMESTAMP_TBL;
                             d1
                -----------------------------
                 Thu Jan 01 00:00:00 1970
                 Mon Feb 10 17:32:01 1997
                 Mon Feb 10 17:32:01 1997
                 Mon Feb 10 17:32:02 1997
                 Mon Feb 10 17:32:01.4 1997
                 Mon Feb 10 17:32:01.5 1997
                 Mon Feb 10 17:32:01.6 1997
                 Thu Jan 02 00:00:00 1997
                 Thu Jan 02 03:04:05 1997
                 Mon Feb 10 17:32:01 1997
                 Mon Feb 10 17:32:01 1997
                 Mon Feb 10 17:32:01 1997
                 Mon Feb 10 17:32:01 1997
                 Tue Jun 10 17:32:01 1997
                 Sat Sep 22 18:19:20 2001
                 Wed Mar 15 08:14:01 2000
                 Wed Mar 15 13:14:02 2000
                 Wed Mar 15 12:14:03 2000
                 Wed Mar 15 03:14:04 2000
                 Wed Mar 15 02:14:05 2000
                 Mon Feb 10 17:32:01 1997
                 Mon Feb 10 17:32:01 1997
                 Mon Feb 10 17:32:00 1997
                 Mon Feb 10 17:32:01 1997
                 Mon Feb 10 17:32:01 1997
                 Mon Feb 10 17:32:01 1997
                 Mon Feb 10 17:32:01 1997
                 Mon Feb 10 17:32:01 1997
                 Mon Feb 10 17:32:01 1997
                 Mon Feb 10 17:32:01 1997
                 Mon Feb 10 17:32:01 1997
                 Mon Feb 10 17:32:01 1997
                 Tue Jun 10 18:32:01 1997
                 Mon Feb 10 17:32:01 1997
                 Tue Feb 11 17:32:01 1997
                 Wed Feb 12 17:32:01 1997
                 Thu Feb 13 17:32:01 1997
                 Fri Feb 14 17:32:01 1997
                 Sat Feb 15 17:32:01 1997
                 Sun Feb 16 17:32:01 1997
                 Sat Feb 16 17:32:01 0097
                 Thu Feb 16 17:32:01 0597
                 Tue Feb 16 17:32:01 1097
                 Sat Feb 16 17:32:01 1697
                 Thu Feb 16 17:32:01 1797
                 Tue Feb 16 17:32:01 1897
                 Sun Feb 16 17:32:01 1997
                 Sat Feb 16 17:32:01 2097
                 Wed Feb 28 17:32:01 1996
                 Thu Feb 29 17:32:01 1996
                 Fri Mar 01 17:32:01 1996
                 Mon Dec 30 17:32:01 1996
                 Tue Dec 31 17:32:01 1996
                 Wed Jan 01 17:32:01 1997
                 Fri Feb 28 17:32:01 1997
                 Sat Mar 01 17:32:01 1997
                 Tue Dec 30 17:32:01 1997
                 Wed Dec 31 17:32:01 1997
                 Fri Dec 31 17:32:01 1999
                 Sat Jan 01 17:32:01 2000
                 Sun Dec 31 17:32:01 2000
                 Mon Jan 01 17:32:01 2001""");
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
        this.q("""
                SELECT d1 FROM TIMESTAMP_TBL
                   WHERE d1 > timestamp '1997-01-02';
                             d1            \s
                ----------------------------
                 Mon Feb 10 17:32:01 1997
                 Mon Feb 10 17:32:01 1997
                 Mon Feb 10 17:32:02 1997
                 Mon Feb 10 17:32:01.4 1997
                 Mon Feb 10 17:32:01.5 1997
                 Mon Feb 10 17:32:01.6 1997
                 Thu Jan 02 03:04:05 1997
                 Mon Feb 10 17:32:01 1997
                 Mon Feb 10 17:32:01 1997
                 Mon Feb 10 17:32:01 1997
                 Mon Feb 10 17:32:01 1997
                 Tue Jun 10 17:32:01 1997
                 Sat Sep 22 18:19:20 2001
                 Wed Mar 15 08:14:01 2000
                 Wed Mar 15 13:14:02 2000
                 Wed Mar 15 12:14:03 2000
                 Wed Mar 15 03:14:04 2000
                 Wed Mar 15 02:14:05 2000
                 Mon Feb 10 17:32:01 1997
                 Mon Feb 10 17:32:01 1997
                 Mon Feb 10 17:32:00 1997
                 Mon Feb 10 17:32:01 1997
                 Mon Feb 10 17:32:01 1997
                 Mon Feb 10 17:32:01 1997
                 Mon Feb 10 17:32:01 1997
                 Mon Feb 10 17:32:01 1997
                 Mon Feb 10 17:32:01 1997
                 Mon Feb 10 17:32:01 1997
                 Mon Feb 10 17:32:01 1997
                 Mon Feb 10 17:32:01 1997
                 Tue Jun 10 18:32:01 1997
                 Mon Feb 10 17:32:01 1997
                 Tue Feb 11 17:32:01 1997
                 Wed Feb 12 17:32:01 1997
                 Thu Feb 13 17:32:01 1997
                 Fri Feb 14 17:32:01 1997
                 Sat Feb 15 17:32:01 1997
                 Sun Feb 16 17:32:01 1997
                 Sun Feb 16 17:32:01 1997
                 Sat Feb 16 17:32:01 2097
                 Fri Feb 28 17:32:01 1997
                 Sat Mar 01 17:32:01 1997
                 Tue Dec 30 17:32:01 1997
                 Wed Dec 31 17:32:01 1997
                 Fri Dec 31 17:32:01 1999
                 Sat Jan 01 17:32:01 2000
                 Sun Dec 31 17:32:01 2000
                 Mon Jan 01 17:32:01 2001""");
    }

    @Test
    public void testLt() {
        // SELECT d1 FROM TIMESTAMP_TBL
        this.q("""
                SELECT d1 FROM TIMESTAMP_TBL
                   WHERE d1 < timestamp '1997-01-02';
                             d1             \s
                -----------------------------
                 Thu Jan 01 00:00:00 1970
                 Sat Feb 16 17:32:01 0097
                 Thu Feb 16 17:32:01 0597
                 Tue Feb 16 17:32:01 1097
                 Sat Feb 16 17:32:01 1697
                 Thu Feb 16 17:32:01 1797
                 Tue Feb 16 17:32:01 1897
                 Wed Feb 28 17:32:01 1996
                 Thu Feb 29 17:32:01 1996
                 Fri Mar 01 17:32:01 1996
                 Mon Dec 30 17:32:01 1996
                 Tue Dec 31 17:32:01 1996
                 Wed Jan 01 17:32:01 1997""");
    }

    @Test
    public void testEq() {
        // SELECT d1 FROM TIMESTAMP_TBL
        //   WHERE d1 = timestamp without time zone '1997-01-02';
        this.q("""
                SELECT d1 FROM TIMESTAMP_TBL
                   WHERE d1 = timestamp '1997-01-02';
                             d1
                -----------------------------
                Thu Jan 02 00:00:00 1997""");
    }

    @Test
    public void testNe() {
        // SELECT d1 FROM TIMESTAMP_TBL
        //   WHERE d1 != timestamp without time zone '1997-01-02';
        this.q("""
                SELECT d1 FROM TIMESTAMP_TBL
                   WHERE d1 != timestamp '1997-01-02';
                             d1             \s
                -----------------------------
                Thu Jan 01 00:00:00 1970
                Mon Feb 10 17:32:01 1997
                Mon Feb 10 17:32:01 1997
                Mon Feb 10 17:32:02 1997
                Mon Feb 10 17:32:01.4 1997
                Mon Feb 10 17:32:01.5 1997
                Mon Feb 10 17:32:01.6 1997
                Thu Jan 02 03:04:05 1997
                Mon Feb 10 17:32:01 1997
                Mon Feb 10 17:32:01 1997
                Mon Feb 10 17:32:01 1997
                Mon Feb 10 17:32:01 1997
                Tue Jun 10 17:32:01 1997
                Sat Sep 22 18:19:20 2001
                Wed Mar 15 08:14:01 2000
                Wed Mar 15 13:14:02 2000
                Wed Mar 15 12:14:03 2000
                Wed Mar 15 03:14:04 2000
                Wed Mar 15 02:14:05 2000
                Mon Feb 10 17:32:01 1997
                Mon Feb 10 17:32:01 1997
                Mon Feb 10 17:32:00 1997
                Mon Feb 10 17:32:01 1997
                Mon Feb 10 17:32:01 1997
                Mon Feb 10 17:32:01 1997
                Mon Feb 10 17:32:01 1997
                Mon Feb 10 17:32:01 1997
                Mon Feb 10 17:32:01 1997
                Mon Feb 10 17:32:01 1997
                Mon Feb 10 17:32:01 1997
                Mon Feb 10 17:32:01 1997
                Tue Jun 10 18:32:01 1997
                Mon Feb 10 17:32:01 1997
                Tue Feb 11 17:32:01 1997
                Wed Feb 12 17:32:01 1997
                Thu Feb 13 17:32:01 1997
                Fri Feb 14 17:32:01 1997
                Sat Feb 15 17:32:01 1997
                Sun Feb 16 17:32:01 1997
                Sat Feb 16 17:32:01 0097
                Thu Feb 16 17:32:01 0597
                Tue Feb 16 17:32:01 1097
                Sat Feb 16 17:32:01 1697
                Thu Feb 16 17:32:01 1797
                Tue Feb 16 17:32:01 1897
                Sun Feb 16 17:32:01 1997
                Sat Feb 16 17:32:01 2097
                Wed Feb 28 17:32:01 1996
                Thu Feb 29 17:32:01 1996
                Fri Mar 01 17:32:01 1996
                Mon Dec 30 17:32:01 1996
                Tue Dec 31 17:32:01 1996
                Wed Jan 01 17:32:01 1997
                Fri Feb 28 17:32:01 1997
                Sat Mar 01 17:32:01 1997
                Tue Dec 30 17:32:01 1997
                Wed Dec 31 17:32:01 1997
                Fri Dec 31 17:32:01 1999
                Sat Jan 01 17:32:01 2000
                Sun Dec 31 17:32:01 2000
                Mon Jan 01 17:32:01 2001""");
    }

    @Test
    public void testLeq() {
        // SELECT d1 FROM TIMESTAMP_TBL
        //   WHERE d1 <= timestamp without time zone '1997-01-02';
       this.q("""
               SELECT d1 FROM TIMESTAMP_TBL
                  WHERE d1 <= timestamp '1997-01-02';
                            d1             \s
               -----------------------------
                Thu Jan 01 00:00:00 1970
                Thu Jan 02 00:00:00 1997
                Sat Feb 16 17:32:01 0097
                Thu Feb 16 17:32:01 0597
                Tue Feb 16 17:32:01 1097
                Sat Feb 16 17:32:01 1697
                Thu Feb 16 17:32:01 1797
                Tue Feb 16 17:32:01 1897
                Wed Feb 28 17:32:01 1996
                Thu Feb 29 17:32:01 1996
                Fri Mar 01 17:32:01 1996
                Mon Dec 30 17:32:01 1996
                Tue Dec 31 17:32:01 1996
                Wed Jan 01 17:32:01 1997""");
    }

    @Test
    public void testGte() {
        // SELECT d1 FROM TIMESTAMP_TBL
        //   WHERE d1 >= timestamp without time zone '1997-01-02';
       this.q("""
               SELECT d1 FROM TIMESTAMP_TBL
                  WHERE d1 >= timestamp '1997-01-02';
                            d1            \s
               ----------------------------
                Mon Feb 10 17:32:01 1997
                Mon Feb 10 17:32:01 1997
                Mon Feb 10 17:32:02 1997
                Mon Feb 10 17:32:01.4 1997
                Mon Feb 10 17:32:01.5 1997
                Mon Feb 10 17:32:01.6 1997
                Thu Jan 02 00:00:00 1997
                Thu Jan 02 03:04:05 1997
                Mon Feb 10 17:32:01 1997
                Mon Feb 10 17:32:01 1997
                Mon Feb 10 17:32:01 1997
                Mon Feb 10 17:32:01 1997
                Tue Jun 10 17:32:01 1997
                Sat Sep 22 18:19:20 2001
                Wed Mar 15 08:14:01 2000
                Wed Mar 15 13:14:02 2000
                Wed Mar 15 12:14:03 2000
                Wed Mar 15 03:14:04 2000
                Wed Mar 15 02:14:05 2000
                Mon Feb 10 17:32:01 1997
                Mon Feb 10 17:32:01 1997
                Mon Feb 10 17:32:00 1997
                Mon Feb 10 17:32:01 1997
                Mon Feb 10 17:32:01 1997
                Mon Feb 10 17:32:01 1997
                Mon Feb 10 17:32:01 1997
                Mon Feb 10 17:32:01 1997
                Mon Feb 10 17:32:01 1997
                Mon Feb 10 17:32:01 1997
                Mon Feb 10 17:32:01 1997
                Mon Feb 10 17:32:01 1997
                Tue Jun 10 18:32:01 1997
                Mon Feb 10 17:32:01 1997
                Tue Feb 11 17:32:01 1997
                Wed Feb 12 17:32:01 1997
                Thu Feb 13 17:32:01 1997
                Fri Feb 14 17:32:01 1997
                Sat Feb 15 17:32:01 1997
                Sun Feb 16 17:32:01 1997
                Sun Feb 16 17:32:01 1997
                Sat Feb 16 17:32:01 2097
                Fri Feb 28 17:32:01 1997
                Sat Mar 01 17:32:01 1997
                Tue Dec 30 17:32:01 1997
                Wed Dec 31 17:32:01 1997
                Fri Dec 31 17:32:01 1999
                Sat Jan 01 17:32:01 2000
                Sun Dec 31 17:32:01 2000
                Mon Jan 01 17:32:01 2001""");
    }

    @Test
    public void testTimestampDiff() {
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
                new DBSPTupleExpression(d == null ?
                        DBSPLiteral.none(new DBSPTypeInteger(CalciteObject.EMPTY, 32, true,true)) :
                        new DBSPI32Literal(-(int)(TableParser.shortIntervalToMilliseconds(d) / 60000), true)), DBSPExpression.class);
        String query = "SELECT TIMESTAMPDIFF(MINUTE, d1, timestamp '1997-01-02') AS diff\n" +
                "   FROM TIMESTAMP_TBL WHERE d1 BETWEEN '1902-01-01' AND '2038-01-01'";
        this.testQuery(query, new Change(new DBSPZSetExpression(results)));
    }

    @Test
    public void testWeek() {
        // SELECT date_trunc( 'week', timestamp '2004-02-29 15:44:17.71393' ) AS week_trunc;
        // Postgres gives a different result from Calcite!
        // This day was a Sunday.  Postgres returns 2004-02-23, the previous Monday.
        this.q("""
                SELECT FLOOR(timestamp '2004-02-29 15:44:17.71393' TO WEEK) AS week_trunc;
                        week_trunc
                --------------------------
                 Sun Feb 29 00:00:00 2004""");
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
        String query = """
                SELECT TIMESTAMPDIFF(SECOND, d1, timestamp '1997-01-02') AS diff
                  FROM TIMESTAMP_TBL
                  WHERE d1 BETWEEN timestamp '1902-01-01'
                   AND timestamp '2038-01-01'""";
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
                new DBSPTupleExpression(d == null ? DBSPLiteral.none(new DBSPTypeInteger(CalciteObject.EMPTY, 32, true,true)) :
                        new DBSPI32Literal(-(int)(TableParser.shortIntervalToMilliseconds(d)/1000), true)), DBSPExpression.class);
        this.testQuery(query, new Change(new DBSPZSetExpression(results)));
    }

    @Test
    public void testDatePart() {
        // SELECT d1 as "timestamp",
        //   date_part( 'year', d1) AS year, date_part( 'month', d1) AS month,
        //   date_part( 'day', d1) AS day, date_part( 'hour', d1) AS hour,
        //   date_part( 'minute', d1) AS minute, date_part( 'second', d1) AS second
        //   FROM TIMESTAMP_TBL;
        // Postgres EXTRACT returns floats for seconds...
        this.q("""
                SELECT d1 as "timestamp",
                EXTRACT(YEAR FROM d1) AS 'year', EXTRACT(month FROM d1) AS 'month',
                EXTRACT(day FROM d1) AS 'day', EXTRACT(hour FROM d1) AS 'hour',
                EXTRACT(minute FROM d1) AS 'minute', EXTRACT(second FROM d1) AS 'second'
                FROM TIMESTAMP_TBL;
                          timestamp          |   year    | month | day | hour | minute | second\s
                -----------------------------+-----------+-------+-----+------+--------+--------
                 Thu Jan 01 00:00:00 1970    |      1970 |     1 |   1 |    0 |      0 |      0
                 Mon Feb 10 17:32:01 1997    |      1997 |     2 |  10 |   17 |     32 |      1
                 Mon Feb 10 17:32:01 1997    |      1997 |     2 |  10 |   17 |     32 |      1
                 Mon Feb 10 17:32:02 1997    |      1997 |     2 |  10 |   17 |     32 |      2
                 Mon Feb 10 17:32:01.4 1997  |      1997 |     2 |  10 |   17 |     32 |    1
                 Mon Feb 10 17:32:01.5 1997  |      1997 |     2 |  10 |   17 |     32 |    1
                 Mon Feb 10 17:32:01.6 1997  |      1997 |     2 |  10 |   17 |     32 |    1
                 Thu Jan 02 00:00:00 1997    |      1997 |     1 |   2 |    0 |      0 |      0
                 Thu Jan 02 03:04:05 1997    |      1997 |     1 |   2 |    3 |      4 |      5
                 Mon Feb 10 17:32:01 1997    |      1997 |     2 |  10 |   17 |     32 |      1
                 Mon Feb 10 17:32:01 1997    |      1997 |     2 |  10 |   17 |     32 |      1
                 Mon Feb 10 17:32:01 1997    |      1997 |     2 |  10 |   17 |     32 |      1
                 Mon Feb 10 17:32:01 1997    |      1997 |     2 |  10 |   17 |     32 |      1
                 Tue Jun 10 17:32:01 1997    |      1997 |     6 |  10 |   17 |     32 |      1
                 Sat Sep 22 18:19:20 2001    |      2001 |     9 |  22 |   18 |     19 |     20
                 Wed Mar 15 08:14:01 2000    |      2000 |     3 |  15 |    8 |     14 |      1
                 Wed Mar 15 13:14:02 2000    |      2000 |     3 |  15 |   13 |     14 |      2
                 Wed Mar 15 12:14:03 2000    |      2000 |     3 |  15 |   12 |     14 |      3
                 Wed Mar 15 03:14:04 2000    |      2000 |     3 |  15 |    3 |     14 |      4
                 Wed Mar 15 02:14:05 2000    |      2000 |     3 |  15 |    2 |     14 |      5
                 Mon Feb 10 17:32:01 1997    |      1997 |     2 |  10 |   17 |     32 |      1
                 Mon Feb 10 17:32:01 1997    |      1997 |     2 |  10 |   17 |     32 |      1
                 Mon Feb 10 17:32:00 1997    |      1997 |     2 |  10 |   17 |     32 |      0
                 Mon Feb 10 17:32:01 1997    |      1997 |     2 |  10 |   17 |     32 |      1
                 Mon Feb 10 17:32:01 1997    |      1997 |     2 |  10 |   17 |     32 |      1
                 Mon Feb 10 17:32:01 1997    |      1997 |     2 |  10 |   17 |     32 |      1
                 Mon Feb 10 17:32:01 1997    |      1997 |     2 |  10 |   17 |     32 |      1
                 Mon Feb 10 17:32:01 1997    |      1997 |     2 |  10 |   17 |     32 |      1
                 Mon Feb 10 17:32:01 1997    |      1997 |     2 |  10 |   17 |     32 |      1
                 Mon Feb 10 17:32:01 1997    |      1997 |     2 |  10 |   17 |     32 |      1
                 Mon Feb 10 17:32:01 1997    |      1997 |     2 |  10 |   17 |     32 |      1
                 Mon Feb 10 17:32:01 1997    |      1997 |     2 |  10 |   17 |     32 |      1
                 Tue Jun 10 18:32:01 1997    |      1997 |     6 |  10 |   18 |     32 |      1
                 Mon Feb 10 17:32:01 1997    |      1997 |     2 |  10 |   17 |     32 |      1
                 Tue Feb 11 17:32:01 1997    |      1997 |     2 |  11 |   17 |     32 |      1
                 Wed Feb 12 17:32:01 1997    |      1997 |     2 |  12 |   17 |     32 |      1
                 Thu Feb 13 17:32:01 1997    |      1997 |     2 |  13 |   17 |     32 |      1
                 Fri Feb 14 17:32:01 1997    |      1997 |     2 |  14 |   17 |     32 |      1
                 Sat Feb 15 17:32:01 1997    |      1997 |     2 |  15 |   17 |     32 |      1
                 Sun Feb 16 17:32:01 1997    |      1997 |     2 |  16 |   17 |     32 |      1
                 Sat Feb 16 17:32:01 0097    |        97 |     2 |  16 |   17 |     32 |      1
                 Thu Feb 16 17:32:01 0597    |       597 |     2 |  16 |   17 |     32 |      1
                 Tue Feb 16 17:32:01 1097    |      1097 |     2 |  16 |   17 |     32 |      1
                 Sat Feb 16 17:32:01 1697    |      1697 |     2 |  16 |   17 |     32 |      1
                 Thu Feb 16 17:32:01 1797    |      1797 |     2 |  16 |   17 |     32 |      1
                 Tue Feb 16 17:32:01 1897    |      1897 |     2 |  16 |   17 |     32 |      1
                 Sun Feb 16 17:32:01 1997    |      1997 |     2 |  16 |   17 |     32 |      1
                 Sat Feb 16 17:32:01 2097    |      2097 |     2 |  16 |   17 |     32 |      1
                 Wed Feb 28 17:32:01 1996    |      1996 |     2 |  28 |   17 |     32 |      1
                 Thu Feb 29 17:32:01 1996    |      1996 |     2 |  29 |   17 |     32 |      1
                 Fri Mar 01 17:32:01 1996    |      1996 |     3 |   1 |   17 |     32 |      1
                 Mon Dec 30 17:32:01 1996    |      1996 |    12 |  30 |   17 |     32 |      1
                 Tue Dec 31 17:32:01 1996    |      1996 |    12 |  31 |   17 |     32 |      1
                 Wed Jan 01 17:32:01 1997    |      1997 |     1 |   1 |   17 |     32 |      1
                 Fri Feb 28 17:32:01 1997    |      1997 |     2 |  28 |   17 |     32 |      1
                 Sat Mar 01 17:32:01 1997    |      1997 |     3 |   1 |   17 |     32 |      1
                 Tue Dec 30 17:32:01 1997    |      1997 |    12 |  30 |   17 |     32 |      1
                 Wed Dec 31 17:32:01 1997    |      1997 |    12 |  31 |   17 |     32 |      1
                 Fri Dec 31 17:32:01 1999    |      1999 |    12 |  31 |   17 |     32 |      1
                 Sat Jan 01 17:32:01 2000    |      2000 |     1 |   1 |   17 |     32 |      1
                 Sun Dec 31 17:32:01 2000    |      2000 |    12 |  31 |   17 |     32 |      1
                 Mon Jan 01 17:32:01 2001    |      2001 |     1 |   1 |   17 |     32 |      1""");
    }

    @Test
    public void testQuarter() {
        // SELECT d1 as "timestamp",
        //   date_part( 'quarter', d1) AS quarter, date_part( 'msec', d1) AS msec,
        //   date_part( 'usec', d1) AS usec
        //   FROM TIMESTAMP_TBL;
        this.q("""
                SELECT d1 as "timestamp",
                   EXTRACT(quarter FROM d1) AS 'quarter', EXTRACT(MILLISECOND FROM d1) AS 'msec',
                   EXTRACT(MICROSECOND FROM d1) AS 'usec'
                   FROM TIMESTAMP_TBL;
                          timestamp          | quarter | msec  |   usec
                -----------------------------+---------+-------+----------
                 Thu Jan 01 00:00:00 1970    |       1 |     0 |        0
                 Mon Feb 10 17:32:01 1997    |       1 |  1000 |  1000000
                 Mon Feb 10 17:32:01 1997    |       1 |  1000 |  1000000
                 Mon Feb 10 17:32:02 1997    |       1 |  2000 |  2000000
                 Mon Feb 10 17:32:01.4 1997  |       1 |  1400 |  1400000
                 Mon Feb 10 17:32:01.5 1997  |       1 |  1500 |  1500000
                 Mon Feb 10 17:32:01.6 1997  |       1 |  1600 |  1600000
                 Thu Jan 02 00:00:00 1997    |       1 |     0 |        0
                 Thu Jan 02 03:04:05 1997    |       1 |  5000 |  5000000
                 Mon Feb 10 17:32:01 1997    |       1 |  1000 |  1000000
                 Mon Feb 10 17:32:01 1997    |       1 |  1000 |  1000000
                 Mon Feb 10 17:32:01 1997    |       1 |  1000 |  1000000
                 Mon Feb 10 17:32:01 1997    |       1 |  1000 |  1000000
                 Tue Jun 10 17:32:01 1997    |       2 |  1000 |  1000000
                 Sat Sep 22 18:19:20 2001    |       3 | 20000 | 20000000
                 Wed Mar 15 08:14:01 2000    |       1 |  1000 |  1000000
                 Wed Mar 15 13:14:02 2000    |       1 |  2000 |  2000000
                 Wed Mar 15 12:14:03 2000    |       1 |  3000 |  3000000
                 Wed Mar 15 03:14:04 2000    |       1 |  4000 |  4000000
                 Wed Mar 15 02:14:05 2000    |       1 |  5000 |  5000000
                 Mon Feb 10 17:32:01 1997    |       1 |  1000 |  1000000
                 Mon Feb 10 17:32:01 1997    |       1 |  1000 |  1000000
                 Mon Feb 10 17:32:00 1997    |       1 |     0 |        0
                 Mon Feb 10 17:32:01 1997    |       1 |  1000 |  1000000
                 Mon Feb 10 17:32:01 1997    |       1 |  1000 |  1000000
                 Mon Feb 10 17:32:01 1997    |       1 |  1000 |  1000000
                 Mon Feb 10 17:32:01 1997    |       1 |  1000 |  1000000
                 Mon Feb 10 17:32:01 1997    |       1 |  1000 |  1000000
                 Mon Feb 10 17:32:01 1997    |       1 |  1000 |  1000000
                 Mon Feb 10 17:32:01 1997    |       1 |  1000 |  1000000
                 Mon Feb 10 17:32:01 1997    |       1 |  1000 |  1000000
                 Mon Feb 10 17:32:01 1997    |       1 |  1000 |  1000000
                 Tue Jun 10 18:32:01 1997    |       2 |  1000 |  1000000
                 Mon Feb 10 17:32:01 1997    |       1 |  1000 |  1000000
                 Tue Feb 11 17:32:01 1997    |       1 |  1000 |  1000000
                 Wed Feb 12 17:32:01 1997    |       1 |  1000 |  1000000
                 Thu Feb 13 17:32:01 1997    |       1 |  1000 |  1000000
                 Fri Feb 14 17:32:01 1997    |       1 |  1000 |  1000000
                 Sat Feb 15 17:32:01 1997    |       1 |  1000 |  1000000
                 Sun Feb 16 17:32:01 1997    |       1 |  1000 |  1000000
                 Sat Feb 16 17:32:01 0097    |       1 |  1000 |  1000000
                 Thu Feb 16 17:32:01 0597    |       1 |  1000 |  1000000
                 Tue Feb 16 17:32:01 1097    |       1 |  1000 |  1000000
                 Sat Feb 16 17:32:01 1697    |       1 |  1000 |  1000000
                 Thu Feb 16 17:32:01 1797    |       1 |  1000 |  1000000
                 Tue Feb 16 17:32:01 1897    |       1 |  1000 |  1000000
                 Sun Feb 16 17:32:01 1997    |       1 |  1000 |  1000000
                 Sat Feb 16 17:32:01 2097    |       1 |  1000 |  1000000
                 Wed Feb 28 17:32:01 1996    |       1 |  1000 |  1000000
                 Thu Feb 29 17:32:01 1996    |       1 |  1000 |  1000000
                 Fri Mar 01 17:32:01 1996    |       1 |  1000 |  1000000
                 Mon Dec 30 17:32:01 1996    |       4 |  1000 |  1000000
                 Tue Dec 31 17:32:01 1996    |       4 |  1000 |  1000000
                 Wed Jan 01 17:32:01 1997    |       1 |  1000 |  1000000
                 Fri Feb 28 17:32:01 1997    |       1 |  1000 |  1000000
                 Sat Mar 01 17:32:01 1997    |       1 |  1000 |  1000000
                 Tue Dec 30 17:32:01 1997    |       4 |  1000 |  1000000
                 Wed Dec 31 17:32:01 1997    |       4 |  1000 |  1000000
                 Fri Dec 31 17:32:01 1999    |       4 |  1000 |  1000000
                 Sat Jan 01 17:32:01 2000    |       1 |  1000 |  1000000
                 Sun Dec 31 17:32:01 2000    |       4 |  1000 |  1000000
                 Mon Jan 01 17:32:01 2001    |       1 |  1000 |  1000000""");
    }

    @Test
    public void testDay() {
        // SELECT d1 as "timestamp",
        //   date_part( 'isoyear', d1) AS isoyear, date_part( 'week', d1) AS week,
        //   date_part( 'isodow', d1) AS isodow, date_part( 'dow', d1) AS dow,
        //   date_part( 'doy', d1) AS doy
        //   FROM TIMESTAMP_TBL;
        // had to adjust column 4.
        this.q("""
                SELECT d1 as "timestamp",
                   extract(isoyear FROM d1) AS 'isoyear', extract(week FROM d1) AS 'week',
                   extract(isodow FROM d1) AS 'isodow', extract(dow FROM d1) AS 'dow',
                   extract(doy FROM d1) AS 'doy'
                   FROM TIMESTAMP_TBL;
                          timestamp          |  isoyear  | week | isodow | dow | doy\s
                -----------------------------+-----------+------+--------+-----+-----
                 Thu Jan 01 00:00:00 1970    |      1970 |    1 |      4 |   5 |   1
                 Mon Feb 10 17:32:01 1997    |      1997 |    7 |      1 |   2 |  41
                 Mon Feb 10 17:32:01 1997    |      1997 |    7 |      1 |   2 |  41
                 Mon Feb 10 17:32:02 1997    |      1997 |    7 |      1 |   2 |  41
                 Mon Feb 10 17:32:01.4 1997  |      1997 |    7 |      1 |   2 |  41
                 Mon Feb 10 17:32:01.5 1997  |      1997 |    7 |      1 |   2 |  41
                 Mon Feb 10 17:32:01.6 1997  |      1997 |    7 |      1 |   2 |  41
                 Thu Jan 02 00:00:00 1997    |      1997 |    1 |      4 |   5 |   2
                 Thu Jan 02 03:04:05 1997    |      1997 |    1 |      4 |   5 |   2
                 Mon Feb 10 17:32:01 1997    |      1997 |    7 |      1 |   2 |  41
                 Mon Feb 10 17:32:01 1997    |      1997 |    7 |      1 |   2 |  41
                 Mon Feb 10 17:32:01 1997    |      1997 |    7 |      1 |   2 |  41
                 Mon Feb 10 17:32:01 1997    |      1997 |    7 |      1 |   2 |  41
                 Tue Jun 10 17:32:01 1997    |      1997 |   24 |      2 |   3 | 161
                 Sat Sep 22 18:19:20 2001    |      2001 |   38 |      6 |   7 | 265
                 Wed Mar 15 08:14:01 2000    |      2000 |   11 |      3 |   4 |  75
                 Wed Mar 15 13:14:02 2000    |      2000 |   11 |      3 |   4 |  75
                 Wed Mar 15 12:14:03 2000    |      2000 |   11 |      3 |   4 |  75
                 Wed Mar 15 03:14:04 2000    |      2000 |   11 |      3 |   4 |  75
                 Wed Mar 15 02:14:05 2000    |      2000 |   11 |      3 |   4 |  75
                 Mon Feb 10 17:32:01 1997    |      1997 |    7 |      1 |   2 |  41
                 Mon Feb 10 17:32:01 1997    |      1997 |    7 |      1 |   2 |  41
                 Mon Feb 10 17:32:00 1997    |      1997 |    7 |      1 |   2 |  41
                 Mon Feb 10 17:32:01 1997    |      1997 |    7 |      1 |   2 |  41
                 Mon Feb 10 17:32:01 1997    |      1997 |    7 |      1 |   2 |  41
                 Mon Feb 10 17:32:01 1997    |      1997 |    7 |      1 |   2 |  41
                 Mon Feb 10 17:32:01 1997    |      1997 |    7 |      1 |   2 |  41
                 Mon Feb 10 17:32:01 1997    |      1997 |    7 |      1 |   2 |  41
                 Mon Feb 10 17:32:01 1997    |      1997 |    7 |      1 |   2 |  41
                 Mon Feb 10 17:32:01 1997    |      1997 |    7 |      1 |   2 |  41
                 Mon Feb 10 17:32:01 1997    |      1997 |    7 |      1 |   2 |  41
                 Mon Feb 10 17:32:01 1997    |      1997 |    7 |      1 |   2 |  41
                 Tue Jun 10 18:32:01 1997    |      1997 |   24 |      2 |   3 | 161
                 Mon Feb 10 17:32:01 1997    |      1997 |    7 |      1 |   2 |  41
                 Tue Feb 11 17:32:01 1997    |      1997 |    7 |      2 |   3 |  42
                 Wed Feb 12 17:32:01 1997    |      1997 |    7 |      3 |   4 |  43
                 Thu Feb 13 17:32:01 1997    |      1997 |    7 |      4 |   5 |  44
                 Fri Feb 14 17:32:01 1997    |      1997 |    7 |      5 |   6 |  45
                 Sat Feb 15 17:32:01 1997    |      1997 |    7 |      6 |   7 |  46
                 Sun Feb 16 17:32:01 1997    |      1997 |    7 |      7 |   1 |  47
                 Sat Feb 16 17:32:01 0097    |        97 |    7 |      6 |   7 |  47
                 Thu Feb 16 17:32:01 0597    |       597 |    7 |      4 |   5 |  47
                 Tue Feb 16 17:32:01 1097    |      1097 |    7 |      2 |   3 |  47
                 Sat Feb 16 17:32:01 1697    |      1697 |    7 |      6 |   7 |  47
                 Thu Feb 16 17:32:01 1797    |      1797 |    7 |      4 |   5 |  47
                 Tue Feb 16 17:32:01 1897    |      1897 |    7 |      2 |   3 |  47
                 Sun Feb 16 17:32:01 1997    |      1997 |    7 |      7 |   1 |  47
                 Sat Feb 16 17:32:01 2097    |      2097 |    7 |      6 |   7 |  47
                 Wed Feb 28 17:32:01 1996    |      1996 |    9 |      3 |   4 |  59
                 Thu Feb 29 17:32:01 1996    |      1996 |    9 |      4 |   5 |  60
                 Fri Mar 01 17:32:01 1996    |      1996 |    9 |      5 |   6 |  61
                 Mon Dec 30 17:32:01 1996    |      1997 |    1 |      1 |   2 | 365
                 Tue Dec 31 17:32:01 1996    |      1997 |    1 |      2 |   3 | 366
                 Wed Jan 01 17:32:01 1997    |      1997 |    1 |      3 |   4 |   1
                 Fri Feb 28 17:32:01 1997    |      1997 |    9 |      5 |   6 |  59
                 Sat Mar 01 17:32:01 1997    |      1997 |    9 |      6 |   7 |  60
                 Tue Dec 30 17:32:01 1997    |      1998 |    1 |      2 |   3 | 364
                 Wed Dec 31 17:32:01 1997    |      1998 |    1 |      3 |   4 | 365
                 Fri Dec 31 17:32:01 1999    |      1999 |   52 |      5 |   6 | 365
                 Sat Jan 01 17:32:01 2000    |      1999 |   52 |      6 |   7 |   1
                 Sun Dec 31 17:32:01 2000    |      2000 |   52 |      7 |   1 | 366
                 Mon Jan 01 17:32:01 2001    |      2001 |    1 |      1 |   2 |   1""");
    }

    @Test
    public void testDayDatePart() {
        // date_part has arguments swapped in Calcite
        // had to adjust column 4.
        // had to change quoting
        this.q("""
                SELECT d1 as "timestamp",
                        date_part(isoyear, d1) AS 'isoyear', date_part(week, d1) AS 'week',
                        date_part(isodow, d1) AS 'isodow',
                        date_part(dow, d1) AS 'dow',
                        date_part(doy, d1) AS 'doy'
                        FROM TIMESTAMP_TBL;
                          timestamp          |  isoyear  | week | isodow | dow | doy\s
                -----------------------------+-----------+------+--------+-----+-----
                 Thu Jan 01 00:00:00 1970    |      1970 |    1 |      4 |   5 |   1
                 Mon Feb 10 17:32:01 1997    |      1997 |    7 |      1 |   2 |  41
                 Mon Feb 10 17:32:01 1997    |      1997 |    7 |      1 |   2 |  41
                 Mon Feb 10 17:32:02 1997    |      1997 |    7 |      1 |   2 |  41
                 Mon Feb 10 17:32:01.4 1997  |      1997 |    7 |      1 |   2 |  41
                 Mon Feb 10 17:32:01.5 1997  |      1997 |    7 |      1 |   2 |  41
                 Mon Feb 10 17:32:01.6 1997  |      1997 |    7 |      1 |   2 |  41
                 Thu Jan 02 00:00:00 1997    |      1997 |    1 |      4 |   5 |   2
                 Thu Jan 02 03:04:05 1997    |      1997 |    1 |      4 |   5 |   2
                 Mon Feb 10 17:32:01 1997    |      1997 |    7 |      1 |   2 |  41
                 Mon Feb 10 17:32:01 1997    |      1997 |    7 |      1 |   2 |  41
                 Mon Feb 10 17:32:01 1997    |      1997 |    7 |      1 |   2 |  41
                 Mon Feb 10 17:32:01 1997    |      1997 |    7 |      1 |   2 |  41
                 Tue Jun 10 17:32:01 1997    |      1997 |   24 |      2 |   3 | 161
                 Sat Sep 22 18:19:20 2001    |      2001 |   38 |      6 |   7 | 265
                 Wed Mar 15 08:14:01 2000    |      2000 |   11 |      3 |   4 |  75
                 Wed Mar 15 13:14:02 2000    |      2000 |   11 |      3 |   4 |  75
                 Wed Mar 15 12:14:03 2000    |      2000 |   11 |      3 |   4 |  75
                 Wed Mar 15 03:14:04 2000    |      2000 |   11 |      3 |   4 |  75
                 Wed Mar 15 02:14:05 2000    |      2000 |   11 |      3 |   4 |  75
                 Mon Feb 10 17:32:01 1997    |      1997 |    7 |      1 |   2 |  41
                 Mon Feb 10 17:32:01 1997    |      1997 |    7 |      1 |   2 |  41
                 Mon Feb 10 17:32:00 1997    |      1997 |    7 |      1 |   2 |  41
                 Mon Feb 10 17:32:01 1997    |      1997 |    7 |      1 |   2 |  41
                 Mon Feb 10 17:32:01 1997    |      1997 |    7 |      1 |   2 |  41
                 Mon Feb 10 17:32:01 1997    |      1997 |    7 |      1 |   2 |  41
                 Mon Feb 10 17:32:01 1997    |      1997 |    7 |      1 |   2 |  41
                 Mon Feb 10 17:32:01 1997    |      1997 |    7 |      1 |   2 |  41
                 Mon Feb 10 17:32:01 1997    |      1997 |    7 |      1 |   2 |  41
                 Mon Feb 10 17:32:01 1997    |      1997 |    7 |      1 |   2 |  41
                 Mon Feb 10 17:32:01 1997    |      1997 |    7 |      1 |   2 |  41
                 Mon Feb 10 17:32:01 1997    |      1997 |    7 |      1 |   2 |  41
                 Tue Jun 10 18:32:01 1997    |      1997 |   24 |      2 |   3 | 161
                 Mon Feb 10 17:32:01 1997    |      1997 |    7 |      1 |   2 |  41
                 Tue Feb 11 17:32:01 1997    |      1997 |    7 |      2 |   3 |  42
                 Wed Feb 12 17:32:01 1997    |      1997 |    7 |      3 |   4 |  43
                 Thu Feb 13 17:32:01 1997    |      1997 |    7 |      4 |   5 |  44
                 Fri Feb 14 17:32:01 1997    |      1997 |    7 |      5 |   6 |  45
                 Sat Feb 15 17:32:01 1997    |      1997 |    7 |      6 |   7 |  46
                 Sun Feb 16 17:32:01 1997    |      1997 |    7 |      7 |   1 |  47
                 Sat Feb 16 17:32:01 0097    |        97 |    7 |      6 |   7 |  47
                 Thu Feb 16 17:32:01 0597    |       597 |    7 |      4 |   5 |  47
                 Tue Feb 16 17:32:01 1097    |      1097 |    7 |      2 |   3 |  47
                 Sat Feb 16 17:32:01 1697    |      1697 |    7 |      6 |   7 |  47
                 Thu Feb 16 17:32:01 1797    |      1797 |    7 |      4 |   5 |  47
                 Tue Feb 16 17:32:01 1897    |      1897 |    7 |      2 |   3 |  47
                 Sun Feb 16 17:32:01 1997    |      1997 |    7 |      7 |   1 |  47
                 Sat Feb 16 17:32:01 2097    |      2097 |    7 |      6 |   7 |  47
                 Wed Feb 28 17:32:01 1996    |      1996 |    9 |      3 |   4 |  59
                 Thu Feb 29 17:32:01 1996    |      1996 |    9 |      4 |   5 |  60
                 Fri Mar 01 17:32:01 1996    |      1996 |    9 |      5 |   6 |  61
                 Mon Dec 30 17:32:01 1996    |      1997 |    1 |      1 |   2 | 365
                 Tue Dec 31 17:32:01 1996    |      1997 |    1 |      2 |   3 | 366
                 Wed Jan 01 17:32:01 1997    |      1997 |    1 |      3 |   4 |   1
                 Fri Feb 28 17:32:01 1997    |      1997 |    9 |      5 |   6 |  59
                 Sat Mar 01 17:32:01 1997    |      1997 |    9 |      6 |   7 |  60
                 Tue Dec 30 17:32:01 1997    |      1998 |    1 |      2 |   3 | 364
                 Wed Dec 31 17:32:01 1997    |      1998 |    1 |      3 |   4 | 365
                 Fri Dec 31 17:32:01 1999    |      1999 |   52 |      5 |   6 | 365
                 Sat Jan 01 17:32:01 2000    |      1999 |   52 |      6 |   7 |   1
                 Sun Dec 31 17:32:01 2000    |      2000 |   52 |      7 |   1 | 366
                 Mon Jan 01 17:32:01 2001    |      2001 |    1 |      1 |   2 |   1""");
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

        // Postgres gives a float for epoch
        this.q("""
                SELECT d1 as "timestamp",
                   extract(decade FROM d1) AS 'decade',
                   extract(century FROM d1) AS 'century',
                   extract(millennium FROM d1) AS 'millennium',
                --   round(extract(julian FROM d1)) AS 'julian',
                   extract(epoch FROM d1) AS 'epoch'
                   FROM TIMESTAMP_TBL;
                          timestamp          |  decade   |  century  | millennium |    epoch    \s
                -----------------------------+-----------+-----------+------------+--------------
                 Thu Jan 01 00:00:00 1970    |       197 |        20 |          2 |            0
                 Mon Feb 10 17:32:01 1997    |       199 |        20 |          2 |    855595921
                 Mon Feb 10 17:32:01 1997    |       199 |        20 |          2 |    855595921
                 Mon Feb 10 17:32:02 1997    |       199 |        20 |          2 |    855595922
                 Mon Feb 10 17:32:01.4 1997  |       199 |        20 |          2 |  855595921 \s
                 Mon Feb 10 17:32:01.5 1997  |       199 |        20 |          2 |  855595921 \s
                 Mon Feb 10 17:32:01.6 1997  |       199 |        20 |          2 |  855595921 \s
                 Thu Jan 02 00:00:00 1997    |       199 |        20 |          2 |    852163200
                 Thu Jan 02 03:04:05 1997    |       199 |        20 |          2 |    852174245
                 Mon Feb 10 17:32:01 1997    |       199 |        20 |          2 |    855595921
                 Mon Feb 10 17:32:01 1997    |       199 |        20 |          2 |    855595921
                 Mon Feb 10 17:32:01 1997    |       199 |        20 |          2 |    855595921
                 Mon Feb 10 17:32:01 1997    |       199 |        20 |          2 |    855595921
                 Tue Jun 10 17:32:01 1997    |       199 |        20 |          2 |    865963921
                 Sat Sep 22 18:19:20 2001    |       200 |        21 |          3 |   1001182760
                 Wed Mar 15 08:14:01 2000    |       200 |        20 |          2 |    953108041
                 Wed Mar 15 13:14:02 2000    |       200 |        20 |          2 |    953126042
                 Wed Mar 15 12:14:03 2000    |       200 |        20 |          2 |    953122443
                 Wed Mar 15 03:14:04 2000    |       200 |        20 |          2 |    953090044
                 Wed Mar 15 02:14:05 2000    |       200 |        20 |          2 |    953086445
                 Mon Feb 10 17:32:01 1997    |       199 |        20 |          2 |    855595921
                 Mon Feb 10 17:32:01 1997    |       199 |        20 |          2 |    855595921
                 Mon Feb 10 17:32:00 1997    |       199 |        20 |          2 |    855595920
                 Mon Feb 10 17:32:01 1997    |       199 |        20 |          2 |    855595921
                 Mon Feb 10 17:32:01 1997    |       199 |        20 |          2 |    855595921
                 Mon Feb 10 17:32:01 1997    |       199 |        20 |          2 |    855595921
                 Mon Feb 10 17:32:01 1997    |       199 |        20 |          2 |    855595921
                 Mon Feb 10 17:32:01 1997    |       199 |        20 |          2 |    855595921
                 Mon Feb 10 17:32:01 1997    |       199 |        20 |          2 |    855595921
                 Mon Feb 10 17:32:01 1997    |       199 |        20 |          2 |    855595921
                 Mon Feb 10 17:32:01 1997    |       199 |        20 |          2 |    855595921
                 Mon Feb 10 17:32:01 1997    |       199 |        20 |          2 |    855595921
                 Tue Jun 10 18:32:01 1997    |       199 |        20 |          2 |    865967521
                 Mon Feb 10 17:32:01 1997    |       199 |        20 |          2 |    855595921
                 Tue Feb 11 17:32:01 1997    |       199 |        20 |          2 |    855682321
                 Wed Feb 12 17:32:01 1997    |       199 |        20 |          2 |    855768721
                 Thu Feb 13 17:32:01 1997    |       199 |        20 |          2 |    855855121
                 Fri Feb 14 17:32:01 1997    |       199 |        20 |          2 |    855941521
                 Sat Feb 15 17:32:01 1997    |       199 |        20 |          2 |    856027921
                 Sun Feb 16 17:32:01 1997    |       199 |        20 |          2 |    856114321
                 Sat Feb 16 17:32:01 0097    |         9 |         1 |          1 | -59102029679
                 Thu Feb 16 17:32:01 0597    |        59 |         6 |          1 | -43323575279
                 Tue Feb 16 17:32:01 1097    |       109 |        11 |          2 | -27545120879
                 Sat Feb 16 17:32:01 1697    |       169 |        17 |          2 |  -8610906479
                 Thu Feb 16 17:32:01 1797    |       179 |        18 |          2 |  -5455232879
                 Tue Feb 16 17:32:01 1897    |       189 |        19 |          2 |  -2299559279
                 Sun Feb 16 17:32:01 1997    |       199 |        20 |          2 |    856114321
                 Sat Feb 16 17:32:01 2097    |       209 |        21 |          3 |   4011874321
                 Wed Feb 28 17:32:01 1996    |       199 |        20 |          2 |    825528721
                 Thu Feb 29 17:32:01 1996    |       199 |        20 |          2 |    825615121
                 Fri Mar 01 17:32:01 1996    |       199 |        20 |          2 |    825701521
                 Mon Dec 30 17:32:01 1996    |       199 |        20 |          2 |    851967121
                 Tue Dec 31 17:32:01 1996    |       199 |        20 |          2 |    852053521
                 Wed Jan 01 17:32:01 1997    |       199 |        20 |          2 |    852139921
                 Fri Feb 28 17:32:01 1997    |       199 |        20 |          2 |    857151121
                 Sat Mar 01 17:32:01 1997    |       199 |        20 |          2 |    857237521
                 Tue Dec 30 17:32:01 1997    |       199 |        20 |          2 |    883503121
                 Wed Dec 31 17:32:01 1997    |       199 |        20 |          2 |    883589521
                 Fri Dec 31 17:32:01 1999    |       199 |        20 |          2 |    946661521
                 Sat Jan 01 17:32:01 2000    |       200 |        20 |          2 |    946747921
                 Sun Dec 31 17:32:01 2000    |       200 |        20 |          2 |    978283921
                 Mon Jan 01 17:32:01 2001    |       200 |        21 |          3 |    978370321""");
    }

    @Test
    public void testCenturies1() {
        // Postgres gives a float for epoch
        // Had to change quoting
        this.q("""
                SELECT d1 as "timestamp",
                   date_part( decade, d1) AS 'decade',
                   date_part( century, d1) AS 'century',
                   date_part( millennium, d1) AS 'millennium',
                   -- round(date_part( 'julian', d1)) AS julian,
                   date_part( epoch, d1) AS 'epoch'
                   FROM TIMESTAMP_TBL;
                          timestamp          |  decade   |  century  | millennium |    epoch    \s
                -----------------------------+-----------+-----------+------------+--------------
                 Thu Jan 01 00:00:00 1970    |       197 |        20 |          2 |            0
                 Mon Feb 10 17:32:01 1997    |       199 |        20 |          2 |    855595921
                 Mon Feb 10 17:32:01 1997    |       199 |        20 |          2 |    855595921
                 Mon Feb 10 17:32:02 1997    |       199 |        20 |          2 |    855595922
                 Mon Feb 10 17:32:01.4 1997  |       199 |        20 |          2 |  855595921 \s
                 Mon Feb 10 17:32:01.5 1997  |       199 |        20 |          2 |  855595921 \s
                 Mon Feb 10 17:32:01.6 1997  |       199 |        20 |          2 |  855595921 \s
                 Thu Jan 02 00:00:00 1997    |       199 |        20 |          2 |    852163200
                 Thu Jan 02 03:04:05 1997    |       199 |        20 |          2 |    852174245
                 Mon Feb 10 17:32:01 1997    |       199 |        20 |          2 |    855595921
                 Mon Feb 10 17:32:01 1997    |       199 |        20 |          2 |    855595921
                 Mon Feb 10 17:32:01 1997    |       199 |        20 |          2 |    855595921
                 Mon Feb 10 17:32:01 1997    |       199 |        20 |          2 |    855595921
                 Tue Jun 10 17:32:01 1997    |       199 |        20 |          2 |    865963921
                 Sat Sep 22 18:19:20 2001    |       200 |        21 |          3 |   1001182760
                 Wed Mar 15 08:14:01 2000    |       200 |        20 |          2 |    953108041
                 Wed Mar 15 13:14:02 2000    |       200 |        20 |          2 |    953126042
                 Wed Mar 15 12:14:03 2000    |       200 |        20 |          2 |    953122443
                 Wed Mar 15 03:14:04 2000    |       200 |        20 |          2 |    953090044
                 Wed Mar 15 02:14:05 2000    |       200 |        20 |          2 |    953086445
                 Mon Feb 10 17:32:01 1997    |       199 |        20 |          2 |    855595921
                 Mon Feb 10 17:32:01 1997    |       199 |        20 |          2 |    855595921
                 Mon Feb 10 17:32:00 1997    |       199 |        20 |          2 |    855595920
                 Mon Feb 10 17:32:01 1997    |       199 |        20 |          2 |    855595921
                 Mon Feb 10 17:32:01 1997    |       199 |        20 |          2 |    855595921
                 Mon Feb 10 17:32:01 1997    |       199 |        20 |          2 |    855595921
                 Mon Feb 10 17:32:01 1997    |       199 |        20 |          2 |    855595921
                 Mon Feb 10 17:32:01 1997    |       199 |        20 |          2 |    855595921
                 Mon Feb 10 17:32:01 1997    |       199 |        20 |          2 |    855595921
                 Mon Feb 10 17:32:01 1997    |       199 |        20 |          2 |    855595921
                 Mon Feb 10 17:32:01 1997    |       199 |        20 |          2 |    855595921
                 Mon Feb 10 17:32:01 1997    |       199 |        20 |          2 |    855595921
                 Tue Jun 10 18:32:01 1997    |       199 |        20 |          2 |    865967521
                 Mon Feb 10 17:32:01 1997    |       199 |        20 |          2 |    855595921
                 Tue Feb 11 17:32:01 1997    |       199 |        20 |          2 |    855682321
                 Wed Feb 12 17:32:01 1997    |       199 |        20 |          2 |    855768721
                 Thu Feb 13 17:32:01 1997    |       199 |        20 |          2 |    855855121
                 Fri Feb 14 17:32:01 1997    |       199 |        20 |          2 |    855941521
                 Sat Feb 15 17:32:01 1997    |       199 |        20 |          2 |    856027921
                 Sun Feb 16 17:32:01 1997    |       199 |        20 |          2 |    856114321
                 Sat Feb 16 17:32:01 0097    |         9 |         1 |          1 | -59102029679
                 Thu Feb 16 17:32:01 0597    |        59 |         6 |          1 | -43323575279
                 Tue Feb 16 17:32:01 1097    |       109 |        11 |          2 | -27545120879
                 Sat Feb 16 17:32:01 1697    |       169 |        17 |          2 |  -8610906479
                 Thu Feb 16 17:32:01 1797    |       179 |        18 |          2 |  -5455232879
                 Tue Feb 16 17:32:01 1897    |       189 |        19 |          2 |  -2299559279
                 Sun Feb 16 17:32:01 1997    |       199 |        20 |          2 |    856114321
                 Sat Feb 16 17:32:01 2097    |       209 |        21 |          3 |   4011874321
                 Wed Feb 28 17:32:01 1996    |       199 |        20 |          2 |    825528721
                 Thu Feb 29 17:32:01 1996    |       199 |        20 |          2 |    825615121
                 Fri Mar 01 17:32:01 1996    |       199 |        20 |          2 |    825701521
                 Mon Dec 30 17:32:01 1996    |       199 |        20 |          2 |    851967121
                 Tue Dec 31 17:32:01 1996    |       199 |        20 |          2 |    852053521
                 Wed Jan 01 17:32:01 1997    |       199 |        20 |          2 |    852139921
                 Fri Feb 28 17:32:01 1997    |       199 |        20 |          2 |    857151121
                 Sat Mar 01 17:32:01 1997    |       199 |        20 |          2 |    857237521
                 Tue Dec 30 17:32:01 1997    |       199 |        20 |          2 |    883503121
                 Wed Dec 31 17:32:01 1997    |       199 |        20 |          2 |    883589521
                 Fri Dec 31 17:32:01 1999    |       199 |        20 |          2 |    946661521
                 Sat Jan 01 17:32:01 2000    |       200 |        20 |          2 |    946747921
                 Sun Dec 31 17:32:01 2000    |       200 |        20 |          2 |    978283921
                 Mon Jan 01 17:32:01 2001    |       200 |        21 |          3 |    978370321""");
    }

    @Test
    public void testMicroseconds() {
        // microsecond, second, epoch are all floats in Postgres, but long in Calcite
        this.q("""
                SELECT d1 as "timestamp",
                   extract(microsecond from d1) AS 'microseconds',
                   extract(millisecond from d1) AS 'milliseconds',
                   extract(second from d1) AS 'seconds',
                   -- round(extract(julian from d1)) AS julian,
                   extract(epoch from d1) AS 'epoch'
                   FROM TIMESTAMP_TBL;
                          timestamp          | microseconds | milliseconds |  seconds  |    epoch
                -----------------------------+--------------+--------------+-----------+------------
                 Thu Jan 01 00:00:00 1970    |            0 |        0     |  0        |            0
                 Mon Feb 10 17:32:01 1997    |      1000000 |     1000     |  1        |    855595921
                 Mon Feb 10 17:32:01 1997    |      1000000 |     1000     |  1        |    855595921
                 Mon Feb 10 17:32:02 1997    |      2000000 |     2000     |  2        |    855595922
                 Mon Feb 10 17:32:01.4 1997  |      1400000 |     1400     |  1        |    855595921
                 Mon Feb 10 17:32:01.5 1997  |      1500000 |     1500     |  1        |    855595921
                 Mon Feb 10 17:32:01.6 1997  |      1600000 |     1600     |  1        |    855595921
                 Thu Jan 02 00:00:00 1997    |            0 |        0     |  0        |    852163200
                 Thu Jan 02 03:04:05 1997    |      5000000 |     5000     |  5        |    852174245
                 Mon Feb 10 17:32:01 1997    |      1000000 |     1000     |  1        |    855595921
                 Mon Feb 10 17:32:01 1997    |      1000000 |     1000     |  1        |    855595921
                 Mon Feb 10 17:32:01 1997    |      1000000 |     1000     |  1        |    855595921
                 Mon Feb 10 17:32:01 1997    |      1000000 |     1000     |  1        |    855595921
                 Tue Jun 10 17:32:01 1997    |      1000000 |     1000     |  1        |    865963921
                 Sat Sep 22 18:19:20 2001    |     20000000 |    20000     | 20        |   1001182760
                 Wed Mar 15 08:14:01 2000    |      1000000 |     1000     |  1        |    953108041
                 Wed Mar 15 13:14:02 2000    |      2000000 |     2000     |  2        |    953126042
                 Wed Mar 15 12:14:03 2000    |      3000000 |     3000     |  3        |    953122443
                 Wed Mar 15 03:14:04 2000    |      4000000 |     4000     |  4        |    953090044
                 Wed Mar 15 02:14:05 2000    |      5000000 |     5000     |  5        |    953086445
                 Mon Feb 10 17:32:01 1997    |      1000000 |     1000     |  1        |    855595921
                 Mon Feb 10 17:32:01 1997    |      1000000 |     1000     |  1        |    855595921
                 Mon Feb 10 17:32:00 1997    |            0 |        0     |  0        |    855595920
                 Mon Feb 10 17:32:01 1997    |      1000000 |     1000     |  1        |    855595921
                 Mon Feb 10 17:32:01 1997    |      1000000 |     1000     |  1        |    855595921
                 Mon Feb 10 17:32:01 1997    |      1000000 |     1000     |  1        |    855595921
                 Mon Feb 10 17:32:01 1997    |      1000000 |     1000     |  1        |    855595921
                 Mon Feb 10 17:32:01 1997    |      1000000 |     1000     |  1        |    855595921
                 Mon Feb 10 17:32:01 1997    |      1000000 |     1000     |  1        |    855595921
                 Mon Feb 10 17:32:01 1997    |      1000000 |     1000     |  1        |    855595921
                 Mon Feb 10 17:32:01 1997    |      1000000 |     1000     |  1        |    855595921
                 Mon Feb 10 17:32:01 1997    |      1000000 |     1000     |  1        |    855595921
                 Tue Jun 10 18:32:01 1997    |      1000000 |     1000     |  1        |    865967521
                 Mon Feb 10 17:32:01 1997    |      1000000 |     1000     |  1        |    855595921
                 Tue Feb 11 17:32:01 1997    |      1000000 |     1000     |  1        |    855682321
                 Wed Feb 12 17:32:01 1997    |      1000000 |     1000     |  1        |    855768721
                 Thu Feb 13 17:32:01 1997    |      1000000 |     1000     |  1        |    855855121
                 Fri Feb 14 17:32:01 1997    |      1000000 |     1000     |  1        |    855941521
                 Sat Feb 15 17:32:01 1997    |      1000000 |     1000     |  1        |    856027921
                 Sun Feb 16 17:32:01 1997    |      1000000 |     1000     |  1        |    856114321
                 Sat Feb 16 17:32:01 0097    |      1000000 |     1000     |  1        | -59102029679
                 Thu Feb 16 17:32:01 0597    |      1000000 |     1000     |  1        | -43323575279
                 Tue Feb 16 17:32:01 1097    |      1000000 |     1000     |  1        | -27545120879
                 Sat Feb 16 17:32:01 1697    |      1000000 |     1000     |  1        |  -8610906479
                 Thu Feb 16 17:32:01 1797    |      1000000 |     1000     |  1        |  -5455232879
                 Tue Feb 16 17:32:01 1897    |      1000000 |     1000     |  1        |  -2299559279
                 Sun Feb 16 17:32:01 1997    |      1000000 |     1000     |  1        |    856114321
                 Sat Feb 16 17:32:01 2097    |      1000000 |     1000     |  1        |   4011874321
                 Wed Feb 28 17:32:01 1996    |      1000000 |     1000     |  1        |    825528721
                 Thu Feb 29 17:32:01 1996    |      1000000 |     1000     |  1        |    825615121
                 Fri Mar 01 17:32:01 1996    |      1000000 |     1000     |  1        |    825701521
                 Mon Dec 30 17:32:01 1996    |      1000000 |     1000     |  1        |    851967121
                 Tue Dec 31 17:32:01 1996    |      1000000 |     1000     |  1        |    852053521
                 Wed Jan 01 17:32:01 1997    |      1000000 |     1000     |  1        |    852139921
                 Fri Feb 28 17:32:01 1997    |      1000000 |     1000     |  1        |    857151121
                 Sat Mar 01 17:32:01 1997    |      1000000 |     1000     |  1        |    857237521
                 Tue Dec 30 17:32:01 1997    |      1000000 |     1000     |  1        |    883503121
                 Wed Dec 31 17:32:01 1997    |      1000000 |     1000     |  1        |    883589521
                 Fri Dec 31 17:32:01 1999    |      1000000 |     1000     |  1        |    946661521
                 Sat Jan 01 17:32:01 2000    |      1000000 |     1000     |  1        |    946747921
                 Sun Dec 31 17:32:01 2000    |      1000000 |     1000     |  1        |    978283921
                 Mon Jan 01 17:32:01 2001    |      1000000 |     1000     |  1        |    978370321""");
    }

    // Postgres supports the ::timestamp syntax for casts
    // Calcite only supports 4 digit year for timestamps.
    // SELECT date_part('epoch', '294270-01-01 00:00:00'::timestamp);

    @Test
    public void testLargeYear() {
        // Timestamp out of range to be represented using milliseconds
        this.q("""
                SELECT extract(epoch from TIMESTAMP '5000-01-01 00:00:00');
                epoch
                ---------
                95617584000""");
    }

    //-- TO_CHAR()
    // TODO: Calcite seems to have 'to_char', but I can't get it to compile
    //SELECT to_char(d1, 'DAY Day day DY Dy dy MONTH Month month RM MON Mon mon')
    //   FROM TIMESTAMP_TBL;

    // select make_timestamp(0, 7, 15, 12, 30, 15);
    // Calcite does not support make_timestamp

    // select * from generate_series('2020-01-01 00:00'::timestamp,
    //                              '2020-01-02 03:00'::timestamp,
    //                              '1 hour'::interval);
    // Calcite does not support generate_series
}
