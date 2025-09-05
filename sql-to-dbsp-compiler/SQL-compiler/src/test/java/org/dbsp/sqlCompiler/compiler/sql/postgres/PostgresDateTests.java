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
import org.dbsp.sqlCompiler.compiler.sql.tools.SqlIoTest;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPIntervalMillisLiteral;
import org.dbsp.sqlCompiler.ir.expression.DBSPZSetExpression;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeMillisInterval;
import org.dbsp.util.Linq;
import org.junit.Test;

/* Tests manually adapted from
 * https://github.com/postgres/postgres/blob/master/src/test/regress/expected/date.out */
public class PostgresDateTests extends SqlIoTest {
    @Override
    public void prepareInputs(DBSPCompiler compiler) {
        String data = "CREATE TABLE DATE_TBL (f1 date);\n" +
                "INSERT INTO DATE_TBL VALUES ('1957-04-09');\n" +
                "INSERT INTO DATE_TBL VALUES ('1957-06-13');\n" +
                "INSERT INTO DATE_TBL VALUES ('1996-02-28');\n" +
                "INSERT INTO DATE_TBL VALUES ('1996-02-29');\n" +
                "INSERT INTO DATE_TBL VALUES ('1996-03-01');\n" +
                "INSERT INTO DATE_TBL VALUES ('1996-03-02');\n" +
                "INSERT INTO DATE_TBL VALUES ('1997-02-28');\n" +
                // illegal date
                // "INSERT INTO DATE_TBL VALUES ('1997-02-29');\n" +
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
        compiler.submitStatementsForCompilation(data);
    }

    @Test
    public void testSelect() {
        this.q("""
                SELECT f1 FROM DATE_TBL;
                f1
                ---------------
                04-09-1957
                06-13-1957
                02-28-1996
                02-29-1996
                03-01-1996
                03-02-1996
                02-28-1997
                03-01-1997
                03-02-1997
                04-01-2000
                04-02-2000
                04-03-2000
                04-08-2038
                04-09-2039
                04-10-2040""");
    }

    @Test
    public void testLt() {
        this.q("""
                SELECT f1 FROM DATE_TBL WHERE f1 < '2000-01-01';
                 f1
                ---------------
                 04-09-1957
                 06-13-1957
                 02-28-1996
                 02-29-1996
                 03-01-1996
                 03-02-1996
                 02-28-1997
                 03-01-1997
                 03-02-1997""");
                //" 04-10-2040 BC";
    }

    @Test
    public void testBetween() {
        this.q("""
                SELECT f1 FROM DATE_TBL
                  WHERE f1 BETWEEN '2000-01-01' AND '2001-01-01';
                 f1
                ----------------
                04-01-2000
                04-02-2000
                04-03-2000""");
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
        // TODO: why is the output an integer instead of an interval in Postgres?
        DBSPZSetExpression result =
                new DBSPZSetExpression(Linq.map(results,
                        l -> new DBSPTupleExpression(new DBSPIntervalMillisLiteral(DBSPTypeMillisInterval.Units.SECONDS,
                                l * 86400 * 1000, true)), DBSPExpression.class));
        this.compare(query, result, true);
    }

    // There is no 'epoch' date in Calcite
    // SELECT f1 - date 'epoch' AS "Days From Epoch" FROM DATE_TBL;
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
    public void testExtract() {
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


        // Deleted 'Julian' column
        // Incremented DOW column
        // Added null row
        this.q("""
                SELECT f1 as "date",
                    EXTRACT(YEAR from f1) AS 'year',
                    EXTRACT(month from f1) AS 'month',
                    EXTRACT(day from f1) AS 'day',
                    EXTRACT(quarter from f1) AS 'quarter',
                    EXTRACT(decade from f1) AS 'decade',
                    EXTRACT(century from f1) AS 'century',
                    EXTRACT(millennium from f1) AS 'millennium',
                    EXTRACT(isoyear from f1) AS 'isoyear',
                    EXTRACT(week from f1) AS 'week',
                    EXTRACT(dow from f1) AS 'dow',
                    EXTRACT(isodow from f1) AS 'isodow',
                    EXTRACT(doy from f1) AS 'doy',
                    EXTRACT(epoch from f1) AS 'epoch'
                    FROM DATE_TBL;
                     date      | year  | month | day | quarter | decade | century | millennium | isoyear | week | dow | isodow | doy |     epoch
                ---------------+-------+-------+-----+---------+--------+---------+------------+---------+------+-----+--------+-----+---------------
                 04-09-1957    |  1957 |     4 |   9 |       2 |    195 |      20 |          2 |    1957 |   15 |   3 |      2 |  99 |    -401760000
                 06-13-1957    |  1957 |     6 |  13 |       2 |    195 |      20 |          2 |    1957 |   24 |   5 |      4 | 164 |    -396144000
                 02-28-1996    |  1996 |     2 |  28 |       1 |    199 |      20 |          2 |    1996 |    9 |   4 |      3 |  59 |     825465600
                 02-29-1996    |  1996 |     2 |  29 |       1 |    199 |      20 |          2 |    1996 |    9 |   5 |      4 |  60 |     825552000
                 03-01-1996    |  1996 |     3 |   1 |       1 |    199 |      20 |          2 |    1996 |    9 |   6 |      5 |  61 |     825638400
                 03-02-1996    |  1996 |     3 |   2 |       1 |    199 |      20 |          2 |    1996 |    9 |   7 |      6 |  62 |     825724800
                 02-28-1997    |  1997 |     2 |  28 |       1 |    199 |      20 |          2 |    1997 |    9 |   6 |      5 |  59 |     857088000
                 03-01-1997    |  1997 |     3 |   1 |       1 |    199 |      20 |          2 |    1997 |    9 |   7 |      6 |  60 |     857174400
                 03-02-1997    |  1997 |     3 |   2 |       1 |    199 |      20 |          2 |    1997 |    9 |   1 |      7 |  61 |     857260800
                 04-01-2000    |  2000 |     4 |   1 |       2 |    200 |      20 |          2 |    2000 |   13 |   7 |      6 |  92 |     954547200
                 04-02-2000    |  2000 |     4 |   2 |       2 |    200 |      20 |          2 |    2000 |   13 |   1 |      7 |  93 |     954633600
                 04-03-2000    |  2000 |     4 |   3 |       2 |    200 |      20 |          2 |    2000 |   14 |   2 |      1 |  94 |     954720000
                 04-08-2038    |  2038 |     4 |   8 |       2 |    203 |      21 |          3 |    2038 |   14 |   5 |      4 |  98 |    2154297600
                 04-09-2039    |  2039 |     4 |   9 |       2 |    203 |      21 |          3 |    2039 |   14 |   7 |      6 |  99 |    2185920000
                 04-10-2040    |  2040 |     4 |  10 |       2 |    204 |      21 |          3 |    2040 |   15 |   3 |      2 | 101 |    2217628800"""
                //" 04-10-2040 BC | -2040 |     4 |  10 |       2 |   -204 |     -21 |         -3 |   -2040 |   15 |   2 |      1 | 100 | -126503251200"
                );
    }

    @Test
    public void testEpoch() {
        this.q("""
                SELECT EXTRACT(EPOCH FROM DATE '1970-01-01');
                 extract
                ---------
                       0""");
    }

    @Test
    public void testCentury() {
        this.q("""
                SELECT EXTRACT(CENTURY FROM DATE '0001-01-01');
                 extract
                ---------
                       1""");
    }

    @Test
    public void testCentury1() {
        this.q("""
                SELECT EXTRACT(CENTURY FROM DATE '1900-12-31');
                 extract
                ---------
                      19""");
    }

    @Test
    public void testCentury2() {
        this.q("""
                SELECT EXTRACT(CENTURY FROM DATE '1901-01-01');
                 extract
                ---------
                      20""");
    }

    @Test
    public void testCentury3() {
        this.q("""
                SELECT EXTRACT(CENTURY FROM DATE '2000-12-31');
                 extract
                ---------
                      20""");
    }

    @Test
    public void testCentury4() {
        this.q("""
                SELECT EXTRACT(CENTURY FROM DATE '2001-01-01');
                 extract
                ---------
                      21""");
    }

    @Test
    public void testMillennium() {
        this.q("""
                SELECT EXTRACT(MILLENNIUM FROM DATE '0001-01-01');
                 extract
                ---------
                       1""");
    }

    @Test
    public void testMillennium1() {
        this.q("""
                SELECT EXTRACT(MILLENNIUM FROM DATE '1000-12-31');
                 extract
                ---------
                       1""");
    }

    @Test
    public void testMillennium2() {
        this.q("""
                SELECT EXTRACT(MILLENNIUM FROM DATE '2000-12-31');
                 extract
                ---------
                       2""");
    }

    // CURRENT_DATE not supported
    // SELECT EXTRACT(CENTURY FROM CURRENT_DATE)>=21 AS True;     -- true

    @Test
    public void testMillennium3() {
        this.q("""
                SELECT EXTRACT(MILLENNIUM FROM DATE '2001-01-01');
                 extract
                ---------
                       3""");
    }

    @Test
    public void testDecade() {
        this.qs("""
                SELECT EXTRACT(DECADE FROM DATE '1994-12-25');
                 extract
                ---------
                     199
                (1 row)

                SELECT EXTRACT(DECADE FROM DATE '0010-01-01');
                 extract
                ---------
                       1
                (1 row)

                SELECT EXTRACT(DECADE FROM DATE '0009-12-31');
                 extract
                ---------
                       0
                (1 row)""");
    }

    @Test
    public void testMicroseconds() {
        this.q("""
                SELECT EXTRACT(MICROSECOND FROM DATE '2020-08-11');
                 extract
                ---------
                       0""");
    }

    @Test
    public void testMilliseconds() {
        this.q("""
                SELECT EXTRACT(MILLISECOND FROM DATE '2020-08-11');
                 extract
                ---------
                       0""");
    }

    @Test
    public void testSeconds() {
        this.q("""
                SELECT EXTRACT(SECOND FROM DATE '2020-08-11');
                 extract
                ---------
                       0""");
    }

    @Test
    public void testSeconds0() {
        this.q("""
                SELECT SECOND(DATE '2020-08-11');
                 extract
                ---------
                       0""");
    }

    @Test
    public void testMinutes() {
        this.q("""
                SELECT EXTRACT(MINUTE FROM DATE '2020-08-11');
                 extract
                ---------
                       0""");
    }

    @Test
    public void testMinutes1() {
        this.q("""
                SELECT MINUTE(DATE '2020-08-11');
                 extract
                ---------
                       0""");
    }

    @Test
    public void testHour() {
        this.q("""
                SELECT EXTRACT(HOUR FROM DATE '2020-08-11');
                 extract
                ---------
                       0""");
    }

    @Test
    public void testHour1() {
        this.q("""
                SELECT HOUR(DATE '2020-08-11');
                 extract
                ---------
                       0""");
    }

    @Test
    public void testDay() {
        this.q("""
                SELECT EXTRACT(DAY FROM DATE '2020-08-11');
                 extract
                ---------
                       11""");
    }

    @Test
    public void testDay1() {
        this.q("""
                SELECT DAYOFMONTH(DATE '2020-08-11');
                 extract
                ---------
                       11""");
    }

    @Test
    public void testMonth() {
        this.q("""
                SELECT EXTRACT(MONTH FROM DATE '2020-08-11');
                 extract
                ---------
                       8""");
    }

    @Test
    public void testMonth1() {
        this.q("""
                SELECT MONTH(DATE '2020-08-11');
                 extract
                ---------
                       8""");
    }

    @Test
    public void testYear() {
        this.q("""
                SELECT EXTRACT(YEAR FROM DATE '2020-08-11');
                 extract
                ---------
                     2020""");
    }

    @Test
    public void testYear1() {
        this.q("""
                SELECT YEAR(DATE '2020-08-11');
                 extract
                ---------
                     2020""");
    }

    @Test
    public void testDecade5() {
        this.q("""
                SELECT EXTRACT(DECADE FROM DATE '2020-08-11');
                 extract
                ---------
                     202""");
    }

    @Test
    public void testCentury5() {
        this.q("""
                SELECT EXTRACT(CENTURY FROM DATE '2020-08-11');
                 extract
                ---------
                     21""");
    }

    @Test
    public void testMillennium5() {
        this.q("""
                SELECT EXTRACT(MILLENNIUM FROM DATE '2020-08-11');
                 extract
                ---------
                     3""");
    }

    @Test
    public void testIsoYear() {
        this.q("""
                SELECT EXTRACT(ISOYEAR FROM DATE '2020-08-11');
                 extract
                ---------
                     2020""");
    }

    @Test
    public void testQuarter() {
        this.q("""
                SELECT EXTRACT(QUARTER FROM DATE '2020-08-11');
                 extract
                ---------
                     3""");
    }

    @Test
    public void testWeek() {
        this.q("""
                SELECT EXTRACT(WEEK FROM DATE '2020-08-11');
                 extract
                ---------
                     33""");
    }

    @Test
    public void testDow() {
        this.q("""
                SELECT EXTRACT(DOW FROM DATE '2020-08-11');
                 extract
                ---------
                     3""");
    }

    @Test
    public void testDow2() {
        this.q("""
                SELECT DAYOFWEEK(DATE '2020-08-11');
                 extract
                ---------
                     3""");
    }

    /*
     * TODO Postgres actually returns 0 for this query, but the Calcite optimizer
     * folds this to 1. Same for previous test.
     */
    @Test
    public void testDow1() {
        // Sunday
        this.q("""
                SELECT EXTRACT(DOW FROM DATE '2020-08-16');
                 extract
                ---------
                     1""");
    }

    @Test
    public void testIsoDow() {
        this.q("""
                SELECT EXTRACT(ISODOW FROM DATE '2020-08-16');
                 extract
                ---------
                     7""");
    }

    @Test
    public void testDoy() {
        this.q("""
                SELECT EXTRACT(DOY FROM DATE '2020-08-11');
                 extract
                ---------
                     224""");
    }

    // SELECT EXTRACT(TIMEZONE      FROM DATE '2020-08-11');
    //ERROR:  unit "timezone" not supported for type date
    //SELECT EXTRACT(TIMEZONE_M    FROM DATE '2020-08-11');
    //ERROR:  unit "timezone_m" not supported for type date
    //SELECT EXTRACT(TIMEZONE_H    FROM DATE '2020-08-11');
    //ERROR:  unit "timezone_h" not supported for type date

    @Test
    public void testDateTrunc() {
        // In the BigQuery library there is a DATE_TRUNC, but arguments are swapped
        // I added many more tests
        this.qs("""
                 SELECT DATE_TRUNC(DATE '1970-03-20', MILLENNIUM);
                  date_trunc
                --------------------------
                 1001-01-01
                (1 row)

                SELECT DATE_TRUNC(DATE '1970-03-20', CENTURY); -- 1901
                          date_trunc
                ------------------------------
                 1901-01-01
                (1 row)

                SELECT DATE_TRUNC(DATE '2004-08-10', CENTURY); -- 2001-01-01
                          date_trunc
                ------------------------------
                 2001-01-01
                (1 row)

                SELECT DATE_TRUNC(DATE '0002-02-04', CENTURY); -- 0001-01-01
                          date_trunc
                ------------------------------
                 0001-01-01
                (1 row)

                SELECT DATE_TRUNC(DATE '1993-12-25', DECADE); -- 1990-01-01
                          date_trunc
                ------------------------------
                 1990-01-01
                (1 row)

                SELECT DATE_TRUNC(DATE '1993-12-25', YEAR);
                          date_trunc
                ------------------------------
                 1993-01-01
                (1 row)

                SELECT DATE_TRUNC(DATE '1993-12-25', MONTH);
                          date_trunc
                ------------------------------
                 1993-12-01
                (1 row)
                
                SELECT DATE_TRUNC(DATE '1993-12-25', WEEK);
                          date_trunc
                ------------------------------
                 1993-12-19
                (1 row)

                SELECT DATE_TRUNC(DATE '1993-12-25', DAY);
                          date_trunc
                ------------------------------
                 1993-12-25
                (1 row)

                SELECT DATE_TRUNC(TIMESTAMP '1970-03-20 04:30:00.00000', MILLENNIUM);
                        date_trunc
                --------------------------
                 1001-01-01 00:00:00
                (1 row)

                SELECT DATE_TRUNC(TIMESTAMP '1970-03-20 04:30:00.00000', CENTURY); -- 1901
                        date_trunc
                --------------------------
                 1901-01-01 00:00:00
                (1 row)

                SELECT DATE_TRUNC(TIMESTAMP '1970-03-20 04:30:00.00000', DECADE);
                        date_trunc
                --------------------------
                 1970-01-01 00:00:00
                (1 row)

                SELECT DATE_TRUNC(TIMESTAMP '1970-03-20 04:30:00.00000', YEAR);
                        date_trunc
                --------------------------
                 1970-01-01 00:00:00
                (1 row)

                SELECT DATE_TRUNC(TIMESTAMP '1970-03-20 04:30:00.00000', MONTH);
                        date_trunc
                --------------------------
                 1970-03-01 00:00:00
                (1 row)

                SELECT DATE_TRUNC(TIMESTAMP '1970-03-20 04:30:00.00000', DAY);
                        date_trunc
                --------------------------
                 1970-03-20 00:00:00
                (1 row)""");
    }

    @Test
    public void testTimeststampTrunc() {
        // Not in Postgres
        this.qs("""
                SELECT timestamp_trunc(TIMESTAMP '1970-03-20 04:30:00.00000', MILLENNIUM);
                        timestamp_trunc
                --------------------------
                 1001-01-01 00:00:00
                (1 row)

                SELECT timestamp_trunc(TIMESTAMP '1970-03-20 04:30:00.00000', CENTURY); -- 1901
                        timestamp_trunc
                --------------------------
                 1901-01-01 00:00:00
                (1 row)

                SELECT timestamp_trunc(TIMESTAMP '1970-03-20 04:30:00.00000', DECADE);
                        timestamp_trunc
                --------------------------
                 1970-01-01 00:00:00
                (1 row)

                SELECT timestamp_trunc(TIMESTAMP '1970-03-20 04:30:00.00000', YEAR);
                        timestamp_trunc
                --------------------------
                 1970-01-01 00:00:00
                (1 row)

                SELECT timestamp_trunc(TIMESTAMP '1970-03-20 04:30:00.00000', MONTH);
                        timestamp_trunc
                --------------------------
                 1970-03-01 00:00:00
                (1 row)

                SELECT timestamp_trunc(TIMESTAMP '1970-03-20 04:30:00.00000', DAY);
                        timestamp_trunc
                --------------------------
                 1970-03-20 00:00:00
                (1 row)

                SELECT timestamp_trunc(timestamp '2015-02-19 12:34:56.78', hour);
                  trunc
                --------
                2015-02-19 12:00:00
                (1 row)

                SELECT timestamp_trunc(timestamp '2015-02-19 12:34:56.78', minute);
                  trunc
                --------
                2015-02-19 12:34:00
                (1 row)

                SELECT timestamp_trunc(timestamp '2015-02-19 12:34:56.78', second);
                  trunc
                --------
                2015-02-19 12:34:56
                (1 row)""");
    }

    @Test
    public void testTimeTrunc() {
        // Not in Postgres
        this.qs("""
                SELECT time_trunc(time '12:34:56.78', hour);
                  trunc
                --------
                12:00:00
                (1 row)

                SELECT time_trunc(time '12:34:56.78', minute);
                  trunc
                --------
                12:34:00
                (1 row)

                SELECT time_trunc(time '12:34:56.78', second);
                  trunc
                --------
                12:34:56
                (1 row)""");
    }

    @Test
    public void illegalDateTrunc() {
        this.queryFailingInCompilation("SELECT DATE_TRUNC(DATE '1993-12-25', HOUR)",
                "'HOUR' is not a valid time frame");
    }

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

    @Test
    // Calcite does not seem to support infinity dates
    public void testInfinity() {
        this.queryFailingInCompilation("SELECT DATE 'infinity'\n",
                "Illegal DATE literal 'infinity'");
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
