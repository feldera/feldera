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
import org.dbsp.sqlCompiler.compiler.sql.tools.Change;
import org.dbsp.sqlCompiler.compiler.sql.tools.CompilerCircuitStream;
import org.dbsp.sqlCompiler.compiler.sql.tools.InputOutputChange;
import org.dbsp.sqlCompiler.compiler.sql.tools.SqlIoTest;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPDecimalLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPDoubleLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPI32Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPI64Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPStringLiteral;
import org.dbsp.sqlCompiler.ir.expression.DBSPZSetExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPU32Literal;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeDecimal;
import org.dbsp.util.Linq;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;

public class CastTests extends SqlIoTest {
    final DBSPTypeDecimal tenTwo = new DBSPTypeDecimal(CalciteObject.EMPTY, 10, 2, true);
    final DBSPTypeDecimal tenFour = new DBSPTypeDecimal(CalciteObject.EMPTY, 10, 4, false);

    @Override
    public void prepareInputs(DBSPCompiler compiler) {
        String ddl = "CREATE TABLE T (\n" +
                "COL1 INT NOT NULL" +
                ", COL2 DOUBLE NOT NULL" +
                ", COL3 VARCHAR NOT NULL" +
                ", COL4 DECIMAL(10,2)" +
                ", COL5 DECIMAL(10,4) NOT NULL" +
                ");" +
                "INSERT INTO T VALUES(10, 12.0, 100100, NULL, 100103);";
        compiler.submitStatementsForCompilation(ddl);
    }

    public Change createInput() {
        return new Change(new DBSPZSetExpression(new DBSPTupleExpression(
                new DBSPI32Literal(10),
                new DBSPDoubleLiteral(12.0),
                new DBSPStringLiteral("100100"),
                DBSPLiteral.none(tenTwo),
                new DBSPDecimalLiteral(tenFour, new BigDecimal(100103)))));
    }

    public void testQuery(String query, DBSPZSetExpression expectedOutput) {
        query = "CREATE VIEW V AS " + query + ";";
        CompilerCircuitStream ccs = this.getCCS(query);
        InputOutputChange change = new InputOutputChange(this.createInput(), new Change(expectedOutput));
        ccs.addChange(change);
    }

    @Test
    public void testTinyInt() {
        this.runtimeConstantFail("SELECT CAST(256 AS TINYINT)", "out of range integral type conversion attempted");
    }

    @Test
    public void castFail() {
        this.runtimeConstantFail("SELECT CAST('blah' AS DECIMAL)",
                "invalid decimal");
    }

    @Test
    public void castFailPosition() {
        // use bround to inhibit compile-time optimization
        this.runtimeConstantFail("""
                        SELECT
                            CAST(bround(100000, 0)
                            AS DECIMAL
                               (3,2))""",
                "line 2 column 5: Cannot represent 100000 as DECIMAL(3, 2): " +
                        "precision of DECIMAL type too small to represent value");
    }

    @Test
    public void intAndString() {
        String query = "SELECT '1' + 2";
        this.testQuery(query, new DBSPZSetExpression(
                new DBSPTupleExpression(new DBSPI32Literal(3))));
    }

    @Test
    public void intAndStringTable() {
        String query = "SELECT T.COL1 + T.COL3 FROM T";
        this.testQuery(query, new DBSPZSetExpression(
                new DBSPTupleExpression(new DBSPI32Literal(100110))));
    }

    @Test
    public void castNull() {
        String query = "SELECT CAST(NULL AS INTEGER)";
        this.testQuery(query, new DBSPZSetExpression(new DBSPTupleExpression(new DBSPI32Literal())));
        query = "SELECT CAST(NULL AS UNSIGNED)";
        this.testQuery(query, new DBSPZSetExpression(new DBSPTupleExpression(new DBSPU32Literal())));
    }

    @Test
    public void castFromFPTest() {
        String query = "SELECT T.COL1 + T.COL2 + T.COL3 + T.COL5 FROM T";
        this.testQuery(query, new DBSPZSetExpression(new DBSPTupleExpression(new DBSPDoubleLiteral(200225.0))));
    }

    @Test
    public void decimalOutOfRange() {
        this.runtimeFail("SELECT CAST(100103123 AS DECIMAL(10, 4))",
                "Cannot represent 100103123 as DECIMAL(10, 4)",
                this.streamWithEmptyChanges());
    }

    @Test
    public void testFpCasts() {
        this.testQuery("SELECT CAST(T.COL2 AS BIGINT) FROM T", new DBSPZSetExpression(new DBSPTupleExpression(new DBSPI64Literal(12))));
    }

    @Test
    public void runtimeOverflowsTests() {
        this.runtimeConstantFail("SELECT CAST(1000 AS TINYINT)",
                "Error converting 1000 to TINYINT");
        this.runtimeConstantFail("SELECT CAST(1000 AS TINYINT UNSIGNED)",
                "Error converting 1000 to TINYINT UNSIGNED");
        this.runtimeConstantFail("SELECT CAST(256 AS TINYINT UNSIGNED)",
                "Error converting 256 to TINYINT UNSIGNED");
        this.runtimeConstantFail("SELECT CAST(-1 AS TINYINT UNSIGNED)",
                "Error converting -1 to TINYINT UNSIGNED");
    }

    @Test
    public void mixedTypesTest() {
        this.qs("SELECT CAST(100 AS UNSIGNED) - 10;" +
                """
                 t
                ---
                 90
                (1 row)
                
                SELECT CAST(100 AS BIGINT UNSIGNED) * 1000;
                 t
                ---
                 100000
                (1 row)
                
                SELECT CAST(100 AS UNSIGNED) * 1.0;
                 t
                ---
                 100.0
                (1 row)
                
                SELECT CAST(100 AS UNSIGNED) * -1.0;
                 t
                ---
                 -100.0
                (1 row)
                
                SELECT CAST(100 AS UNSIGNED) * -1.0e0;
                 t
                ---
                 -100.0
                (1 row)
                
                SELECT CAST(100 AS TINYINT UNSIGNED) + 300;
                 t
                ---
                 400
                (1 row)""");
        this.runtimeConstantFail("SELECT CAST(10 AS UNSIGNED) - 100",
                "'10 - 100' causes overflow");
        this.runtimeConstantFail("SELECT CAST(10 AS UNSIGNED) / -1",
                "Error converting -1 to INTEGER UNSIGNED");
        this.runtimeConstantFail("SELECT CAST(10 AS TINYINT UNSIGNED) + CAST(250 AS TINYINT UNSIGNED)",
                "'10 + 250' causes overflow");
    }

    @Test
    public void timeCastTests() {
        this.qs("""
                SELECT CAST(1000 AS TIMESTAMP);
                 t
                ---
                 1970-01-01 00:00:01
                (1 row)
                
                SELECT CAST(3600000 AS TIMESTAMP);
                 t
                ---
                 1970-01-01 01:00:00
                (1 row)
                
                SELECT CAST(-1000 AS TIMESTAMP);
                 t
                ---
                 1969-12-31 23:59:59
                (1 row)
                
                SELECT CAST(TIMESTAMP '1970-01-01 00:00:01.234' AS INTEGER);
                 i
                ---
                 1234
                (1 row)
                
                SELECT CAST(T.COL1 * 1000 AS TIMESTAMP) FROM T;
                 t
                ---
                 1970-01-01 00:00:10
                (1 row)
                
                SELECT CAST(T.COL2 * 1000 AS TIMESTAMP) FROM T;
                 t
                ---
                 1970-01-01 00:00:12
                (1 row)
                
                SELECT CAST(CAST(T.COL2 * 1000 AS TIMESTAMP) AS INTEGER) FROM T;
                 i
                ---
                 12000
                (1 row)
                
                SELECT CAST(CAST(T.COL2 / 10 AS TIMESTAMP) AS DOUBLE) FROM T;
                 i
                ---
                 0
                (1 row)""");
    }

    @Test
    public void testCastStringToComplexLongInterval() {
        this.qs("""
                SELECT CAST('1-1' AS INTERVAL YEAR TO MONTH);
                 i
                ---
                 13 months
                (1 row)
                
                SELECT CAST('+1-1' AS INTERVAL YEAR TO MONTH);
                 i
                ---
                 13 months
                (1 row)
                
                SELECT CAST('-1-1' AS INTERVAL YEAR TO MONTH);
                 i
                ---
                 13 months ago
                (1 row)""");
    }

    @Test
    public void testCastStringToComplexShortInterval() {
        this.qs("""
                SELECT CAST('100 1' AS INTERVAL DAY TO HOUR);
                 i
                ---
                 100 days 1 hours
                (1 row)
                
                SELECT CAST('-100 1' AS INTERVAL DAY TO HOUR);
                 i
                ---
                 100 days 1 hours ago
                (1 row)
                
                SELECT CAST('100:1' AS INTERVAL HOURS TO MINUTES);
                 i
                ---
                 100 hours 1 min
                (1 row)
                
                SELECT CAST('-100:1' AS INTERVAL HOURS TO MINUTES);
                 i
                ---
                 100 hours 1 mins ago
                (1 row)""");
    }

    @Test
    public void testCastStringToComplexShortInterval2() {
        this.qs("""
                SELECT CAST('10 10:1' AS INTERVAL DAYS TO MINUTES);
                 i
                ---
                 10 days 10 hours 1 mins
                (1 row)

                SELECT CAST('-10 10:1' AS INTERVAL DAYS TO MINUTES);
                 i
                ---
                 10 days 10 hours 1 mins ago
                (1 row)

                SELECT CAST('100:1:1' AS INTERVAL HOUR TO SECOND);
                 i
                ---
                 100 hours 61 secs
                (1 row)
                
                SELECT CAST('-100:1:1' AS INTERVAL HOUR TO SECOND);
                 i
                ---
                 100 hours 61 secs ago
                (1 row)
                
                SELECT CAST('-100 10:1:1' AS INTERVAL DAYS TO SECOND);
                 i
                ---
                 100 days 10 hours 61 secs ago
                (1 row)
                
                SELECT CAST('100 10:1:1' AS INTERVAL DAYS TO SECOND);
                 i
                ---
                 100 days 10 hours 61 secs
                (1 row)""");
    }

    @Test
    public void testCastStringToComplexShortInterval1() {
        this.qs("""
                SELECT CAST('100:1' AS INTERVAL MINUTES TO SECONDS);
                 i
                ---
                 100 mins 1 secs
                (1 row)
                
                SELECT CAST('-100:1' AS INTERVAL MINUTES TO SECONDS);
                 i
                ---
                 100 mins 1 secs ago
                (1 row)
                
                SELECT CAST('1:1.1' AS INTERVAL MINUTES TO SECONDS);
                 i
                ---
                 61.1 secs
                (1 row)
                
                SELECT CAST('-1:1.1' AS INTERVAL MINUTES TO SECONDS);
                 i
                ---
                 61.1 secs ago
                (1 row)
                
                SELECT CAST('+1:1.111111' AS INTERVAL MINUTES TO SECONDS);
                 i
                ---
                 61.111 secs
                (1 row)
                
                SELECT CAST('-1:1.111111' AS INTERVAL MINUTES TO SECONDS);
                 i
                ---
                 61.111 secs ago
                (1 row)""");
    }

    @Test
    public void testCastStringToSimpleInterval() {
        this.qs("""
                SELECT CAST('1' AS INTERVAL YEAR);
                 i
                ---
                 1 year
                (1 row)
                
                SELECT CAST('1' AS INTERVAL MONTH);
                 i
                ---
                 1 month
                (1 row)
                
                SELECT CAST('-1' AS INTERVAL YEAR);
                 i
                ---
                 1 year ago
                (1 row)
                
                SELECT CAST('-1' AS INTERVAL MONTH);
                 i
                ---
                 1 month ago
                (1 row)
                
                SELECT CAST('1' AS INTERVAL DAYS);
                 i
                ---
                 24 hours
                (1 row)
                
                SELECT CAST('1' AS INTERVAL HOURS);
                 i
                ---
                 1 hour
                (1 row)
                
                SELECT CAST('1' AS INTERVAL MINUTES);
                 i
                ---
                 1 min
                (1 row)
                
                SELECT CAST('1' AS INTERVAL SECONDS);
                 i
                ---
                 1 sec
                (1 row)
                
                SELECT CAST('-1' AS INTERVAL DAYS);
                 i
                ---
                 24 hours ago
                (1 row)
                
                SELECT CAST('-1' AS INTERVAL HOURS);
                 i
                ---
                 1 hour ago
                (1 row)
                
                SELECT CAST('-1' AS INTERVAL MINUTES);
                 i
                ---
                 1 min ago
                (1 row)
                
                SELECT CAST('-1' AS INTERVAL SECONDS);
                 i
                ---
                 1 sec ago
                (1 row)
                
                SELECT CAST('-1000' AS INTERVAL SECONDS);
                 i
                ---
                 1000 sec ago
                (1 row)
                
                SELECT CAST('1000.23' AS INTERVAL SECONDS);
                 i
                ---
                 1000.23 secs
                (1 row)""");
    }

    @Test
    public void testFailingTimeCasts() {
        this.statementsFailingInCompilation("CREATE VIEW V AS SELECT CAST(1000 AS TIME)",
                "Cast function cannot convert value of type INTEGER NOT NULL to type TIME");
        this.statementsFailingInCompilation("CREATE VIEW V AS SELECT CAST(TIME '10:00:00' AS INTEGER)",
                "Cast function cannot convert value of type TIME(0) NOT NULL to type INTEGER");

        this.statementsFailingInCompilation("CREATE VIEW V AS SELECT CAST(1000 AS DATE)",
                "Cast function cannot convert value of type INTEGER NOT NULL to type DATE");
        this.statementsFailingInCompilation("CREATE VIEW V AS SELECT CAST(CAST(1000 AS UNSIGNED) AS DATE)",
                "Cast function cannot convert value of type INTEGER UNSIGNED NOT NULL to type DATE");
        this.statementsFailingInCompilation("CREATE VIEW V AS SELECT CAST(DATE '2024-01-01' AS INTEGER)",
                "Cast function cannot convert value of type DATE NOT NULL to type INTEGER");

        this.statementsFailingInCompilation("CREATE VIEW V AS SELECT CAST(X'01' AS TIME)",
                "Cast function cannot convert BINARY value to ");
    }

    @Test
    public void testAllCasts() {
        String[] types = new String[] {
                "NULL",
                "BOOLEAN",
                "TINYINT",
                "SMALLINT",
                "INTEGER",
                "BIGINT",
                "TINYINT UNSIGNED",
                "SMALLINT UNSIGNED",
                "INTEGER UNSIGNED",
                "BIGINT UNSIGNED",
                "DECIMAL(10, 2)",
                "REAL",
                "DOUBLE",
                "CHAR(6)",
                "VARCHAR",
                "BINARY",
                "VARBINARY",
                // long
                "INTERVAL YEARS TO MONTHS",
                "INTERVAL YEARS",
                "INTERVAL MONTHS",
                // short
                "INTERVAL DAYS",
                "INTERVAL HOURS",
                "INTERVAL DAYS TO HOURS",
                "INTERVAL MINUTES",
                "INTERVAL DAYS TO MINUTES",
                "INTERVAL HOURS TO MINUTES",
                "INTERVAL SECONDS",
                "INTERVAL DAYS TO SECONDS",
                "INTERVAL HOURS TO SECONDS",
                "INTERVAL MINUTES TO SECONDS",
                "TIME",
                "TIMESTAMP",
                "DATE",
                // "GEOMETRY",
                "ROW(lf INTEGER, rf VARCHAR)",
                "INT ARRAY",
                "MAP<INT, VARCHAR>",
                "VARIANT",
                "UUID"
        };
        String[] values = new String[] {
                "NULL",   // NULL
                "'true'", // boolean
                "1",    // tinyint
                "1",    // smallint
                "1",    // integer
                "1",    // bigint
                "1",    // tinyint unsigned
                "1",    // smallint unsigned
                "1",    // integer unsigned
                "1",    // bigint unsigned
                "1.1",  // decimal
                "1.1e0", // real
                "1.1e0", // double
                "'chars'", // char(6)
                "'string'", // varchar
                "x'0123'", // BINARY
                "x'3456'", // VARBINARY
                "'1-2'",   // INTERVAL YEARS TO MONTHS
                "'1'",     // INTERVAL YEARS
                "'2'",     // INTERVAL MONTHS
                "'1'",     // INTERVAL DAYS
                "'2'",     // INTERVAL HOURS
                "'1 2'",   // INTERVAL DAYS TO HOURS
                "'3'",     // INTERVAL MINUTES
                "'1 2:3'", // INTERVAL DAYS TO MINUTES
                "'2:3'",   // INTERVAL HOURS TO MINUTES
                "'4'",     // INTERVAL SECONDS
                "'1 2:3:4'", // INTERVAL DAYS TO SECONDS
                "'2:3:4'", // INTERVAL HOURS TO SECONDS
                "'3:4'",   // INTERVAL MINUTES TO SECONDS
                "'10:00:00'",  // TIME
                "'2000-01-01 10:00:00'", // TIMESTAMP
                "'2000-01-01'", // DATE
                "ROW(1, 'string')", // ROW
                "ARRAY[1, 2, 3]",   // ARRAY
                "MAP[1, 'a', 2, 'b']", // MAP
                "1", // VARIANT
                "UUID '123e4567-e89b-12d3-a456-426655440000'" // UUID
        };

        enum CanConvert {
            T, // yes
            F, // no
            N, // not implemented; this should not appear in the table below.
        }

        final CanConvert T = CanConvert.T;
        final CanConvert F = CanConvert.F;
        // TODO: https://issues.apache.org/jira/browse/CALCITE-6779
        final CanConvert N = CanConvert.N;

        // Rows and columns match the array of types above.
        final CanConvert[][] legal = {
// To:   N, B, I8,16,32,64,U8,U6,U3,U6,De,r, d, c, v, b, vb,ym,y, m, d, h, dh,m,dm,hm, s, ds,hs,ms,t, ts,dt,ro,a, m, V, U
/*From                                                                                                                    */
/* N */{ F, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, F },
/* B */{ F, T, F, F, F, F, F, F, F, F, F, F, F, T, T, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, T, F },
/* I8*/{ F, T, T, T, T, T, T, T, T, T, T, T, T, T, T, F, F, F, T, T, T, T, F, T, F, F, T, F, F, F, F, T, F, F, F, F, T, F },
/*I16*/{ F, T, T, T, T, T, T, T, T, T, T, T, T, T, T, F, F, F, T, T, T, T, F, T, F, F, T, F, F, F, F, T, F, F, F, F, T, F },
/*I32*/{ F, T, T, T, T, T, T, T, T, T, T, T, T, T, T, F, F, F, T, T, T, T, F, T, F, F, T, F, F, F, F, T, F, F, F, F, T, F },
/*I64*/{ F, T, T, T, T, T, T, T, T, T, T, T, T, T, T, F, F, F, T, T, T, T, F, T, F, F, T, F, F, F, F, T, F, F, F, F, T, F },
/* U8*/{ F, T, T, T, T, T, T, T, T, T, T, T, T, T, T, F, F, F, T, T, T, T, F, T, F, F, T, F, F, F, F, T, F, F, F, F, T, F },
/*U16*/{ F, T, T, T, T, T, T, T, T, T, T, T, T, T, T, F, F, F, T, T, T, T, F, T, F, F, T, F, F, F, F, T, F, F, F, F, T, F },
/*U32*/{ F, T, T, T, T, T, T, T, T, T, T, T, T, T, T, F, F, F, T, T, T, T, F, T, F, F, T, F, F, F, F, T, F, F, F, F, T, F },
/*U64*/{ F, T, T, T, T, T, T, T, T, T, T, T, T, T, T, F, F, F, T, T, T, T, F, T, F, F, T, F, F, F, F, T, F, F, F, F, T, F },
/*Dec*/{ F, T, T, T, T, T, T, T, T, T, T, T, T, T, T, F, F, F, T, T, T, T, F, T, F, F, T, F, F, F, F, T, F, F, F, F, T, F },
/* r */{ F, T, T, T, T, T, T, T, T, T, T, T, T, T, T, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, T, F, F, F, F, T, F },
/* d */{ F, T, T, T, T, T, T, T, T, T, T, T, T, T, T, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, T, F, F, F, F, T, F },
/*chr*/{ F, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, F, F, F, T, T },
/* v */{ F, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, F, F, F, T, T },
/* b */{ F, F, F, F, F, F, F, F, F, F, F, F, F, T, T, T, T, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, T, T },
/*vb */{ F, F, F, F, F, F, F, F, F, F, F, F, F, T, T, T, T, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, T, T },
/*ym */{ F, F, F, F, F, F, F, F, F, F, F, F, F, T, T, F, F, T, T, T, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, T, F },
/* y */{ F, F, T, T, T, T, T, T, T, T, T, F, F, T, T, F, F, T, T, T, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, T, F },
/* m */{ F, F, T, T, T, T, T, T, T, T, T, F, F, T, T, F, F, T, T, T, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, T, F },
/* d */{ F, F, T, T, T, T, T, T, T, T, T, F, F, T, T, F, F, F, F, F, T, T, T, T, T, T, T, T, T, T, F, F, F, F, F, F, T, F },
/* h*/ { F, F, T, T, T, T, T, T, T, T, T, F, F, T, T, F, F, F, F, F, T, T, T, T, T, T, T, T, T, T, F, F, F, F, F, F, T, F },
/* dh*/{ F, F, F, F, F, F, F, F, F, F, F, F, F, T, T, F, F, F, F, F, T, T, T, T, T, T, T, T, T, T, F, F, F, F, F, F, T, F },
/* m */{ F, F, T, T, T, T, T, T, T, T, T, F, F, T, T, F, F, F, F, F, T, T, T, T, T, T, T, T, T, T, F, F, F, F, F, F, T, F },
/* dm*/{ F, F, F, F, F, F, F, F, F, F, F, F, F, T, T, F, F, F, F, F, T, T, T, T, T, T, T, T, T, T, F, F, F, F, F, F, T, F },
/* hm*/{ F, F, F, F, F, F, F, F, F, F, F, F, F, T, T, F, F, F, F, F, T, T, T, T, T, T, T, T, T, T, F, F, F, F, F, F, T, F },
/* s */{ F, F, T, T, T, T, T, T, T, T, T, F, F, T, T, F, F, F, F, F, T, T, T, T, T, T, T, T, T, T, F, F, F, F, F, F, T, F },
/* ds*/{ F, F, F, F, F, F, F, F, F, F, F, F, F, T, T, F, F, F, F, F, T, T, T, T, T, T, T, T, T, T, F, F, F, F, F, F, T, F },
/* hs*/{ F, F, F, F, F, F, F, F, F, F, F, F, F, T, T, F, F, F, F, F, T, T, T, T, T, T, T, T, T, T, F, F, F, F, F, F, T, F },
/* ms*/{ F, F, F, F, F, F, F, F, F, F, F, F, F, T, T, F, F, F, F, F, T, T, T, T, T, T, T, T, T, T, F, F, F, F, F, F, T, F },
/* t */{ F, F, F, F, F, F, F, F, F, F, F, F, F, T, T, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, T, T, F, F, F, F, T, F },
/* ts*/{ F, F, T, T, T, T, T, T, T, T, T, T, T, T, T, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, T, T, T, F, F, F, T, F },
/* dt*/{ F, F, F, F, F, F, F, F, F, F, F, F, F, T, T, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, T, T, F, F, F, T, F },
/*row*/{ F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, T, F, F, T, F },
/* a */{ F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, T, F, T, F },
/* m */{ F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, T, T, F },
/* V */{ F, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T },
/* U */{ F, F, F, F, F, F, F, F, F, F, F, F, F, T, T, T, T, F, F, F, F, F, F, F, F, F, F, F, F, F, N, N, N, F, F, F, T, T },
        };

        Assert.assertEquals(types.length, legal.length);
        Assert.assertEquals(types.length, legal[0].length);
        StringBuilder program = new StringBuilder();
        program.append("CREATE VIEW V AS SELECT ");
        boolean first = true;
        for (int i = 0; i < types.length; i++) {
            String type = types[i];
            String value = values[i];
            if (legal[i][i] == CanConvert.F)
                continue;
            if (!first)
                program.append(", ");
            first = false;
            program.append("CAST(").append(value).append(" AS ").append(type).append(")\n");
        }
        program.append(";\n");

        for (int i = 0; i < types.length; i++) {
            String value = values[i];
            String from = types[i];
            if (!Linq.any(legal[i], p -> p == T)) continue;
            program.append("CREATE VIEW V").append(i).append(" AS SELECT ");

            first = true;
            for (int j = 0; j < types.length; j++) {
                String to = types[j];
                CanConvert ok = legal[i][j];
                if (ok == CanConvert.F) {
                    if (!value.equals("NULL") && !from.equals("NULL") && !to.equals("NULL")) {
                        String statement = "CREATE VIEW V AS SELECT CAST(CAST(" + value + " AS " + from + ") AS " + to + ")";
                        this.statementsFailingInCompilation(statement, "Cast function cannot convert");
                    }
                    continue;
                }
                if (ok == CanConvert.N) continue;
                if (!first)
                    program.append(", ");
                first = false;
                program.append("CAST(");
                if (value.equals("NULL"))
                    // Special case
                    program.append(value);
                else
                    program.append("CAST(").append(value).append(" AS ").append(from).append(")");
                program.append(" AS ").append(to).append(")");
                program.append("\n");
            }
            program.append(";\n");
        }
        this.compileRustTestCase(program.toString());
    }
}
