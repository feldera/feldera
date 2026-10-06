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

import org.dbsp.sqlCompiler.compiler.CompilerOptions;
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
import java.util.ArrayList;
import java.util.List;

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
        return new Change("T", new DBSPZSetExpression(new DBSPTupleExpression(
                new DBSPI32Literal(10),
                new DBSPDoubleLiteral(12.0),
                new DBSPStringLiteral("100100"),
                DBSPLiteral.none(tenTwo),
                new DBSPDecimalLiteral(tenFour, new BigDecimal(100103)))));
    }

    public void testQuery(String query, DBSPZSetExpression expectedOutput) {
        query = "CREATE VIEW V AS " + query + ";";
        CompilerCircuitStream ccs = this.getCCS(query);
        InputOutputChange change = new InputOutputChange(this.createInput(), new Change("V", expectedOutput));
        ccs.addChange(change);
    }

    @Test
    public void testTinyInt() {
        this.runtimeConstantFail("SELECT CAST(256 AS TINYINT)", "number too large to fit in target type");
    }

    @Test
    public void castFail() {
        this.runtimeConstantFail("SELECT CAST('blah' AS DECIMAL)",
                "While converting 'blah' to DECIMAL: parse error");
    }

    @Test
    public void castFailPosition() {
        // use bround to inhibit compile-time optimization;
        // line numbers include the 2 lines of DDL from prepareInputs
        this.runtimeConstantFail("""
                        SELECT
                            CAST(bround(100000, 0)
                            AS DECIMAL
                               (3,2))""",
                "line 4 column 5: Cannot represent 100000 as DECIMAL(3, 2): " +
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
                "Error converting 100103123 to DECIMAL(10, 4): Value out of range",
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
        this.qst("SELECT CAST(100 AS UNSIGNED) - 10;" +
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
        this.qst("""
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
        this.qst("""
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
        this.qst("""
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
        this.qst("""
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
        this.qst("""
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
        this.qst("""
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
                "CAST cannot be used to convert BINARY(1) to TIME");
    }

    @Test
    public void testIntervalCast() {
        this.getCCS("CREATE VIEW V AS SELECT " +
                "CAST(CAST('3:4' AS INTERVAL MINUTES TO SECONDS) AS INTERVAL HOURS TO MINUTES)," +
                "CAST(CAST('3:4' AS INTERVAL MINUTES TO SECONDS) AS INTERVAL HOURS);");
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
                "TIMESTAMP WITH TIME ZONE",
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
                "'2000-01-01 10:00:00 America/New_York'", // TIMESTAMP WITH TIME ZONE
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

        // Rows and columns match the array of types above.
        final CanConvert[][] legal = {
//             <--integers----------->                      <-Long-> <---short------------------->
// To:   N, B, I8,16,32,64,U8,U6,U3,U6,De,r, d, c, v, b, vb,ym,y, m, d, h, dh,m,dm,hm, s, ds,hs,ms,t, ts,tz,dt,ro,a, m, V, U
/*From                                                                                                                       */
/* N */{ F, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, F },
/* B */{ F, T, F, F, F, F, F, F, F, F, F, F, F, T, T, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, T, F },
/* I8*/{ F, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, F, T, T, T, T, F, T, F, F, T, F, F, F, F, T, T, F, F, F, F, T, F },
/*I16*/{ F, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, F, T, T, T, T, F, T, F, F, T, F, F, F, F, T, T, F, F, F, F, T, F },
/*I32*/{ F, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, F, T, T, T, T, F, T, F, F, T, F, F, F, F, T, T, F, F, F, F, T, F },
/*I64*/{ F, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, F, T, T, T, T, F, T, F, F, T, F, F, F, F, T, T, F, F, F, F, T, F },
/* U8*/{ F, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, F, T, T, T, T, F, T, F, F, T, F, F, F, F, T, T, F, F, F, F, T, F },
/*U16*/{ F, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, F, T, T, T, T, F, T, F, F, T, F, F, F, F, T, T, F, F, F, F, T, F },
/*U32*/{ F, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, F, T, T, T, T, F, T, F, F, T, F, F, F, F, T, T, F, F, F, F, T, F },
/*U64*/{ F, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, F, T, T, T, T, F, T, F, F, T, F, F, F, F, T, T, F, F, F, F, T, F },
/*Dec*/{ F, T, T, T, T, T, T, T, T, T, T, T, T, T, T, F, F, F, T, T, T, T, F, T, F, F, T, F, F, F, F, T, T, F, F, F, F, T, F },
/* r */{ F, T, T, T, T, T, T, T, T, T, T, T, T, T, T, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, T, T, F, F, F, F, T, F },
/* d */{ F, T, T, T, T, T, T, T, T, T, T, T, T, T, T, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, T, T, F, F, F, F, T, F },
/*chr*/{ F, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, F, F, F, T, T },
/* v */{ F, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, F, F, F, T, T },
/* b */{ F, F, F, F, F, F, F, F, F, F, F, F, F, T, T, T, T, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, T, T },
/*vb */{ F, F, F, F, F, F, F, F, F, F, F, F, F, T, T, T, T, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, T, T },
/*ym */{ F, F, F, F, F, F, F, F, F, F, F, F, F, T, T, F, F, T, T, T, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, T, F },
/* y */{ F, F, T, T, T, T, T, T, T, T, T, F, F, T, T, F, F, T, T, T, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, T, F },
/* m */{ F, F, T, T, T, T, T, T, T, T, T, F, F, T, T, F, F, T, T, T, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, T, F },
/* d */{ F, F, T, T, T, T, T, T, T, T, T, F, F, T, T, F, F, F, F, F, T, T, T, T, T, T, T, T, T, T, F, F, F, F, F, F, F, T, F },
/* h*/ { F, F, T, T, T, T, T, T, T, T, T, F, F, T, T, F, F, F, F, F, T, T, T, T, T, T, T, T, T, T, F, F, F, F, F, F, F, T, F },
/* dh*/{ F, F, F, F, F, F, F, F, F, F, F, F, F, T, T, F, F, F, F, F, T, T, T, T, T, T, T, T, T, T, F, F, F, F, F, F, F, T, F },
/* m */{ F, F, T, T, T, T, T, T, T, T, T, F, F, T, T, F, F, F, F, F, T, T, T, T, T, T, T, T, T, T, F, F, F, F, F, F, F, T, F },
/* dm*/{ F, F, F, F, F, F, F, F, F, F, F, F, F, T, T, F, F, F, F, F, T, T, T, T, T, T, T, T, T, T, F, F, F, F, F, F, F, T, F },
/* hm*/{ F, F, F, F, F, F, F, F, F, F, F, F, F, T, T, F, F, F, F, F, T, T, T, T, T, T, T, T, T, T, F, F, F, F, F, F, F, T, F },
/* s */{ F, F, T, T, T, T, T, T, T, T, T, F, F, T, T, F, F, F, F, F, T, T, T, T, T, T, T, T, T, T, F, F, F, F, F, F, F, T, F },
/* ds*/{ F, F, F, F, F, F, F, F, F, F, F, F, F, T, T, F, F, F, F, F, T, T, T, T, T, T, T, T, T, T, F, F, F, F, F, F, F, T, F },
/* hs*/{ F, F, F, F, F, F, F, F, F, F, F, F, F, T, T, F, F, F, F, F, T, T, T, T, T, T, T, T, T, T, F, F, F, F, F, F, F, T, F },
/* ms*/{ F, F, F, F, F, F, F, F, F, F, F, F, F, T, T, F, F, F, F, F, T, T, T, T, T, T, T, T, T, T, F, F, F, F, F, F, F, T, F },
/* t */{ F, F, F, F, F, F, F, F, F, F, F, F, F, T, T, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, T, T, T, F, F, F, F, T, F },
/* ts*/{ F, F, T, T, T, T, T, T, T, T, T, T, T, T, T, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, T, T, T, T, F, F, F, T, F },
/* tz*/{ F, F, T, T, T, T, T, T, T, T, T, T, T, T, T, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, T, T, T, T, F, F, F, T, F },
/* dt*/{ F, F, F, F, F, F, F, F, F, F, F, F, F, T, T, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, T, T, T, F, F, F, T, F },
/*row*/{ F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, T, F, F, T, F },
/* a */{ F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, T, F, T, F },
/* m */{ F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, T, T, F },
/* V */{ F, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T, T },
/* U */{ F, F, F, F, F, F, F, F, F, F, F, F, F, T, T, T, T, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, T, T },
        };

        Assert.assertEquals(types.length, legal.length);
        Assert.assertEquals(types.length, legal[0].length);
        StringBuilder program = new StringBuilder();
        boolean first = true;
        program.append("CREATE LOCAL VIEW T AS SELECT ");
        for (int i = 0; i < types.length; i++) {
            if (!first)
                program.append(", ");
            first = false;
            if (i > 0)
                program.append("CAST(");
            program.append(values[i]);
            if (i > 0)
                program.append(" AS ").append(types[i]).append(")");
            program.append(" AS col").append(i).append("\n");
        }
        program.append(";\n");

        first = true;
        program.append("CREATE VIEW V AS SELECT ");
        for (int i = 0; i < types.length; i++) {
            String type = types[i];
            String value = "col" + i;
            if (legal[i][i] == CanConvert.F)
                continue;
            if (!first)
                program.append(", ");
            first = false;
            program.append("CAST(").append(value).append(" AS ").append(type).append(")\n");
        }
        program.append(" FROM T;\n");

        for (int i = 0; i < types.length; i++) {
            String value = values[i];
            String coli = "col" + i;
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
                        // Calcite and our extra validator produce different error messages,
                        // but they both include the string 'convert'
                        this.statementsFailingInCompilation(statement, "convert");
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
                    program.append("CAST(").append(coli).append(" AS ").append(from).append(")");
                program.append(" AS ").append(to).append(")");
                program.append("\n");
            }
            program.append(" FROM T;\n");
        }
        // Disable Calcite optimizations so it doesn't do constant-folding
        // Despite this, this program still does not generate all possible cast com
        // binations.
        CompilerOptions options = this.testOptions();
        options.languageOptions.optimizationLevel = 0;
        DBSPCompiler compiler = new DBSPCompiler(options);
        compiler.submitStatementsForCompilation(program.toString());
        this.getCCS(compiler);
    }

    @Test
    public void issue5894() {
        this.qst("""
                SELECT INTERVAL '1' YEAR;
                 r
                ---
                 1 year
                (1 row)
                
                SELECT CAST('1' AS INTERVAL YEAR);
                 r
                ---
                 1 year
                (1 row)
                
                SELECT INTERVAL 1 YEAR;
                 r
                ---
                 1 year
                (1 row)
                
                SELECT CAST(CAST('1' AS INTERVAL YEAR) AS TINYINT);
                 r
                ---
                 1
                (1 row)
                
                SELECT CAST(INTERVAL '1' YEAR AS TINYINT);
                 r
                ---
                 1
                (1 row)""");
    }

    @Test
    public void issue5895() {
        this.q("""
                SELECT CAST(INTERVAL '1000' MONTH AS INT);
                 r
                -----
                 1000""");
    }

    @Test
    public void issue6257() {
        // Calcite rounds by truncating, so we are tied to that behavior
        this.qst("""
                SELECT CAST(INTERVAL '10' DAY AS BIGINT);
                 r
                ---
                 10
                (1 row)
                
                SELECT SAFE_CAST(INTERVAL '1000' DAY AS TINYINT);
                 r
                ---
                NULL
                (1 row)
                
                SELECT CAST(INTERVAL '10.6' SECONDS AS INT);
                 r
                ---
                 10
                (1 row)
                
                SELECT CAST(INTERVAL '10.135' SECONDS AS DECIMAL(10, 2));
                 r
                ---
                 10.13
                (1 row)
                
                SELECT CAST(INTERVAL '-10.135' SECONDS AS DECIMAL(10, 2));
                 r
                ---
                 -10.13
                (1 row)
                
                SELECT SAFE_CAST(INTERVAL '1000.123' SECONDS AS DECIMAL(2, 2));
                 r
                ---
                NULL
                (1 row)""");
        this.statementsFailingInCompilation("CREATE VIEW V AS SELECT CAST(INTERVAL '10' MONTHS AS DOUBLE)",
                "Cast function cannot convert value of type INTERVAL MONTH NOT NULL to type DOUBLE NOT NULL");
        this.statementsFailingInCompilation("CREATE VIEW V AS SELECT CAST(INTERVAL '10' SECONDS AS REAL)",
                "Cast function cannot convert value of type INTERVAL SECOND NOT NULL to type REAL NOT NULL");
    }

    /** Casts of the values to each target type.
     *
     * @param source      Type of the values.
     * @param castLiteral If true, a value is a literal of a different type, cast to the source type.
     *                    If false, a value is a literal of the source type, or a string literal
     *                    when the source type is VARCHAR.
     * @param values      Values that the casts convert.
     * @param targets     Target types of the casts. */
    record FoldedCasts(String source, boolean castLiteral, List<String> values, List<String> targets) {}

    /** The compiler folds a cast of a literal into a constant, using Calcite's evaluation.
     * Each folded cast must produce the same value as the runtime cast of the same value
     * stored in a table.
     *
     * <p>For example, {@code new FoldedCasts("TINYINT", true, List.of("0", "1"), List.of("BOOLEAN"))}
     * as the second entry generates:
     * <pre>
     * CREATE TABLE S1 (id INT NOT NULL, v TINYINT);
     *
     * SELECT 'S1: TINYINT' AS source, id,
     *        'BOOLEAN=' || COALESCE(CAST(f0 AS VARCHAR), 'NULL') || '; ' AS folded,
     *        'BOOLEAN=' || COALESCE(CAST(r0 AS VARCHAR), 'NULL') || '; ' AS runtime
     * FROM (SELECT id,
     *              CASE id WHEN 0 THEN CAST(CAST(0 AS TINYINT) AS BOOLEAN)
     *                      WHEN 1 THEN CAST(CAST(1 AS TINYINT) AS BOOLEAN) END AS f0,
     *              CAST(v AS BOOLEAN) AS r0
     *       FROM S1)
     * WHERE f0 IS DISTINCT FROM r0
     * </pre>
     * and these insertions into the table:
     * <pre>
     * INSERT INTO S1 VALUES(0, 0);
     * INSERT INTO S1 VALUES(1, 1);
     * </pre>
     * The compiler folds the casts in the CASE branches into constants; the cast of column
     * v runs at runtime. */
    @Test
    public void foldedCastsMatchRuntime() {
        List<String> integers = List.of("TINYINT", "SMALLINT", "INTEGER", "BIGINT");
        List<FoldedCasts> casts = List.of(
                new FoldedCasts("BOOLEAN", false, List.of("TRUE", "FALSE"),
                        List.of("CHAR(5)", "VARCHAR")),
                new FoldedCasts("TINYINT", true, List.of("0", "1", "-128", "127"),
                        List.of("SMALLINT", "INTEGER", "BIGINT", "DECIMAL(5, 1)", "REAL", "DOUBLE",
                                "BOOLEAN", "CHAR(4)", "VARCHAR", "VARBINARY")),
                new FoldedCasts("SMALLINT", true, List.of("0", "-32768", "32767"),
                        List.of("INTEGER", "BIGINT", "DECIMAL(7, 1)", "REAL", "DOUBLE",
                                "BOOLEAN", "VARCHAR", "VARBINARY")),
                new FoldedCasts("SMALLINT", true, List.of("-100", "100"), List.of("TINYINT")),
                new FoldedCasts("INTEGER", true, List.of("0", "-2147483648", "2147483647", "123456789"),
                        List.of("BIGINT", "DECIMAL(12, 2)", "REAL", "DOUBLE", "BOOLEAN", "VARCHAR", "VARBINARY")),
                new FoldedCasts("INTEGER", true, List.of("-100", "100"), List.of("TINYINT", "SMALLINT")),
                new FoldedCasts("BIGINT", true,
                        List.of("0", "-9223372036854775808", "9223372036854775807", "9007199254740993"),
                        List.of("DECIMAL(20, 0)", "REAL", "DOUBLE", "BOOLEAN", "VARCHAR", "VARBINARY")),
                new FoldedCasts("BIGINT", true, List.of("-100", "100"),
                        List.of("TINYINT", "SMALLINT", "INTEGER")),
                new FoldedCasts("DECIMAL(10, 3)", true,
                        List.of("0", "1.5", "-1.5", "2.5", "1.999", "-99.999", "0.001"),
                        List.of("TINYINT", "SMALLINT", "INTEGER", "BIGINT", "DECIMAL(5, 1)",
                                "DECIMAL(12, 5)", "REAL", "DOUBLE", "BOOLEAN")),
                new FoldedCasts("DOUBLE", true,
                        List.of("0e0", "-1.5e0", "2.5e0", "0.1e0", "1e-7", "123.456e0", "-99.99e0"),
                        List.of("TINYINT", "SMALLINT", "INTEGER", "BIGINT", "REAL", "BOOLEAN")),
                new FoldedCasts("DOUBLE", true, List.of("1e18", "-9.2e18"), List.of("BIGINT")),
                new FoldedCasts("REAL", true, List.of("0e0", "-1.5e0", "0.1e0", "99.5e0", "2.5e0"),
                        List.of("TINYINT", "INTEGER", "BIGINT", "BOOLEAN")),
                new FoldedCasts("VARCHAR", false, List.of("'true'", "'FALSE'", "'TrUe'"),
                        List.of("BOOLEAN")),
                new FoldedCasts("VARCHAR", false, List.of("'12'", "'-7'", "'+5'", "'0'", "' 42 '"),
                        integers),
                new FoldedCasts("VARCHAR", false,
                        List.of("'1.5'", "'-2e3'", "'0.1'", "'1e-7'", "'  3.25  '"),
                        List.of("REAL", "DOUBLE")),
                new FoldedCasts("VARCHAR", false, List.of("'2024-02-29'", "'0001-01-01'", "'9999-12-31'"),
                        List.of("DATE")),
                new FoldedCasts("VARCHAR", false,
                        List.of("'2000-01-01 00:00:00'", "'2024-02-29 10:20:30.5'", "'0001-01-01 00:00:00'",
                                "'9999-12-31 23:59:59.999999'"),
                        List.of("TIMESTAMP")),
                new FoldedCasts("VARCHAR", false, List.of("'00:00:00'", "'23:59:59.123456'", "'10:20:30.123456789'"),
                        List.of("TIME")),
                new FoldedCasts("VARCHAR", false,
                        List.of("'123e4567-e89b-12d3-a456-426614174000'", "'ABCDEF01-2345-6789-ABCD-EF0123456789'"),
                        List.of("UUID")),
                new FoldedCasts("VARCHAR", false, List.of("'ab'", "'abcdef'", "'x'", "''"),
                        List.of("CHAR(4)", "VARCHAR(3)", "VARCHAR", "VARBINARY")),
                new FoldedCasts("VARBINARY", true, List.of("x'0102'", "x''", "x'ff00'"),
                        List.of("VARCHAR", "BINARY(2)")),
                new FoldedCasts("VARBINARY", true, List.of("x'123e4567e89b12d3a456426614174000'"),
                        List.of("UUID")),
                new FoldedCasts("DATE", false, List.of("DATE '2024-02-29'", "DATE '0001-01-01'", "DATE '9999-12-31'"),
                        List.of("VARCHAR", "CHAR(10)")),
                new FoldedCasts("UUID", true, List.of("'123e4567-e89b-12d3-a456-426614174000'"),
                        List.of("VARCHAR", "CHAR(36)", "VARBINARY")));
        this.checkFoldedCasts(casts);
    }

    /** Compile a program that compares each folded cast in {@code casts} with the runtime
     * cast of the same value, and check that no comparison differs. */
    void checkFoldedCasts(List<FoldedCasts> casts) {
        StringBuilder program = new StringBuilder();
        StringBuilder inserts = new StringBuilder();
        List<String> mismatches = new ArrayList<>();
        for (int t = 0; t < casts.size(); t++) {
            FoldedCasts cast = casts.get(t);
            String table = "S" + t;
            program.append("CREATE TABLE ").append(table)
                    .append(" (id INT NOT NULL, v ").append(cast.source()).append(");\n");
            for (int i = 0; i < cast.values().size(); i++)
                inserts.append("INSERT INTO ").append(table)
                        .append(" VALUES(").append(i).append(", ").append(cast.values().get(i)).append(");\n");
            List<String> columns = new ArrayList<>();
            List<String> different = new ArrayList<>();
            List<String> folded = new ArrayList<>();
            List<String> runtime = new ArrayList<>();
            for (int j = 0; j < cast.targets().size(); j++) {
                String target = cast.targets().get(j);
                // Each branch is a cast of a constant, which the compiler folds
                StringBuilder foldedCast = new StringBuilder("CASE id");
                for (int i = 0; i < cast.values().size(); i++) {
                    String value = cast.values().get(i);
                    String literal = cast.castLiteral() ? "CAST(" + value + " AS " + cast.source() + ")" : value;
                    foldedCast.append(" WHEN ").append(i)
                            .append(" THEN CAST(").append(literal).append(" AS ").append(target).append(")");
                }
                foldedCast.append(" END");
                columns.add(foldedCast + " AS f" + j);
                columns.add("CAST(v AS " + target + ") AS r" + j);
                different.add("f" + j + " IS DISTINCT FROM r" + j);
                folded.add(showValue(target, "f" + j));
                runtime.add(showValue(target, "r" + j));
            }
            mismatches.add("SELECT '" + table + ": " + cast.source() + "' AS source, id, " +
                    String.join(" || ", folded) + " AS folded, " +
                    String.join(" || ", runtime) + " AS runtime FROM (" +
                    "SELECT id, " + String.join(", ", columns) + " FROM " + table + ") WHERE " +
                    String.join(" OR ", different));
        }
        program.append("CREATE VIEW MISMATCHES AS\n")
                .append(String.join("\nUNION ALL\n", mismatches))
                .append(";\n");
        CompilerCircuitStream ccs = this.getCCS(program.toString());
        ccs.step(inserts.toString(), " source | id | folded | runtime | weight\n---");
    }

    /** A string with the target type and the value of a column, for the output of a mismatch */
    static String showValue(String target, String column) {
        return "'" + target + "=' || COALESCE(CAST(" + column + " AS VARCHAR), 'NULL') || '; '";
    }
}
