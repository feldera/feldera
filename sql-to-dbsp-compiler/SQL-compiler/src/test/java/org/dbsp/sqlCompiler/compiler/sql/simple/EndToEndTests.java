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
 *
 *
 */

package org.dbsp.sqlCompiler.compiler.sql.simple;

import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.sql.tools.BaseSQLTests;
import org.dbsp.sqlCompiler.compiler.sql.tools.Change;
import org.dbsp.sqlCompiler.compiler.sql.tools.InputOutputChange;
import org.dbsp.sqlCompiler.compiler.sql.tools.InputOutputChangeStream;
import org.dbsp.sqlCompiler.ir.expression.DBSPArrayExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPGeoPointConstructor;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPBinaryLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPBoolLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPDecimalLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPDoubleLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPI32Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPI64Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPNullLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPStringLiteral;
import org.dbsp.sqlCompiler.ir.expression.DBSPZSetExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeCode;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeBool;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeDecimal;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeDouble;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeGeoPoint;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeInteger;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeString;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.math.BigDecimal;

/**
 * Test end-to-end by compiling some DDL statements and view
 * queries by compiling them to rust and executing them
 * by inserting data in the input tables and reading data
 * from the declared views. */
public class EndToEndTests extends BaseSQLTests {
    public static final String E2E_TABLE = """
            CREATE TABLE T (
            COL1 INT NOT NULL
            , COL2 DOUBLE PRECISION NOT NULL
            , COL3 BOOLEAN NOT NULL
            , COL4 VARCHAR NOT NULL
            , COL5 INT
            , COL6 DECIMAL(6, 2))""";

    public static final DBSPTypeDecimal D62 = new DBSPTypeDecimal(CalciteObject.EMPTY, 6, 2, true);
    public static final DBSPTupleExpression E0 = new DBSPTupleExpression(
            new DBSPI32Literal(10),
            new DBSPDoubleLiteral(12.0),
            new DBSPBoolLiteral(true),
            new DBSPStringLiteral("Hi"),
            DBSPLiteral.none(new DBSPTypeInteger(CalciteObject.EMPTY, 32, true,true)),
            DBSPLiteral.none(D62)
    );
    public static final DBSPTupleExpression E1 = new DBSPTupleExpression(
            new DBSPI32Literal(10),
            new DBSPDoubleLiteral(1.0),
            new DBSPBoolLiteral(false),
            new DBSPStringLiteral("Hi"),
            new DBSPI32Literal(1, true),
            new DBSPDecimalLiteral(D62, BigDecimal.ZERO)
    );

    /**
     * Returns the table T containing:
     * -------------------------------------------
     * | 10 | 12e0 | true  | Hi | NULL    | NULL |
     * | 10 |  1e0 | false | Hi | Some[1] |  0.0 |
     * -------------------------------------------
     INSERT INTO T VALUES (10, 12e0, true, 'Hi', NULL, NULL);
     INSERT INTO T VALUES (10, 1e0, false, 'Hi', 1, 0e0);
     */
    static final Change INPUT = new Change(new DBSPZSetExpression(E0, E1));

    public DBSPCompiler compileQuery(String query) {
        DBSPCompiler compiler = this.testCompiler();
        compiler.submitStatementForCompilation(E2E_TABLE);
        compiler.submitStatementForCompilation(query);
        return compiler;
    }

    void testQueryBase(String query, InputOutputChangeStream streams) {
        query = "CREATE VIEW V AS " + query;
        DBSPCompiler compiler = this.compileQuery(query);
        this.getCCS(compiler, streams);
    }

    public void invokeTestQueryBase(String query, InputOutputChangeStream streams) {
        this.testQueryBase(query, streams);
    }

    /** Use this function to test queries whose result does not change when table T is modified. */
    void testConstantOutput(String query, DBSPZSetExpression output) {
        this.testQueryBase(query, new InputOutputChange(INPUT, new Change(output)).toStream());
    }

    /** Use this function to test queries that compute aggregates */
    void testAggregate(String query,
                       DBSPZSetExpression firstOutput,
                       DBSPZSetExpression outputForEmptyInput) {
        this.testQueryBase(query, new InputOutputChange(INPUT, new Change(firstOutput)).toStream());
    }

    static final DBSPZSetExpression z0 = new DBSPZSetExpression(E0);
    static final DBSPZSetExpression z1 = new DBSPZSetExpression(E1);
    static final DBSPZSetExpression empty = DBSPZSetExpression.emptyWithElementType(z0.getElementType());

    public void testQuery(String query, DBSPZSetExpression expectedOutput) {
        this.testQuery(query, new Change(expectedOutput));
    }

    public void testQuery(String query, Change expectedOutput) {
        this.testQueryBase(query, new InputOutputChange(INPUT, expectedOutput).toStream());
    }

    @Test
    public void testNullableCompare() {
        String query = "SELECT T.COL5 > T.COL1 FROM T";
        this.testQuery(query, new DBSPZSetExpression(
                new DBSPTupleExpression(new DBSPBoolLiteral()),
                new DBSPTupleExpression(new DBSPBoolLiteral(false, true))));
    }

    @Test
    public void testAbs() {
        String query = "SELECT ABS(T.COL2) FROM T";
        Assert.assertNotNull(E0.fields);
        Assert.assertNotNull(E1.fields);
        this.testQuery(query, new DBSPZSetExpression(
                new DBSPTupleExpression(E0.fields[1]),
                new DBSPTupleExpression(E1.fields[1])));
    }

    @Test
    public void testNullableCastCompare() {
        String query = "SELECT T.COL5 > T.COL2 FROM T";
        this.testQuery(query, new DBSPZSetExpression(
                new DBSPTupleExpression(new DBSPBoolLiteral()),
                new DBSPTupleExpression(new DBSPBoolLiteral(false, true))));
    }

    @Test
    public void testNullableCastCompare2() {
        String query = "SELECT T.COL5 > T.COL6 FROM T";
        this.testQuery(query, new DBSPZSetExpression(
                new DBSPTupleExpression(new DBSPBoolLiteral()),
                new DBSPTupleExpression(new DBSPBoolLiteral(true, true))));
    }

    @Test
    public void testNullableCompare2() {
        String query = "SELECT T.COL5 > 10 FROM T";
        this.testQuery(query, new DBSPZSetExpression(
                new DBSPTupleExpression(new DBSPBoolLiteral()),
                new DBSPTupleExpression(new DBSPBoolLiteral(false, true))));
    }

    @Test
    public void testBoolean() {
        String query = "SELECT T.COL3 OR T.COL1 > 10 FROM T";
        this.testQuery(query, new DBSPZSetExpression(
                new DBSPTupleExpression(new DBSPBoolLiteral(true)),
                new DBSPTupleExpression(new DBSPBoolLiteral(false))));
    }

    @Test
    public void testNullableBoolean2() {
        String query = "SELECT T.COL5 > 10 AND T.COL3 FROM T";
        this.testQuery(query, new DBSPZSetExpression(
                new DBSPTupleExpression(new DBSPBoolLiteral()),
                new DBSPTupleExpression(new DBSPBoolLiteral(false, true))));
    }

    @Test
    public void testNullableBoolean3() {
        String query = "SELECT T.COL3 AND T.COL5 > 10 FROM T";
        this.testQuery(query, new DBSPZSetExpression(
                new DBSPTupleExpression(new DBSPBoolLiteral()),
                new DBSPTupleExpression(new DBSPBoolLiteral(false, true))));
    }

    @Test
    public void overDecimalTest() {
        String query = "SELECT T.COL1, COUNT(*) OVER (ORDER BY T.COL6 RANGE UNBOUNDED PRECEDING) FROM T";
        this.testQuery(query, new DBSPZSetExpression(
                new DBSPTupleExpression(new DBSPI32Literal(10), new DBSPI64Literal(1)),
                new DBSPTupleExpression(new DBSPI32Literal(10), new DBSPI64Literal(2))));
    }

    @Test
    public void overTest() {
        String query = "SELECT T.COL1, COUNT(*) OVER (ORDER BY T.COL1 RANGE UNBOUNDED PRECEDING) FROM T";
        DBSPExpression t = new DBSPTupleExpression(new DBSPI32Literal(10), new DBSPI64Literal(2));
        this.testQuery(query, new DBSPZSetExpression(t, t));
    }

    @Test
    public void overSumTest() {
        String query = "SELECT T.COL1, SUM(T.COL2) OVER (ORDER BY T.COL1 RANGE UNBOUNDED PRECEDING) FROM T";
        DBSPExpression t = new DBSPTupleExpression(new DBSPI32Literal(10), new DBSPDoubleLiteral(13.0));
        this.testQuery(query, new DBSPZSetExpression(t, t));
    }

    @Test
    public void overAvgTest() {
        String query = "SELECT T.COL1, AVG(T.COL6) OVER (ORDER BY T.COL1 RANGE UNBOUNDED PRECEDING) FROM T";
        DBSPExpression t = new DBSPTupleExpression(
                new DBSPI32Literal(10), new DBSPDecimalLiteral(0, true));
        this.testQuery(query, new DBSPZSetExpression(t, t));
    }

    @Test
    public void lagTest() {
        String query = "SELECT T.COL1, LAG(T.COL1) OVER (ORDER BY T.COL1) FROM T";
        this.testQuery(query, new DBSPZSetExpression(
                new DBSPTupleExpression(
                        new DBSPI32Literal(10),
                        DBSPLiteral.none(new DBSPTypeInteger(
                                CalciteObject.EMPTY, 32, true, true))),
                new DBSPTupleExpression(
                        new DBSPI32Literal(10),
                        new DBSPI32Literal(10, true))));
    }

    @Test
    public void testConcat() {
        String query = "SELECT T.COL4 || ' ' || T.COL4 FROM T";
        DBSPExpression lit = new DBSPTupleExpression(new DBSPStringLiteral("Hi Hi"));
        this.testQuery(query, new DBSPZSetExpression(lit, lit));
    }

    @Test
    public void testCast() {
        String query = "SELECT CAST(T.COL1 AS VARCHAR) FROM T";
        DBSPExpression lit = new DBSPTupleExpression(new DBSPStringLiteral("10"));
        this.testQuery(query, new DBSPZSetExpression(lit, lit));
    }

    @Test
    public void testArray() {
        String query = "SELECT ELEMENT(ARRAY [2])";
        DBSPZSetExpression result = new DBSPZSetExpression(new DBSPTupleExpression(new DBSPI32Literal(2, true)));
        this.testConstantOutput(query, result);
    }

    @Test
    public void testNull() {
        String query = "SELECT NULL";
        DBSPZSetExpression result = new DBSPZSetExpression(new DBSPTupleExpression(DBSPNullLiteral.INSTANCE));
        this.testConstantOutput(query, result);
    }

    @Test
    public void testArrayIndex() {
        String query = "SELECT (ARRAY [2])[1]";
        DBSPZSetExpression result = new DBSPZSetExpression(new DBSPTupleExpression(new DBSPI32Literal(2, true)));
        this.testConstantOutput(query, result);
    }

    @Test
    public void testArrayIndexOutOfBounds() {
        String query = "SELECT (ARRAY [2])[3]";
        DBSPZSetExpression result = new DBSPZSetExpression(new DBSPTupleExpression(DBSPLiteral.none(new DBSPTypeInteger(CalciteObject.EMPTY, 32, true,true))));
        this.testConstantOutput(query, result);
    }

    @Test
    public void testConcatNull() {
        String query = "SELECT T.COL4 || NULL FROM T";
        DBSPExpression lit = new DBSPTupleExpression(DBSPLiteral.none(DBSPTypeString.varchar(true)));
        this.testQuery(query, new DBSPZSetExpression(lit, lit));
    }

    @Test
    public void testConcatNull2() {
        String query = "SELECT CONCAT(T.COL4, NULL) FROM T";
        DBSPExpression lit = new DBSPTupleExpression(DBSPLiteral.none(DBSPTypeString.varchar(true)));
        this.testQuery(query, new DBSPZSetExpression(lit, lit));
    }

    @Test
    public void overTwiceTest() {
        DBSPExpression t = new DBSPTupleExpression(
                new DBSPI32Literal(10),
                new DBSPDoubleLiteral(13.0),
                new DBSPI64Literal(2));
        String query = """
                SELECT T.COL1,
                SUM(T.COL2) OVER (ORDER BY T.COL1 RANGE UNBOUNDED PRECEDING),
                COUNT(*) OVER (ORDER BY T.COL1 RANGE UNBOUNDED PRECEDING) FROM T""";
        this.testQuery(query, new DBSPZSetExpression(t, t));
    }

    @Test
    public void overConstantWindowTest() {
        DBSPExpression t = new DBSPTupleExpression(
                new DBSPI32Literal(10),
                new DBSPI64Literal(0));
        String query = "SELECT T.COL1, " +
                "COUNT(*) OVER (ORDER BY T.COL1 RANGE BETWEEN 2 PRECEDING AND 1 PRECEDING) FROM T";
        this.testQuery(query, new DBSPZSetExpression(t, t));
    }

    @Test
    public void overTwiceDifferentTest() {
        DBSPExpression t = new DBSPTupleExpression(
                new DBSPI32Literal(10),
                new DBSPDoubleLiteral(13.0),
                new DBSPI64Literal(0));
        String query = """
                SELECT T.COL1,
                SUM(T.COL2) OVER (ORDER BY T.COL1 RANGE UNBOUNDED PRECEDING),
                COUNT(*) OVER (ORDER BY T.COL1 RANGE BETWEEN 2 PRECEDING AND 1 PRECEDING) FROM T""";
        this.testQuery(query, new DBSPZSetExpression(t, t));
    }

    @Test
    public void correlatedAggregate() {
        String query = """
                SELECT Sum(r.COL1 * r.COL5) FROM T r
                WHERE
                0.5 * (SELECT Sum(r1.COL5) FROM T r1) =
                (SELECT Sum(r2.COL5) FROM T r2 WHERE r2.COL1 = r.COL1)""";
        this.testAggregate(query,
                new DBSPZSetExpression(
                        new DBSPTupleExpression(DBSPLiteral.none(
                                new DBSPTypeInteger(CalciteObject.EMPTY, 32, true,true)))),
                new DBSPZSetExpression(
                        new DBSPTupleExpression(DBSPLiteral.none(
                                new DBSPTypeInteger(CalciteObject.EMPTY, 32, true,true)))));

        // TODO
        //query = "SELECT Sum(b.price * b.volume) FROM bids b\n" +
        //        "WHERE\n" +
        //        "0.75 * (SELECT Sum(b1.volume) FROM bids b1)\n" +
        //        "< (SELECT Sum(b2.volume) FROM bids b2\n" +
        //        "WHERE b2.price <= b.price)";
    }

    @Test
    public void someTest() {
        String query = "SELECT SOME(T.COL3) FROM T";
        this.testAggregate(query,
                new DBSPZSetExpression(
                        new DBSPTupleExpression(new DBSPBoolLiteral(true, true))),
                new DBSPZSetExpression(
                        new DBSPTupleExpression(new DBSPBoolLiteral())));
    }

    @Test
    public void orTest() {
        String query = "SELECT LOGICAL_OR(T.COL3) FROM T";
        this.testAggregate(query,
                new DBSPZSetExpression(
                        new DBSPTupleExpression(new DBSPBoolLiteral(true, true))),
                new DBSPZSetExpression(
                        new DBSPTupleExpression(new DBSPBoolLiteral())));
    }

    @Test
    public void everyTest() {
        String query = "SELECT EVERY(T.COL3) FROM T";
        this.testAggregate(query,
                new DBSPZSetExpression(
                        new DBSPTupleExpression(new DBSPBoolLiteral(false, true))),
                new DBSPZSetExpression(
                        new DBSPTupleExpression(new DBSPBoolLiteral())));
    }

    @Test
    public void projectTest() {
        String query = "SELECT T.COL3 FROM T";
        this.testQuery(query,
                new DBSPZSetExpression(
                        new DBSPTupleExpression(new DBSPBoolLiteral(true)),
                        new DBSPTupleExpression(new DBSPBoolLiteral(false))));
    }

    @Test
    public void withTest() {
        String query = "WITH v AS (SELECT COL3 FROM T)\n" +
                "SELECT * FROM v";
        this.testQuery(query, new DBSPZSetExpression(
                new DBSPTupleExpression(new DBSPBoolLiteral(true)),
                new DBSPTupleExpression(new DBSPBoolLiteral(false))));
    }

    @Test
    public void testSumFilter() {
        String query = "SELECT T.COL3, " +
                "SUM(T.COL1) FILTER (WHERE T.COL1 > 20)\n" +
                "FROM T GROUP BY T.COL3";
        this.testQuery(query, new DBSPZSetExpression(
                new DBSPTupleExpression(
                        new DBSPBoolLiteral(true),
                        DBSPLiteral.none(new DBSPTypeInteger(CalciteObject.EMPTY, 32, true,true))
                ),
                new DBSPTupleExpression(new DBSPBoolLiteral(false),
                        DBSPLiteral.none(new DBSPTypeInteger(CalciteObject.EMPTY, 32, true,true))
                )));
    }

    @Test
    public void testCountFilter() {
        String query = "SELECT T.COL3, " +
                "COUNT(*) FILTER (WHERE T.COL1 > 20)\n" +
                "FROM T GROUP BY T.COL3";
        this.testQuery(query, new DBSPZSetExpression(
                new DBSPTupleExpression(
                        new DBSPBoolLiteral(true),
                        new DBSPI64Literal(0, false)
                ),
                new DBSPTupleExpression(new DBSPBoolLiteral(false),
                        new DBSPI64Literal(0, false)
                )));
    }

    @Test
    public void testSumCountFilter() {
        String query = """
                SELECT T.COL3, COUNT(*) FILTER (WHERE T.COL1 > 20),
                SUM(T.COL1) FILTER (WHERE T.COL1 > 20)
                FROM T GROUP BY T.COL3""";
        this.testQuery(query, new DBSPZSetExpression(
                new DBSPTupleExpression(
                        new DBSPBoolLiteral(true),
                        new DBSPI64Literal(0, false),
                        DBSPLiteral.none(new DBSPTypeInteger(CalciteObject.EMPTY, 32, true,true))
                ),
                new DBSPTupleExpression(new DBSPBoolLiteral(false),
                        new DBSPI64Literal(0, false),
                        DBSPLiteral.none(new DBSPTypeInteger(CalciteObject.EMPTY, 32, true,true))
                )));
    }

    @Test
    public void intersectTest() {
        String query = "SELECT * FROM T INTERSECT (SELECT * FROM T)";
        this.testQuery(query, INPUT);
    }

    @Test
    public void plusNullTest() {
        String query = "SELECT T.COL1 + T.COL5 FROM T";
        this.testQuery(query,
                new DBSPZSetExpression(
                        new DBSPTupleExpression(new DBSPI32Literal(11, true)),
                        new DBSPTupleExpression(DBSPLiteral.none(new DBSPTypeInteger(CalciteObject.EMPTY, 32, true,true)))));
    }

    @Test
    public void negateNullTest() {
        String query = "SELECT -T.COL5 FROM T";
        this.testQuery(query,
                new DBSPZSetExpression(
                        new DBSPTupleExpression(new DBSPI32Literal(-1, true)),
                        new DBSPTupleExpression(DBSPLiteral.none(new DBSPTypeInteger(CalciteObject.EMPTY, 32, true,true)))));
    }

    @Test
    public void testVariant() {
        String query = "SELECT CAST(CAST(T.COL5 as VARIANT) AS INTEGER) FROM T";
        DBSPI32Literal one = new DBSPI32Literal(1, true);
        this.testQuery(query,
                new DBSPZSetExpression(
                        new DBSPTupleExpression(one),
                        new DBSPTupleExpression(DBSPLiteral.none(one.getType()))));
    }

    @Test
    public void projectNullTest() {
        String query = "SELECT T.COL5 FROM T";
        DBSPI32Literal one = new DBSPI32Literal(1, true);
        this.testQuery(query,
                new DBSPZSetExpression(
                        new DBSPTupleExpression(one),
                        new DBSPTupleExpression(DBSPLiteral.none(one.getType()))));
    }

    @Test
    public void idTest() {
        String query = "SELECT * FROM T";
        this.testQuery(query, INPUT);
    }

    @Test
    public void unionTest() {
        String query = "(SELECT * FROM T) UNION (SELECT * FROM T)";
        this.testQuery(query, INPUT);
    }

    @Test
    public void unionAllTest() {
        String query = "(SELECT * FROM T) UNION ALL (SELECT * FROM T)";
        var set = INPUT.getSet(0).clone();
        set.append(set);
        Change doubleOutput = new Change(set);
        this.testQuery(query, doubleOutput);
    }

    @Test
    public void joinTest() {
        String query = "SELECT T1.COL3, T2.COL3 AS C3 FROM T AS T1 JOIN T AS T2 ON T1.COL1 = T2.COL1";
        this.testQuery(query, new DBSPZSetExpression(
                new DBSPTupleExpression(new DBSPBoolLiteral(false), new DBSPBoolLiteral(false)),
                new DBSPTupleExpression(new DBSPBoolLiteral(false), new DBSPBoolLiteral(true)),
                new DBSPTupleExpression(new DBSPBoolLiteral(true), new DBSPBoolLiteral(false)),
                new DBSPTupleExpression(new DBSPBoolLiteral(true), new DBSPBoolLiteral(true))));
    }

    @Test
    public void joinFPTest() {
        String query = "SELECT T1.COL3, T2.COL3 AS C3 FROM T AS T1 JOIN T AS T2 ON T1.COL2 = T2.COL6";
        this.testQuery(query, DBSPZSetExpression.emptyWithElementType(
                new DBSPTypeTuple(DBSPTypeBool.create(false), DBSPTypeBool.create(false))));
    }

    @Test
    public void joinNullableTest() {
        String query = "SELECT T1.COL3, T2.COL3 AS C3 FROM T AS T1 JOIN T AS T2 ON T1.COL1 = T2.COL5";
        this.testQuery(query, DBSPZSetExpression.emptyWithElementType(
                new DBSPTypeTuple(DBSPTypeBool.create(false), DBSPTypeBool.create(false))));
    }

    @Test
    public void zero() {
        String query = "SELECT 0";
        this.testConstantOutput(query, new DBSPZSetExpression(
                new DBSPTupleExpression(new DBSPI32Literal(0))));
    }

    @Test
    public void geoPointTest() {
        String query = "SELECT ST_POINT(0, 0)";
        this.testConstantOutput(query, new DBSPZSetExpression(
                new DBSPTupleExpression(
                        new DBSPGeoPointConstructor(CalciteObject.EMPTY,
                                new DBSPDoubleLiteral(0),
                                new DBSPDoubleLiteral(0),
                                DBSPTypeGeoPoint.INSTANCE).some())));
    }

    @Test
    public void geoDistanceTest() {
        String query = "SELECT ST_DISTANCE(ST_POINT(0, 0), ST_POINT(0,1))";
        this.testConstantOutput(query, new DBSPZSetExpression(
                new DBSPTupleExpression(new DBSPDoubleLiteral(1.0, true))));
    }

    @Test
    public void leftOuterJoinTest() {
        String query = "SELECT T1.COL3, T2.COL3 AS C3 FROM T AS T1 LEFT JOIN T AS T2 ON T1.COL1 = T2.COL5";
        this.testQuery(query, new DBSPZSetExpression(
                new DBSPTupleExpression(new DBSPBoolLiteral(false), new DBSPBoolLiteral()),
                new DBSPTupleExpression(new DBSPBoolLiteral(true), new DBSPBoolLiteral())
        ));
    }

    @Test
    public void rightOuterJoinTest() {
        String query = "SELECT T1.COL3, T2.COL3 AS C3 FROM T AS T1 RIGHT JOIN T AS T2 ON T1.COL1 = T2.COL5";
        this.testQuery(query, new DBSPZSetExpression(
                new DBSPTupleExpression(new DBSPBoolLiteral(), new DBSPBoolLiteral(false)),
                new DBSPTupleExpression(new DBSPBoolLiteral(), new DBSPBoolLiteral(true))
        ));
    }

    @Test
    public void fullOuterJoinTest() {
        String query = "SELECT T1.COL3, T2.COL3 AS C3 FROM T AS T1 FULL OUTER JOIN T AS T2 ON T1.COL1 = T2.COL5";
        this.testQuery(query, new DBSPZSetExpression(
                new DBSPTupleExpression(new DBSPBoolLiteral(false, true), new DBSPBoolLiteral()),
                new DBSPTupleExpression(new DBSPBoolLiteral(true, true), new DBSPBoolLiteral()),
                new DBSPTupleExpression(new DBSPBoolLiteral(), new DBSPBoolLiteral(false, true)),
                new DBSPTupleExpression(new DBSPBoolLiteral(), new DBSPBoolLiteral(true, true))
        ));
    }

    @Test
    public void emptyWhereTest() {
        String query = "SELECT * FROM T WHERE FALSE";
        this.testQuery(query, empty);
    }

    @Test
    public void whereTest() {
        String query = "SELECT T.COL1 FROM T WHERE COL3";
        this.testQuery(query, new DBSPZSetExpression(
                new DBSPTupleExpression(
                        new DBSPI32Literal(10))));
    }

    @Test
    public void whereImplicitCastTest() {
        String query = "SELECT * FROM T WHERE COL2 < COL1";
        this.testQuery(query, z1);
    }

    @Test
    public void whereExplicitCastTest() {
        String query = "SELECT * FROM T WHERE COL2 < CAST(COL1 AS DOUBLE)";
        this.testQuery(query, z1);
    }

    @Test
    public void whereExplicitCastTestNull() {
        String query = "SELECT * FROM T WHERE COL2 < CAST(COL5 AS DOUBLE)";
        this.testQuery(query, empty);
    }

    @Test
    public void whereExplicitImplicitCastTest() {
        String query = "SELECT * FROM T WHERE COL2 < CAST(COL1 AS REAL)";
        this.testQuery(query, z1);
    }

    @Test
    public void whereExplicitImplicitCastTestNull() {
        String query = "SELECT * FROM T WHERE COL2 < CAST(COL5 AS REAL)";
        this.testQuery(query, empty);
    }

    @Test
    public void whereExpressionTest() {
        String query = "SELECT * FROM T WHERE COL2 < 0";
        this.testQuery(query, DBSPZSetExpression.emptyWithElementType(z0.getElementType()));
    }

    @Test
    public void exceptTest() {
        String query = "SELECT * FROM T EXCEPT (SELECT * FROM T WHERE COL3)";
        this.testQuery(query, z1);
    }

    @Test
    public void constantFoldTest() {
        String query = "SELECT 1 + 2";
        this.testConstantOutput(query, new DBSPZSetExpression(new DBSPTupleExpression(new DBSPI32Literal(3))));
    }

    @Test
    public void groupByTest() {
        String query = "SELECT COL1 FROM T GROUP BY COL1";
        this.testQuery(query, new DBSPZSetExpression(
                new DBSPTupleExpression(new DBSPI32Literal(10))));
    }

    @Test
    public void groupByCountTest() {
        String query = "SELECT COL1, COUNT(col2) FROM T GROUP BY COL1, COL3";
        DBSPExpression row = new DBSPTupleExpression(new DBSPI32Literal(10), new DBSPI64Literal(1));
        this.testQuery(query, new DBSPZSetExpression(row, row));
    }

    @Test
    public void groupBySumTest() {
        String query = "SELECT COL1, SUM(col2) FROM T GROUP BY COL1, COL3";
        this.testQuery(query, new DBSPZSetExpression(
                new DBSPTupleExpression(new DBSPI32Literal(10), new DBSPDoubleLiteral(1)),
                new DBSPTupleExpression(new DBSPI32Literal(10), new DBSPDoubleLiteral(12))));
    }

    @Test
    public void divTest() {
        String query = "SELECT T.COL1 / T.COL5 FROM T";
        this.testQuery(query, new DBSPZSetExpression(
                new DBSPTupleExpression(DBSPLiteral.none(
                        new DBSPTypeInteger(CalciteObject.EMPTY, 32, true,true))),
                new DBSPTupleExpression(new DBSPI32Literal(10, true))));
    }

    @Test
    public void testIllegalDecimal() {
        String query = "SELECT CAST(12.34 AS DECIMAL(1, 2))";
        this.queryFailingInCompilation(query, "DECIMAL type must have scale <= precision");
    }

    @Test
    public void decimalParse() {
        // This is the same as DECIMAL(N, 0), so the result is rounded down
        String query = "SELECT CAST('0.5' AS DECIMAL)";
        this.testConstantOutput(query, new DBSPZSetExpression(
                new DBSPTupleExpression(
                        new DBSPDecimalLiteral(new DBSPTypeDecimal(CalciteObject.EMPTY, DBSPTypeDecimal.MAX_PRECISION, 0, false), new BigDecimal("0")))));
    }

    @Test
    public void decimalParseWithPrecisionScale() {
        String query = "SELECT CAST('0.5' AS DECIMAL(2, 1))";
        this.testConstantOutput(query, new DBSPZSetExpression(
                new DBSPTupleExpression(
                        new DBSPDecimalLiteral(new DBSPTypeDecimal(CalciteObject.EMPTY, 2, 1, false), new BigDecimal("0.5"))
                )
        ));
    }

    @Test
    public void divIntTest() {
        String query = "SELECT T.COL5 / T.COL5 FROM T";
        this.testQuery(query, new DBSPZSetExpression(
                new DBSPTupleExpression(DBSPLiteral.none(
                        new DBSPTypeInteger(CalciteObject.EMPTY, 32, true,true))),
                new DBSPTupleExpression(new DBSPI32Literal(1, true))));
    }

    @Test
    public void divTest2() {
        String query = "SELECT 5 / COUNT(*) - COUNT(*) FROM T";
        this.testQuery(query, new DBSPZSetExpression(
                new DBSPTupleExpression(new DBSPI64Literal(0))));
    }

    @Test
    public void divZeroTest() {
        String query = "SELECT 1 / 0";
        this.runtimeConstantFail(query, "attempt to divide by zero");
    }

    @Test
    public void divZero0() {
        String query = "SELECT 'Infinity' / 0";
        this.runtimeConstantFail(query, "invalid digit found in string");
    }

    @Test
    public void nestedDivTest() {
        String query = "SELECT 2 / (1 / 0)";
        this.runtimeConstantFail(query, "attempt to divide by zero");
    }

    @Test
    public void testByteArray() {
        String query = "SELECT x'012345ab'";
        this.testConstantOutput(query, new DBSPZSetExpression(
                new DBSPTupleExpression(new DBSPBinaryLiteral(new byte[]{ 0x01, 0x23, 0x45, (byte)0xAB }))));
    }

    @Test
    public void floatDivTest() {
        String query = "SELECT CAST(T.COL6 AS DOUBLE) / T.COL6 FROM T";
        this.testQuery(query, new DBSPZSetExpression(
                new DBSPTupleExpression(DBSPLiteral.none(
                        new DBSPTypeDouble(CalciteObject.EMPTY,true))),
                new DBSPTupleExpression(new DBSPDoubleLiteral(Double.NaN, true))));
    }

    @Test
    public void countDistinctTest() {
        String query = "SELECT 100 + COUNT(DISTINCT - T.COL5) FROM T";
        this.testAggregate(query,
                new DBSPZSetExpression(
                        new DBSPTupleExpression(
                                new DBSPI64Literal(101))),
                new DBSPZSetExpression(new DBSPTupleExpression(new DBSPI64Literal(100))));
    }

    @Test
    public void writeLogTest() {
        String query = "SELECT WRITELOG('Hello %% message\n', COL1) FROM (SELECT DISTINCT T.COL1 FROM T)";
        this.testQuery(query, new DBSPZSetExpression(
                new DBSPTupleExpression(
                        new DBSPI32Literal(10))));
    }

    @Test
    public void distinctTest() {
        String query = "SELECT DISTINCT T.COL1 FROM T";
        this.testQuery(query, new DBSPZSetExpression(
                new DBSPTupleExpression(
                        new DBSPI32Literal(10))));
    }

    @Test
    public void nullDistinctTest() {
        String query = "SELECT DISTINCT 0 + NULL, T.COL1 FROM T";
        DBSPI32Literal ten = new DBSPI32Literal(10);
        this.testQuery(query, new DBSPZSetExpression(
                new DBSPTupleExpression(DBSPLiteral.none(ten.getType().withMayBeNull(true)),
                        ten)));
    }

    @Test
    public void aggregateDistinctTest() {
        String query = "SELECT SUM(DISTINCT T.COL1), SUM(T.COL2) FROM T";
        this.testAggregate(query,
                new DBSPZSetExpression(
                        new DBSPTupleExpression(
                                new DBSPI32Literal(10, true),
                                new DBSPDoubleLiteral(13.0, true))),
                new DBSPZSetExpression(
                        new DBSPTupleExpression(
                                DBSPLiteral.none(new DBSPTypeInteger(CalciteObject.EMPTY, 32, true,true)),
                                DBSPLiteral.none(new DBSPTypeDouble(CalciteObject.EMPTY,true)))));
    }

    @Test
    public void aggregateTest() {
        String query = "SELECT SUM(T.COL1) FROM T";
        this.testAggregate(query,
                new DBSPZSetExpression(
                        new DBSPTupleExpression(new DBSPI32Literal(20, true))),
                new DBSPZSetExpression(
                        new DBSPTupleExpression(DBSPLiteral.none(
                                new DBSPTypeInteger(CalciteObject.EMPTY, 32, true,true)))));
    }

    @Test
    public void aggregateTwiceTest() {
        String query = "SELECT COUNT(T.COL1), SUM(T.COL1) FROM T";
        this.testAggregate(query, new DBSPZSetExpression(
                        new DBSPTupleExpression(
                                new DBSPI64Literal(2), new DBSPI32Literal(20, true))),
                new DBSPZSetExpression(
                        new DBSPTupleExpression(new DBSPI64Literal(0),
                                DBSPLiteral.none(
                                        new DBSPTypeInteger(CalciteObject.EMPTY, 32, true,true)))));
    }

    @Test
    public void linearNonLinearTest() {
        String query = "SELECT MAX(T.COL1), SUM(T.COL1) FROM T";
        this.testAggregate(query,
                new DBSPZSetExpression(
                        new DBSPTupleExpression(
                                new DBSPI32Literal(10, true),
                                new DBSPI32Literal(20, true)
                        )),
                new DBSPZSetExpression(
                        new DBSPTupleExpression(
                                DBSPTypeInteger.getType(CalciteObject.EMPTY, DBSPTypeCode.INT32,true).none(),
                                DBSPTypeInteger.getType(CalciteObject.EMPTY, DBSPTypeCode.INT32, true).none())
                        ));
    }

    @Test
    public void maxTest() {
        String query = "SELECT MAX(T.COL1) FROM T";
        this.testAggregate(query,
                new DBSPZSetExpression(
                        new DBSPTupleExpression(new DBSPI32Literal(10, true))),
                new DBSPZSetExpression(
                        new DBSPTupleExpression(DBSPLiteral.none(
                                DBSPTypeInteger.getType(CalciteObject.EMPTY, DBSPTypeCode.INT32, true)))));
    }

    @Test
    public void minTest() {
        String query = "SELECT MIN(T.COL1) FROM T";
        this.testAggregate(query,
                new DBSPZSetExpression(
                        new DBSPTupleExpression(new DBSPI32Literal(10, true))),
                new DBSPZSetExpression(
                        new DBSPTupleExpression(DBSPLiteral.none(
                                DBSPTypeInteger.getType(CalciteObject.EMPTY, DBSPTypeCode.INT32, true)))));
    }

    @Test @Ignore
    public void argMinTest() {
        String query = "SELECT ARG_MIN(T.COL2, T.COL3) FROM T";
        this.testAggregate(query,
                new DBSPZSetExpression(
                        new DBSPTupleExpression(new DBSPBoolLiteral(false, true))),
                new DBSPZSetExpression(
                        new DBSPTupleExpression(DBSPLiteral.none(DBSPTypeBool.create(true)))));
    }

    @Test
    public void maxConst() {
        String query = "SELECT MAX(6) FROM T";
        this.testAggregate(query,
                new DBSPZSetExpression(
                        new DBSPTupleExpression(new DBSPI32Literal(6, true))),
                new DBSPZSetExpression(
                        new DBSPTupleExpression(DBSPLiteral.none(
                                new DBSPTypeInteger(CalciteObject.EMPTY, 32, true,true)))));
    }

    @Test
    public void emptyAggregate() {
        String query = "SELECT COUNT(*), COUNT(DISTINCT COL1) FROM T WHERE COL1 > 10000";
        this.testConstantOutput(query, new DBSPZSetExpression(
                new DBSPTupleExpression(
                        new DBSPI64Literal(0), new DBSPI64Literal(0))));
    }

    @Test
    public void constAggregateExpression() {
        String query = "SELECT 34 / SUM (1) FROM T GROUP BY COL1";
        this.testQuery(query, new DBSPZSetExpression(
                 new DBSPTupleExpression(
                        new DBSPI32Literal(17))));
    }

    @Test
    public void inTest() {
        String query = "SELECT 3 in (SELECT COL5 FROM T)";
        this.testAggregate(query,
                new DBSPZSetExpression(new DBSPTupleExpression(
                        DBSPLiteral.none(new DBSPTypeBool(CalciteObject.EMPTY, true)))),
                new DBSPZSetExpression(new DBSPTupleExpression(
                        new DBSPBoolLiteral(false, true)))
        );
    }

    @Test
    public void constAggregateExpression2() {
        String query = "SELECT 34 / AVG (1) FROM T GROUP BY COL1";
        this.testQuery(query, new DBSPZSetExpression(
                 new DBSPTupleExpression(
                        new DBSPI32Literal(34))));
    }

    @Test
    public void constAggregateDoubleExpression() {
        String query = "SELECT 34 / SUM (1), 20 / SUM(2) FROM T GROUP BY COL1";
        this.testQuery(query, new DBSPZSetExpression(
                 new DBSPTupleExpression(
                        new DBSPI32Literal(17), new DBSPI32Literal(5))));
    }

    @Test
    public void aggregateFloatTest() {
        String query = "SELECT SUM(T.COL2) FROM T";
        this.testAggregate(query,
                new DBSPZSetExpression(
                        new DBSPTupleExpression(new DBSPDoubleLiteral(13.0, true))),
                new DBSPZSetExpression(
                        new DBSPTupleExpression(DBSPLiteral.none(new DBSPTypeDouble(CalciteObject.EMPTY,true)))));
    }

    @Test
    public void optionAggregateTest() {
        String query = "SELECT SUM(T.COL5) FROM T";
        this.testAggregate(query,
                new DBSPZSetExpression(
                        new DBSPTupleExpression(new DBSPI32Literal(1, true))),
                new DBSPZSetExpression(
                        new DBSPTupleExpression(DBSPLiteral.none(
                                new DBSPTypeInteger(CalciteObject.EMPTY, 32, true,true)))));
    }

    @Test
    public void aggregateFalseTest() {
        String query = "SELECT SUM(T.COL1) FROM T WHERE FALSE";
        this.testConstantOutput(query, new DBSPZSetExpression(
                 new DBSPTupleExpression(
                        DBSPLiteral.none(new DBSPTypeInteger(CalciteObject.EMPTY, 32, true,true)))));
    }

    @Test
    public void averageTest() {
        String query = "SELECT AVG(T.COL1) FROM T";
        DBSPZSetExpression output = new DBSPZSetExpression(
                new DBSPTupleExpression(
                        new DBSPI32Literal(10, true)));
        this.testAggregate(query, output, new DBSPZSetExpression(
                new DBSPTupleExpression(DBSPLiteral.none(
                        new DBSPTypeInteger(CalciteObject.EMPTY, 32, true,true)))));
    }

    @Test
    public void averageFpTest() {
        String query = "SELECT AVG(T.COL2) FROM T";
        DBSPZSetExpression output = new DBSPZSetExpression(
                new DBSPTupleExpression(
                        new DBSPDoubleLiteral(6.5, true)));
        this.testAggregate(query, output, new DBSPZSetExpression(
                new DBSPTupleExpression(DBSPLiteral.none(
                        new DBSPTypeDouble(CalciteObject.EMPTY, true)))));
    }

    @Test
    public void averageNullableTest() {
        String query = "SELECT AVG(T.COL6) FROM T";
        DBSPZSetExpression output = new DBSPZSetExpression(
                new DBSPTupleExpression(
                        new DBSPDecimalLiteral(D62, new BigDecimal(0))));
        this.testAggregate(query, output, new DBSPZSetExpression(
                new DBSPTupleExpression(DBSPLiteral.none(D62))));
    }

    @Test
    public void cartesianTest() {
        String query = "SELECT * FROM T, T AS X";
        DBSPExpression inResult = DBSPTupleExpression.flatten(E0, E0);
        DBSPZSetExpression result = new DBSPZSetExpression( inResult);
        result.append(DBSPTupleExpression.flatten(E0, E1));
        result.append(DBSPTupleExpression.flatten(E1, E0));
        result.append(DBSPTupleExpression.flatten(E1, E1));
        this.testQuery(query, result);
    }

    @Test
    public void foldTest() {
        String query = "SELECT + 91 + NULLIF ( + 93, + 38 )";
        this.testConstantOutput(query, new DBSPZSetExpression(
                 new DBSPTupleExpression(
                new DBSPI32Literal(184, true))));
    }

    @Test
    public void orderbyTest() {
        String query = "SELECT * FROM T ORDER BY T.COL2";
        this.testQuery(query, new DBSPZSetExpression(
                new DBSPArrayExpression(E1, E0)
        ));
    }

    @Test
    public void limitTest() {
        String query = "SELECT * FROM T ORDER BY T.COL2 LIMIT 1";
        this.testQuery(query, new DBSPZSetExpression(
                new DBSPArrayExpression(E1)
        ));
    }

    @Test
    public void limitTest2() {
        String query = "SELECT * FROM T ORDER BY T.COL2 LIMIT 3";
        this.testQuery(query, new DBSPZSetExpression(
                new DBSPArrayExpression(E1, E0)
        ));
    }

    @Test
    public void nestedOrderbyTest() {
        // If the optimizer doesn't remove the inner ORDER BY this test
        // fails because the types don't match in Rust.
        String query = "SELECT COL1 FROM (SELECT * FROM T ORDER BY T.COL2)";
        this.testQuery(query, new DBSPZSetExpression(
                new DBSPTupleExpression(new DBSPI32Literal(10)),
                new DBSPTupleExpression(new DBSPI32Literal(10))));
    }

    @Test
    public void orderbyDescendingTest() {
        String query = "SELECT * FROM T ORDER BY T.COL2 DESC";
        this.testQuery(query, new DBSPZSetExpression(
                new DBSPArrayExpression(E0, E1)
        ));
    }

    @Test
    public void orderby2Test() {
        String query = "SELECT * FROM T ORDER BY T.COL2, T.COL1";
        this.testQuery(query, new DBSPZSetExpression(
                new DBSPArrayExpression(E1, E0)
        ));
    }
}
