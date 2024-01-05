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

import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.CalciteObject;
import org.dbsp.sqlCompiler.compiler.sql.BaseSQLTests;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPBinaryLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPBoolLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPDecimalLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPDoubleLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPGeoPointLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPI32Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPI64Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPNullLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPStringLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPVecLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPZSetLiteral;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeBool;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeDecimal;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeDouble;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeInteger;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeString;
import org.junit.Ignore;
import org.junit.Test;

import java.math.BigDecimal;

/**
 * Test end-to-end by compiling some DDL statements and view
 * queries by compiling them to rust and executing them
 * by inserting data in the input tables and reading data
 * from the declared views.
 */
public class EndToEndTests extends BaseSQLTests {
    static final String E2E_TABLE = "CREATE TABLE T (\n" +
            "COL1 INT NOT NULL" +
            ", COL2 DOUBLE NOT NULL" +
            ", COL3 BOOLEAN NOT NULL" +
            ", COL4 VARCHAR NOT NULL" +
            ", COL5 INT" +
            ", COL6 DOUBLE" +
            ")";

    public DBSPCompiler compileQuery(String query) {
        DBSPCompiler compiler = this.testCompiler();
        compiler.compileStatement(E2E_TABLE);
        compiler.compileStatement(query);
        return compiler;
    }

    void testQueryBase(String query, InputOutputPair... streams) {
        query = "CREATE VIEW V AS " + query;
        DBSPCompiler compiler = this.compileQuery(query);
        DBSPCircuit circuit = getCircuit(compiler);
        this.addRustTestCase(query, compiler, circuit, streams);
    }

    public void invokeTestQueryBase(String query, InputOutputPair... streams) {
        this.testQueryBase(query, streams);
    }

    /** Use this function to test queries whose result does not change when table T is modified. */
    void testConstantOutput(String query, DBSPZSetLiteral.Contents output) {
        this.testQueryBase(query, new InputOutputPair(createInput(), output));
    }

    /** Use this function to test queries that compute aggregates */
    void testAggregate(String query,
                       DBSPZSetLiteral.Contents firstOutput,
                       DBSPZSetLiteral.Contents outputForEmptyInput) {
        this.testQueryBase(query, new InputOutputPair(createInput(), firstOutput));
    }

    /**
     * Returns the table T containing:
     * -------------------------------------------
     * | 10 | 12.0 | true  | Hi | NULL    | NULL |
     * | 10 |  1.0 | false | Hi | Some[1] |  0.0 |
     * -------------------------------------------
     * INSERT INTO T VALUES (10, 12, true, 'Hi', NULL, NULL);
     * INSERT INTO T VALUES (10, 1.0, false, 'Hi', 1, 0.0);
     */
    static DBSPZSetLiteral.Contents createInput() {
        return new DBSPZSetLiteral.Contents(e0, e1);
    }

    public static final DBSPTupleExpression e0 = new DBSPTupleExpression(
            new DBSPI32Literal(10),
            new DBSPDoubleLiteral(12.0),
            new DBSPBoolLiteral(true),
            new DBSPStringLiteral("Hi"),
            DBSPLiteral.none(new DBSPTypeInteger(CalciteObject.EMPTY, 32, true,true)),
            DBSPLiteral.none(new DBSPTypeDouble(CalciteObject.EMPTY,true))
    );
    public static final DBSPTupleExpression e1 = new DBSPTupleExpression(
            new DBSPI32Literal(10),
            new DBSPDoubleLiteral(1.0),
            new DBSPBoolLiteral(false),
            new DBSPStringLiteral("Hi"),
            new DBSPI32Literal(1, true),
            new DBSPDoubleLiteral(0.0, true)
    );

    public static final DBSPTupleExpression e0NoDouble = new DBSPTupleExpression(
            new DBSPI32Literal(10),
            new DBSPBoolLiteral(true),
            new DBSPStringLiteral("Hi"),
            DBSPLiteral.none(new DBSPTypeInteger(CalciteObject.EMPTY, 32, true,true))
    );
    public static final DBSPTupleExpression e1NoDouble = new DBSPTupleExpression(
            new DBSPI32Literal(10),
            new DBSPBoolLiteral(false),
            new DBSPStringLiteral("Hi"),
            new DBSPI32Literal(1, true)
    );
    static final DBSPZSetLiteral.Contents z0 = new DBSPZSetLiteral.Contents(e0);
    static final DBSPZSetLiteral.Contents z1 = new DBSPZSetLiteral.Contents(e1);
    static final DBSPZSetLiteral.Contents empty = DBSPZSetLiteral.Contents.emptyWithElementType(z0.getElementType());

    public void testQuery(String query, DBSPZSetLiteral.Contents expectedOutput) {
        DBSPZSetLiteral.Contents input = createInput();
        this.testQueryBase(query, new InputOutputPair(input, expectedOutput));
    }

    @Test
    public void testNullableCompare() {
        String query = "SELECT T.COL5 > T.COL1 FROM T";
        this.testQuery(query, new DBSPZSetLiteral.Contents(
                new DBSPTupleExpression(new DBSPBoolLiteral()),
                new DBSPTupleExpression(new DBSPBoolLiteral(false, true))));
    }

    @Test
    public void testAbs() {
        String query = "SELECT ABS(T.COL2) FROM T";
        this.testQuery(query, new DBSPZSetLiteral.Contents(
                new DBSPTupleExpression(e0.fields[1]),
                new DBSPTupleExpression(e1.fields[1])));
    }

    @Test
    public void testNullableCastCompare() {
        String query = "SELECT T.COL5 > T.COL2 FROM T";
        this.testQuery(query, new DBSPZSetLiteral.Contents(
                new DBSPTupleExpression(new DBSPBoolLiteral()),
                new DBSPTupleExpression(new DBSPBoolLiteral(false, true))));
    }

    @Test
    public void testNullableCastCompare2() {
        String query = "SELECT T.COL5 > T.COL6 FROM T";
        this.testQuery(query, new DBSPZSetLiteral.Contents(
                new DBSPTupleExpression(new DBSPBoolLiteral()),
                new DBSPTupleExpression(new DBSPBoolLiteral(true, true))));
    }

    @Test
    public void testNullableCompare2() {
        String query = "SELECT T.COL5 > 10 FROM T";
        this.testQuery(query, new DBSPZSetLiteral.Contents(
                new DBSPTupleExpression(new DBSPBoolLiteral()),
                new DBSPTupleExpression(new DBSPBoolLiteral(false, true))));
    }

    @Test
    public void testBoolean() {
        String query = "SELECT T.COL3 OR T.COL1 > 10 FROM T";
        this.testQuery(query, new DBSPZSetLiteral.Contents(
                new DBSPTupleExpression(new DBSPBoolLiteral(true)),
                new DBSPTupleExpression(new DBSPBoolLiteral(false))));
    }

    @Test
    public void testNullableBoolean2() {
        String query = "SELECT T.COL5 > 10 AND T.COL3 FROM T";
        this.testQuery(query, new DBSPZSetLiteral.Contents(
                new DBSPTupleExpression(new DBSPBoolLiteral()),
                new DBSPTupleExpression(new DBSPBoolLiteral(false, true))));
    }

    @Test
    public void testNullableBoolean3() {
        String query = "SELECT T.COL3 AND T.COL5 > 10 FROM T";
        this.testQuery(query, new DBSPZSetLiteral.Contents(
                new DBSPTupleExpression(new DBSPBoolLiteral()),
                new DBSPTupleExpression(new DBSPBoolLiteral(false, true))));
    }

    @Test
    public void overTest() {
        DBSPExpression t = new DBSPTupleExpression(new DBSPI32Literal(10), new DBSPI64Literal(2));
        String query = "SELECT T.COL1, COUNT(*) OVER (ORDER BY T.COL1 RANGE UNBOUNDED PRECEDING) FROM T";
        this.testQuery(query, new DBSPZSetLiteral.Contents(t, t));
    }

    @Test
    public void overSumTest() {
        DBSPExpression t = new DBSPTupleExpression(new DBSPI32Literal(10), new DBSPDoubleLiteral(13.0));
        String query = "SELECT T.COL1, SUM(T.COL2) OVER (ORDER BY T.COL1 RANGE UNBOUNDED PRECEDING) FROM T";
        this.testQuery(query, new DBSPZSetLiteral.Contents(t, t));
    }

    @Test
    public void testConcat() {
        String query = "SELECT T.COL4 || ' ' || T.COL4 FROM T";
        DBSPExpression lit = new DBSPTupleExpression(new DBSPStringLiteral("Hi Hi"));
        this.testQuery(query, new DBSPZSetLiteral.Contents(lit, lit));
    }

    @Test
    public void testCast() {
        String query = "SELECT CAST(T.COL1 AS VARCHAR) FROM T";
        DBSPExpression lit = new DBSPTupleExpression(new DBSPStringLiteral("10"));
        this.testQuery(query, new DBSPZSetLiteral.Contents(lit, lit));
    }

    @Test
    public void testArray() {
        String query = "SELECT ELEMENT(ARRAY [2])";
        DBSPZSetLiteral.Contents result = new DBSPZSetLiteral.Contents(new DBSPTupleExpression(new DBSPI32Literal(2)));
        this.testConstantOutput(query, result);
    }

    @Test
    public void testNull() {
        String query = "SELECT NULL";
        DBSPZSetLiteral.Contents result = new DBSPZSetLiteral.Contents(new DBSPTupleExpression(new DBSPNullLiteral()));
        this.testConstantOutput(query, result);
    }

    @Test
    public void testArrayIndex() {
        String query = "SELECT (ARRAY [2])[1]";
        DBSPZSetLiteral.Contents result = new DBSPZSetLiteral.Contents(new DBSPTupleExpression(new DBSPI32Literal(2, true)));
        this.testConstantOutput(query, result);
    }

    @Test
    public void testArrayIndexOutOfBounds() {
        String query = "SELECT (ARRAY [2])[3]";
        DBSPZSetLiteral.Contents result = new DBSPZSetLiteral.Contents(new DBSPTupleExpression(DBSPLiteral.none(new DBSPTypeInteger(CalciteObject.EMPTY, 32, true,true))));
        this.testConstantOutput(query, result);
    }

    @Test @Ignore("Calcite's semantics requires this to crash at runtime")
    // Calcite's semantics requires this to crash at runtime;
    // we should change the semantics of ELEMENT
    // to return NULL when the array has more than 1 element.
    public void testArrayElement() {
        String query = "SELECT ELEMENT(ARRAY [2, 3])";
        DBSPZSetLiteral.Contents result =
                new DBSPZSetLiteral.Contents(new DBSPTupleExpression(DBSPLiteral.none(new DBSPTypeInteger(CalciteObject.EMPTY, 32, true,true))));
        this.testQuery(query, result);
    }

    @Test
    public void testConcatNull() {
        String query = "SELECT T.COL4 || NULL FROM T";
        DBSPExpression lit = new DBSPTupleExpression(DBSPLiteral.none(
                new DBSPTypeString(CalciteObject.EMPTY, DBSPTypeString.UNLIMITED_PRECISION, false, true)));
        this.testQuery(query, new DBSPZSetLiteral.Contents(lit, lit));
    }

    @Test
    public void testConcatNull2() {
        String query = "SELECT CONCAT(T.COL4, NULL) FROM T";
        DBSPExpression lit = new DBSPTupleExpression(DBSPLiteral.none(
                new DBSPTypeString(CalciteObject.EMPTY, DBSPTypeString.UNLIMITED_PRECISION, false, true)));
        this.testQuery(query, new DBSPZSetLiteral.Contents(lit, lit));
    }

    @Test
    public void overTwiceTest() {
        DBSPExpression t = new DBSPTupleExpression(
                new DBSPI32Literal(10),
                new DBSPDoubleLiteral(13.0),
                new DBSPI64Literal(2));
        String query = "SELECT T.COL1, " +
                "SUM(T.COL2) OVER (ORDER BY T.COL1 RANGE UNBOUNDED PRECEDING), " +
                "COUNT(*) OVER (ORDER BY T.COL1 RANGE UNBOUNDED PRECEDING) FROM T";
        this.testQuery(query, new DBSPZSetLiteral.Contents(t, t));
    }

    @Test
    public void overConstantWindowTest() {
        DBSPExpression t = new DBSPTupleExpression(
                new DBSPI32Literal(10),
                new DBSPI64Literal(2));
        String query = "SELECT T.COL1, " +
                "COUNT(*) OVER (ORDER BY T.COL1 RANGE BETWEEN 2 PRECEDING AND 1 PRECEDING) FROM T";
        this.testQuery(query, new DBSPZSetLiteral.Contents(t, t));
    }

    @Test
    public void overTwiceDifferentTest() {
        DBSPExpression t = new DBSPTupleExpression(
                new DBSPI32Literal(10),
                new DBSPDoubleLiteral(13.0),
                new DBSPI64Literal(2));
        String query = "SELECT T.COL1, " +
                "SUM(T.COL2) OVER (ORDER BY T.COL1 RANGE UNBOUNDED PRECEDING), " +
                "COUNT(*) OVER (ORDER BY T.COL1 RANGE BETWEEN 2 PRECEDING AND 1 PRECEDING) FROM T";
        this.testQuery(query, new DBSPZSetLiteral.Contents(t, t));
    }

    @Test
    public void correlatedAggregate() {
        String query = """
                SELECT Sum(r.COL1 * r.COL5) FROM T r
                WHERE
                0.5 * (SELECT Sum(r1.COL5) FROM T r1) =
                (SELECT Sum(r2.COL5) FROM T r2 WHERE r2.COL1 = r.COL1)""";
        this.testAggregate(query,
                new DBSPZSetLiteral.Contents(
                        new DBSPTupleExpression(DBSPLiteral.none(
                                new DBSPTypeInteger(CalciteObject.EMPTY, 32, true,true)))),
                new DBSPZSetLiteral.Contents(
                        new DBSPTupleExpression(DBSPLiteral.none(
                                new DBSPTypeInteger(CalciteObject.EMPTY, 32, true,true)))));

        /* TODO
        query = "SELECT Sum(b.price * b.volume) FROM bids b\n" +
                "WHERE\n" +
                "0.75 * (SELECT Sum(b1.volume) FROM bids b1)\n" +
                "< (SELECT Sum(b2.volume) FROM bids b2\n" +
                "WHERE b2.price <= b.price)";
         */
    }

    @Test
    public void someTest() {
        String query = "SELECT SOME(T.COL3) FROM T";
        this.testAggregate(query,
                new DBSPZSetLiteral.Contents(
                        new DBSPTupleExpression(new DBSPBoolLiteral(true, true))),
                new DBSPZSetLiteral.Contents(
                        new DBSPTupleExpression(new DBSPBoolLiteral())));
    }

    @Test
    public void orTest() {
        String query = "SELECT LOGICAL_OR(T.COL3) FROM T";
        this.testAggregate(query,
                new DBSPZSetLiteral.Contents(
                        new DBSPTupleExpression(new DBSPBoolLiteral(true, true))),
                new DBSPZSetLiteral.Contents(
                        new DBSPTupleExpression(new DBSPBoolLiteral())));
    }

    @Test
    public void everyTest() {
        String query = "SELECT EVERY(T.COL3) FROM T";
        this.testAggregate(query,
                new DBSPZSetLiteral.Contents(
                        new DBSPTupleExpression(new DBSPBoolLiteral(false, true))),
                new DBSPZSetLiteral.Contents(
                        new DBSPTupleExpression(new DBSPBoolLiteral())));
    }

    @Test
    public void projectTest() {
        String query = "SELECT T.COL3 FROM T";
        this.testQuery(query,
                new DBSPZSetLiteral.Contents(
                        new DBSPTupleExpression(new DBSPBoolLiteral(true)),
                        new DBSPTupleExpression(new DBSPBoolLiteral(false))));
    }

    @Test
    public void withTest() {
        String query = "WITH v AS (SELECT COL3 FROM T)\n" +
                "SELECT * FROM v";
        this.testQuery(query, new DBSPZSetLiteral.Contents(
                new DBSPTupleExpression(new DBSPBoolLiteral(true)),
                new DBSPTupleExpression(new DBSPBoolLiteral(false))));
    }

    @Test
    public void testSumFilter() {
        String query = "SELECT T.COL3, " +
                "SUM(T.COL1) FILTER (WHERE T.COL1 > 20)\n" +
                "FROM T GROUP BY T.COL3";
        this.testQuery(query, new DBSPZSetLiteral.Contents(
                new DBSPTupleExpression(
                        new DBSPBoolLiteral(true),
                        DBSPLiteral.none(new DBSPTypeInteger(CalciteObject.EMPTY, 32, true,false))
                ),
                new DBSPTupleExpression(new DBSPBoolLiteral(false),
                        DBSPLiteral.none(new DBSPTypeInteger(CalciteObject.EMPTY, 32, true,false))
                )));
    }

    @Test
    public void testCountFilter() {
        String query = "SELECT T.COL3, " +
                "COUNT(*) FILTER (WHERE T.COL1 > 20)\n" +
                "FROM T GROUP BY T.COL3";
        this.testQuery(query, new DBSPZSetLiteral.Contents(
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
        this.testQuery(query, new DBSPZSetLiteral.Contents(
                new DBSPTupleExpression(
                        new DBSPBoolLiteral(true),
                        new DBSPI64Literal(0, false),
                        DBSPLiteral.none(new DBSPTypeInteger(CalciteObject.EMPTY, 32, true,false))
                ),
                new DBSPTupleExpression(new DBSPBoolLiteral(false),
                        new DBSPI64Literal(0, false),
                        DBSPLiteral.none(new DBSPTypeInteger(CalciteObject.EMPTY, 32, true,false))
                )));
    }

    @Test @Ignore("JSON_OBJECT not yet implemented")
    public void jsonTest() {
        String query = """
                select JSON_OBJECT(
                    KEY 'level1'
                    VALUE(T.COL1))
                from T""";
        this.testQuery(query, new DBSPZSetLiteral.Contents(
                new DBSPTupleExpression(new DBSPStringLiteral(""))));
    }

    @Test
    public void intersectTest() {
        String query = "SELECT * FROM T INTERSECT (SELECT * FROM T)";
        this.testQuery(query, createInput());
    }

    @Test
    public void plusNullTest() {
        String query = "SELECT T.COL1 + T.COL5 FROM T";
        this.testQuery(query,
                new DBSPZSetLiteral.Contents(
                        new DBSPTupleExpression(new DBSPI32Literal(11, true)),
                        new DBSPTupleExpression(DBSPLiteral.none(new DBSPTypeInteger(CalciteObject.EMPTY, 32, true,true)))));
    }

    @Test
    public void negateNullTest() {
        String query = "SELECT -T.COL5 FROM T";
        this.testQuery(query,
                new DBSPZSetLiteral.Contents(
                        new DBSPTupleExpression(new DBSPI32Literal(-1, true)),
                        new DBSPTupleExpression(DBSPLiteral.none(new DBSPTypeInteger(CalciteObject.EMPTY, 32, true,true)))));
    }

    @Test
    public void projectNullTest() {
        String query = "SELECT T.COL5 FROM T";
        this.testQuery(query,
                new DBSPZSetLiteral.Contents(
                        new DBSPTupleExpression(new DBSPI32Literal(1, true)),
                        new DBSPTupleExpression(DBSPLiteral.none(new DBSPTypeInteger(CalciteObject.EMPTY, 32, true,true)))));
    }

    @Test
    public void idTest() {
        String query = "SELECT * FROM T";
        this.testQuery(query, createInput());
    }

    @Test
    public void unionTest() {
        String query = "(SELECT * FROM T) UNION (SELECT * FROM T)";
        this.testQuery(query, createInput());
    }

    @Test
    public void unionAllTest() {
        String query = "(SELECT * FROM T) UNION ALL (SELECT * FROM T)";
        DBSPZSetLiteral.Contents output = createInput();
        output.add(output);
        this.testQuery(query, output);
    }

    @Test
    public void joinTest() {
        String query = "SELECT T1.COL3, T2.COL3 AS C3 FROM T AS T1 JOIN T AS T2 ON T1.COL1 = T2.COL1";
        this.testQuery(query, new DBSPZSetLiteral.Contents(
                new DBSPTupleExpression(new DBSPBoolLiteral(false), new DBSPBoolLiteral(false)),
                new DBSPTupleExpression(new DBSPBoolLiteral(false), new DBSPBoolLiteral(true)),
                new DBSPTupleExpression(new DBSPBoolLiteral(true), new DBSPBoolLiteral(false)),
                new DBSPTupleExpression(new DBSPBoolLiteral(true), new DBSPBoolLiteral(true))));
    }

    @Test
    public void joinNullableTest() {
        String query = "SELECT T1.COL3, T2.COL3 AS C3 FROM T AS T1 JOIN T AS T2 ON T1.COL1 = T2.COL5";
        this.testQuery(query, DBSPZSetLiteral.Contents.emptyWithElementType(
                new DBSPTypeTuple(DBSPTypeBool.create(false), DBSPTypeBool.create(false))));
    }

    @Test
    public void zero() {
        String query = "SELECT 0";
        this.testConstantOutput(query, new DBSPZSetLiteral.Contents(
                new DBSPTupleExpression(new DBSPI32Literal(0))));
    }

    @Test @Ignore("https://github.com/feldera/feldera/issues/1207")
    public void geoPointTest() {
        String query = "SELECT ST_POINT(0, 0)";
        this.testConstantOutput(query, new DBSPZSetLiteral.Contents(
                new DBSPTupleExpression(
                        new DBSPGeoPointLiteral(CalciteObject.EMPTY,
                                new DBSPDoubleLiteral(0),
                                new DBSPDoubleLiteral(0), false).some())));
    }

    @Test
    public void geoDistanceTest() {
        String query = "SELECT ST_DISTANCE(ST_POINT(0, 0), ST_POINT(0,1))";
        this.testConstantOutput(query, new DBSPZSetLiteral.Contents(
                new DBSPTupleExpression(new DBSPDoubleLiteral(1.0, true))));
    }

    @Test
    public void leftOuterJoinTest() {
        String query = "SELECT T1.COL3, T2.COL3 AS C3 FROM T AS T1 LEFT JOIN T AS T2 ON T1.COL1 = T2.COL5";
        this.testQuery(query, new DBSPZSetLiteral.Contents(
                new DBSPTupleExpression(new DBSPBoolLiteral(false), new DBSPBoolLiteral()),
                new DBSPTupleExpression(new DBSPBoolLiteral(true), new DBSPBoolLiteral())
        ));
    }

    @Test
    public void rightOuterJoinTest() {
        String query = "SELECT T1.COL3, T2.COL3 AS C3 FROM T AS T1 RIGHT JOIN T AS T2 ON T1.COL1 = T2.COL5";
        this.testQuery(query, new DBSPZSetLiteral.Contents(
                new DBSPTupleExpression(new DBSPBoolLiteral(), new DBSPBoolLiteral(false)),
                new DBSPTupleExpression(new DBSPBoolLiteral(), new DBSPBoolLiteral(true))
        ));
    }

    @Test
    public void fullOuterJoinTest() {
        String query = "SELECT T1.COL3, T2.COL3 AS C3 FROM T AS T1 FULL OUTER JOIN T AS T2 ON T1.COL1 = T2.COL5";
        this.testQuery(query, new DBSPZSetLiteral.Contents(
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
        String query = "SELECT * FROM T WHERE COL3";
        this.testQuery(query, z0);
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
        this.testQuery(query, DBSPZSetLiteral.Contents.emptyWithElementType(z0.getElementType()));
    }

    @Test
    public void exceptTest() {
        String query = "SELECT * FROM T EXCEPT (SELECT * FROM T WHERE COL3)";
        this.testQuery(query, z1);
    }

    @Test
    public void constantFoldTest() {
        String query = "SELECT 1 + 2";
        this.testConstantOutput(query, new DBSPZSetLiteral.Contents(new DBSPTupleExpression(new DBSPI32Literal(3))));
    }

    @Test
    public void groupByTest() {
        String query = "SELECT COL1 FROM T GROUP BY COL1";
        this.testQuery(query, new DBSPZSetLiteral.Contents(
                new DBSPTupleExpression(new DBSPI32Literal(10))));
    }

    @Test
    public void groupByCountTest() {
        String query = "SELECT COL1, COUNT(col2) FROM T GROUP BY COL1, COL3";
        DBSPExpression row =  new DBSPTupleExpression(new DBSPI32Literal(10), new DBSPI64Literal(1));
        this.testQuery(query, new DBSPZSetLiteral.Contents( row, row));
    }

    @Test
    public void groupBySumTest() {
        String query = "SELECT COL1, SUM(col2) FROM T GROUP BY COL1, COL3";
        this.testQuery(query, new DBSPZSetLiteral.Contents(
                new DBSPTupleExpression(new DBSPI32Literal(10), new DBSPDoubleLiteral(1)),
                new DBSPTupleExpression(new DBSPI32Literal(10), new DBSPDoubleLiteral(12))));
    }

    @Test
    public void divTest() {
        String query = "SELECT T.COL1 / T.COL5 FROM T";
        this.testQuery(query, new DBSPZSetLiteral.Contents(
                new DBSPTupleExpression(DBSPLiteral.none(
                        new DBSPTypeInteger(CalciteObject.EMPTY, 32, true,true))),
                new DBSPTupleExpression(new DBSPI32Literal(10, true))));
    }

    @Test
    public void testIllegalDecimal() {
        String query = "SELECT CAST(12.34 AS DECIMAL(1, 2))";
        this.testNegativeQuery(query, "DECIMAL type must have scale <= precision");
    }

    @Test
    public void decimalParse() {
        String query = "SELECT CAST('0.5' AS DECIMAL)";
        this.testConstantOutput(query, new DBSPZSetLiteral.Contents(
                new DBSPTupleExpression(
                        new DBSPDecimalLiteral(new DBSPTypeDecimal(CalciteObject.EMPTY, DBSPTypeDecimal.MAX_PRECISION, DBSPTypeDecimal.MAX_SCALE, false), new BigDecimal("0.5")))));
    }

    @Test
    public void divIntTest() {
        String query = "SELECT T.COL5 / T.COL5 FROM T";
        this.testQuery(query, new DBSPZSetLiteral.Contents(
                new DBSPTupleExpression(DBSPLiteral.none(
                        new DBSPTypeInteger(CalciteObject.EMPTY, 32, true,true))),
                new DBSPTupleExpression(new DBSPI32Literal(1, true))));
    }

    @Test
    public void divZeroTest() {
        String query = "SELECT 1 / 0";
        this.runtimeFail(query, "attempt to divide by zero", this.getEmptyIOPair());
    }

    @Test
    public void divZero0() {
        String query = "SELECT 'Infinity' / 0";
        this.runtimeFail(query, "InvalidDigit", this.getEmptyIOPair());
    }

    @Test
    public void nestedDivTest() {
        String query = "SELECT 2 / (1 / 0)";
        this.runtimeFail(query, "attempt to divide by zero", this.getEmptyIOPair());
    }

    @Test
    public void testByteArray() {
        String query = "SELECT x'012345ab'";
        this.testConstantOutput(query, new DBSPZSetLiteral.Contents(
                new DBSPTupleExpression(new DBSPBinaryLiteral(new byte[]{ 0x01, 0x23, 0x45, (byte)0xAB }))));
    }

    @Test
    public void floatDivTest() {
        String query = "SELECT T.COL6 / T.COL6 FROM T";
        this.testQuery(query, new DBSPZSetLiteral.Contents(
                new DBSPTupleExpression(DBSPLiteral.none(
                        new DBSPTypeDouble(CalciteObject.EMPTY,true))),
                new DBSPTupleExpression(new DBSPDoubleLiteral(Double.NaN, true))));
    }

    @Test
    public void countDistinctTest() {
        String query = "SELECT 100 + COUNT(DISTINCT - T.COL5) FROM T";
        this.testAggregate(query,
                new DBSPZSetLiteral.Contents(
                        new DBSPTupleExpression(
                                new DBSPI64Literal(101))),
                new DBSPZSetLiteral.Contents(new DBSPTupleExpression(new DBSPI64Literal(100))));
    }

    @Test
    public void writeLogTest() {
        String query = "SELECT WRITELOG('Hello %% message\n', COL1) FROM (SELECT DISTINCT T.COL1 FROM T)";
        this.testQuery(query, new DBSPZSetLiteral.Contents(
                new DBSPTupleExpression(
                        new DBSPI32Literal(10))));
    }

    @Test
    public void distinctTest() {
        String query = "SELECT DISTINCT T.COL1 FROM T";
        this.testQuery(query, new DBSPZSetLiteral.Contents(
                new DBSPTupleExpression(
                        new DBSPI32Literal(10))));
    }

    @Test
    public void nullDistinctTest() {
        String query = "SELECT DISTINCT 0 + NULL, T.COL1 FROM T";
        DBSPI32Literal ten = new DBSPI32Literal(10);
        this.testQuery(query, new DBSPZSetLiteral.Contents(
                new DBSPTupleExpression(DBSPLiteral.none(ten.getType().setMayBeNull(true)),
                        ten)));
    }

    @Test
    public void aggregateDistinctTest() {
        String query = "SELECT SUM(DISTINCT T.COL1), SUM(T.COL2) FROM T";
        this.testAggregate(query,
                new DBSPZSetLiteral.Contents(
                        new DBSPTupleExpression(
                                new DBSPI32Literal(10, true),
                                new DBSPDoubleLiteral(13.0, true))),
                new DBSPZSetLiteral.Contents(
                        new DBSPTupleExpression(
                                DBSPLiteral.none(new DBSPTypeInteger(CalciteObject.EMPTY, 32, true,true)),
                                DBSPLiteral.none(new DBSPTypeDouble(CalciteObject.EMPTY,true)))));
    }

    @Test
    public void aggregateTest() {
        String query = "SELECT SUM(T.COL1) FROM T";
        this.testAggregate(query,
                new DBSPZSetLiteral.Contents(
                        new DBSPTupleExpression(new DBSPI32Literal(20, true))),
                new DBSPZSetLiteral.Contents(
                        new DBSPTupleExpression(DBSPLiteral.none(
                                new DBSPTypeInteger(CalciteObject.EMPTY, 32, true,true)))));
    }

    @Test
    public void aggregateTwiceTest() {
        String query = "SELECT COUNT(T.COL1), SUM(T.COL1) FROM T";
        this.testAggregate(query, new DBSPZSetLiteral.Contents(
                        new DBSPTupleExpression(
                                new DBSPI64Literal(2), new DBSPI32Literal(20, true))),
                new DBSPZSetLiteral.Contents(
                        new DBSPTupleExpression(new DBSPI64Literal(0),
                                DBSPLiteral.none(
                                        new DBSPTypeInteger(CalciteObject.EMPTY, 32, true,true)))));
    }

    @Test
    public void maxTest() {
        String query = "SELECT MAX(T.COL1) FROM T";
        this.testAggregate(query,
                new DBSPZSetLiteral.Contents(
                        new DBSPTupleExpression(new DBSPI32Literal(10, true))),
                new DBSPZSetLiteral.Contents(
                        new DBSPTupleExpression(DBSPLiteral.none(
                                new DBSPTypeInteger(CalciteObject.EMPTY, 32, true,true)))));
    }

    @Test
    public void maxConst() {
        String query = "SELECT MAX(6) FROM T";
        this.testAggregate(query,
                new DBSPZSetLiteral.Contents(
                        new DBSPTupleExpression(new DBSPI32Literal(6, true))),
                new DBSPZSetLiteral.Contents(
                        new DBSPTupleExpression(DBSPLiteral.none(
                                new DBSPTypeInteger(CalciteObject.EMPTY, 32, true,true)))));
    }

    @Test
    public void constAggregateExpression() {
        String query = "SELECT 34 / SUM (1) FROM T GROUP BY COL1";
        this.testQuery(query, new DBSPZSetLiteral.Contents(
                 new DBSPTupleExpression(
                        new DBSPI32Literal(17))));
    }

    @Test
    public void inTest() {
        String query = "SELECT 3 in (SELECT COL5 FROM T)";
        this.testAggregate(query,
                new DBSPZSetLiteral.Contents(new DBSPTupleExpression(
                        DBSPLiteral.none(new DBSPTypeBool(CalciteObject.EMPTY, true)))),
                new DBSPZSetLiteral.Contents(new DBSPTupleExpression(
                        new DBSPBoolLiteral(false, true)))
        );
    }

    @Test
    public void constAggregateExpression2() {
        String query = "SELECT 34 / AVG (1) FROM T GROUP BY COL1";
        this.testQuery(query, new DBSPZSetLiteral.Contents(
                 new DBSPTupleExpression(
                        new DBSPI32Literal(34))));
    }

    @Test
    public void constAggregateDoubleExpression() {
        String query = "SELECT 34 / SUM (1), 20 / SUM(2) FROM T GROUP BY COL1";
        this.testQuery(query, new DBSPZSetLiteral.Contents(
                 new DBSPTupleExpression(
                        new DBSPI32Literal(17), new DBSPI32Literal(5))));
    }

    @Test
    public void aggregateFloatTest() {
        String query = "SELECT SUM(T.COL2) FROM T";
        this.testAggregate(query,
                new DBSPZSetLiteral.Contents(
                        new DBSPTupleExpression(new DBSPDoubleLiteral(13.0, true))),
                new DBSPZSetLiteral.Contents(
                        new DBSPTupleExpression(DBSPLiteral.none(new DBSPTypeDouble(CalciteObject.EMPTY,true)))));
    }

    @Test
    public void optionAggregateTest() {
        String query = "SELECT SUM(T.COL5) FROM T";
        this.testAggregate(query,
                new DBSPZSetLiteral.Contents(
                        new DBSPTupleExpression(new DBSPI32Literal(1, true))),
                new DBSPZSetLiteral.Contents(
                        new DBSPTupleExpression(DBSPLiteral.none(
                                new DBSPTypeInteger(CalciteObject.EMPTY, 32, true,true)))));
    }

    @Test
    public void aggregateFalseTest() {
        String query = "SELECT SUM(T.COL1) FROM T WHERE FALSE";
        this.testConstantOutput(query, new DBSPZSetLiteral.Contents(
                 new DBSPTupleExpression(
                        DBSPLiteral.none(new DBSPTypeInteger(CalciteObject.EMPTY, 32, true,true)))));
    }

    @Test
    public void averageTest() {
        String query = "SELECT AVG(T.COL1) FROM T";
        DBSPZSetLiteral.Contents output = new DBSPZSetLiteral.Contents(
                new DBSPTupleExpression(
                        new DBSPI32Literal(10, true)));
        this.testAggregate(query, output, new DBSPZSetLiteral.Contents(
                new DBSPTupleExpression(DBSPLiteral.none(
                        new DBSPTypeInteger(CalciteObject.EMPTY, 32, true,true)))));
    }

    @Test
    public void cartesianTest() {
        String query = "SELECT * FROM T, T AS X";
        DBSPExpression inResult = DBSPTupleExpression.flatten(e0, e0);
        DBSPZSetLiteral.Contents result = new DBSPZSetLiteral.Contents( inResult);
        result.add(DBSPTupleExpression.flatten(e0, e1));
        result.add(DBSPTupleExpression.flatten(e1, e0));
        result.add(DBSPTupleExpression.flatten(e1, e1));
        this.testQuery(query, result);
    }

    @Test
    public void foldTest() {
        String query = "SELECT + 91 + NULLIF ( + 93, + 38 )";
        this.testConstantOutput(query, new DBSPZSetLiteral.Contents(
                 new DBSPTupleExpression(
                new DBSPI32Literal(184, true))));
    }

    @Test
    public void orderbyTest() {
        String query = "SELECT * FROM T ORDER BY T.COL2";
        this.testQuery(query, new DBSPZSetLiteral.Contents(
                new DBSPVecLiteral(e1, e0)
        ));
    }

    @Test
    public void limitTest() {
        String query = "SELECT * FROM T ORDER BY T.COL2 LIMIT 1";
        this.testQuery(query, new DBSPZSetLiteral.Contents(
                new DBSPVecLiteral(e1)
        ));
    }

    @Test
    public void nestedOrderbyTest() {
        // If the optimizer doesn't remove the inner ORDER BY this test
        // fails because the types don't match in Rust.
        String query = "SELECT COL1 FROM (SELECT * FROM T ORDER BY T.COL2)";
        this.testQuery(query, new DBSPZSetLiteral.Contents(
                new DBSPTupleExpression(new DBSPI32Literal(10)),
                new DBSPTupleExpression(new DBSPI32Literal(10))));
    }

    @Test
    public void orderbyDescendingTest() {
        String query = "SELECT * FROM T ORDER BY T.COL2 DESC";
        this.testQuery(query, new DBSPZSetLiteral.Contents(
                new DBSPVecLiteral(e0, e1)
        ));
    }

    @Test
    public void orderby2Test() {
        String query = "SELECT * FROM T ORDER BY T.COL2, T.COL1";
        this.testQuery(query, new DBSPZSetLiteral.Contents(
                new DBSPVecLiteral(e1, e0)
        ));
    }
}
