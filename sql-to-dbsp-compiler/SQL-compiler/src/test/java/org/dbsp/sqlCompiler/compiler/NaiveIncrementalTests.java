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

package org.dbsp.sqlCompiler.compiler;

import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.*;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeBool;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeDecimal;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeDouble;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeInteger;
import org.junit.Ignore;
import org.junit.Test;

import java.math.BigDecimal;

// Runs the EndToEnd tests but on an input stream with 3 elements each and
// using an incremental non-optimized circuit.
public class NaiveIncrementalTests extends EndToEndTests {
    public void invokeTestQueryBase(String query, InputOutputPair... streams) {
        super.testQueryBase(query, true, false, false, streams);
    }

    @Override
    void testQuery(String query,
                   DBSPZSetLiteral.Contents firstOutput) {
        DBSPZSetLiteral.Contents input = this.createInput();
        DBSPZSetLiteral.Contents secondOutput = DBSPZSetLiteral.Contents.emptyWithElementType(
                firstOutput.getElementType());
        DBSPZSetLiteral.Contents thirdOutput = secondOutput.minus(firstOutput);
        this.invokeTestQueryBase(query,
                // Add first input
                new InputOutputPair(input, firstOutput),
                // Add an empty input
                new InputOutputPair(empty, secondOutput),
                // Subtract the first input
                new InputOutputPair(input.negate(), thirdOutput)
        );
    }

    @Test @Override @Ignore("Crashes https://github.com/feldera/dbsp/issues/34")
    public void idTest() {
        String query = "SELECT * FROM T";
        this.testQuery(query, this.createInput());
    }

    void testConstantOutput(String query,
                            DBSPZSetLiteral.Contents output) {
        DBSPZSetLiteral.Contents input = this.createInput();
        DBSPZSetLiteral.Contents e = DBSPZSetLiteral.Contents.emptyWithElementType(output.getElementType());
        this.invokeTestQueryBase(query,
                // Add first input
                new InputOutputPair(input, output),
                // Add an empty input
                new InputOutputPair(NaiveIncrementalTests.empty, e),
                // Subtract the first input
                new InputOutputPair(input.negate(), e)
        );
    }

    void testAggregate(String query,
                       DBSPZSetLiteral.Contents firstOutput,
                       DBSPZSetLiteral.Contents outputForEmptyInput) {
        DBSPZSetLiteral.Contents input = this.createInput();
        DBSPZSetLiteral.Contents secondOutput = DBSPZSetLiteral.Contents.emptyWithElementType(
                firstOutput.getElementType());
        DBSPZSetLiteral.Contents thirdOutput = outputForEmptyInput.minus(firstOutput);
        this.invokeTestQueryBase(query,
                // Add first input
                new InputOutputPair(input, firstOutput),
                // Add an empty input
                new InputOutputPair(empty, secondOutput),
                // Subtract the first input
                new InputOutputPair(input.negate(), thirdOutput)
        );
    }

    @Test @Override
    public void constantFoldTest() {
        String query = "SELECT 1 + 2";
        this.testConstantOutput(query, new DBSPZSetLiteral.Contents(new DBSPTupleExpression(new DBSPI32Literal(3))));
    }

    @Test @Override
    public void zero() {
        String query = "SELECT 0";
        DBSPZSetLiteral.Contents result = new DBSPZSetLiteral.Contents(
                new DBSPTupleExpression(new DBSPI32Literal(0)));
        this.testConstantOutput(query, result);
    }

    @Test @Override
    public void customDivisionTest() {
        // Use a custom division operator.
        String query = "SELECT DIVISION(1, 0)";
        this.testConstantOutput(query, new DBSPZSetLiteral.Contents(
                new DBSPTupleExpression(DBSPLiteral.none(
                        DBSPTypeInteger.SIGNED_32.setMayBeNull(true)))));
    }

    @Test @Override
    public void inTest() {
        String query = "SELECT 3 in (SELECT COL5 FROM T)";
        this.testAggregate(query,
                new DBSPZSetLiteral.Contents(new DBSPTupleExpression(DBSPLiteral.none(DBSPTypeBool.NULLABLE_INSTANCE))),
                new DBSPZSetLiteral.Contents(new DBSPTupleExpression(new DBSPBoolLiteral(false, true)))
        );
    }

    @Test @Override
    public void correlatedAggregate() {
        String query = "SELECT Sum(r.COL1 * r.COL5) FROM T r\n" +
                "WHERE\n" +
                "0.5 * (SELECT Sum(r1.COL5) FROM T r1) =\n" +
                "(SELECT Sum(r2.COL5) FROM T r2 WHERE r2.COL1 = r.COL1)";
        this.testAggregate(query,
                new DBSPZSetLiteral.Contents(new DBSPTupleExpression(DBSPLiteral.none(DBSPTypeInteger.SIGNED_32.setMayBeNull(true)))),
                new DBSPZSetLiteral.Contents(new DBSPTupleExpression(DBSPLiteral.none(DBSPTypeInteger.SIGNED_32.setMayBeNull(true)))));
    }

    @Test @Override
    public void divZeroTest() {
        String query = "SELECT 1 / 0";
        this.testConstantOutput(query, new DBSPZSetLiteral.Contents(
                new DBSPTupleExpression(DBSPLiteral.none(
                        DBSPTypeInteger.SIGNED_32.setMayBeNull(true)))));
    }

    @Test
    public void decimalParse() {
        String query = "SELECT CAST('0.5' AS DECIMAL)";
        this.testConstantOutput(query, new DBSPZSetLiteral.Contents(
                new DBSPTupleExpression(
                        new DBSPDecimalLiteral(null, DBSPTypeDecimal.DEFAULT,
                                new BigDecimal("0.5")))));
    }

    @Test
    public void decimalParseFail() {
        String query = "SELECT CAST('blah' AS DECIMAL)";
        this.testConstantOutput(query, new DBSPZSetLiteral.Contents(
                new DBSPTupleExpression(
                        new DBSPDecimalLiteral(null, DBSPTypeDecimal.DEFAULT,
                                new BigDecimal(0)))));
    }

    @Test
    public void divZero() {
        String query = "SELECT 'Infinity' / 0";
        this.testConstantOutput(query, new DBSPZSetLiteral.Contents(
                new DBSPTupleExpression(
                        new DBSPDecimalLiteral(null, DBSPTypeDecimal.DEFAULT_NULLABLE,
                                null))));
    }


    @Test @Override
    public void nestedDivTest() {
        String query = "SELECT 2 / (1 / 0)";
        this.testConstantOutput(query, new DBSPZSetLiteral.Contents(
                new DBSPTupleExpression(DBSPLiteral.none(
                        DBSPTypeInteger.SIGNED_32.setMayBeNull(true)))));
    }

    @Test @Override
    public void geoPointTest() {
        String query = "SELECT ST_POINT(0, 0)";
        this.testConstantOutput(query, new DBSPZSetLiteral.Contents(
                new DBSPTupleExpression(
                        new DBSPGeoPointLiteral(null,
                                new DBSPDoubleLiteral(0), new DBSPDoubleLiteral(0)).some())));
    }

    @Override @Test
    public void geoDistanceTest() {
        String query = "SELECT ST_DISTANCE(ST_POINT(0, 0), ST_POINT(0,1))";
        this.testConstantOutput(query, new DBSPZSetLiteral.Contents(
                new DBSPTupleExpression(new DBSPDoubleLiteral(1).some())));
    }

    @Test @Override
    public void foldTest() {
        String query = "SELECT + 91 + NULLIF ( + 93, + 38 )";
        DBSPZSetLiteral.Contents result = new DBSPZSetLiteral.Contents(
                new DBSPTupleExpression(
                new DBSPI32Literal(184, true)));
        this.testConstantOutput(query, result);
    }

    @Test @Override
    public void aggregateFalseTest() {
        String query = "SELECT SUM(T.COL1) FROM T WHERE FALSE";
        DBSPZSetLiteral.Contents result = new DBSPZSetLiteral.Contents(
                new DBSPTupleExpression(
                DBSPLiteral.none(DBSPTypeInteger.SIGNED_32.setMayBeNull(true))));
        this.testConstantOutput(query, result);
    }

    @Test @Override
    public void constAggregateDoubleExpression() {
        String query = "SELECT 34 / SUM (1), 20 / SUM(2) FROM T GROUP BY COL1";
        this.testQuery(query,
                new DBSPZSetLiteral.Contents(
                        new DBSPTupleExpression(
                                new DBSPI32Literal(17, true),
                                new DBSPI32Literal(5, true))));
    }

    @Test @Override
    public void constAggregateExpression2() {
        String query = "SELECT 34 / AVG (1) FROM T GROUP BY COL1";
        this.testQuery(query,
                new DBSPZSetLiteral.Contents(
                        new DBSPTupleExpression(new DBSPI32Literal(34, true))));
    }

    @Test @Override
    public void maxConst() {
        String query = "SELECT MAX(6) FROM T";
        this.testAggregate(query,
                new DBSPZSetLiteral.Contents(
                        new DBSPTupleExpression(new DBSPI32Literal(6, true))),
                new DBSPZSetLiteral.Contents(
                        new DBSPTupleExpression(DBSPLiteral.none(DBSPTypeInteger.SIGNED_32.setMayBeNull(true)))));
    }

    @Test @Override
    public void maxTest() {
        String query = "SELECT MAX(T.COL1) FROM T";
        this.testAggregate(query,
                new DBSPZSetLiteral.Contents(
                        new DBSPTupleExpression(new DBSPI32Literal(10, true))),
                new DBSPZSetLiteral.Contents(
                        new DBSPTupleExpression(DBSPLiteral.none(DBSPTypeInteger.SIGNED_32.setMayBeNull(true)))));
    }

    @Test @Override
    public void averageTest() {
        String query = "SELECT AVG(T.COL1) FROM T";
        DBSPZSetLiteral.Contents output = new DBSPZSetLiteral.Contents(
                new DBSPTupleExpression(
                new DBSPI32Literal(10, true)));
        this.testAggregate(query, output, new DBSPZSetLiteral.Contents(
                new DBSPTupleExpression(DBSPLiteral.none(DBSPTypeInteger.SIGNED_32.setMayBeNull(true)))));
    }

    @Test @Override
    public void aggregateFloatTest() {
        String query = "SELECT SUM(T.COL2) FROM T";
        this.testAggregate(query,
                new DBSPZSetLiteral.Contents(
                        new DBSPTupleExpression(new DBSPDoubleLiteral(13.0, true))),
                new DBSPZSetLiteral.Contents(
                        new DBSPTupleExpression(DBSPLiteral.none(DBSPTypeDouble.NULLABLE_INSTANCE))));
    }

    @Test @Override
    public void optionAggregateTest() {
        String query = "SELECT SUM(T.COL5) FROM T";
        this.testAggregate(query,
                new DBSPZSetLiteral.Contents(
                        new DBSPTupleExpression(new DBSPI32Literal(1, true))),
                new DBSPZSetLiteral.Contents(
                        new DBSPTupleExpression(DBSPLiteral.none(DBSPTypeInteger.NULLABLE_SIGNED_32))));
    }

    @Test @Override
    public void aggregateTest() {
        String query = "SELECT SUM(T.COL1) FROM T";
        this.testAggregate(query,
                new DBSPZSetLiteral.Contents(
                        new DBSPTupleExpression(new DBSPI32Literal(20, true))),
                new DBSPZSetLiteral.Contents(
                        new DBSPTupleExpression(DBSPLiteral.none(DBSPTypeInteger.NULLABLE_SIGNED_32))));
    }

    @Test @Override
    public void aggregateDistinctTest() {
        String query = "SELECT SUM(DISTINCT T.COL1), SUM(T.COL2) FROM T";
        this.testAggregate(query,
                new DBSPZSetLiteral.Contents(
                        new DBSPTupleExpression(
                                new DBSPI32Literal(10, true),
                                new DBSPDoubleLiteral(13.0, true))),
                new DBSPZSetLiteral.Contents(
                        new DBSPTupleExpression(
                                DBSPLiteral.none(DBSPTypeInteger.NULLABLE_SIGNED_32),
                                DBSPLiteral.none(DBSPTypeDouble.NULLABLE_INSTANCE))));
    }

    @Test
    public void testArray() {
        String query = "SELECT ELEMENT(ARRAY [2])";
        DBSPZSetLiteral.Contents result = new DBSPZSetLiteral.Contents(new DBSPTupleExpression(new DBSPI32Literal(2)));
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
        DBSPZSetLiteral.Contents result = new DBSPZSetLiteral.Contents(new DBSPTupleExpression(DBSPLiteral.none(DBSPTypeInteger.NULLABLE_SIGNED_32)));
        this.testConstantOutput(query, result);
    }
}
