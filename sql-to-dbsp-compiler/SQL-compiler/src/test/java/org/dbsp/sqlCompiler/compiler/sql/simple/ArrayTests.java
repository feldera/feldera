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

package org.dbsp.sqlCompiler.compiler.sql.simple;

import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.compiler.sql.BaseSQLTests;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.CalciteObject;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPI32Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPNullLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPVecLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPZSetLiteral;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeInteger;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Objects;

public class ArrayTests extends BaseSQLTests {
    public DBSPCompiler compileQuery(String statements, String query) {
        DBSPCompiler compiler = this.testCompiler();
        compiler.compileStatements(statements);
        compiler.compileStatement(query);
        return compiler;
    }

    void testQuery(String statements, String query, InputOutputPair... streams) {
        query = "CREATE VIEW V AS " + query;
        DBSPCompiler compiler = this.compileQuery(statements, query);
        DBSPCircuit circuit = getCircuit(compiler);
        this.addRustTestCase(query, compiler, circuit, streams);
    }

    @Test
    public void testArray() {
        String ddl = """
                CREATE TABLE ARR_TABLE (
                ID INTEGER,
                VALS INTEGER ARRAY,
                VALVALS VARCHAR(10) ARRAY)""";
        String query = "SELECT *, CARDINALITY(VALS), ARRAY[ID, 5], VALS[1] FROM ARR_TABLE";
        this.testQuery(ddl, query);
    }

    @Test
    public void testUnnest() {
        String query = "SELECT * FROM UNNEST(ARRAY [1, 2, 3, 4, 5])";
        DBSPZSetLiteral.Contents result = null;
        for (int i = 1; i < 6; i++) {
            DBSPTupleExpression tuple = new DBSPTupleExpression(new DBSPI32Literal(i));
            if (i == 1)
                result = new DBSPZSetLiteral.Contents(tuple);
            else
                Objects.requireNonNull(result).add(tuple);
        }

        this.testQuery("", query, new InputOutputPair(
                new DBSPZSetLiteral.Contents[0], new DBSPZSetLiteral.Contents[]{ result }));
    }

    @Test
    public void testUnnestDuplicate() {
        String query = "SELECT * FROM UNNEST(ARRAY [1, 1, 1])";
        DBSPZSetLiteral.Contents result = null;
        for (int i = 1; i < 4; i++) {
            DBSPTupleExpression tuple = new DBSPTupleExpression(new DBSPI32Literal(1));
            if (i == 1)
                result = new DBSPZSetLiteral.Contents(tuple);
            else
                Objects.requireNonNull(result).add(tuple);
        }

        this.testQuery("", query, new InputOutputPair(
                new DBSPZSetLiteral.Contents[0], new DBSPZSetLiteral.Contents[]{ result }));
    }

    @Test
    public void testUnnestNull() {
        String query = "SELECT * FROM UNNEST(ARRAY [1, 2, 3, 4, NULL])";
        DBSPZSetLiteral.Contents result = null;
        for (int i = 1; i < 5; i++) {
            DBSPTupleExpression tuple = new DBSPTupleExpression(new DBSPI32Literal(i, true));
            if (i == 1)
                result = new DBSPZSetLiteral.Contents(tuple);
            else
                Objects.requireNonNull(result).add(tuple);
        }
        result.add(new DBSPTupleExpression(DBSPLiteral.none(new DBSPTypeInteger(CalciteObject.EMPTY, 32, true,true))));
        this.testQuery("", query, new InputOutputPair(
                new DBSPZSetLiteral.Contents[0], new DBSPZSetLiteral.Contents[]{ result }));
    }

    @Test
    public void testUnnestOrdinality() {
        String query = "SELECT * FROM UNNEST(ARRAY [1, 2, 3, 4, 5]) WITH ORDINALITY";
        DBSPZSetLiteral.Contents result = null;
        for (int i = 1; i < 6; i++) {
            DBSPTupleExpression tuple = new DBSPTupleExpression(
                    new DBSPI32Literal(i),
                    new DBSPI32Literal(i));
            if (i == 1)
                result = new DBSPZSetLiteral.Contents(tuple);
            else
                Objects.requireNonNull(result).add(tuple);
        }
        this.testQuery("", query, new InputOutputPair(
                new DBSPZSetLiteral.Contents[0], new DBSPZSetLiteral.Contents[]{ result }));
    }

    @Test
    public void testUnnestOrdinalityNull() {
        String query = "SELECT * FROM UNNEST(ARRAY [1, 2, 3, 4, 5, NULL]) WITH ORDINALITY";
        DBSPZSetLiteral.Contents result = null;
        for (int i = 1; i < 6; i++) {
            DBSPTupleExpression tuple = new DBSPTupleExpression(
                    new DBSPI32Literal(i, true),
                    new DBSPI32Literal(i));
            if (i == 1)
                result = new DBSPZSetLiteral.Contents(tuple);
            else
                Objects.requireNonNull(result).add(tuple);
        }
        result.add(new DBSPTupleExpression(
                DBSPLiteral.none(new DBSPTypeInteger(CalciteObject.EMPTY, 32, true,true)),
                new DBSPI32Literal(6)));

        this.testQuery("", query, new InputOutputPair(
                new DBSPZSetLiteral.Contents[0], new DBSPZSetLiteral.Contents[]{ result }));
    }

    @Test @Ignore("UNNEST with 2 arguments not yet implemented")
    public void testUnnest2() {
        String query = "SELECT * FROM UNNEST(ARRAY [1, 2, 3, 4, 5], ARRAY[3, 2, 1])";
        DBSPZSetLiteral.Contents result = null;
        for (int i = 1; i < 6; i++) {
            DBSPTupleExpression tuple = new DBSPTupleExpression(
                    new DBSPI32Literal(i),
                    i < 4 ? new DBSPI32Literal(4 - 1, true) :
                            DBSPLiteral.none(new DBSPTypeInteger(CalciteObject.EMPTY, 32, true,true)));
            if (i == 1)
                result = new DBSPZSetLiteral.Contents(tuple);
            else
                Objects.requireNonNull(result).add(tuple);
        }

        this.testQuery("", query, new InputOutputPair(
                new DBSPZSetLiteral.Contents[0], new DBSPZSetLiteral.Contents[]{ result }));
    }

    @Test
    public void testUnnest1() {
        String ddl = "CREATE TABLE ARR_TABLE (\n"
                + "VALS INTEGER ARRAY NOT NULL,"
                + "ID INTEGER NOT NULL)";
        String query = "SELECT VAL, ID FROM " +
                "ARR_TABLE, UNNEST(VALS) AS VAL";
        DBSPZSetLiteral.Contents input = new DBSPZSetLiteral.Contents(
                new DBSPTupleExpression(
                        new DBSPVecLiteral(
                                new DBSPI32Literal(1),
                                new DBSPI32Literal(2),
                                new DBSPI32Literal(3)),
                        new DBSPI32Literal(6)),
                new DBSPTupleExpression(
                        new DBSPVecLiteral(
                                new DBSPI32Literal(1),
                                new DBSPI32Literal(2),
                                new DBSPI32Literal(3)),
                        new DBSPI32Literal(7))
                );
        DBSPZSetLiteral.Contents result = new DBSPZSetLiteral.Contents(
                new DBSPTupleExpression(new DBSPI32Literal(1), new DBSPI32Literal(6)),
                new DBSPTupleExpression(new DBSPI32Literal(2), new DBSPI32Literal(6)),
                new DBSPTupleExpression(new DBSPI32Literal(3), new DBSPI32Literal(6)),
                new DBSPTupleExpression(new DBSPI32Literal(1), new DBSPI32Literal(7)),
                new DBSPTupleExpression(new DBSPI32Literal(2), new DBSPI32Literal(7)),
                new DBSPTupleExpression(new DBSPI32Literal(3), new DBSPI32Literal(7))
        );
        this.testQuery(ddl, query, new InputOutputPair(input, result));
    }

    @Test
    public void testDoubleUnnest1() {
        String ddl = "CREATE TABLE ARR_TABLE (\n"
                + "VALS0 INTEGER ARRAY NOT NULL,"
                + "VALS1 INTEGER ARRAY NOT NULL,"
                + "ID INTEGER NOT NULL)";
        String query = "SELECT VAL0, VAL1, ID FROM " +
                "ARR_TABLE, UNNEST(VALS0) AS VAL0, UNNEST(VALS1) AS VAL1";
        DBSPZSetLiteral.Contents input = new DBSPZSetLiteral.Contents(
                new DBSPTupleExpression(
                        new DBSPVecLiteral(
                                new DBSPI32Literal(1),
                                new DBSPI32Literal(2),
                                new DBSPI32Literal(3)),
                        new DBSPVecLiteral(
                                new DBSPI32Literal(4),
                                new DBSPI32Literal(5),
                                new DBSPI32Literal(6)),
                        new DBSPI32Literal(7)),
                new DBSPTupleExpression(
                        new DBSPVecLiteral(
                                new DBSPI32Literal(8),
                                new DBSPI32Literal(9),
                                new DBSPI32Literal(10)),
                        new DBSPVecLiteral(
                                new DBSPI32Literal(11),
                                new DBSPI32Literal(12),
                                new DBSPI32Literal(13)),
                        new DBSPI32Literal(14))
        );
        DBSPZSetLiteral.Contents result = DBSPZSetLiteral.Contents.emptyWithElementType(
            new DBSPTypeTuple(new DBSPTypeInteger(CalciteObject.EMPTY, 32, true,false), new DBSPTypeInteger(CalciteObject.EMPTY, 32, true,false), new DBSPTypeInteger(CalciteObject.EMPTY, 32, true,false))
        );
        for (int i = 1; i < 4; i++)
            for (int j = 4; j < 7; j++) {
                DBSPExpression tuple = new DBSPTupleExpression(new DBSPI32Literal(i), new DBSPI32Literal(j), new DBSPI32Literal(7));
                result.add(tuple);
            }
        for (int i = 8; i < 11; i++)
            for (int j = 11; j < 14; j++) {
                DBSPExpression tuple = new DBSPTupleExpression(new DBSPI32Literal(i), new DBSPI32Literal(j), new DBSPI32Literal(14));
                result.add(tuple);
            }
        this.testQuery(ddl, query, new InputOutputPair(input, result));
    }

    @Test
    public void test2DArray() {
        String ddl = "CREATE TABLE ARR_TABLE (\n"
                + "VALS INTEGER ARRAY ARRAY)";
        String query = "SELECT *, CARDINALITY(VALS), VALS[1] FROM ARR_TABLE";
        this.testQuery(ddl, query);
    }

    @Test @Ignore("https://issues.apache.org/jira/projects/CALCITE/issues/CALCITE-6228")
    public void test2DArrayElements() {
        String ddl = "CREATE TABLE ARR_TABLE (\n"
                + "VALS INTEGER ARRAY ARRAY)";
        String query = "SELECT *, CARDINALITY(VALS), VALS[1], ELEMENT(VALS), ELEMENT(VALS[1]) FROM ARR_TABLE";
        this.testQuery(ddl, query);
    }

    @Test
    public void testConstants() {
        String query = "SELECT ARRAY[2,3][2], CARDINALITY(ARRAY[2,3]), ELEMENT(ARRAY[2])";
        this.testQuery("", query, new InputOutputPair(new DBSPZSetLiteral.Contents[0],
                new DBSPZSetLiteral.Contents[]{ new DBSPZSetLiteral.Contents(
                        new DBSPTupleExpression(
                                new DBSPI32Literal(3, true),
                                new DBSPI32Literal(2),
                                new DBSPI32Literal(2))
                ) }));
    }

    @Test @Ignore("https://issues.apache.org/jira/projects/CALCITE/issues/CALCITE-6227")
    public void testElementNull() {
        this.testQuery("", "SELECT ELEMENT(NULL)", new InputOutputPair(new DBSPZSetLiteral.Contents[0],
                new DBSPZSetLiteral.Contents[]{ new DBSPZSetLiteral.Contents(
                        new DBSPTupleExpression(new DBSPNullLiteral())) }));
    }

    @Test
    public void testOutOfBounds() {
        this.runtimeConstantFail("SELECT ELEMENT(ARRAY [2, 3])", "array that does not have exactly 1 element");
    }
}
