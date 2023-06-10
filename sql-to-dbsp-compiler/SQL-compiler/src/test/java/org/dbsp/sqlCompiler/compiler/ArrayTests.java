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

package org.dbsp.sqlCompiler.compiler;

import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.compiler.backend.DBSPCompiler;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPI32Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPVecLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPZSetLiteral;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeInteger;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Objects;

public class ArrayTests extends BaseSQLTests {
    @Test
    public void testArray() {
        String ddl = "CREATE TABLE ARR_TABLE (\n"
                + "ID INTEGER,\n"
                + "VALS INTEGER ARRAY,\n"
                + "VALVALS VARCHAR(10) ARRAY)";
        DBSPCompiler compiler = testCompiler();
        compiler.compileStatement(ddl);
        String query = "CREATE VIEW V AS SELECT *, CARDINALITY(VALS), ARRAY[ID, 5], VALS[1] FROM ARR_TABLE";
        compiler.compileStatements(query);
        if (compiler.hasErrors())
            compiler.showErrors(System.err);
        DBSPCircuit circuit = getCircuit(compiler);
        this.addRustTestCase(compiler, circuit);
    }

    @Test
    public void testUnnest() {
        DBSPCompiler compiler = testCompiler();
        String query = "CREATE VIEW V AS SELECT * FROM UNNEST(ARRAY [1, 2, 3, 4, 5])";
        compiler.compileStatements(query);
        if (compiler.hasErrors())
            compiler.showErrors(System.err);
        DBSPCircuit circuit = getCircuit(compiler);

        DBSPZSetLiteral.Contents result = null;
        for (int i = 1; i < 6; i++) {
            DBSPTupleExpression tuple = new DBSPTupleExpression(new DBSPI32Literal(i));
            if (i == 1)
                result = new DBSPZSetLiteral.Contents(tuple);
            else
                Objects.requireNonNull(result).add(tuple);
        }

        this.addRustTestCase(compiler, circuit, new InputOutputPair(
                new DBSPZSetLiteral.Contents[0], new DBSPZSetLiteral.Contents[]{ result }));
    }

    @Test
    public void testUnnestDuplicate() {
        DBSPCompiler compiler = testCompiler();
        String query = "CREATE VIEW V AS SELECT * FROM UNNEST(ARRAY [1, 1, 1])";
        compiler.compileStatements(query);
        if (compiler.hasErrors())
            compiler.showErrors(System.err);
        DBSPCircuit circuit = getCircuit(compiler);

        DBSPZSetLiteral.Contents result = null;
        for (int i = 1; i < 4; i++) {
            DBSPTupleExpression tuple = new DBSPTupleExpression(new DBSPI32Literal(1));
            if (i == 1)
                result = new DBSPZSetLiteral.Contents(tuple);
            else
                Objects.requireNonNull(result).add(tuple);
        }

        this.addRustTestCase(compiler, circuit, new InputOutputPair(
                new DBSPZSetLiteral.Contents[0], new DBSPZSetLiteral.Contents[]{ result }));
    }

    @Test
    public void testUnnestNull() {
        DBSPCompiler compiler = testCompiler();
        String query = "CREATE VIEW V AS SELECT * FROM UNNEST(ARRAY [1, 2, 3, 4, NULL])";
        compiler.compileStatements(query);
        if (compiler.hasErrors())
            compiler.showErrors(System.err);
        DBSPCircuit circuit = getCircuit(compiler);

        DBSPZSetLiteral.Contents result = null;
        for (int i = 1; i < 5; i++) {
            DBSPTupleExpression tuple = new DBSPTupleExpression(new DBSPI32Literal(i, true));
            if (i == 1)
                result = new DBSPZSetLiteral.Contents(tuple);
            else
                Objects.requireNonNull(result).add(tuple);
        }
        result.add(new DBSPTupleExpression(DBSPLiteral.none(DBSPTypeInteger.NULLABLE_SIGNED_32)));

        this.addRustTestCase(compiler, circuit, new InputOutputPair(
                new DBSPZSetLiteral.Contents[0], new DBSPZSetLiteral.Contents[]{ result }));
    }

    @Test
    public void testUnnestOrdinality() {
        DBSPCompiler compiler = testCompiler();
        String query = "CREATE VIEW V AS SELECT * FROM UNNEST(ARRAY [1, 2, 3, 4, 5]) WITH ORDINALITY";
        compiler.compileStatements(query);
        if (compiler.hasErrors())
            compiler.showErrors(System.err);
        DBSPCircuit circuit = getCircuit(compiler);

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

        this.addRustTestCase(compiler, circuit, new InputOutputPair(
                new DBSPZSetLiteral.Contents[0], new DBSPZSetLiteral.Contents[]{ result }));
    }

    @Test
    public void testUnnestOrdinalityNull() {
        DBSPCompiler compiler = testCompiler();
        String query = "CREATE VIEW V AS SELECT * FROM UNNEST(ARRAY [1, 2, 3, 4, 5, NULL]) WITH ORDINALITY";
        compiler.compileStatements(query);
        if (compiler.hasErrors())
            compiler.showErrors(System.err);
        DBSPCircuit circuit = getCircuit(compiler);

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
                DBSPLiteral.none(DBSPTypeInteger.NULLABLE_SIGNED_32),
                new DBSPI32Literal(6)));

        this.addRustTestCase(compiler, circuit, new InputOutputPair(
                new DBSPZSetLiteral.Contents[0], new DBSPZSetLiteral.Contents[]{ result }));
    }

    @Test @Ignore("the Calcite type seems to be wrong, it has no nullable columns")
    public void testUnnest2() {
        DBSPCompiler compiler = testCompiler();
        String query = "CREATE VIEW V AS SELECT * FROM UNNEST(ARRAY [1, 2, 3, 4, 5], ARRAY[3, 2, 1])";
        compiler.compileStatements(query);
        if (compiler.hasErrors())
            compiler.showErrors(System.err);
        DBSPCircuit circuit = getCircuit(compiler);

        DBSPZSetLiteral.Contents result = null;
        for (int i = 1; i < 6; i++) {
            DBSPTupleExpression tuple = new DBSPTupleExpression(
                    new DBSPI32Literal(i),
                    i < 4 ? new DBSPI32Literal(4 - 1) : DBSPLiteral.none(DBSPTypeInteger.NULLABLE_SIGNED_32));
            if (i == 1)
                result = new DBSPZSetLiteral.Contents(tuple);
            else
                Objects.requireNonNull(result).add(tuple);
        }

        this.addRustTestCase(compiler, circuit, new InputOutputPair(
                new DBSPZSetLiteral.Contents[0], new DBSPZSetLiteral.Contents[]{ result }));
    }

    @Test
    public void testUnnest1() {
        String ddl = "CREATE TABLE ARR_TABLE (\n"
                + "VALS INTEGER ARRAY NOT NULL,"
                + "ID INTEGER NOT NULL)";
        DBSPCompiler compiler = testCompiler();
        compiler.compileStatement(ddl);
        String query = "CREATE VIEW V AS SELECT VAL, ID FROM " +
                "ARR_TABLE, UNNEST(VALS) AS VAL";
        compiler.compileStatements(query);
        if (compiler.hasErrors())
            compiler.showErrors(System.err);
        DBSPCircuit circuit = getCircuit(compiler);
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
        this.addRustTestCase(compiler, circuit, new InputOutputPair(input, result));
    }

    @Test
    public void testDoubleUnnest1() {
        String ddl = "CREATE TABLE ARR_TABLE (\n"
                + "VALS0 INTEGER ARRAY NOT NULL,"
                + "VALS1 INTEGER ARRAY NOT NULL,"
                + "ID INTEGER NOT NULL)";
        DBSPCompiler compiler = testCompiler();
        compiler.compileStatement(ddl);
        String query = "CREATE VIEW V AS SELECT VAL0, VAL1, ID FROM " +
                "ARR_TABLE, UNNEST(VALS0) AS VAL0, UNNEST(VALS1) AS VAL1";
        compiler.compileStatements(query);
        if (compiler.hasErrors())
            compiler.showErrors(System.err);
        DBSPCircuit circuit = getCircuit(compiler);
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
            new DBSPTypeTuple(DBSPTypeInteger.SIGNED_32, DBSPTypeInteger.SIGNED_32, DBSPTypeInteger.SIGNED_32)
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
        this.addRustTestCase(compiler, circuit, new InputOutputPair(input, result));
    }

    @Test @Ignore("TODO: handle nested arrays")
    public void test2DArray() {
        String ddl = "CREATE TABLE ARR_TABLE (\n"
                + "VALS INTEGER ARRAY ARRAY)";
        DBSPCompiler compiler = testCompiler();
        compiler.compileStatement(ddl);
        String query = "CREATE VIEW V AS SELECT *, CARDINALITY(VALS), VALS[1] FROM ARR_TABLE";
        compiler.compileStatements(query);
        if (compiler.hasErrors())
            compiler.showErrors(System.err);
        DBSPCircuit circuit = getCircuit(compiler);
        this.addRustTestCase(compiler, circuit);
    }
}
