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

import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.sql.tools.BaseSQLTests;
import org.dbsp.sqlCompiler.compiler.sql.tools.Change;
import org.dbsp.sqlCompiler.compiler.sql.tools.CompilerCircuitStream;
import org.dbsp.sqlCompiler.compiler.sql.tools.InputOutputChange;
import org.dbsp.sqlCompiler.compiler.sql.tools.InputOutputChangeStream;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPI32Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPNullLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPStringLiteral;
import org.dbsp.sqlCompiler.ir.expression.DBSPArrayExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPZSetExpression;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeInteger;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeArray;
import org.dbsp.util.Linq;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Objects;

public class ArrayTests extends BaseSQLTests {
    public DBSPCompiler compileQuery(String statements, String query) {
        DBSPCompiler compiler = this.testCompiler();
        compiler.submitStatementsForCompilation(statements);
        compiler.submitStatementForCompilation(query);
        return compiler;
    }

    void testQuery(String statements, String query, InputOutputChangeStream streams) {
        query = "CREATE VIEW V AS " + query;
        DBSPCompiler compiler = this.compileQuery(statements, query);
        CompilerCircuitStream ccs = new CompilerCircuitStream(compiler, streams);
        this.addRustTestCase(ccs);
    }

    private void testQuery(String statements, String query) {
        this.testQuery(statements, query, new InputOutputChangeStream());
    }

    @Test
    public void issue1922() {
        String statements = """
                CREATE TYPE foo_struct AS (
                    id bigint NOT NULL
                );

                create table bar (
                    vals foo_struct ARRAY
                );""";
        String query = "SELECT * FROM bar, UNNEST(bar.vals)";
        DBSPTypeArray vecType = new DBSPTypeArray(
                new DBSPTypeTuple(
                        CalciteObject.EMPTY, true,
                        Linq.list(new DBSPTypeInteger(CalciteObject.EMPTY, 64, true, false))
                ), true);
        // null vector
        Change input = new Change(new DBSPZSetExpression(new DBSPTupleExpression(
                Linq.list(new DBSPArrayExpression(vecType, true)), false)));
        Change output = new Change();
        InputOutputChangeStream stream = new InputOutputChangeStream().addPair(input, output);
        this.testQuery(statements, query, stream);
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
        String query = "SELECT i*2 FROM UNNEST(ARRAY [1, 2, 3, 4, 5]) AS T(i)";
        DBSPZSetExpression result = null;
        for (int i = 1; i < 6; i++) {
            DBSPTupleExpression tuple = new DBSPTupleExpression(new DBSPI32Literal(i * 2));
            if (i == 1)
                result = new DBSPZSetExpression(tuple);
            else
                Objects.requireNonNull(result).add(tuple);
        }

        this.testQuery("", query, new InputOutputChange(
                new Change(), new Change(result)).toStream());
    }

    @Test
    public void testUnnestDuplicate() {
        String query = "SELECT * FROM UNNEST(ARRAY [1, 1, 1])";
        DBSPZSetExpression result = null;
        for (int i = 1; i < 4; i++) {
            DBSPTupleExpression tuple = new DBSPTupleExpression(new DBSPI32Literal(1));
            if (i == 1)
                result = new DBSPZSetExpression(tuple);
            else
                Objects.requireNonNull(result).add(tuple);
        }

        this.testQuery("", query, new InputOutputChange(
                new Change(), new Change(result)).toStream());
    }

    @Test
    public void testUnnestNull() {
        String query = "SELECT * FROM UNNEST(ARRAY [1, 2, 3, 4, NULL])";
        DBSPZSetExpression result = null;
        for (int i = 1; i < 5; i++) {
            DBSPTupleExpression tuple = new DBSPTupleExpression(new DBSPI32Literal(i, true));
            if (i == 1)
                result = new DBSPZSetExpression(tuple);
            else
                Objects.requireNonNull(result).add(tuple);
        }
        result.add(new DBSPTupleExpression(
                DBSPLiteral.none(new DBSPTypeInteger(CalciteObject.EMPTY, 32, true,true))));
        this.testQuery("", query, new InputOutputChange(
                new Change(), new Change(result)).toStream());
    }

    @Test
    public void testUnnestOrdinality() {
        String query = "SELECT * FROM UNNEST(ARRAY [1, 2, 3, 4, 5]) WITH ORDINALITY";
        DBSPZSetExpression result = null;
        for (int i = 1; i < 6; i++) {
            DBSPTupleExpression tuple = new DBSPTupleExpression(
                    new DBSPI32Literal(i),
                    new DBSPI32Literal(i));
            if (i == 1)
                result = new DBSPZSetExpression(tuple);
            else
                Objects.requireNonNull(result).add(tuple);
        }
        this.testQuery("", query, new InputOutputChange(
                new Change(), new Change(result)).toStream());
    }

    @Test
    public void testUnnestOrdinalityNull() {
        String query = "SELECT * FROM UNNEST(ARRAY [1, 2, 3, 4, 5, NULL]) WITH ORDINALITY";
        DBSPZSetExpression result = null;
        for (int i = 1; i < 6; i++) {
            DBSPTupleExpression tuple = new DBSPTupleExpression(
                    new DBSPI32Literal(i, true),
                    new DBSPI32Literal(i));
            if (i == 1)
                result = new DBSPZSetExpression(tuple);
            else
                Objects.requireNonNull(result).add(tuple);
        }
        result.add(new DBSPTupleExpression(
                DBSPLiteral.none(new DBSPTypeInteger(CalciteObject.EMPTY, 32, true,true)),
                new DBSPI32Literal(6)));

        this.testQuery("", query, new InputOutputChange(
                new Change(), new Change(result)).toStream());
    }

    @Test @Ignore("UNNEST with 2 arguments not yet implemented")
    public void testUnnest2() {
        String query = "SELECT * FROM UNNEST(ARRAY [1, 2, 3, 4, 5], ARRAY[3, 2, 1])";
        DBSPZSetExpression result = null;
        for (int i = 1; i < 6; i++) {
            DBSPTupleExpression tuple = new DBSPTupleExpression(
                    new DBSPI32Literal(i),
                    i < 4 ? new DBSPI32Literal(4 - 1, true) :
                            DBSPLiteral.none(new DBSPTypeInteger(CalciteObject.EMPTY, 32, true,true)));
            if (i == 1)
                result = new DBSPZSetExpression(tuple);
            else
                Objects.requireNonNull(result).add(tuple);
        }

        this.testQuery("", query, new InputOutputChange(
                new Change(), new Change(result)).toStream());
    }

    @Test
    public void testUnnest1() {
        String ddl = "CREATE TABLE ARR_TABLE (\n"
                + "VALS INTEGER ARRAY NOT NULL,"
                + "ID INTEGER NOT NULL)";
        String query = "SELECT VAL * 2, ID FROM " +
                "ARR_TABLE, UNNEST(VALS) AS VAL";
        DBSPZSetExpression input = new DBSPZSetExpression(
                new DBSPTupleExpression(
                        new DBSPArrayExpression(
                                new DBSPI32Literal(1, true),
                                new DBSPI32Literal(2, true),
                                new DBSPI32Literal(3, true)),
                        new DBSPI32Literal(6)),
                new DBSPTupleExpression(
                        new DBSPArrayExpression(
                                new DBSPI32Literal(1, true),
                                new DBSPI32Literal(2, true),
                                new DBSPI32Literal(3, true)),
                        new DBSPI32Literal(7))
                );
        DBSPZSetExpression result = new DBSPZSetExpression(
                new DBSPTupleExpression(new DBSPI32Literal(2, true), new DBSPI32Literal(6)),
                new DBSPTupleExpression(new DBSPI32Literal(4, true), new DBSPI32Literal(6)),
                new DBSPTupleExpression(new DBSPI32Literal(6, true), new DBSPI32Literal(6)),
                new DBSPTupleExpression(new DBSPI32Literal(2, true), new DBSPI32Literal(7)),
                new DBSPTupleExpression(new DBSPI32Literal(4, true), new DBSPI32Literal(7)),
                new DBSPTupleExpression(new DBSPI32Literal(6, true), new DBSPI32Literal(7))
        );
        this.testQuery(ddl, query, new InputOutputChange(
                new Change(input), new Change(result)).toStream());
    }

    @Test
    public void testDoubleUnnest1() {
        String ddl = "CREATE TABLE ARR_TABLE (\n"
                + "VALS0 INTEGER ARRAY NOT NULL,"
                + "VALS1 INTEGER ARRAY NOT NULL,"
                + "ID INTEGER NOT NULL)";
        String query = "SELECT VAL0, VAL1, ID FROM " +
                "ARR_TABLE, UNNEST(VALS0) AS VAL0, UNNEST(VALS1) AS VAL1";
        DBSPZSetExpression input = new DBSPZSetExpression(
                new DBSPTupleExpression(
                        new DBSPArrayExpression(
                                new DBSPI32Literal(1, true),
                                new DBSPI32Literal(2, true),
                                new DBSPI32Literal(3, true)),
                        new DBSPArrayExpression(
                                new DBSPI32Literal(4, true),
                                new DBSPI32Literal(5, true),
                                new DBSPI32Literal(6, true)),
                        new DBSPI32Literal(7)),
                new DBSPTupleExpression(
                        new DBSPArrayExpression(
                                new DBSPI32Literal(8, true),
                                new DBSPI32Literal(9, true),
                                new DBSPI32Literal(10, true)),
                        new DBSPArrayExpression(
                                new DBSPI32Literal(11, true),
                                new DBSPI32Literal(12, true),
                                new DBSPI32Literal(13, true)),
                        new DBSPI32Literal(14))
        );
        DBSPZSetExpression result = DBSPZSetExpression.emptyWithElementType(
            new DBSPTypeTuple(
                    new DBSPTypeInteger(CalciteObject.EMPTY, 32, true, true),
                    new DBSPTypeInteger(CalciteObject.EMPTY, 32, true, true),
                    new DBSPTypeInteger(CalciteObject.EMPTY, 32, true, false))
        );
        for (int i = 1; i < 4; i++)
            for (int j = 4; j < 7; j++) {
                DBSPExpression tuple = new DBSPTupleExpression(
                        new DBSPI32Literal(i, true),
                        new DBSPI32Literal(j, true),
                        new DBSPI32Literal(7));
                result.add(tuple);
            }
        for (int i = 8; i < 11; i++)
            for (int j = 11; j < 14; j++) {
                DBSPExpression tuple = new DBSPTupleExpression(
                        new DBSPI32Literal(i, true),
                        new DBSPI32Literal(j, true),
                        new DBSPI32Literal(14));
                result.add(tuple);
            }
        this.testQuery(ddl, query, new InputOutputChange(new Change(input), new Change(result)).toStream());
    }

    @Test
    public void test2DArray() {
        String ddl = "CREATE TABLE ARR_TABLE (\n"
                + "VALS INTEGER ARRAY ARRAY)";
        String query = "SELECT *, CARDINALITY(VALS), VALS[1] FROM ARR_TABLE";
        this.testQuery(ddl, query);
    }

    @Test
    public void test2DArrayElements() {
        String ddl = "CREATE TABLE ARR_TABLE (\n"
                + "VALS INTEGER ARRAY ARRAY)";
        String query = "SELECT *, CARDINALITY(VALS), VALS[1], ELEMENT(VALS), ELEMENT(VALS[1]) FROM ARR_TABLE";
        this.testQuery(ddl, query);
    }

    @Test
    public void testConstants() {
        String query = "SELECT ARRAY[2,3][2], CARDINALITY(ARRAY[2,3]), ELEMENT(ARRAY[2])";
        this.testQuery("", query, new InputOutputChange(new Change(),
                new Change(new DBSPZSetExpression(
                        new DBSPTupleExpression(
                                new DBSPI32Literal(3, true),
                                new DBSPI32Literal(2),
                                new DBSPI32Literal(2, true))
                ))).toStream());
    }

    @Test
    public void testElementNull() {
        this.testQuery("", "SELECT ELEMENT(NULL)", new InputOutputChange(new Change(),
                new Change(new DBSPZSetExpression(
                        new DBSPTupleExpression(new DBSPNullLiteral())))).toStream());
    }

    @Test
    public void testOutOfBounds() {
        this.runtimeConstantFail("SELECT ELEMENT(ARRAY [2, 3])", "array that does not have exactly 1 element");
    }

    @Test
    public void testArrayAppend() {
        String ddl = "CREATE TABLE ARR_TBL(val INTEGER ARRAY NOT NULL)";

        DBSPZSetExpression input = new DBSPZSetExpression(
                new DBSPTupleExpression(
                        new DBSPArrayExpression(
                                new DBSPI32Literal(1, true),
                                new DBSPI32Literal(2, true),
                                new DBSPI32Literal(3, true))
                ),
                new DBSPTupleExpression(
                        new DBSPArrayExpression(
                                new DBSPI32Literal(5, true),
                                new DBSPI32Literal(6, true),
                                new DBSPI32Literal(7, true))
                )
        );
        DBSPZSetExpression result = new DBSPZSetExpression(
                new DBSPTupleExpression(
                        new DBSPArrayExpression(
                                new DBSPI32Literal(1, true),
                                new DBSPI32Literal(2, true),
                                new DBSPI32Literal(3, true),
                                new DBSPI32Literal(4, true))
                ),
                new DBSPTupleExpression(
                        new DBSPArrayExpression(
                                new DBSPI32Literal(5, true),
                                new DBSPI32Literal(6, true),
                                new DBSPI32Literal(7, true),
                                new DBSPI32Literal(4, true))
                )
        );

        this.testQuery(ddl, "SELECT ARRAY_APPEND(val, 4) FROM ARR_TBL",
                new InputOutputChangeStream().addPair(new Change(input), new Change(result)));
    }

    @Test
    public void testArrayAppendNullable() {
        String ddl = "CREATE TABLE ARR_TBL(val INTEGER ARRAY)";
        DBSPZSetExpression input = new DBSPZSetExpression(
                new DBSPTupleExpression(
                        new DBSPArrayExpression(true,
                                new DBSPI32Literal(1, true),
                                new DBSPI32Literal(2, true),
                                new DBSPI32Literal(3, true))
                ),
                new DBSPTupleExpression(
                        new DBSPArrayExpression(true,
                                new DBSPI32Literal(5, true),
                                new DBSPI32Literal(6, true),
                                new DBSPI32Literal(7, true))
                )
        );
        DBSPZSetExpression result = new DBSPZSetExpression(
                new DBSPTupleExpression(
                        new DBSPArrayExpression(true,
                                new DBSPI32Literal(1, true),
                                new DBSPI32Literal(2, true),
                                new DBSPI32Literal(3, true),
                                new DBSPI32Literal(4, true))
                ),
                new DBSPTupleExpression(
                        new DBSPArrayExpression(true,
                                new DBSPI32Literal(5, true),
                                new DBSPI32Literal(6, true),
                                new DBSPI32Literal(7, true),
                                new DBSPI32Literal(4, true))
                )
        );

        this.testQuery(ddl, "SELECT ARRAY_APPEND(val, 4) FROM ARR_TBL",
                new InputOutputChangeStream().addPair(new Change(input), new Change(result)));
    }

    @Test
    public void testArrayAppendInnerNullable() {
        String ddl = "CREATE TABLE ARR_TBL(val INTEGER ARRAY NULL)";
        DBSPZSetExpression input = new DBSPZSetExpression(
                new DBSPTupleExpression(
                        new DBSPArrayExpression(true,
                                DBSPLiteral.none(new DBSPTypeInteger(CalciteObject.EMPTY, 32, true,true)),
                                new DBSPI32Literal(1, true),
                                new DBSPI32Literal(2, true),
                                new DBSPI32Literal(3, true))
                ),
                new DBSPTupleExpression(
                        new DBSPArrayExpression(true,
                                DBSPLiteral.none(new DBSPTypeInteger(CalciteObject.EMPTY, 32, true,true)),
                                new DBSPI32Literal(5, true),
                                new DBSPI32Literal(6, true),
                                new DBSPI32Literal(7, true))
                )
        );
        DBSPZSetExpression result = new DBSPZSetExpression(
                new DBSPTupleExpression(
                        new DBSPArrayExpression(true,
                                DBSPLiteral.none(new DBSPTypeInteger(CalciteObject.EMPTY, 32, true,true)),
                                new DBSPI32Literal(1, true),
                                new DBSPI32Literal(2, true),
                                new DBSPI32Literal(3, true),
                                new DBSPI32Literal(4, true))
                ),
                new DBSPTupleExpression(
                        new DBSPArrayExpression(true,
                                DBSPLiteral.none(new DBSPTypeInteger(CalciteObject.EMPTY, 32, true,true)),
                                new DBSPI32Literal(5, true),
                                new DBSPI32Literal(6, true),
                                new DBSPI32Literal(7, true),
                                new DBSPI32Literal(4, true))
                )
        );

        this.testQuery(ddl, "SELECT ARRAY_APPEND(val, 4) FROM ARR_TBL",
                new InputOutputChangeStream().addPair(new Change(input), new Change(result)));
    }

    @Test
    public void testArrayMaxNullable() {
        String ddl = "CREATE TABLE ARR_TBL(val INTEGER ARRAY)";
        DBSPZSetExpression input = new DBSPZSetExpression(
                new DBSPTupleExpression(
                        new DBSPArrayExpression(true,
                                new DBSPI32Literal(1, true),
                                new DBSPI32Literal(2, true),
                                new DBSPI32Literal(3, true))
                ),
                new DBSPTupleExpression(
                        new DBSPArrayExpression(true,
                                new DBSPI32Literal(5, true),
                                new DBSPI32Literal(6, true),
                                new DBSPI32Literal(7, true))
                )
        );
        DBSPZSetExpression result = new DBSPZSetExpression(
                new DBSPTupleExpression(
                        new DBSPI32Literal(3, true)
                ),
                new DBSPTupleExpression(
                        new DBSPI32Literal(7, true)
                )
        );

        this.testQuery(ddl, "SELECT ARRAY_MAX(val) FROM ARR_TBL",
                new InputOutputChangeStream().addPair(new Change(input), new Change(result)));
    }

    @Test
    public void testArraySubquery() {
        String ddl = "CREATE TABLE PAIRS(x INT NOT NULL, s VARCHAR NOT NULL);";
        DBSPZSetExpression input = new DBSPZSetExpression(
                new DBSPTupleExpression(
                        new DBSPI32Literal(10), new DBSPStringLiteral("hello")),
                new DBSPTupleExpression(
                        new DBSPI32Literal(5), new DBSPStringLiteral("there")));
        DBSPZSetExpression result = new DBSPZSetExpression(
                new DBSPTupleExpression(
                    new DBSPArrayExpression(
                        new DBSPTupleExpression(
                            new DBSPI32Literal(5), new DBSPStringLiteral("there")),
                        new DBSPTupleExpression(
                            new DBSPI32Literal(10), new DBSPStringLiteral("hello"))
                    )));
        this.testQuery(ddl, "SELECT ARRAY(SELECT * FROM PAIRS)",
                new InputOutputChangeStream().addPair(new Change(input), new Change(result)));
    }

    @Test
    public void testArrayMaxInnerNullable() {
        String ddl = "CREATE TABLE ARR_TBL(val INTEGER ARRAY)";

        DBSPZSetExpression input = new DBSPZSetExpression(
                new DBSPTupleExpression(
                        new DBSPArrayExpression(true,
                                new DBSPI32Literal(1, true),
                                new DBSPI32Literal(2, true),
                                DBSPNullLiteral.none(new DBSPTypeInteger(CalciteObject.EMPTY, 32, true, true)),
                                new DBSPI32Literal(3, true))
                ),
                new DBSPTupleExpression(
                        new DBSPArrayExpression(true,
                                new DBSPI32Literal(5, true),
                                new DBSPI32Literal(6, true),
                                new DBSPI32Literal(7, true))
                )
        );
        DBSPZSetExpression result = new DBSPZSetExpression(
                new DBSPTupleExpression(
                        new DBSPI32Literal(3, true)
                ),
                new DBSPTupleExpression(
                        new DBSPI32Literal(7, true)
                )
        );

        this.testQuery(ddl, "SELECT ARRAY_MAX(val) FROM ARR_TBL",
                new InputOutputChangeStream().addPair(new Change(input), new Change(result)));
    }

    @Test
    public void testArrayMinNullable() {
        String ddl = "CREATE TABLE ARR_TBL(val INTEGER ARRAY)";

        DBSPZSetExpression input = new DBSPZSetExpression(
                new DBSPTupleExpression(
                        new DBSPArrayExpression(true,
                                new DBSPI32Literal(1, true),
                                new DBSPI32Literal(2, true),
                                new DBSPI32Literal(3, true))
                ),
                new DBSPTupleExpression(
                        new DBSPArrayExpression(true,
                                new DBSPI32Literal(5, true),
                                new DBSPI32Literal(6, true),
                                new DBSPI32Literal(7, true))
                )
        );
        DBSPZSetExpression result = new DBSPZSetExpression(
                new DBSPTupleExpression(
                        new DBSPI32Literal(1, true)
                ),
                new DBSPTupleExpression(
                        new DBSPI32Literal(5, true)
                )
        );

        this.testQuery(ddl, "SELECT ARRAY_MIN(val) FROM ARR_TBL",
                new InputOutputChangeStream().addPair(new Change(input), new Change(result)));
    }

    @Test
    public void testArrayMinInnerNullable() {
        String ddl = "CREATE TABLE ARR_TBL(val INTEGER ARRAY)";

        DBSPZSetExpression input = new DBSPZSetExpression(
                new DBSPTupleExpression(
                        new DBSPArrayExpression(true,
                                new DBSPI32Literal(1, true),
                                new DBSPI32Literal(2, true),
                                DBSPNullLiteral.none(new DBSPTypeInteger(CalciteObject.EMPTY, 32, true, true)),
                                new DBSPI32Literal(3, true))
                ),
                new DBSPTupleExpression(
                        new DBSPArrayExpression(true,
                                new DBSPI32Literal(5, true),
                                new DBSPI32Literal(6, true),
                                new DBSPI32Literal(7, true))
                )
        );
        DBSPZSetExpression result = new DBSPZSetExpression(
                new DBSPTupleExpression(
                        new DBSPI32Literal(1, true)
                ),
                new DBSPTupleExpression(
                        new DBSPI32Literal(5, true)
                )
        );

        this.testQuery(ddl, "SELECT ARRAY_MIN(val) FROM ARR_TBL",
                new InputOutputChangeStream().addPair(new Change(input), new Change(result)));
    }

    @Test
    public void testSafeOrdinal() {
        String sql = """
                SELECT a[SAFE_OFFSET(2)], a[SAFE_OFFSET(3)] FROM
                (SELECT ARRAY[1, 2] a)""";
        this.testQuery("", sql,
                new InputOutputChangeStream().addPair(new Change(),
                        new Change(new DBSPZSetExpression(
                                new DBSPTupleExpression(
                                        new DBSPI32Literal(2, true),
                                        new DBSPI32Literal())))));
    }
}
