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
import org.dbsp.sqlCompiler.compiler.frontend.TableData;
import org.dbsp.sqlCompiler.compiler.sql.tools.BaseSQLTests;
import org.dbsp.sqlCompiler.compiler.sql.tools.Change;
import org.dbsp.sqlCompiler.compiler.sql.tools.CompilerCircuitStream;
import org.dbsp.sqlCompiler.compiler.sql.tools.InputOutputChange;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPBoolLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPDoubleLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPI32Literal;
import org.dbsp.sqlCompiler.ir.expression.DBSPZSetExpression;
import org.junit.Test;

/** Tests where multiple views are defined in the same circuit. */
public class MultiViewTests extends BaseSQLTests {
    /** Two output views. */
    @Test
    public void twoViewTest() {
        String query1 = "CREATE VIEW V1 AS SELECT T.COL3 FROM T";
        String query2 = "CREATE VIEW V2 as SELECT T.COL2 FROM T";

        DBSPCompiler compiler = testCompiler();
        compiler.submitStatementForCompilation(EndToEndTests.E2E_TABLE);
        compiler.submitStatementForCompilation(query1);
        compiler.submitStatementForCompilation(query2);

        CompilerCircuitStream ccs = this.getCCS(compiler);
        Change inputChange = EndToEndTests.INPUT;
        Change outputChange = new Change(
                new TableData("V1", new DBSPZSetExpression(
                        new DBSPTupleExpression(new DBSPBoolLiteral(true)),
                        new DBSPTupleExpression(new DBSPBoolLiteral(false)))),
                new TableData("V2", new DBSPZSetExpression(
                        new DBSPTupleExpression(new DBSPDoubleLiteral(12.0)),
                        new DBSPTupleExpression(new DBSPDoubleLiteral(1.0)))));
        ccs.addPair(inputChange, outputChange);
    }

    /** A view is an input for another view. */
    @Test
    public void nestedViewTest() {
        String query1 = "CREATE VIEW V1 AS SELECT T.COL3 FROM T";
        String query2 = "CREATE VIEW V2 as SELECT * FROM V1";

        DBSPCompiler compiler = testCompiler();
        compiler.submitStatementForCompilation(EndToEndTests.E2E_TABLE);
        compiler.submitStatementForCompilation(query1);
        compiler.submitStatementForCompilation(query2);
        CompilerCircuitStream ccs = this.getCCS(compiler);

        InputOutputChange change = new InputOutputChange(
                EndToEndTests.INPUT,
                new Change(
                        new TableData("V1", new DBSPZSetExpression(
                                new DBSPTupleExpression(new DBSPBoolLiteral(true)),
                                new DBSPTupleExpression(new DBSPBoolLiteral(false)))),
                        new TableData("V2", new DBSPZSetExpression(
                                new DBSPTupleExpression(new DBSPBoolLiteral(true)),
                                new DBSPTupleExpression(new DBSPBoolLiteral(false))))));
        ccs.addChange(change);
    }

    /** A view is used twice. */
    @Test
    public void multiViewTest() {
        String query1 = "CREATE VIEW V1 AS SELECT T.COL3 AS COL3 FROM T";
        String query2 = "CREATE VIEW V2 as SELECT DISTINCT COL1 FROM (SELECT * FROM V1 JOIN T ON V1.COL3 = T.COL3)";

        DBSPCompiler compiler = testCompiler();
        compiler.submitStatementForCompilation(EndToEndTests.E2E_TABLE);
        compiler.submitStatementForCompilation(query1);
        compiler.submitStatementForCompilation(query2);

        CompilerCircuitStream ccs = this.getCCS(compiler);
        InputOutputChange change = new InputOutputChange(
                EndToEndTests.INPUT,
                new Change(new TableData("V1", new DBSPZSetExpression(
                        new DBSPTupleExpression(new DBSPBoolLiteral(true)),
                        new DBSPTupleExpression(new DBSPBoolLiteral(false)))),
                        new TableData("V2", new DBSPZSetExpression(
                                new DBSPTupleExpression(new DBSPI32Literal(10)))))
        );
        ccs.addChange(change);
    }
}
