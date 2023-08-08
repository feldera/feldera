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

import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.compiler.backend.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.CalciteObject;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.*;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeDecimal;
import org.junit.Test;

import java.math.BigDecimal;

public class CastTests extends BaseSQLTests {
    final DBSPTypeDecimal tenTwo = new DBSPTypeDecimal(CalciteObject.EMPTY, 10, 2, true);
    final DBSPTypeDecimal tenFour = new DBSPTypeDecimal(CalciteObject.EMPTY, 10, 4, false);

    public DBSPCompiler compileQuery(String query) {
        DBSPCompiler compiler = this.testCompiler();
        String ddl = "CREATE TABLE T (\n" +
                "COL1 INT NOT NULL" +
                ", COL2 DOUBLE NOT NULL" +
                ", COL3 VARCHAR NOT NULL" +
                ", COL4 DECIMAL(10,2)" +
                ", COL5 DECIMAL(10,4) NOT NULL" +
                ")";
        compiler.compileStatement(ddl);
        compiler.compileStatement(query);
        return compiler;
    }

    public DBSPZSetLiteral.Contents createInput() {
        return new DBSPZSetLiteral.Contents(new DBSPTupleExpression(
                new DBSPI32Literal(10),
                new DBSPDoubleLiteral(12.0),
                new DBSPStringLiteral("100100"),
                DBSPLiteral.none(tenTwo),
                new DBSPDecimalLiteral(tenFour, new BigDecimal(100103123))));
    }

    public void testQuery(String query, DBSPZSetLiteral.Contents expectedOutput) {
        query = "CREATE VIEW V AS " + query;
        DBSPCompiler compiler = this.compileQuery(query);
        DBSPCircuit circuit = getCircuit(compiler);
        InputOutputPair streams = new InputOutputPair(this.createInput(), expectedOutput);
        this.addRustTestCase(query, compiler, circuit, streams);
    }

    @Test
    public void intAndString() {
        String query = "SELECT '1' + 2";
        this.testQuery(query, new DBSPZSetLiteral.Contents(
                new DBSPTupleExpression(new DBSPI32Literal(3))));
    }

    @Test
    public void intAndStringTable() {
        String query = "SELECT T.COL1 + T.COL3 FROM T";
        this.testQuery(query, new DBSPZSetLiteral.Contents(
                new DBSPTupleExpression(new DBSPI32Literal(100110))));
    }

    @Test
    public void castFromFPTest() {
        String query = "SELECT T.COL1 + T.COL2 + T.COL3 + T.COL5 FROM T";
        this.testQuery(query, new DBSPZSetLiteral.Contents(new DBSPTupleExpression(new DBSPDoubleLiteral(100203245.0))));
    }

    @Test
    public void castFromIntToDecimalTest() {
        String query = "SELECT CAST(T.COL1 AS DECIMAL(10, 10)) FROM T";
        this.testQuery(query, new DBSPZSetLiteral.Contents(new DBSPTupleExpression(
                new DBSPDecimalLiteral(new DBSPTypeDecimal(CalciteObject.EMPTY, 10, 10, false),
                        new BigDecimal("10")))));
    }
}
