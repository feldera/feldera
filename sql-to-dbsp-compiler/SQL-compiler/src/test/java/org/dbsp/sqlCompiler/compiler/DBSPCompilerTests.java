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

package org.dbsp.sqlCompiler.compiler;

import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.compiler.backend.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.backend.rust.RustFileWriter;
import org.dbsp.sqlCompiler.compiler.frontend.TableContents;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPZSetLiteral;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Paths;

import static org.dbsp.sqlCompiler.compiler.BaseSQLTests.testFilePath;

/**
 * Tests that invoke the CalciteToDBSPCompiler.
 */
public class DBSPCompilerTests {
    static final CompilerOptions OPTIONS = new CompilerOptions();
    private static final String DDL = "CREATE TABLE T (\n" +
            "COL1 INT NOT NULL" +
            ", COL2 DOUBLE NOT NULL" +
            ", COL3 BOOLEAN NOT NULL" +
            ", COL4 VARCHAR NOT NULL" +
            ")";

    @Test
    public void docTest() throws IOException {
        // The example given in the documentation
        String statements = "-- define Person table\n" +
                "CREATE TABLE Person\n" +
                "(\n" +
                "    name    VARCHAR,\n" +
                "    age     INT,\n" +
                "    present BOOLEAN\n" +
                ");\n" +
                "CREATE VIEW Adult AS SELECT Person.name FROM Person WHERE Person.age > 18;";
        DBSPCompiler compiler = new DBSPCompiler(OPTIONS);
        compiler.compileStatements(statements);
        DBSPCircuit dbsp = compiler.getFinalCircuit("circuit");
        PrintStream outputStream = new PrintStream(Files.newOutputStream(Paths.get(testFilePath)));
        RustFileWriter writer = new RustFileWriter(compiler, outputStream);
        writer.emitCodeWithHandle(true);
        writer.add(dbsp);
        writer.write();
    }

    @Test
    public void DDLTest() {
        DBSPCompiler compiler = new DBSPCompiler(OPTIONS);
        compiler.compileStatement(DDL);
        DBSPCircuit dbsp = compiler.getFinalCircuit("circuit");
        Assert.assertNotNull(dbsp);
    }

    @Test
    public void DDLAndInsertTest() {
        DBSPCompiler compiler = new DBSPCompiler(OPTIONS);
        String insert = "INSERT INTO T VALUES(0, 0.0, true, 'Hi')";
        compiler.compileStatement(DDL);
        compiler.compileStatement(insert);
        TableContents tableContents = compiler.getTableContents();
        DBSPZSetLiteral.Contents t = tableContents.getTableContents("T");
        Assert.assertNotNull(t);
        Assert.assertEquals(1, t.size());
    }
}
