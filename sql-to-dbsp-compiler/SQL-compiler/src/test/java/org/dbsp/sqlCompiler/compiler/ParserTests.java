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

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.ddl.SqlCreateTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.CalciteCompiler;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test our parser extensions.
 */
public class ParserTests {
    static final CompilerOptions options = new CompilerOptions();
    static final IErrorReporter errorReporter = new StderrErrorReporter();

    CalciteCompiler getCompiler() {
        return new CalciteCompiler(options, errorReporter);
    }

    @Test
    public void DDLTest() throws SqlParseException {
        CalciteCompiler calcite = this.getCompiler();
        String ddl = "CREATE TABLE T (\n" +
                "COL1 INT" +
                ", COL2 DOUBLE" +
                ", COL3 BOOLEAN" +
                ", COL4 VARCHAR" +
                ")";
        String ddl1 = "CREATE VIEW V AS SELECT * FROM T";

        SqlNode node = calcite.parse(ddl);
        Assert.assertNotNull(node);
        node = calcite.parse(ddl1);
        Assert.assertNotNull(node);
    }

    @Test
    public void createTypeTest() throws SqlParseException {
        CalciteCompiler calcite = this.getCompiler();
        String ddl = "CREATE TYPE address_typ AS (\n" +
                "   street          VARCHAR(30),\n" +
                "   city            VARCHAR(30),\n" +
                "   state           CHAR(2),\n" +
                "   postal_code     VARCHAR(6))";
        SqlNode node = calcite.parse(ddl);
        Assert.assertNotNull(node);
    }

    @Test
    public void latenessTest() throws SqlParseException {
        CalciteCompiler calcite = this.getCompiler();
        String ddl = "CREATE TABLE st(\n" +
                "   ts       TIMESTAMP LATENESS INTERVAL '5:00' HOURS TO MINUTES,\n" +
                "   name     VARCHAR)";
        SqlNode node = calcite.parse(ddl);
        Assert.assertNotNull(node);
        Assert.assertTrue(node instanceof SqlCreateTable);
        SqlCreateTable create = (SqlCreateTable) node;
        Assert.assertNotNull(create.columnList);
    }

    @Test
    public void SourceNameTest() throws SqlParseException {
        // Tests that a table can be named 'source'.
        CalciteCompiler calcite = this.getCompiler();
        String ddl = "CREATE TABLE SOURCE (COL INT)";
        SqlNode node = calcite.parse(ddl);
        Assert.assertNotNull(node);
    }

    @Test
    public void testNumber() throws SqlParseException {
        // Tests that 'NUMBER' can be used as a type
        CalciteCompiler calcite = this.getCompiler();
        String ddl = "CREATE TABLE SOURCE (COL NUMBER)";
        SqlNode node = calcite.parse(ddl);
        Assert.assertNotNull(node);
    }

    @Test
    public void oneLetterTest() throws SqlParseException {
        // Tests that 'G' can be used as a column name
        CalciteCompiler calcite = this.getCompiler();
        String ddl = "CREATE TABLE SOURCE (A CHAR, D CHAR, G CHAR, OF CHAR, M CHAR)";
        SqlNode node = calcite.parse(ddl);
        Assert.assertNotNull(node);
    }

    @Test
    public void DropTest() throws SqlParseException {
        CalciteCompiler calcite = this.getCompiler();
        String ddl = "DROP TABLE T";
        SqlNode node = calcite.parse(ddl);
        Assert.assertNotNull(node);

        calcite = this.getCompiler();
        ddl = "DROP TABLE IF EXISTS T";
        node = calcite.parse(ddl);
        Assert.assertNotNull(node);

        calcite = this.getCompiler();
        ddl = "DROP VIEW V";
        node = calcite.parse(ddl);
        Assert.assertNotNull(node);

        calcite = this.getCompiler();
        ddl = "DROP VIEW IF EXISTS V";
        node = calcite.parse(ddl);
        Assert.assertNotNull(node);
    }

    @Test
    public void DDLQueryTest() throws SqlParseException {
        CalciteCompiler calcite = this.getCompiler();
        String query = "CREATE TABLE R AS\n" +
                "SELECT cast(1 as int64) as primary_key,\n" +
                "       cast(1 as int64) as id, cast(\"a1\" as string) as a UNION ALL\n" +
                "  SELECT 2, 2, \"a2\"";
        SqlNode node = calcite.parse(query);
        Assert.assertNotNull(node);
    }

    @Test
    public void commentsTest() throws SqlParseException {
        String query = "--- Line comment\n" +
                "/* Second comment\n" +
                "SELECT * FROM T\n" +
                "*/\n" +
                "CREATE VIEW V AS SELECT 0";
        CalciteCompiler calcite = this.getCompiler();
        SqlNode node = calcite.parseStatements(query);
        Assert.assertNotNull(node);
    }

    @Test
    public void primaryKeyTest() throws SqlParseException {
        // MYSQL syntax for primary keys
        String query =
                "create table git_commit (\n" +
                "    git_commit_id bigint not null primary key\n" +
                ")";
        CalciteCompiler calcite = this.getCompiler();
        SqlNode node = calcite.parseStatements(query);
        Assert.assertNotNull(node);
    }

    @Test
    public void primaryKeyTest0() throws SqlParseException {
        // standard syntax for primary keys
        String query =
                "create table git_commit (\n" +
                        "    git_commit_id bigint not null,\n" +
                        "    PRIMARY KEY (git_commit_id)\n" +
                        ")";
        CalciteCompiler calcite = this.getCompiler();
        SqlNode node = calcite.parseStatements(query);
        Assert.assertNotNull(node);
    }

    @Test
    public void foreignKeyTest() throws SqlParseException {
        // MYSQL syntax for FOREIGN KEY
        String query =
                "create table git_commit (\n" +
                "    git_commit_id bigint not null,\n" +
                "    repository_id bigint not null,\n" +
                "    commit_id varchar not null,\n" +
                "    commit_date timestamp not null,\n" +
                "    commit_owner varchar not null\n" +
                ");\n" +
                "create table pipeline_sources (\n" +
                "    git_commit_id bigint not null foreign key references git_commit(git_commit_id),\n" +
                "    pipeline_id bigint not null\n" +
                ")";
        CalciteCompiler calcite = this.getCompiler();
        SqlNode node = calcite.parseStatements(query);
        Assert.assertNotNull(node);
    }

    @Test
    public void  keyAndSeparateForeignKeyTest() throws SqlParseException {
        String query = "CREATE TABLE productvariant_t (\n" +
                "    id BIGINT NOT NULL PRIMARY KEY,\n" +
                "    FOREIGN KEY (id) REFERENCES inventoryitem_t (id)\n" +
                ");";
        CalciteCompiler calcite = this.getCompiler();
        SqlNode node = calcite.parseStatements(query);
        Assert.assertNotNull(node);
    }

    @Test
    public void defaultColumnValueTest() throws SqlParseException {
        String query = "CREATE TABLE productvariant_t (\n" +
                "    id BIGINT DEFAULT NULL,\n" +
                "    str VARCHAR DEFAULT ''\n" +
                ");";
        CalciteCompiler calcite = this.getCompiler();
        SqlNode node = calcite.parseStatements(query);
        Assert.assertNotNull(node);
    }

   @Test
    public void keyAndForeignKeyTest() throws SqlParseException {
        String query = "CREATE TABLE productvariant_t (\n" +
                "    id BIGINT NOT NULL PRIMARY KEY FOREIGN KEY REFERENCES inventoryitem_t (id)\n" +
                ");";
        CalciteCompiler calcite = this.getCompiler();
        SqlNode node = calcite.parseStatements(query);
        Assert.assertNotNull(node);
    }

    @Test
    public void duplicatedForeignKey() throws SqlParseException {
        // A column can participate in multiple foreign key constraints
        String query = "create table git_commit (\n" +
                "    git_commit_id bigint not null FOREIGN KEY REFERENCES other(other) FOREIGN KEY REFERENCES other2(other2))";
        CalciteCompiler calcite = this.getCompiler();
        SqlNode node = calcite.parseStatements(query);
        Assert.assertNotNull(node);
    }
}
