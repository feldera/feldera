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

package org.dbsp.sqlCompiler.compiler.sql;

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.ddl.SqlCreateTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.IErrorReporter;
import org.dbsp.sqlCompiler.compiler.StderrErrorReporter;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.CalciteCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.SqlCreateFunctionDeclaration;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.SqlExtendedColumnDeclaration;
import org.dbsp.sqlCompiler.compiler.frontend.parser.SqlCreateLocalView;
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
        String ddl2 = "CREATE LOCAL VIEW V2 AS SELECT * FROM T";

        SqlNode node = calcite.parse(ddl);
        Assert.assertNotNull(node);
        Assert.assertTrue(node instanceof SqlCreateTable);

        node = calcite.parse(ddl1);
        Assert.assertNotNull(node);
        Assert.assertTrue(node instanceof SqlCreateLocalView);

        node = calcite.parse(ddl2);
        Assert.assertNotNull(node);
        Assert.assertTrue(node instanceof SqlCreateLocalView);
        Assert.assertTrue(((SqlCreateLocalView) node).isLocal);
    }

    @Test
    public void createFunctionTest() throws SqlParseException {
        CalciteCompiler calcite = this.getCompiler();
        String ddl = """
                CREATE FUNCTION to_json(data VARCHAR) RETURNS VARBINARY;
                CREATE FUNCTION from_json(data VARBINARY) RETURNS VARCHAR;
                """;
        SqlNodeList list = calcite.parseStatements(ddl);
        Assert.assertNotNull(list);
        Assert.assertEquals(2, list.size());
    }

    @Test
    public void createFunctionBodyTest() throws SqlParseException {
        CalciteCompiler calcite = this.getCompiler();
        String ddl = """
                CREATE FUNCTION dbl(n INTEGER) RETURNS INTEGER AS n * 2;
                """;
        SqlNodeList list = calcite.parseStatements(ddl);
        Assert.assertNotNull(list);
        Assert.assertEquals(1, list.size());
        SqlNode first = list.get(0);
        Assert.assertTrue(first instanceof SqlCreateFunctionDeclaration);
        SqlCreateFunctionDeclaration func = (SqlCreateFunctionDeclaration) first;
        Assert.assertNotNull(func.getBody());
    }

    @Test
    public void createTypeTest() throws SqlParseException {
        CalciteCompiler calcite = this.getCompiler();
        String ddl = """
                CREATE TYPE address_typ AS (
                   street          VARCHAR(30),
                   city            VARCHAR(30),
                   state           CHAR(2),
                   postal_code     VARCHAR(6));
                CREATE TYPE person_type AS (
                   firstname       VARCHAR(30),
                   lastname        VARCHAR(30),
                   address         ADDRESS_TYP);
                CREATE TABLE T(p PERSON_TYPE);""";
        SqlNode node = calcite.parseStatements(ddl);
        Assert.assertNotNull(node);
    }

    @Test
    public void latenessTest() throws SqlParseException {
        CalciteCompiler calcite = this.getCompiler();
        String ddl = """
                CREATE TABLE st(
                   ts       TIMESTAMP LATENESS INTERVAL '5:00' HOURS TO MINUTES,
                   name     VARCHAR)""";
        SqlNode node = calcite.parse(ddl);
        Assert.assertNotNull(node);
        Assert.assertTrue(node instanceof SqlCreateTable);
        SqlCreateTable create = (SqlCreateTable) node;
        Assert.assertNotNull(create.columnList);
        SqlExtendedColumnDeclaration decl = (SqlExtendedColumnDeclaration) create.columnList.get(0);
        Assert.assertNotNull(decl.lateness);
    }

    @Test
    public void watermarkTest() throws SqlParseException {
        CalciteCompiler calcite = this.getCompiler();
        String ddl = """
                CREATE TABLE st(
                   ts       TIMESTAMP WATERMARK INTERVAL '5:00' HOURS TO MINUTES,
                   name     VARCHAR)""";
        SqlNode node = calcite.parse(ddl);
        Assert.assertNotNull(node);
        Assert.assertTrue(node instanceof SqlCreateTable);
        SqlCreateTable create = (SqlCreateTable) node;
        Assert.assertNotNull(create.columnList);
        SqlExtendedColumnDeclaration decl = (SqlExtendedColumnDeclaration) create.columnList.get(0);
        Assert.assertNotNull(decl.watermark);
    }

    @Test
    public void sourceNameTest() throws SqlParseException {
        // Tests that a table can be named 'source'.
        CalciteCompiler calcite = this.getCompiler();
        String ddl = "CREATE TABLE SOURCE (COL INT)";
        SqlNode node = calcite.parse(ddl);
        Assert.assertNotNull(node);
    }

    @Test
    public void removeTest() throws SqlParseException {
        // Tests the newly added REMOVE statement
        CalciteCompiler calcite = this.getCompiler();
        String ddl = "REMOVE FROM SOURCE VALUES(1, 2, 3)";
        SqlNode node = calcite.parse(ddl);
        Assert.assertNotNull(node);
    }

    @Test
    public void latenessStatementTest() throws SqlParseException {
        // Tests the LATENESS statement
        CalciteCompiler calcite = this.getCompiler();
        String ddl = "LATENESS V.COL1 INTERVAL '1:00' HOUR TO MINUTES";
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
        String query = """
                CREATE TABLE R AS
                SELECT cast(1 as int64) as primary_key,
                       cast(1 as int64) as id, cast("a1" as string) as a UNION ALL
                  SELECT 2, 2, "a2\"""";
        SqlNode node = calcite.parse(query);
        Assert.assertNotNull(node);
    }

    @Test
    public void commentsTest() throws SqlParseException {
        String query = """
                --- Line comment
                /* Second comment
                SELECT * FROM T
                */
                CREATE VIEW V AS SELECT 0""";
        CalciteCompiler calcite = this.getCompiler();
        SqlNode node = calcite.parseStatements(query);
        Assert.assertNotNull(node);
    }

    @Test
    public void primaryKeyTest() throws SqlParseException {
        // MYSQL syntax for primary keys
        String query =
                """
                        create table git_commit (
                            git_commit_id bigint not null primary key
                        )""";
        CalciteCompiler calcite = this.getCompiler();
        SqlNode node = calcite.parseStatements(query);
        Assert.assertNotNull(node);
    }

    @Test
    public void primaryKeyTest0() throws SqlParseException {
        // standard syntax for primary keys
        String query =
                """
                        create table git_commit (
                            git_commit_id bigint not null,
                            PRIMARY KEY (git_commit_id)
                        )""";
        CalciteCompiler calcite = this.getCompiler();
        SqlNode node = calcite.parseStatements(query);
        Assert.assertNotNull(node);
    }

    @Test
    public void foreignKeyTest() throws SqlParseException {
        // MYSQL syntax for FOREIGN KEY
        String query =
                """
                        create table git_commit (
                            git_commit_id bigint not null,
                            repository_id bigint not null,
                            commit_id varchar not null,
                            commit_date timestamp not null,
                            commit_owner varchar not null
                        );
                        create table pipeline_sources (
                            git_commit_id bigint not null foreign key references git_commit(git_commit_id),
                            pipeline_id bigint not null
                        )""";
        CalciteCompiler calcite = this.getCompiler();
        SqlNode node = calcite.parseStatements(query);
        Assert.assertNotNull(node);
    }

    @Test
    public void  keyAndSeparateForeignKeyTest() throws SqlParseException {
        String query = """
                CREATE TABLE productvariant_t (
                    id BIGINT NOT NULL PRIMARY KEY,
                    FOREIGN KEY (id) REFERENCES inventoryitem_t (id)
                );""";
        CalciteCompiler calcite = this.getCompiler();
        SqlNode node = calcite.parseStatements(query);
        Assert.assertNotNull(node);
    }

    @Test
    public void defaultColumnValueTest() throws SqlParseException {
        String query = """
                CREATE TABLE productvariant_t (
                    id BIGINT DEFAULT NULL,
                    str VARCHAR DEFAULT ''
                );""";
        CalciteCompiler calcite = this.getCompiler();
        SqlNode node = calcite.parseStatements(query);
        Assert.assertNotNull(node);
    }

   @Test
    public void keyAndForeignKeyTest() throws SqlParseException {
        String query = """
                CREATE TABLE productvariant_t (
                    id BIGINT NOT NULL PRIMARY KEY FOREIGN KEY REFERENCES inventoryitem_t (id)
                );""";
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
