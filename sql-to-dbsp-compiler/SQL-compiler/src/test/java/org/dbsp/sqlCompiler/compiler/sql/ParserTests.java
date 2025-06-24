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
import org.apache.calcite.sql.parser.SqlParseException;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.StderrErrorReporter;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.ParsedStatement;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.SqlToRelCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteRelNode;
import org.dbsp.sqlCompiler.compiler.frontend.parser.SqlCreateFunctionDeclaration;
import org.dbsp.sqlCompiler.compiler.frontend.parser.SqlCreateIndex;
import org.dbsp.sqlCompiler.compiler.frontend.parser.SqlCreateView;
import org.dbsp.sqlCompiler.compiler.frontend.parser.SqlDeclareView;
import org.dbsp.sqlCompiler.compiler.frontend.parser.SqlExtendedColumnDeclaration;
import org.dbsp.sqlCompiler.compiler.frontend.parser.SqlCreateTable;
import org.dbsp.sqlCompiler.compiler.frontend.parser.SqlForeignKey;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

/** Test SQL parser extensions. */
public class ParserTests {
    static final CompilerOptions options = new CompilerOptions();

    SqlToRelCompiler getCompiler() {
        return new SqlToRelCompiler(options, new StderrErrorReporter());
    }

    @Test
    public void ddlTest() throws SqlParseException {
        SqlToRelCompiler calcite = this.getCompiler();
        String ddl = "CREATE TABLE T (\n" +
                "COL1 INT" +
                ", COL2 DOUBLE" +
                ", COL3 BOOLEAN" +
                ", COL4 VARCHAR" +
                ")";
        String ddl1 = "CREATE VIEW V AS SELECT * FROM T";
        String ddl2 = "CREATE LOCAL VIEW V2 AS SELECT * FROM T";
        String ddl3 = "CREATE MATERIALIZED VIEW V3 AS SELECT * FROM T";

        SqlNode node = calcite.parse(ddl);
        Assert.assertNotNull(node);
        Assert.assertTrue(node instanceof SqlCreateTable);
        SqlCreateTable table = (SqlCreateTable) node;
        Assert.assertNull(table.tableProperties);

        node = calcite.parse(ddl1);
        Assert.assertNotNull(node);
        Assert.assertTrue(node instanceof SqlCreateView);
        SqlCreateView clv = (SqlCreateView) node;
        Assert.assertNull(clv.viewProperties);

        node = calcite.parse(ddl2);
        Assert.assertNotNull(node);
        Assert.assertTrue(node instanceof SqlCreateView);
        Assert.assertSame(SqlCreateView.ViewKind.LOCAL, ((SqlCreateView) node).viewKind);

        node = calcite.parse(ddl3);
        Assert.assertNotNull(node);
        Assert.assertTrue(node instanceof SqlCreateView);
        Assert.assertSame(SqlCreateView.ViewKind.MATERIALIZED, ((SqlCreateView) node).viewKind);
    }

    @Test
    public void parseRecursiveTest() throws SqlParseException {
        SqlToRelCompiler compiler = this.getCompiler();
        List<ParsedStatement> node = compiler.parseStatements("""
                DECLARE RECURSIVE VIEW V(x INT);
                CREATE VIEW V AS SELECT * FROM V;
                """);
        Assert.assertNotNull(node);
        Assert.assertEquals(2, node.size());
        Assert.assertTrue(node.get(0).statement() instanceof SqlDeclareView);
        Assert.assertTrue(node.get(1).statement() instanceof SqlCreateView);
    }

    @Test
    public void parseCreateIndexTest() throws SqlParseException {
        SqlToRelCompiler compiler = this.getCompiler();
        List<ParsedStatement> node = compiler.parseStatements("""
                CREATE TABLE T(x INT, y BIGINT);
                CREATE VIEW V AS SELECT * FROM T;
                CREATE INDEX VINDEX ON V(x);
                """);
        Assert.assertNotNull(node);
        Assert.assertEquals(3, node.size());
        Assert.assertTrue(node.get(0).statement() instanceof SqlCreateTable);
        Assert.assertTrue(node.get(1).statement() instanceof SqlCreateView);
        Assert.assertTrue(node.get(2).statement() instanceof SqlCreateIndex);
    }

    @Test
    public void testPlusNoop() throws SqlParseException {
        CompilerOptions options = new CompilerOptions();
        options.languageOptions.unaryPlusNoop = true;
        var compiler = new SqlToRelCompiler(options, new StderrErrorReporter());
        String sql = "SELECT +'blah'";
        SqlNode node = compiler.parse(sql);
        Assert.assertEquals("SELECT 'blah'", node.toSqlString(CalciteRelNode.DIALECT).toString());

        options = new CompilerOptions();
        compiler = new SqlToRelCompiler(options, new StderrErrorReporter());
        node = compiler.parse(sql);
        Assert.assertEquals("SELECT + 'blah'", node.toSqlString(CalciteRelNode.DIALECT).toString());
    }

    @Test
    public void connectorTest() throws SqlParseException {
        SqlToRelCompiler calcite = this.getCompiler();
        String table = """
               CREATE TABLE T (
               COL1 INT
               , COL2 DOUBLE
               , COL3 BOOLEAN
               , COL4 VARCHAR
               ) WITH (
                  'connector' = 'kafka',
                  'url' = 'localhost'
               )""";

        String view = """
               CREATE VIEW V WITH (
                  'connector' = 'kafka',
                  'url' = 'localhost',
                  'port' = '8080'
               ) AS SELECT * FROM T""";

        SqlNode node = calcite.parse(table);
        Assert.assertNotNull(node);
        Assert.assertTrue(node instanceof SqlCreateTable);
        SqlCreateTable tbl = (SqlCreateTable) node;
        Assert.assertNotNull(tbl.tableProperties);
        Assert.assertEquals(4, tbl.tableProperties.size());

        node = calcite.parse(view);
        Assert.assertNotNull(node);
        Assert.assertTrue(node instanceof SqlCreateView);
        SqlCreateView v = (SqlCreateView) node;
        Assert.assertNotNull(v.viewProperties);
        Assert.assertEquals(6, v.viewProperties.size());
    }

    @Test
    public void createFunctionTest() throws SqlParseException {
        SqlToRelCompiler calcite = this.getCompiler();
        String ddl = """
                CREATE FUNCTION to_json(data VARCHAR) RETURNS VARBINARY;
                CREATE FUNCTION from_json(data VARBINARY) RETURNS VARCHAR;
                CREATE FUNCTION no_args() RETURNS TIMESTAMP AS TIMESTAMP '2024-01-01 00:00:00';
                """;
        List<ParsedStatement> list = calcite.parseStatements(ddl);
        Assert.assertNotNull(list);
        Assert.assertEquals(3, list.size());
    }

    @Test
    public void createFunctionBodyTest() throws SqlParseException {
        SqlToRelCompiler calcite = this.getCompiler();
        String ddl = """
                CREATE FUNCTION dbl(n INTEGER) RETURNS INTEGER AS n * 2;
                """;
        List<ParsedStatement> list = calcite.parseStatements(ddl);
        Assert.assertNotNull(list);
        Assert.assertEquals(1, list.size());
        SqlNode first = list.get(0).statement();
        Assert.assertTrue(first instanceof SqlCreateFunctionDeclaration);
        SqlCreateFunctionDeclaration func = (SqlCreateFunctionDeclaration) first;
        Assert.assertNotNull(func.getBody());
    }

    @Test
    public void createTypeTest() throws SqlParseException {
        SqlToRelCompiler calcite = this.getCompiler();
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
        List<ParsedStatement> node = calcite.parseStatements(ddl);
        Assert.assertNotNull(node);
    }

    @Test
    public void mapTypeTest() throws SqlParseException {
        SqlToRelCompiler calcite = this.getCompiler();
        String ddl = """
                CREATE TABLE T (
                   data     MAP<INT, INT>
                );""";
        List<ParsedStatement> node = calcite.parseStatements(ddl);
        Assert.assertNotNull(node);
    }

    @Test
    public void latenessTest() throws SqlParseException {
        SqlToRelCompiler calcite = this.getCompiler();
        String ddl = """
                CREATE TABLE st(
                   ts       TIMESTAMP LATENESS INTERVAL '5:00' HOURS TO MINUTES,
                   name     VARCHAR)""";
        SqlNode node = calcite.parse(ddl);
        Assert.assertNotNull(node);
        Assert.assertTrue(node instanceof SqlCreateTable);
        SqlCreateTable create = (SqlCreateTable) node;
        Assert.assertNotNull(create.columnsOrForeignKeys);
        SqlExtendedColumnDeclaration decl = (SqlExtendedColumnDeclaration) create.columnsOrForeignKeys.get(0);
        Assert.assertNotNull(decl.lateness);
    }

    @Test
    public void watermarkTest() throws SqlParseException {
        SqlToRelCompiler calcite = this.getCompiler();
        String ddl = """
                CREATE TABLE st(
                   ts       TIMESTAMP WATERMARK INTERVAL '5:00' HOURS TO MINUTES,
                   name     VARCHAR)""";
        SqlNode node = calcite.parse(ddl);
        Assert.assertNotNull(node);
        Assert.assertTrue(node instanceof SqlCreateTable);
        SqlCreateTable create = (SqlCreateTable) node;
        Assert.assertNotNull(create.columnsOrForeignKeys);
        SqlExtendedColumnDeclaration decl = (SqlExtendedColumnDeclaration) create.columnsOrForeignKeys.get(0);
        Assert.assertNotNull(decl.watermark);
    }

    @Test
    public void sourceNameTest() throws SqlParseException {
        // Tests that a table can be named 'source'.
        SqlToRelCompiler calcite = this.getCompiler();
        String ddl = "CREATE TABLE SOURCE (COL INT)";
        SqlNode node = calcite.parse(ddl);
        Assert.assertNotNull(node);
    }

    @Test
    public void removeTest() throws SqlParseException {
        // Tests the newly added REMOVE statement
        SqlToRelCompiler calcite = this.getCompiler();
        String ddl = "REMOVE FROM SOURCE VALUES(1, 2, 3)";
        SqlNode node = calcite.parse(ddl);
        Assert.assertNotNull(node);
    }

    @Test
    public void latenessStatementTest() throws SqlParseException {
        // Tests the LATENESS statement
        SqlToRelCompiler calcite = this.getCompiler();
        String ddl = "LATENESS V.COL1 INTERVAL '1:00' HOUR TO MINUTES";
        SqlNode node = calcite.parse(ddl);
        Assert.assertNotNull(node);
    }

    @Test
    public void testNumber() throws SqlParseException {
        // Tests that 'NUMBER' can be used as a type
        SqlToRelCompiler calcite = this.getCompiler();
        String ddl = "CREATE TABLE SOURCE (COL NUMBER)";
        SqlNode node = calcite.parse(ddl);
        Assert.assertNotNull(node);
    }

    @Test
    public void oneLetterTest() throws SqlParseException {
        // Tests that 'G' can be used as a column name
        SqlToRelCompiler calcite = this.getCompiler();
        String ddl = "CREATE TABLE SOURCE (A CHAR, D CHAR, G CHAR, OF CHAR, M CHAR)";
        SqlNode node = calcite.parse(ddl);
        Assert.assertNotNull(node);
    }

    @Test
    public void DropTest() throws SqlParseException {
        SqlToRelCompiler calcite = this.getCompiler();
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
    public void commentsTest() throws SqlParseException {
        String query = """
                --- Line comment
                /* Second comment
                SELECT * FROM T
                */
                CREATE VIEW V AS SELECT 0""";
        SqlToRelCompiler calcite = this.getCompiler();
        List<ParsedStatement> node = calcite.parseStatements(query);
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
        SqlToRelCompiler calcite = this.getCompiler();
        List<ParsedStatement> node = calcite.parseStatements(query);
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
        SqlToRelCompiler calcite = this.getCompiler();
        List<ParsedStatement> node = calcite.parseStatements(query);
        Assert.assertNotNull(node);
    }

    @Test
    public void foreignKeyTest() throws SqlParseException {
        // MYSQL syntax for FOREIGN KEY
        String query =
                """
                create table git_commit (
                    git_commit_id bigint not null primary key,
                    repository_id bigint not null,
                    commit_id varchar not null,
                    commit_date timestamp not null,
                    commit_owner varchar not null
                );
                create table pipeline_sources (
                    git_commit_id bigint not null foreign key references git_commit(git_commit_id),
                    pipeline_id bigint not null
                )""";
        SqlToRelCompiler calcite = this.getCompiler();
        List<ParsedStatement> node = calcite.parseStatements(query);
        Assert.assertNotNull(node);
        Assert.assertEquals(2, node.size());
        SqlNode table = node.get(1).statement();
        Assert.assertTrue(table instanceof SqlCreateTable);
        SqlCreateTable ct = (SqlCreateTable) table;
        Assert.assertEquals(2, ct.columnsOrForeignKeys.size());
        SqlNode first = ct.columnsOrForeignKeys.get(0);
        Assert.assertTrue(first instanceof SqlExtendedColumnDeclaration);
        SqlExtendedColumnDeclaration decl = (SqlExtendedColumnDeclaration) first;
        Assert.assertEquals(1, decl.foreignKeyColumns.size());
        Assert.assertEquals(1, decl.foreignKeyTables.size());
    }

    @Test
    public void  keyAndSeparateForeignKeyTest() throws SqlParseException {
        String query = """
                CREATE TABLE productvariant_t (
                    id BIGINT NOT NULL PRIMARY KEY,
                    FOREIGN KEY (id) REFERENCES inventoryitem_t (id)
                );""";
        SqlToRelCompiler calcite = this.getCompiler();
        List<ParsedStatement> node = calcite.parseStatements(query);
        Assert.assertNotNull(node);
        Assert.assertEquals(1, node.size());
        SqlNode table = node.get(0).statement();
        Assert.assertTrue(table instanceof SqlCreateTable);
        SqlCreateTable ct = (SqlCreateTable) table;
        Assert.assertEquals(2, ct.columnsOrForeignKeys.size());
        SqlNode first = ct.columnsOrForeignKeys.get(0);
        Assert.assertTrue(first instanceof SqlExtendedColumnDeclaration);
        SqlExtendedColumnDeclaration decl = (SqlExtendedColumnDeclaration) first;
        Assert.assertTrue(decl.primaryKey);
        SqlNode second = ct.columnsOrForeignKeys.get(1);
        Assert.assertTrue(second instanceof SqlForeignKey);
        SqlForeignKey fk = (SqlForeignKey) second;
        Assert.assertEquals(1, fk.columnList.size());
        Assert.assertEquals(1, fk.otherColumnList.size());
        Assert.assertEquals("inventoryitem_t", fk.otherTable.getSimple());
    }

    @Test
    public void defaultColumnValueTest() throws SqlParseException {
        String query = """
                CREATE TABLE productvariant_t (
                    id BIGINT DEFAULT NULL,
                    str VARCHAR DEFAULT ''
                );""";
        SqlToRelCompiler calcite = this.getCompiler();
        List<ParsedStatement> node = calcite.parseStatements(query);
        Assert.assertNotNull(node);
    }

   @Test
    public void keyAndForeignKeyTest() throws SqlParseException {
        String query = """
                CREATE TABLE productvariant_t (
                    id BIGINT NOT NULL PRIMARY KEY FOREIGN KEY REFERENCES inventoryitem_t (id)
                );""";
        SqlToRelCompiler calcite = this.getCompiler();
        List<ParsedStatement> node = calcite.parseStatements(query);
        Assert.assertNotNull(node);
    }

    @Test
    public void duplicatedForeignKey() throws SqlParseException {
        // A column can participate in multiple foreign key constraints
        String query = """
                create table git_commit (
                    git_commit_id bigint not null FOREIGN KEY REFERENCES other(other) FOREIGN KEY REFERENCES other2(other2)
                )""";
        SqlToRelCompiler calcite = this.getCompiler();
        List<ParsedStatement> node = calcite.parseStatements(query);
        Assert.assertNotNull(node);
    }
}
