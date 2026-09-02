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

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlSetOption;
import org.apache.calcite.sql.parser.SqlAbstractParserImpl;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.type.SqlTypeName;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.StderrErrorReporter;
import org.dbsp.sqlCompiler.compiler.errors.SourceFileContents;
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
import org.dbsp.sqlCompiler.compiler.frontend.statements.CreateViewStatement;
import org.dbsp.sqlCompiler.compiler.frontend.statements.RelStatement;
import org.dbsp.generated.parser.DbspParserImpl;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.StringReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.Locale;

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
    public void testSetOption() throws SqlParseException {
        SqlToRelCompiler compiler = this.getCompiler();
        List<ParsedStatement> node = compiler.parseStatements("SET FELDERA_VARIABLE = 1;");
        Assert.assertNotNull(node);
        Assert.assertEquals(1, node.size());
        Assert.assertTrue(node.get(0).statement() instanceof SqlSetOption);
    }

    @Test
    public void hintTest() throws SqlParseException {
        SqlToRelCompiler compiler = this.getCompiler();
        List<ParsedStatement> node = compiler.parseStatements("""
                CREATE TABLE T(x INT);
                CREATE VIEW V AS SELECT /*+ broadcast(T) */ * FROM T JOIN T AS S USING (x);""");
        Assert.assertNotNull(node);
        Assert.assertEquals(2, node.size());
        Assert.assertTrue(node.get(1).statement() instanceof SqlCreateView);
        SqlNode query = ((SqlCreateView) node.get(1).statement()).query;
        Assert.assertTrue(query instanceof SqlSelect);
        Assert.assertNotNull(((SqlSelect) query).getHints());
        Assert.assertEquals(1, ((SqlSelect) query).getHints().size());

        var sources = new SourceFileContents();
        compiler.compile(node.get(0), sources);
        RelStatement q = compiler.compile(node.get(1), sources);
        Assert.assertTrue(q instanceof CreateViewStatement);
        RelNode rel = q.to(CreateViewStatement.class).getRel();
        Assert.assertTrue(rel instanceof LogicalProject);
        LogicalProject proj = (LogicalProject) rel;
        Assert.assertEquals(1, proj.getHints().size());
        Assert.assertTrue(proj.getInput() instanceof LogicalJoin);
        LogicalJoin join = (LogicalJoin) proj.getInput();
        Assert.assertEquals(1, join.getHints().size());
        Assert.assertEquals("broadcast", join.getHints().get(0).hintName);
    }

    @Test
    public void hintGrammarExampleTests() throws SqlParseException {
        // Test the example from the grammar documentation
        SqlToRelCompiler compiler = this.getCompiler();
        List<ParsedStatement> node = compiler.parseStatements("""
                CREATE TABLE T(x INT);
                CREATE TABLE S(x INT);
                CREATE VIEW V AS SELECT /*+ hint1, hint2(a='1', b='2') */ *
                FROM T /*+ hint3(5, 'x') */
                JOIN S /*+ hint4(c='id'), hint5 */ on T.x = S.x;""");
        Assert.assertNotNull(node);
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
    public void createAggregateTEst() throws SqlParseException {
        SqlToRelCompiler calcite = this.getCompiler();
        String ddl = """
                CREATE AGGREGATE x(data VARCHAR) RETURNS VARBINARY;
                CREATE TYPE I128 AS (a BIGINT, b BIGINT);
                CREATE LINEAR AGGREGATE yx(arg I128) RETURNS I128;
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
    public void internedTest() throws SqlParseException {
        SqlToRelCompiler calcite = this.getCompiler();
        String ddl = """
                CREATE TABLE st(
                   ts       TIMESTAMP,
                   name     VARCHAR INTERNED)""";
        SqlNode node = calcite.parse(ddl);
        Assert.assertNotNull(node);
        Assert.assertTrue(node instanceof SqlCreateTable);
        SqlCreateTable create = (SqlCreateTable) node;
        Assert.assertNotNull(create.columnsOrForeignKeys);
        SqlExtendedColumnDeclaration decl = (SqlExtendedColumnDeclaration) create.columnsOrForeignKeys.get(1);
        Assert.assertTrue(decl.interned);
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

    /** Use `keyword` as a column name, both in a table declaration and in a query.
     * Check that the resulting program compiles. */
    void columnNamed(String keyword) throws SqlParseException {
        SqlToRelCompiler compiler = this.getCompiler();
        List<ParsedStatement> statements = compiler.parseStatements(
                "CREATE TABLE T(" + keyword + " INT);\n" +
                "CREATE VIEW V AS SELECT " + keyword + " FROM T WHERE " + keyword + " > 0");
        Assert.assertEquals(keyword, 2, statements.size());
        SourceFileContents sources = new SourceFileContents();
        compiler.compile(statements.get(0), sources);
        RelStatement view = compiler.compile(statements.get(1), sources);
        RelNode rel = view.to(CreateViewStatement.class).getRel();
        Assert.assertEquals(keyword,
                List.of(keyword.toLowerCase(Locale.ENGLISH)), rel.getRowType().getFieldNames());
    }

    @Test
    public void unusedKeywordTest() throws SqlParseException {
        // Calcite's grammar declares these keywords but Feldera does not reserve them:
        for (String keyword: new String[] {
                "CONTAINS_SUBSTR", "DISCARD", "GRANT", "JSON_SCOPE",
                "PLANS", "SEED", "SEQUENCES", "TEMP" })
            this.columnNamed(keyword);

        // The function call parses without either of its keywords being reserved
        SqlToRelCompiler compiler = this.getCompiler();
        Assert.assertNotNull(compiler.parse(
                "SELECT contains_substr(s, 'cd', json_scope => 'JSON_KEYS') FROM T"));
    }

    /** Words that need double quotes to serve as identifiers, extracted from the parser implementation. */
    static Set<String> reservedKeywords() {
        SqlAbstractParserImpl.Metadata metadata =
                new DbspParserImpl(new StringReader("")).getMetadata();
        Set<String> reserved = new TreeSet<>();
        for (String token: metadata.getTokens()) {
            // The token list also holds operators such as "(" and ">="
            if (token.matches("[A-Z][A-Z0-9_-]*")
                    && metadata.isKeyword(token) && !metadata.isNonReservedKeyword(token))
                reserved.add(token);
        }
        return reserved;
    }

    /** Checks that the documentation for reserved keywords matches the implementation. */
    @Test
    public void documentedReservedKeywordsTest() throws IOException {
        Path grammar = Path.of("..", "..", "docs.feldera.com", "docs", "sql", "grammar.md");
        Assert.assertTrue("Cannot find " + grammar.toAbsolutePath(), Files.exists(grammar));

        // The table in the section "Reserved keywords": | S | `SELECT`, `SET`, ... |
        String text = Files.readString(grammar);
        int section = text.indexOf("\n## Reserved keywords");
        Assert.assertTrue(grammar + " has no section 'Reserved keywords'", section >= 0);
        int nextSection = text.indexOf("\n## ", section + 1);
        String table = text.substring(section, nextSection < 0 ? text.length() : nextSection);
        Set<String> documented = new TreeSet<>();
        Matcher row = Pattern.compile("^\\| ([A-Z]) \\| (.*) \\|$", Pattern.MULTILINE).matcher(table);
        while (row.find())
            for (String word: row.group(2).split(","))
                documented.add(word.replace("`", "").trim());

        Set<String> reserved = reservedKeywords();
        Set<String> undocumented = new TreeSet<>(reserved);
        undocumented.removeAll(documented);
        Set<String> stale = new TreeSet<>(documented);
        stale.removeAll(reserved);
        Assert.assertTrue("The reserved keyword table in " + grammar + " no longer matches the parser"
                        + "\nreserved but not documented: " + undocumented
                        + "\ndocumented but not reserved: " + stale,
                undocumented.isEmpty() && stale.isEmpty());
    }

    @Test
    public void keyColumnTest() throws SqlParseException {
        // KEY is not a reserved keyword
        SqlToRelCompiler compiler = this.getCompiler();
        List<ParsedStatement> statements = compiler.parseStatements("""
                CREATE TABLE T(key INT NOT NULL PRIMARY KEY);
                CREATE VIEW V AS SELECT key FROM T;""");
        Assert.assertEquals(2, statements.size());
        SourceFileContents sources = new SourceFileContents();
        compiler.compile(statements.get(0), sources);
        RelStatement view = compiler.compile(statements.get(1), sources);
        RelNode rel = view.to(CreateViewStatement.class).getRel();
        Assert.assertEquals(List.of("key"), rel.getRowType().getFieldNames());
    }

    @Test
    public void calciteReservedKeywordTest() throws SqlParseException {
        // A sample of words that core Calcite reserves and Feldera does not.  One word
        // per grammar rule that competes with an identifier: a function name, a type
        // name, an infix operator, and a clause word.
        for (String keyword: new String[] { "ABS", "CAST", "DOUBLE", "VARCHAR", "AND", "AS" })
            this.columnNamed(keyword);
    }

    @Test
    public void literalKeywordColumnTest() throws SqlParseException {
        // TRUE is not reserved, so it can be used as a column name, but a bare true
        // is the boolean literal rather than the INTEGER column
        SqlToRelCompiler compiler = this.getCompiler();
        List<ParsedStatement> statements = compiler.parseStatements("""
                CREATE TABLE T(true INT);
                CREATE VIEW V AS SELECT true FROM T;""");
        SourceFileContents sources = new SourceFileContents();
        compiler.compile(statements.get(0), sources);
        RelStatement view = compiler.compile(statements.get(1), sources);
        RelNode rel = view.to(CreateViewStatement.class).getRel();
        Assert.assertEquals(SqlTypeName.BOOLEAN,
                rel.getRowType().getFieldList().get(0).getType().getSqlTypeName());
    }
}
