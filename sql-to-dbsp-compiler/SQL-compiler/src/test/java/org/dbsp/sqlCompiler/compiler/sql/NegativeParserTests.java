package org.dbsp.sqlCompiler.compiler.sql;

import org.dbsp.sqlCompiler.CompilerMain;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.TestUtil;
import org.dbsp.sqlCompiler.compiler.errors.CompilerMessages;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;

/**
 * Parser tests that are expected to fail.
 */
public class NegativeParserTests extends BaseSQLTests {
    @Override
    public CompilerOptions testOptions(boolean incremental, boolean optimize) {
        CompilerOptions options = super.testOptions(incremental, optimize);
        options.languageOptions.throwOnError = false;
        return options;
    }

    @Test
    public void validateKey() {
        String ddl = """
                create table git_commit (
                    git_commit_id bigint not null,
                    PRIMARY KEY (unknown)
                )""";
        DBSPCompiler compiler = this.testCompiler();
        compiler.compileStatement(ddl);
        TestUtil.assertMessagesContain(compiler.messages, "does not correspond to a column");
    }

    @Test
    public void testDuplicateTable() {
        String ddl = """
                CREATE TABLE T(T INT);
                CREATE TABLE T(T INT);
                """;
        DBSPCompiler compiler = this.testCompiler();
        compiler.compileStatements(ddl);
        TestUtil.assertMessagesContain(compiler.messages, "Duplicate declaration");
    }

    @Test
    public void duplicatedKey() {
        String ddl = """
                create table git_commit (
                    git_commit_id bigint not null PRIMARY KEY,
                    PRIMARY KEY (git_commit_id)
                )""";
        DBSPCompiler compiler = this.testCompiler();
        compiler.compileStatement(ddl);
        TestUtil.assertMessagesContain(compiler.messages, "in table with another PRIMARY KEY constraint");
    }

    @Test
    public void doubleDefaultTest() {
        String ddl = """
                CREATE TABLE productvariant_t (
                    id BIGINT DEFAULT NULL DEFAULT 1
                );""";
        DBSPCompiler compiler = this.testCompiler();
        compiler.compileStatement(ddl);
        TestUtil.assertMessagesContain(compiler.messages, "Column ID already has a default value");
    }

    @Test
    public void duplicatedKey2() {
        String ddl = "create table git_commit (\n" +
                "    git_commit_id bigint not null PRIMARY KEY PRIMARY KEY)";
        DBSPCompiler compiler = this.testCompiler();
        compiler.compileStatement(ddl);
        TestUtil.assertMessagesContain(compiler.messages, "Column GIT_COMMIT_ID already declared a primary key");
    }

    @Test
    public void duplicatedKey0() {
        String ddl = """
                create table git_commit (
                    git_commit_id bigint not null,
                    PRIMARY KEY (git_commit_id, git_commit_id)
                )""";
        DBSPCompiler compiler = this.testCompiler();
        compiler.compileStatement(ddl);
        TestUtil.assertMessagesContain(compiler.messages, "already declared as key");
    }

    @Test
    public void emptyPrimaryKey() {
        String ddl = """
                create table git_commit (
                    git_commit_id bigint not null,
                    PRIMARY KEY ()
                )""";
        DBSPCompiler compiler = this.testCompiler();
        compiler.compileStatement(ddl);
        TestUtil.assertMessagesContain(compiler.messages, "Error parsing SQL");
    }

    @Test
    public void testErrorMessage() {
        // TODO: this test may become invalid once we add support, so we need
        // here some truly invalid SQL.
        DBSPCompiler compiler = this.testCompiler();
        compiler.compileStatements("""
                create table PART_ORDER (
                    id bigint,
                    part bigint,
                    customer bigint,
                    target_date date
                );

                create table FULFILLMENT (
                    part_order bigint,
                    fulfillment_date date
                );

                create view FLAGGED_ORDER as
                select
                    part_order.customer,
                    AVG(DATEDIFF(day, part_order.target_date, fulfillment.fulfillment_date))
                    OVER (PARTITION BY part_order.customer
                          RANGE BETWEEN INTERVAL 90 days PRECEDING and CURRENT ROW) as avg_delay
                from
                    part_order
                    join
                    fulfillment
                    on part_order.id = fulfillment.part_order;
                """);
        TestUtil.assertMessagesContain(compiler.messages, 
                "Window specification must contain an ORDER BY clause");
    }

    @Test
    public void testTypeErrorMessage() {
        // TODO: this test may become invalid once we add support for ROW types
        DBSPCompiler compiler = this.testCompiler();
        compiler.compileStatements("CREATE VIEW V AS SELECT ROW(2, 2);\n");
        TestUtil.assertMessagesContain(compiler.messages, "error: Not yet implemented: ROW");
    }

    @Test
    public void duplicateColumnTest() {
        DBSPCompiler compiler = this.testCompiler();
        // allow multiple errors to be reported
        String ddl = "CREATE TABLE T (\n" +
                "COL1 INT" +
                ", COL1 DOUBLE" +
                ")";
        compiler.compileStatement(ddl);
        TestUtil.assertMessagesContain(compiler.messages, "Column with name 'COL1' already defined");
    }

    @Test
    public void testRejectFloatType() {
        String statement = "CREATE TABLE T(c1 FLOAT)";
        DBSPCompiler compiler = this.testCompiler();
        compiler.compileStatement(statement);
        Assert.assertTrue(compiler.hasErrors());
        TestUtil.assertMessagesContain(compiler.messages, "Do not use");
    }

    @Test
    public void errorTest() throws IOException, SQLException {
        String[] statements = new String[]{
                "This is not SQL"
        };
        File file = createInputScript(statements);
        CompilerMessages messages = CompilerMain.execute("-o", BaseSQLTests.testFilePath, file.getPath());
        Assert.assertEquals(messages.exitCode, 1);
        Assert.assertEquals(messages.errorCount(), 1);
        CompilerMessages.Error msg = messages.getError(0);
        Assert.assertFalse(msg.warning);
        Assert.assertEquals(msg.message, "Non-query expression encountered in illegal context");

        statements = new String[] {
                "CREATE VIEW V AS SELECT * FROM T"
        };
        file = createInputScript(statements);
        messages = CompilerMain.execute("-o", BaseSQLTests.testFilePath, file.getPath());
        Assert.assertEquals(messages.exitCode, 1);
        Assert.assertEquals(messages.errorCount(), 1);
        msg = messages.getError(0);
        Assert.assertFalse(msg.warning);
        Assert.assertEquals(msg.message, "Object 'T' not found");

        statements = new String[] {
                "CREATE VIEW V AS SELECT ST_MAKELINE(ST_POINT(0,0), ST_POINT(0, 0))"
        };
        file = createInputScript(statements);
        messages = CompilerMain.execute("-o", BaseSQLTests.testFilePath, file.getPath());
        Assert.assertEquals(messages.exitCode, 1);
        Assert.assertEquals(messages.errorCount(), 1);
        msg = messages.getError(0);
        Assert.assertFalse(msg.warning);
        Assert.assertEquals(msg.message, "cannot convert GEOMETRY literal to class org.locationtech.jts.geom.Point\n" +
                "LINESTRING (0 0, 0 0):GEOMETRY");
    }

    @Test
    public void compilerError() throws IOException, SQLException {
        String statement = "CREATE TABLE T (\n" +
                "  COL1 INT NOT NULL" +
                ", COL2 GARBAGE";
        File file = createInputScript(statement);
        CompilerMessages messages = CompilerMain.execute(file.getPath(), "-o", "/dev/null");
        Assert.assertEquals(messages.exitCode, 1);
        Assert.assertEquals(messages.errorCount(), 1);
        CompilerMessages.Error error = messages.messages.get(0);
        Assert.assertTrue(error.message.startsWith("Encountered \"<EOF>\""));
    }

    @Test
    public void warningTest() throws IOException, SQLException {
        String statements = """
                CREATE TABLE T (COL1 INT);
                CREATE TABLE S (COL1 INT);
                CREATE VIEW V AS SELECT * FROM S""";
        File file = createInputScript(statements);
        CompilerMessages messages = CompilerMain.execute(file.getPath(), "-o", "/dev/null");
        Assert.assertEquals(messages.exitCode, 0);
        Assert.assertEquals(messages.warningCount(), 1);
        Assert.assertEquals(messages.errorCount(), 0);
        CompilerMessages.Error error = messages.messages.get(0);
        Assert.assertTrue(error.warning);
        Assert.assertTrue(error.message.contains("Table 'T' is not used"));
    }
}
