package org.dbsp.sqlCompiler.compiler.sql;

import org.dbsp.sqlCompiler.CompilerMain;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.TestUtil;
import org.dbsp.sqlCompiler.compiler.errors.CompilerMessages;
import org.dbsp.sqlCompiler.compiler.sql.tools.BaseSQLTests;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;

/** Parser tests that are expected to fail. */
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
        this.statementsFailingInCompilation(ddl, "does not correspond to a column");
    }

    @Test
    public void testDuplicateTable() {
        String ddl = """
                CREATE TABLE T(T INT);
                CREATE TABLE T(T INT);
                """;
        this.statementsFailingInCompilation(ddl, "Duplicate declaration");
    }

    @Test
    public void testTableProperties() {
        String ddl = "CREATE TABLE T(T INT) WITH ( 5 )";
        this.statementsFailingInCompilation(ddl, "Error parsing SQL: Encountered \"5\" at");

        // properties need to be SQL strings
        ddl = "CREATE TABLE T(T INT) WITH ( a = b )";
        this.statementsFailingInCompilation(ddl, "Error parsing SQL: Encountered \"a\" at");

        // properties need to be SQL strings
        ddl = "CREATE TABLE T(T INT) WITH ( NULL = 'NULL' )";
        this.statementsFailingInCompilation(ddl, "Error parsing SQL: Encountered \"NULL\" at");

        // properties cannot be an empty list
        ddl = "CREATE TABLE T(T INT) WITH ( )";
        this.statementsFailingInCompilation(ddl, "Error parsing SQL: Encountered \")\" at");
    }

    @Test
    public void duplicatedKey() {
        String ddl = """
                create table git_commit (
                    git_commit_id bigint not null PRIMARY KEY,
                    PRIMARY KEY (git_commit_id)
                )""";
        this.statementsFailingInCompilation(ddl,  "in table with another PRIMARY KEY constraint");
    }

    @Test
    public void doubleDefaultTest() {
        String ddl = """
                CREATE TABLE productvariant_t (
                    id BIGINT DEFAULT NULL DEFAULT 1
                );""";
        this.statementsFailingInCompilation(ddl,  "Column 'id' already has a default value");
    }

    @Test
    public void duplicatedKey2() {
        String ddl = "create table git_commit (\n" +
                "    git_commit_id bigint not null PRIMARY KEY PRIMARY KEY)";
        this.statementsFailingInCompilation(ddl, "Column 'git_commit_id' already declared a primary key");
    }

    @Test
    public void duplicatedKey0() {
        String ddl = """
                create table git_commit (
                    git_commit_id bigint not null,
                    PRIMARY KEY (git_commit_id, git_commit_id)
                )""";
        this.statementsFailingInCompilation(ddl,  "already declared as key");
    }

    @Test
    public void emptyPrimaryKey() {
        String ddl = """
                create table git_commit (
                    git_commit_id bigint not null,
                    PRIMARY KEY ()
                )""";
        this.statementsFailingInCompilation(ddl,  "Error parsing SQL");
    }

    @Test
    public void testErrorMessage() {
        // TODO: this test may become invalid once we add support, so we need
        // here some truly invalid SQL.
        DBSPCompiler compiler = this.testCompiler();
        compiler.submitStatementsForCompilation("""
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
        TestUtil.assertMessagesContain(compiler,
                "Window specification must contain an ORDER BY clause");
    }

    @Test
    public void duplicateColumnTest() {
        String ddl = "CREATE TABLE T (\n" +
                "COL1 INT" +
                ", COL1 DOUBLE" +
                ");";
        this.statementsFailingInCompilation(ddl, "Column with name 'col1' already defined");
    }

    @Test
    public void testRejectFloatType() {
        String statement = "CREATE TABLE T(c1 FLOAT);";
        this.statementsFailingInCompilation(statement, "Do not use");
    }

    @Test
    public void errorTest() throws IOException, SQLException {
        File file = createInputScript("This is not SQL");
        CompilerMessages messages = CompilerMain.execute("--noRust", file.getPath());
        Assert.assertEquals(1, messages.exitCode);
        Assert.assertEquals(1, messages.errorCount());
        CompilerMessages.Message msg = messages.getMessage(0);
        Assert.assertFalse(msg.warning);
        Assert.assertEquals("Non-query expression encountered in illegal context", msg.message);

        file = createInputScript("CREATE VIEW V AS SELECT * FROM T;");
        messages = CompilerMain.execute("--noRust", file.getPath());
        Assert.assertEquals(1, messages.exitCode);
        Assert.assertEquals(1, messages.errorCount());
        msg = messages.getMessage(0);
        Assert.assertFalse(msg.warning);
        Assert.assertEquals("Object 't' not found", msg.message);

        file = createInputScript("CREATE VIEW V AS SELECT ST_MAKELINE(ST_POINT(0,0), ST_POINT(0, 0));");
        messages = CompilerMain.execute("--noRust", file.getPath());
        Assert.assertEquals(1, messages.exitCode);
        Assert.assertEquals(1, messages.errorCount());
        msg = messages.getMessage(0);
        Assert.assertFalse(msg.warning);
        TestUtil.assertMessagesContain(messages,
                "cannot convert GEOMETRY literal to class org.locationtech.jts.geom.Point\n" +
                "LINESTRING (0 0, 0 0):GEOMETRY");
    }

    @Test
    public void compilerError() throws IOException, SQLException {
        String statement = "CREATE TABLE T (\n" +
                "  COL1 INT NOT NULL" +
                ", COL2 GARBAGE";
        File file = createInputScript(statement);
        CompilerMessages messages = CompilerMain.execute(file.getPath(), "--noRust");
        Assert.assertEquals(1, messages.exitCode);
        Assert.assertEquals(1, messages.errorCount());
        CompilerMessages.Message error = messages.messages.get(0);
        Assert.assertTrue(error.message.startsWith("Encountered \"<EOF>\""));
    }

    @Test
    public void warningTest() throws IOException, SQLException {
        String statements = """
                CREATE TABLE T (COL1 INT);
                CREATE TABLE S (COL1 INT);
                CREATE VIEW V AS SELECT * FROM S""";
        File file = createInputScript(statements);
        CompilerMessages messages = CompilerMain.execute(file.getPath(), "--noRust");
        Assert.assertEquals(0, messages.exitCode);
        Assert.assertEquals(1, messages.warningCount());
        Assert.assertEquals(0, messages.errorCount());
        CompilerMessages.Message error = messages.messages.get(0);
        Assert.assertTrue(error.warning);
        Assert.assertTrue(error.message.contains("Table 't' is not used"));
        Assert.assertTrue(messages.toString().contains("CREATE TABLE T"));
    }
}
