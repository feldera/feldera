package org.dbsp.sqlCompiler.compiler.sql;

import org.dbsp.sqlCompiler.CompilerMain;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.TestUtil;
import org.dbsp.sqlCompiler.compiler.errors.CompilerMessages;
import org.dbsp.sqlCompiler.compiler.sql.tools.BaseSQLTests;
import org.dbsp.sqlCompiler.compiler.sql.tools.DiagnosticAssertions;
import org.dbsp.sqlCompiler.compiler.sql.tools.DiagnosticAssertions.Target;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;

/** Parser tests that are expected to fail. */
public class NegativeParserTests extends BaseSQLTests {
    @Override
    public CompilerOptions testOptions() {
        CompilerOptions options = super.testOptions();
        options.languageOptions.throwOnError = false;
        options.ioOptions.quiet = false;
        return options;
    }

    @Test
    public void validateKey() {
        String ddl = """
                create table git_commit (
                    git_commit_id bigint not null,
                    PRIMARY KEY (unknown)
                )""";
        this.statementsFailingInCompilation(ddl, "does not correspond to a column",
                Diagnostic.error("No such column", 3, 18,
                        "    3|    PRIMARY KEY (unknown)\n" +
                        "                       ^^^^^^^\n" +
                        "    4|)\n"),
                Diagnostic.error("Compilation error", 0, 0, ""));
    }

    @Test
    public void testDuplicateTable() {
        String ddl = """
                CREATE TABLE T(T INT);
                CREATE TABLE T(T INT);
                """;
        this.statementsFailingInCompilation(ddl, "Duplicate declaration",
                Diagnostic.error("Duplicate declaration", 2, 1,
                        "    1|CREATE TABLE T(T INT);\n" +
                        "    2|CREATE TABLE T(T INT);\n" +
                        "      ^^^^^^^^^^^^^^^^^^^^^\n"),
                Diagnostic.error("Duplicate declaration", 1, 1,
                        "    1|CREATE TABLE T(T INT);\n" +
                        "      ^^^^^^^^^^^^^^^^^^^^^\n" +
                        "    2|CREATE TABLE T(T INT);\n"),
                Diagnostic.error("Compilation error", 0, 0, ""));
    }

    @Test
    public void testTableProperties() {
        String ddl = "CREATE TABLE T(T INT) WITH ( 5 )";
        this.statementsFailingInCompilation(ddl, "Error parsing SQL: Encountered \"5\".\n",
                Diagnostic.error("Error parsing SQL", 1, 30,
                        "    1|CREATE TABLE T(T INT) WITH ( 5 )\n" +
                        "                                   ^\n"));

        // properties need to be SQL strings
        ddl = "CREATE TABLE T(T INT) WITH ( a = b )";
        this.statementsFailingInCompilation(ddl, "Error parsing SQL: Encountered \"a\".\n",
                Diagnostic.error("Error parsing SQL", 1, 30,
                        "    1|CREATE TABLE T(T INT) WITH ( a = b )\n" +
                        "                                   ^\n"));

        // properties need to be SQL strings
        ddl = "CREATE TABLE T(T INT) WITH ( NULL = 'NULL' )";
        this.statementsFailingInCompilation(ddl, "Error parsing SQL: Encountered \"NULL\".\n",
                Diagnostic.error("Error parsing SQL", 1, 30,
                        "    1|CREATE TABLE T(T INT) WITH ( NULL = 'NULL' )\n" +
                        "                                   ^^^^\n"));

        // properties cannot be an empty list
        ddl = "CREATE TABLE T(T INT) WITH ( )";
        this.statementsFailingInCompilation(ddl, "Error parsing SQL: Encountered \")\".\n",
                Diagnostic.error("Error parsing SQL", 1, 30,
                        "    1|CREATE TABLE T(T INT) WITH ( )\n" +
                        "                                   ^\n"));
    }

    @Test
    public void duplicatedKey() {
        String ddl = """
                create table git_commit (
                    git_commit_id bigint not null PRIMARY KEY,
                    PRIMARY KEY (git_commit_id)
                )""";
        this.statementsFailingInCompilation(ddl, "in table with another PRIMARY KEY constraint",
                Diagnostic.error("Duplicate key", 2, 5,
                        "    2|    git_commit_id bigint not null PRIMARY KEY,\n" +
                        "          ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^\n" +
                        "    3|    PRIMARY KEY (git_commit_id)\n"),
                Diagnostic.error("Duplicate key", 3, 5,
                        "    3|    PRIMARY KEY (git_commit_id)\n" +
                        "          ^^^^^^^^^^^^^^^^^^^^^^^^^^^\n" +
                        "    4|)\n"),
                Diagnostic.error("Compilation error", 0, 0, ""));
    }

    @Test
    public void doubleDefaultTest() {
        String ddl = """
                CREATE TABLE productvariant_t (
                    id BIGINT DEFAULT NULL DEFAULT 1
                );""";
        this.statementsFailingInCompilation(ddl, "Column 'id' already has a default value",
                Diagnostic.error("Compilation error", 2, 36,
                        "    2|    id BIGINT DEFAULT NULL DEFAULT 1\n" +
                        "                                         ^\n" +
                        "    3|);\n"));
    }

    @Test
    public void defaultValueErrorPosition() {
        // The error points at the literal in the program, and is reported once
        String ddl = """
                CREATE TABLE T (
                    a DOUBLE DEFAULT 1e400
                );""";
        this.statementsFailingInCompilation(ddl,
                Diagnostic.error("Compilation error", 2, 22,
                        "    2|    a DOUBLE DEFAULT 1e400\n" +
                        "                           ^^^^^\n" +
                        "    3|);\n"));
    }

    @Test
    public void latenessErrorPosition() {
        // The error points at the lateness expression in the program
        String ddl = """
                CREATE TABLE T (
                    ts TIMESTAMP LATENESS INTERVAL 1e400 SECONDS
                );""";
        this.statementsFailingInCompilation(ddl,
                Diagnostic.error("Compilation error", 2, 27,
                        "    2|    ts TIMESTAMP LATENESS INTERVAL 1e400 SECONDS\n" +
                        "                                ^^^^^^^^^^^^^^^^^^^^^^\n" +
                        "    3|);\n"));
    }

    @Test
    public void duplicatedKey2() {
        String ddl = "create table git_commit (\n" +
                "    git_commit_id bigint not null PRIMARY KEY PRIMARY KEY)";
        this.statementsFailingInCompilation(ddl, "Column 'git_commit_id' already declared a primary key",
                Diagnostic.error("Compilation error", 2, 47,
                        "    1|create table git_commit (\n" +
                        "    2|    git_commit_id bigint not null PRIMARY KEY PRIMARY KEY)\n" +
                        "                                                    ^^^^^^^^^^^\n"));
    }

    @Test
    public void duplicatedKey0() {
        String ddl = """
                create table git_commit (
                    git_commit_id bigint not null,
                    PRIMARY KEY (git_commit_id, git_commit_id)
                )""";
        this.statementsFailingInCompilation(ddl, "already declared as key",
                Diagnostic.error("Duplicate key column", 3, 33,
                        "    3|    PRIMARY KEY (git_commit_id, git_commit_id)\n" +
                        "                                      ^^^^^^^^^^^^^\n" +
                        "    4|)\n"),
                Diagnostic.error("Duplicate key column", 3, 18,
                        "    3|    PRIMARY KEY (git_commit_id, git_commit_id)\n" +
                        "                       ^^^^^^^^^^^^^\n" +
                        "    4|)\n"),
                Diagnostic.error("Compilation error", 0, 0, ""));
    }

    @Test
    public void emptyPrimaryKey() {
        String ddl = """
                create table git_commit (
                    git_commit_id bigint not null,
                    PRIMARY KEY ()
                )""";
        this.statementsFailingInCompilation(ddl, "Error parsing SQL",
                Diagnostic.error("Error parsing SQL", 3, 18,
                        "    3|    PRIMARY KEY ()\n" +
                        "                       ^\n" +
                        "    4|)\n"));
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
        this.statementsFailingInCompilation(ddl, "Column with name 'col1' already defined",
                Diagnostic.error("Duplicate name", 2, 11,
                        "    1|CREATE TABLE T (\n" +
                        "    2|COL1 INT, COL1 DOUBLE);\n" +
                        "                ^^^^\n"),
                Diagnostic.error("Duplicate name", 2, 1,
                        "    1|CREATE TABLE T (\n" +
                        "    2|COL1 INT, COL1 DOUBLE);\n" +
                        "      ^^^^^^^^\n"),
                Diagnostic.error("Compilation error", 0, 0, ""));
    }

    @Test
    public void testRejectFloatType() {
        String statement = "CREATE TABLE T(c1 FLOAT);";
        this.statementsFailingInCompilation(statement, "Do not use",
                Diagnostic.error("Illegal type", 1, 19,
                        "    1|CREATE TABLE T(c1 FLOAT);\n" +
                        "                        ^^^^^\n"));
    }

    /** The body of a function is compiled inside a generated program; an error on
     * the second line of the body must point at that line of the user's program */
    @Test
    public void functionBodyErrorPosition() {
        this.statementsFailingInCompilation("""
                CREATE TABLE T(x INT);
                CREATE FUNCTION f(a INT) RETURNS INT AS
                  a +
                  nosuch;
                CREATE VIEW V AS SELECT f(x) FROM T;""",
                "(no input file):4:3: error: Compilation error: Column 'nosuch' not found in any table\n");
    }

    @Test
    public void errorTest() throws IOException, SQLException {
        File file = createInputScript("This is not SQL");
        CompilerMessages messages = CompilerMain.execute("--noRust", file.getPath());
        Assert.assertEquals(1, messages.exitCode);
        Assert.assertEquals(1, messages.errorCount());
        CompilerMessages.Message msg = messages.getMessage(0);
        Assert.assertFalse(msg.warning);
        // The message continues with the tokens that the parser expected
        DiagnosticAssertions.assertStartsWith(Target.MESSAGE_BODY, "Encountered \"not SQL\".", msg.message);

        file = createInputScript("CREATE VIEW V AS SELECT * FROM T;");
        messages = CompilerMain.execute("--noRust", file.getPath());
        Assert.assertEquals(1, messages.exitCode);
        Assert.assertEquals(1, messages.errorCount());
        msg = messages.getMessage(0);
        Assert.assertFalse(msg.warning);
        TestUtil.assertMessagesContain(messages, "Object 't' not found");

        file = createInputScript("CREATE VIEW V AS SELECT ST_MAKELINE(ST_POINT(0,0), ST_POINT(0, 0));");
        messages = CompilerMain.execute("--noRust", file.getPath());
        Assert.assertEquals(1, messages.exitCode);
        Assert.assertEquals(1, messages.errorCount());
        msg = messages.getMessage(0);
        Assert.assertFalse(msg.warning);
        TestUtil.assertMessagesContain(messages,
                "cannot convert GEOMETRY literal to class org.locationtech.jts.geom.Point\n" +
                "'LINESTRING (0 0, 0 0)'");
    }

    @Test
    public void issue6069() {
        var cc = this.getCC("""
                CREATE TABLE T(x DECIMAL);
                CREATE VIEW V AS SELECT CAST(x AS DECIMAL) FROM T;""");
        TestUtil.assertMessagesContain(cc.compiler.messages, """
                (no input file): DECIMAL precision and scale unspecified
                (no input file):1:18: warning: DECIMAL precision and scale unspecified: DECIMAL/NUMERIC type used without specifying precision and scale.
                This is interpreted as DECIMAL(38, 0)
                    1|CREATE TABLE T(x DECIMAL);
                                       ^^^^^^^
                    2|CREATE VIEW V AS SELECT CAST(x AS DECIMAL) FROM T;
                (no input file): DECIMAL precision and scale unspecified
                (no input file):2:35: warning: DECIMAL precision and scale unspecified: DECIMAL/NUMERIC type used without specifying precision and scale.
                This is interpreted as DECIMAL(38, 0)
                    1|CREATE TABLE T(x DECIMAL);
                    2|CREATE VIEW V AS SELECT CAST(x AS DECIMAL) FROM T;
                                                        ^^^^^^^""");
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
        DiagnosticAssertions.assertStartsWith(Target.MESSAGE_BODY, "Encountered \"<EOF>\"", error.message);
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
        DiagnosticAssertions.assertContains(Target.MESSAGE_BODY, "Table 't' is not used", error.message);
        DiagnosticAssertions.assertContains(Target.SOURCE, "CREATE TABLE T", messages.toString());
    }

    @Test
    public void predefinedFunctionNameTest() {
        // The rule holds for every predefined function, not just for the 'user' of
        // RegressionTests.issue3006, and quoting the name does not evade it
        String message = "has the same name as a predefined function";
        this.statementsFailingInCompilation("CREATE TABLE abs(x INT);", message);
        this.statementsFailingInCompilation("CREATE TABLE \"now\"(x INT);", message);
    }

    @Test
    public void niladicFunctionColumnTest() {
        // SQL calls USER without parentheses, so a bare 'user' is that call and not the
        // column; MetadataTests.predefinedNameColumnTest shows how to reach the column
        this.statementsFailingInCompilation("""
                CREATE TABLE T(user INT);
                CREATE VIEW V AS SELECT user FROM T;""",
                "Function 'USER' is not supported.  If you meant a column with this name, "
                + "qualify it with a table name or quote it: \"user\"");
    }

    @Test
    public void niladicDateFunctionTest() {
        // The date and time functions have a supported replacement, named first
        this.statementsFailingInCompilation("""
                CREATE TABLE T(x INT);
                CREATE VIEW V AS SELECT current_date FROM T;""",
                "Function 'CURRENT_DATE' is not supported; use CAST(NOW() AS DATE) instead.");
    }

    @Test
    public void distinctOnTest() {
        // The newline checks that nothing follows the URL
        this.statementsFailingInCompilation("""
                CREATE TABLE raw(id INT, ver INT, v INT);
                CREATE VIEW v AS SELECT DISTINCT ON (id) id, ver, v FROM raw ORDER BY id, ver DESC;""", """
                2:18: error: Not supported: SELECT DISTINCT ON is not supported.
                See https://docs.feldera.com/sql/unsupported-operations#select-distinct-on
                """);
    }
}
