package org.dbsp.sqlCompiler.compiler.sql.functions;

import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.sql.SqlIoTest;
import org.junit.Test;

public class FunctionsTest extends SqlIoTest {
    @Override
    public void prepareData(DBSPCompiler compiler) {
        compiler.compileStatements("");
    }

    @Test
    public void testLeft() {
        this.q("SELECT LEFT('string', 1);\n" +
                "result\n" +
                "---------\n" +
                " s");
        this.q("SELECT LEFT('string', 0);\n" +
                "result\n" +
                "---------\n" +
                " ");
        this.q("SELECT LEFT('string', 100);\n" +
                "result\n" +
                "---------\n" +
                " string");
        this.q("SELECT LEFT('string', -2);\n" +
                "result\n" +
                "---------\n" +
                " ");
    }

    @Test
    public void testLeftNull() {
        this.q("SELECT LEFT(NULL, 100);\n" +
                "result\n" +
                "---------\n" +
                "NULL");
    }

    @Test
    public void testConcat() {
        this.q("SELECT CONCAT('string', 1);\n" +
                "result\n" +
                "---------\n" +
                " string1");
        this.q("SELECT CONCAT('string', 1, true);\n" +
                "result\n" +
                "---------\n" +
                " string1TRUE");
    }

    @Test
    public void testCoalesce() {
        this.q("SELECT COALESCE(NULL, 5);\n" +
                "result\n" +
                "------\n" +
                "5");
    }
}
