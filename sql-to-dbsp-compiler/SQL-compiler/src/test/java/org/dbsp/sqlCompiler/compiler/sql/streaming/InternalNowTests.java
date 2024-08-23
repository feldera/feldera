package org.dbsp.sqlCompiler.compiler.sql.streaming;

import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.sql.StreamingTestBase;
import org.junit.Test;

/** Tests that exercise the internal implementation of NOW as a source operator */
public class InternalNowTests extends StreamingTestBase {
    @Test
    public void testNow() {
        String sql = """
                CREATE VIEW V AS SELECT 1, NOW() > TIMESTAMP '2022-12-12 00:00:00';""";
        DBSPCompiler compiler = this.testCompiler();
        compiler.compileStatements(sql);
        CompilerCircuitStream ccs = new CompilerCircuitStream(compiler);
        ccs.step("",
                """
                         c | compare | weight
                        ----------------------
                         1 | true    | 1""");
        this.addRustTestCase("testNow", ccs);
    }
}
