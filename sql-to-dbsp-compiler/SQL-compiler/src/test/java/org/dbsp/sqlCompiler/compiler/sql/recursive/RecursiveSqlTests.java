package org.dbsp.sqlCompiler.compiler.sql.recursive;

import org.dbsp.sqlCompiler.compiler.sql.tools.BaseSQLTests;
import org.junit.Ignore;
import org.junit.Test;

public class RecursiveSqlTests extends BaseSQLTests {
    @Test @Ignore("No back-end support yet")
    public void testFrontEnd() {
        String sql = """
                CREATE RECURSIVE VIEW V(v INT);
                CREATE VIEW V AS SELECT DISTINCT v FROM V UNION SELECT 1;""";
        this.compileRustTestCase(sql);
    }
}
