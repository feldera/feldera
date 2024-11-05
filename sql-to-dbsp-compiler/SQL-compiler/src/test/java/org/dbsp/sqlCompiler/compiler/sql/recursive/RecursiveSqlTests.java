package org.dbsp.sqlCompiler.compiler.sql.recursive;

import org.dbsp.sqlCompiler.compiler.sql.tools.BaseSQLTests;
import org.junit.Test;

public class RecursiveSqlTests extends BaseSQLTests {
    @Test
    public void testFrontEnd() {
        this.showFinal();
        String sql = """
                CREATE RECURSIVE VIEW V(v INT);
                CREATE VIEW V AS SELECT v FROM V UNION SELECT 1;""";
        this.compileRustTestCase(sql);
    }

    @Test
    public void typeMismatchTest() {
        // Declared type does not match
        String sql = """
                CREATE RECURSIVE VIEW V(v INT);
                CREATE VIEW V AS SELECT CAST(V.v AS VARCHAR) FROM V UNION SELECT '1';""";
        this.statementsFailingInCompilation(sql, "does not match the declared type");

        // Declared column name does not match
        sql = """
                CREATE RECURSIVE VIEW V(v INT);
                CREATE VIEW V (v0) AS SELECT DISTINCT v FROM V UNION SELECT 1;""";
        this.statementsFailingInCompilation(sql, "does not match the declared type");

        // Declared recursive view not used anywhere
        sql = """
                CREATE RECURSIVE VIEW V(v INT);""";
        this.shouldWarn(sql, "Unused view declaration");
    }
}
