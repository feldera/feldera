package org.dbsp.sqlCompiler.compiler.sql.postgres;

import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.sql.tools.SqlIoTest;
import org.junit.Test;

/**
 * <a href="https://github.com/postgres/postgres/blob/master/src/test/regress/expected/collate.out">...</a>
 */
public class PostgresCollateTests extends SqlIoTest {
    @Override
    public void prepareInputs(DBSPCompiler compiler) {
        // Per-column collation not supported in Calcite
        String sql = """
                CREATE TABLE collate_test10 (
                    a int,
                    x text, -- COLLATE "C",
                    y text -- COLLATE "POSIX"
                );
                INSERT INTO collate_test10 VALUES (1, 'hij', 'hij'), (2, 'HIJ', 'HIJ');""";
        compiler.compileStatements(sql);
    }

    @Test
    public void testUpperLower() {
        this.q("""
                SELECT a, lower(x), lower(y), upper(x), upper(y), initcap(x), initcap(y) FROM collate_test10;
                 a | lower | lower | upper | upper | initcap | initcap
                ---+-------+-------+-------+-------+---------+---------
                 1 | hij| hij| HIJ| HIJ| Hij| Hij
                 2 | hij| hij| HIJ| HIJ| Hij| Hij""");
    }
}
