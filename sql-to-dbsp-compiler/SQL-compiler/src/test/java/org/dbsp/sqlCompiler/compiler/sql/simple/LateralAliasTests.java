package org.dbsp.sqlCompiler.compiler.sql.simple;

import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.sql.tools.SqlIoTest;
import org.junit.Test;

public class LateralAliasTests extends SqlIoTest {
    @Override
    public void prepareInputs(DBSPCompiler compiler) {
        compiler.submitStatementsForCompilation("""
                CREATE TABLE T(x INT);
                INSERT INTO T VALUES(3);""");
    }

    @Test
    public void lateralTest() {
        // Tests from the SQL documentation in docs/sql/identifiers.md
        this.qs("""
                SELECT 1 as X, X+X as Y;
                 x | y
                -------
                 1 | 2
                (1 row)
                
                SELECT x+1 as Y
                FROM T -- T has a column x
                GROUP BY Y;
                 y
                ---
                 4
                (1 row)
                
                SELECT x+1 as Y
                FROM T
                GROUP BY Y
                HAVING Y > 0;
                 y
                ---
                 4
                (1 row)
                
                SELECT 1 as X, 1+1 as Y;
                 x | y
                -------
                 1 | 2
                (1 row)
                
                SELECT x+1 as Y
                FROM T
                GROUP BY x+1;
                 y
                ---
                 4
                (1 row)
                
                SELECT x+1 as Y
                FROM T
                GROUP BY x+1
                HAVING x+1 > 0;
                 y
                ---
                 4
                (1 row)
                
                SELECT 1+1 as x, x+x as Y FROM T;
                 x | y
                -------
                 2 | 6
                (1 row)""");
    }
}
