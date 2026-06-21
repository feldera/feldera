package org.dbsp.sqlCompiler.compiler.sql.simple;

import org.dbsp.sqlCompiler.compiler.sql.tools.SqlIoTest;
import org.junit.Test;

/** Tests for various string functions which are not derived from other sources */
public class StringTests extends SqlIoTest {
    @Test
    public void testReverseNegative() {
        this.statementsFailingInCompilation("CREATE VIEW V AS SELECT REVERSE(ARRAY(2));",
                "Cannot apply 'REVERSE' to arguments of type 'REVERSE(<INTEGER ARRAY>)'. " +
                        "Supported form(s): 'REVERSE(<CHARACTER>)'");
    }

    @Test
    public void testReverse() {
        this.qst("""
                SELECT REVERSE('Feldera');
                 r
                ---
                 aredleF
                (1 row)
                
                SELECT REVERSE('');
                 r
                ---
                \s
                (1 row)
                
                SELECT REVERSE(NULL);
                 r
                ---
                NULL
                (1 row)
                
                SELECT REVERSE('josé');
                 r
                ---
                 ésoj
                (1 row)""");
    }
}
