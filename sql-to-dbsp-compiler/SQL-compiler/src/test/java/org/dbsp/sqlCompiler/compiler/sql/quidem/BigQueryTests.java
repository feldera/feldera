package org.dbsp.sqlCompiler.compiler.sql.quidem;

import org.dbsp.sqlCompiler.compiler.sql.tools.SqlIoTest;
import org.junit.Test;

/** Tests from babel/src/test/resources/sql/big-query.iq */
public class BigQueryTests extends SqlIoTest {
    @Test
    public void testRegexpReplace() {
        this.qs("""
                -- REGEXP_REPLACE(value, regexp, replacement)
                --
                -- Returns a STRING where all substrings of value that match regexp are replaced with replacement.
                -- Supports backslashed-escaped digits in replacement argument for corresponding capturing groups
                -- in regexp. Returns an exception if regex is invalid.
                SELECT REGEXP_REPLACE('qw1e1rt1y', '1', 'X');
                +----------+
                | EXPR$0   |
                +----------+
                | qwXeXrtXy|
                +----------+
                (1 row)
                
                SELECT REGEXP_REPLACE('a0b1c2d3', 'a|d', 'X');
                +---------+
                | EXPR$0  |
                +---------+
                | X0b1c2X3|
                +---------+
                (1 row)
                
                SELECT REGEXP_REPLACE('1=00--20=0', '(-)', '#');
                +-----------+
                | EXPR$0    |
                +-----------+
                | 1=00##20=0|
                +-----------+
                (1 row)""");
    }
}
