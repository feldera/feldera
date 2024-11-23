package org.dbsp.sqlCompiler.compiler.sql.simple;

import org.dbsp.sqlCompiler.compiler.sql.tools.SqlIoTest;
import org.junit.Test;

/** Tests taken from Calcite SqlOperatorTest */
public class CalciteSqlOperatorTest extends SqlIoTest {
    @Test
    public void testRegexReplace2Func() {
        this.qs("""
                select regexp_replace('a b c', 'b');
                 r
                ---
                 a  c
                (1 row)

                select regexp_replace('abc1 def2 ghi3', '[a-z]+');
                 r
                ---
                 1 2 3
                (1 row)

                select regexp_replace('100-200', '(\\d+)');
                 r
                ---
                 -
                (1 row)

                select regexp_replace('100-200', '(-)');
                 r
                ---
                 100200
                (1 row)""");
    }

    @Test
    public void testRegexReplace3Func() {
        this.qs("""
                select regexp_replace('a b c', 'b', 'X');
                 r
                ---
                 a X c
                (1 row)
                
                select regexp_replace('abc def ghi', '[a-z]+', 'X');
                 r
                ---
                 X X X
                (1 row)
                
                select regexp_replace('100-200', '(\\d+)', 'num');
                 r
                ---
                 num-num
                (1 row)
                
                select regexp_replace('100-200', '(-)', '###');
                 r
                ---
                 100###200
                (1 row)
                
                select regexp_replace(cast(null as varchar), '(-)', '###');
                 r
                ---
                NULL
                (1 row)
                
                select regexp_replace('100-200', cast(null as varchar), '###');
                 r
                ---
                NULL
                (1 row)
                
                select regexp_replace('100-200', '(-)', cast(null as varchar));
                 r
                ---
                NULL
                (1 row)
                
                select regexp_replace('abc\t
                def\t
                ghi', '\t', '+');
                 r
                ---
                 abc+\\ndef+\\nghi
                (1 row)
                
                select regexp_replace('abc\t\ndef\t\nghi', '\t\n', '+');
                 r
                ---
                 abc+def+ghi
                (1 row)
                
                select regexp_replace('abc\t\ndef\t\nghi', '\\w+', '+');
                 r
                ---
                 +\t\\n+\t\\n+
                (1 row)""");
    }

    @Test public void testRegexpReplaceCapture() {
        // modified from BigQuery, which uses a different syntax for capture groups.
        this.qs("""
                select regexp_replace('abc16', 'b(.*)(\\d)', '$2${1}X');
                 r
                ---
                 a6c1X
                (1 row)
                
                select regexp_replace('a\\bc56a\\bc37', 'b(.)(\\d)', '$2${0}X');
                 r
                ----
                 a\\5bc5X6a\\3bc3X7
                (1 row)
                
                select regexp_replace('abcdefghijabc', 'abc(.)', '$$123xyz');
                 r
                ---
                 $123xyzefghijabc
                (1 row)
                
                select regexp_replace('abcdefghijabc', 'abc(.)', '\1xy');
                 r
                ---
                 \1xyefghijabc
                (1 row)
                
                select regexp_replace('abc123', 'b(.*)(\\d)', '\\$ $\\');
                 r
                ---
                 a\\$ $\\
                (1 row)""");
    }

    @Test
    public void testDocs() {
        this.qs("""
                select regexp_replace('1078910', '[^01]');
                 r
                -----
                 1010
                (1 row)
                
                select regexp_replace('deep fried', '(?<first>\\w+)\\s+(?<second>\\w+)', '${first}_$second');
                 r
                ---
                 deep_fried
                (1 row)
                
                select regexp_replace('Springsteen, Bruce', '([^,\\s]+),\\s+(\\S+)', '$2 $1');
                 r
                ----
                 Bruce Springsteen
                (1 row)
                
                select regexp_replace('Springsteen, Bruce', '(?<last>[^,\\s]+),\\s+(?<first>\\S+)', '$first $last');
                 r
                ---
                 Bruce Springsteen
                (1 row)""");
    }
    
    @Test
    public void testConcatWithSeparator() {
        this.qs("""
                select concat_ws(',', 'a');
                 r
                ---
                 a
                (1 row)
                
                select concat_ws(',', 'a', 'b', null, 'c');
                 r
                ---
                 a,b,c
                (1 row)
                
                select concat_ws(',', cast('a' as varchar), cast('b' as varchar));
                 r
                ---
                 a,b
                (1 row)
                
                select concat_ws(',', cast('a' as varchar(2)), cast('b' as varchar(1)));
                 r
                ---
                 a,b
                (1 row)
                
                select concat_ws(',', '', '', '');
                 r
                ---
                 ,,
                (1 row)
                
                select concat_ws(',', null, null, null);
                 r
                ----
                \s
                (1 row)
                
                -- returns null if the separator is null
                select concat_ws(null, 'a', 'b');
                 r
                ---
                NULL
                (1 row)
                
                select concat_ws(null, null, null);
                 r
                ---
                NULL
                (1 row)
                
                select concat_ws('', cast('a' as varchar(2)), cast('b' as varchar(1)));
                 r
                ---
                 ab
                (1 row)
                
                select concat_ws('', '', '', '');
                 r
                ---
                \s
                (1 row)""");
        this.statementsFailingInCompilation("create view V as SELECT concat_ws(',')",
                "Invalid number of arguments to function 'CONCAT_WS'");
    }
}
