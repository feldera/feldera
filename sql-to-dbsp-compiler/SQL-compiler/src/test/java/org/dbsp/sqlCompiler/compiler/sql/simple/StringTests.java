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

    @Test
    public void testNonBmpLiterals() {
        // The optimized and unoptimized circuits must agree on characters above U+FFFF.
        // Validated on Postgres; '<' with the "C" collation, which orders by code point.
        this.qst("""
                SELECT CHAR_LENGTH('🐍'), POSITION('x' IN '🐍x'), SUBSTRING('🐍x' FROM 2), LEFT('🐍x', 1),
                       OVERLAY('🐍x' PLACING 'y' FROM 2 FOR 1);
                 c | p | s | l | o
                -------------------
                 1 | 2 | x | 🐍 | 🐍y
                (1 row)

                SELECT CHAR_LENGTH(CHR(128013)), CHAR_LENGTH(U&'\\D83D\\DC0D'), 'ｚ' < '🐍';
                 c | u | lt
                ------------
                 1 | 1 | true
                (1 row)""");
    }

    @Test
    public void testNonBmpCastsAndRanges() {
        // Each constant matches the same expression on a column, which the runtime computes
        var ccs = this.getCCS("""
                CREATE TABLE T(s VARCHAR);
                CREATE VIEW V AS SELECT CAST(s AS VARCHAR(1)), CAST('🐍x' AS VARCHAR(1)),
                    CHAR_LENGTH(CAST(LEFT(s, 1) AS CHAR(2))), CHAR_LENGTH(CAST('🐍' AS CHAR(2))),
                    TRIM(LEADING '🐍' FROM s) FROM T;""");
        ccs.step("INSERT INTO T VALUES('🐍x')", """
                 a | b | c | d | e | weight
                ----------------------------
                 🐍| 🐍| 2 | 2 | x| 1""");
        // Validated on Postgres with the "C" collation, which orders by code point
        ccs = this.getCCS("""
                CREATE TABLE T(s VARCHAR);
                CREATE VIEW V AS SELECT s FROM T WHERE s > 'ｚ' AND s < '🐍';""");
        ccs.step("INSERT INTO T VALUES('𝔸'), ('a'), ('🐍')", """
                 s | weight
                ------------
                 𝔸| 1""");
    }

    @Test
    public void testNonBmpFieldName() {
        // A ROW field name stays a literal, even with a character above U+FFFF
        this.getCC("""
                CREATE TYPE pair AS ("🐍" INT);
                CREATE TABLE T(r pair);
                CREATE VIEW V AS SELECT r['🐍'] FROM T;""");
    }

    /** LPAD and RPAD count characters, also above U+FFFF.  Validated on Postgres. */
    @Test
    public void testPad() {
        this.qst("""
                SELECT LPAD('hi', 5, 'xy'), RPAD('hi', 5, 'xy'), LPAD('hello', 2, 'x'), RPAD('hello', 2),
                       '[' || LPAD('hi', 4) || ']', '[' || RPAD('hi', 4) || ']';
                 a     | b     | c  | d  | e      | f
                ------------------------------------------
                 xyxhi | hixyx | he | he | [  hi] | [hi  ]
                (1 row)

                SELECT '[' || LPAD('hi', -1, 'x') || ']', '[' || RPAD('hi', 0) || ']', LPAD('hi', 5, ''), RPAD('hi', 5, ''),
                       LPAD('hi', 5, 'abcdefg'), RPAD('hi', 5, 'abcdefg');
                 a  | b  | c  | d  | e     | f
                -------------------------------------
                 [] | [] | hi | hi | abchi | hiabc
                (1 row)

                SELECT LPAD('abc', 3, 'x'), RPAD('abc', 3, 'x'), LPAD('', 3, 'ab'), RPAD('', 3, 'ab'),
                       LPAD('a', CAST(3 AS BIGINT), 'x');
                 a   | b   | c   | d   | e
                -----------------------------
                 abc | abc | aba | aba | xxa
                (1 row)

                SELECT LPAD(NULL, 3), RPAD('a', NULL), LPAD('a', 3, NULL);
                 a    | b    | c
                ------------------
                NULL|NULL|NULL
                (1 row)

                SELECT LPAD('ä', 4, '😀b'), RPAD('ä😀', 5, 'é'), LPAD('😀😀😀', 2, 'x'), RPAD('😀😀😀', 2, 'x'),
                       CHAR_LENGTH(LPAD('ä', 4, '😀b'));
                 a      | b      | c    | d    | e
                --------------------------------------
                 😀b😀ä | ä😀ééé | 😀😀 | 😀😀 | 4
                (1 row)""");
    }

    /** A NULL argument gives NULL.  Validated on Postgres. */
    @Test
    public void testPadNull() {
        var ccs = this.getCCS("""
                CREATE TABLE T(id INT, s VARCHAR, n INT, f VARCHAR);
                CREATE VIEW V AS SELECT id, LPAD(s, n, f) AS l, RPAD(s, n, f) AS r, '[' || LPAD(s, n) || ']' AS d FROM T;""")
                .withStringTrim();
        ccs.stepWeightOne("INSERT INTO T VALUES(1, 'ä', 3, '😀'), (2, NULL, 3, 'x'), (3, 'a', NULL, 'x'), (4, 'a', 3, NULL);", """
                 id | l    | r    | d
                ---------------------------
                 1  | 😀😀ä | ä😀😀 | [  ä]
                 2  |NULL  |NULL  |NULL
                 3  |NULL  |NULL  |NULL
                 4  |NULL  |NULL  | [  a]""");
    }

    /** A CHAR(n) argument keeps its trailing spaces, as in the other string functions */
    @Test
    public void testPadChar() {
        var ccs = this.getCCS("""
                CREATE TABLE T(c CHAR(4));
                CREATE VIEW V AS SELECT '[' || LPAD(c, 6, 'x') || ']' AS l, '[' || RPAD(c, 6, 'x') || ']' AS r,
                    '[' || LPAD(c, 3) || ']' AS t FROM T;""")
                .withStringTrim();
        ccs.stepWeightOne("INSERT INTO T VALUES('ab');", """
                 l        | r        | t
                -----------------------------
                 [xxab  ] | [ab  xx] | [ab ]""");
    }

    /** String and binary literals of different lengths combine as varying values: no padding,
     * exact comparisons.  Validated on Postgres. */
    @Test
    public void testStringLiteralTyping() {
        this.qst("""
                SELECT '[' || CASE WHEN 1 < 2 THEN 'a' ELSE 'bcd' END || ']', '[' || COALESCE(NULLIF('a', 'x'), 'bcd') || ']',
                       'a' = 'a ', 'a ' > 'a', 'a' IN ('a ', 'bc'), 'a' IN ('a', 'bc'), ARRAY_CONTAINS(ARRAY['a ', 'bc'], 'a'),
                       '[' || ARRAY['a', 'bcd'][1] || ']';
                 a   | b   | c     | d    | e     | f    | g     | h
                --------------------------------------------------------
                 [a] | [a] | false | true | false | true | false | [a]
                (1 row)

                SELECT '[' || s || ']' FROM (SELECT 'a' AS s UNION ALL SELECT 'bcd');
                 s
                -------
                 [a]
                 [bcd]
                (2 rows)

                SELECT ARRAY_CONTAINS(ARRAY[x'0100', x'0203'], x'01'), ARRAY_CONTAINS(ARRAY[x'0100', x'0203'], x'0100');
                 a     | b
                --------------
                 false | true
                (1 row)""");
        var ccs = this.getCCS("""
                CREATE TABLE T(v VARCHAR);
                CREATE VIEW V AS SELECT v = 'a ' AS e, v IN ('a ', 'bc') AS i,
                    '[' || CASE WHEN v = 'a' THEN 'x' ELSE 'yyy' END || ']' AS c FROM T;""")
                .withStringTrim();
        ccs.stepWeightOne("INSERT INTO T VALUES('a');", """
                 e     | i     | c
                -------------------
                 false | false | [x]""");
    }

    @Test
    public void testUnpairedSurrogate() {
        this.statementsFailingInCompilation("CREATE VIEW V AS SELECT U&'\\D83D';",
                "String literal contains the unpaired surrogate U+D83D");
    }
}
