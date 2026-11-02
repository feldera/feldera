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

    @Test
    public void testUnpairedSurrogate() {
        this.statementsFailingInCompilation("CREATE VIEW V AS SELECT U&'\\D83D';",
                "String literal contains the unpaired surrogate U+D83D");
    }
}
