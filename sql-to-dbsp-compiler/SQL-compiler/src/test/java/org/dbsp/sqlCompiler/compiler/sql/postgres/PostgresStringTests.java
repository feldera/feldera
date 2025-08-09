package org.dbsp.sqlCompiler.compiler.sql.postgres;

import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.sql.tools.SqlIoTest;
import org.junit.Ignore;
import org.junit.Test;

/** https://github.com/postgres/postgres/blob/master/src/test/regress/expected/strings.out */
@SuppressWarnings("JavadocLinkAsPlainText")
public class PostgresStringTests extends SqlIoTest {
    @Override
    public void prepareInputs(DBSPCompiler compiler) {
        String data =
                "CREATE TABLE CHAR_TBL(f1 char(4));\n" +
                "INSERT INTO CHAR_TBL (f1) VALUES\n" +
                "  ('a'),\n" +
                "  ('ab'),\n" +
                "  ('abcd'),\n" +
                "  ('abcd    ');" +
                "CREATE TABLE VARCHAR_TBL(f1 varchar(4));\n" +
                "INSERT INTO VARCHAR_TBL (f1) VALUES\n" +
                "  ('a'),\n" +
                "  ('ab'),\n" +
                "  ('abcd'),\n" +
                "  ('abcd    ');\n" +
                "CREATE TABLE UVARCHAR_TBL(f1 varchar);\n" +
                "INSERT INTO UVARCHAR_TBL (f1) VALUES\n" +
                "  ('a'),\n" +
                "  ('ab'),\n" +
                "  ('abcd'),\n" +
                "  ('abcd    ');\n" +
                "CREATE TABLE TEXT_TBL (f1 text);\n" +
                "INSERT INTO TEXT_TBL VALUES\n" +
                "  ('doh!'),\n" +
                "  ('hi de ho neighbor');\n" +
                // "CREATE TABLE byteatest (a bytea PRIMARY KEY, b int);\n"
                "\n";
        compiler.submitStatementsForCompilation(data);
    }

    @Test
    public void continuationTest() {
        this.q("""
                SELECT 'first line'
                ' - next line'
                \t' - third line'
                \tAS "Three lines to one";
                         Three lines to one         \s
                -------------------------------------
                 first line - next line - third line""");
    }

    @Test
    public void illegalContinuationTest() {
        // Cannot continue a string without a newline
        this.queryFailingInCompilation("""
                        SELECT 'first line' ' - next line'
                        \tAS "Illegal comment within continuation"
                        """,
                "String literal continued on same line");
    }

    @Test
    public void unicodeIdentifierTest() {
        // Calcite does not support 6-digits escapes like Postgres, so
        // I have modified +000061 to just 0061
        this.q("""
                SELECT U&'d\\0061t\\0061' AS U&"d\\0061t\\0061";
                 data
                ------
                 data""");
    }

    @Test
    public void unicodeNewEscapeTest() {
        this.q(
                """
                        SELECT U&'d!0061t!0061' UESCAPE '!' AS U&"d*0061t\\0061" UESCAPE '*';
                         data
                        ------
                         data""");
    }

    @Test
    public void namesWithSlashTest() {
        this.q("""
                SELECT U&'a\\\\b' AS "a\\b";
                 a\\b
                -----
                 a\\b""");
    }

    @Test
    public void backslashWithSpacesTest() {
        this.q("""
                SELECT U&' \\' UESCAPE '!' AS "tricky";
                tricky
                --------
                  \\""");
    }

    @Test
    public void invalidUnicodeTest() {
        this.queryFailingInCompilation("SELECT U&'wrong: \\061'\n",
                "Unicode escape sequence starting at character 7 is not exactly four hex digits");
    }

    // Lots of other escaping tests skipped, many using the E escaping notation from Postgres

    @Test
    public void testCharN() {
        this.q("""
                SELECT CAST(f1 AS text) AS "text(char)", 1 FROM CHAR_TBL;
                 text(char) | 1
                ----------------
                 a   | 1
                 ab  | 1
                 abcd| 1
                 abcd| 1""");
    }

    @Test
    public void testBinary() {
        this.q("""
                SELECT x'DeAdBeEf';
                   bytea
                ------------
                 deadbeef""");

    }

    @Test
    public void invalidDigits() {
        this.queryFailingInCompilation("SELECT x'A b'",
                "Binary literal string must contain only characters");
        this.queryFailingInCompilation("SELECT x'Ax'",
                "Binary literal string must contain an even number of hexits");
        this.queryFailingInCompilation("SELECT x'A'",
                "Binary literal string must contain an even number of hexits");
    }

    @Test
    public void testVarcharN() {
        this.q("""
                SELECT CAST(f1 AS text) AS "text(varchar)", 1 FROM VARCHAR_TBL;
                 text(varchar) | 1
                -------------------
                 a|    1
                 ab|   1
                 abcd| 1
                 abcd| 1""");
    }

    @Test
    public void testVarchar() {
        this.q("""
                SELECT f1 AS "text(varchar)", 1 FROM UVARCHAR_TBL;
                 text(varchar) | 1
                -------------------
                 a| 1
                 ab| 1
                 abcd| 1
                 abcd    | 1""");
    }

    // 'name' not supported
    // SELECT CAST(name 'namefield' AS text) AS "text(name)"

    @Test
    public void testTextTbl() {
        this.q("""
                SELECT CAST(f1 AS char(10)) AS "char(text)" FROM TEXT_TBL;
                char(text)
                ------------
                 doh!     \s
                 hi de ho n""");
    }

    @Test
    public void testWiderChar() {
        this.q("""
                SELECT CAST(f1 AS char(20)) AS "char(text)" FROM TEXT_TBL;
                      char(text)
                ----------------------
                 doh!               \s
                 hi de ho neighbor  \s""");
    }

    @Test
    public void testWiderVarchar() {
        this.q("""
                SELECT CAST(f1 AS char(10)) AS "char(varchar)" FROM VARCHAR_TBL;
                 char(varchar)
                ---------------
                 a        \s
                 ab       \s
                 abcd     \s
                 abcd     \s""");
    }

    @Test
    public void testTextTbl2() {
        this.q("""
                SELECT CAST(f1 AS varchar) AS "varchar(text)" FROM TEXT_TBL;
                   varchar(text)
                -------------------
                 doh!
                 hi de ho neighbor""");
    }

    @Test
    public void testCharTbl() {
        this.q("""
                SELECT CAST(f1 AS varchar) AS "varchar(char)", 1 FROM CHAR_TBL;
                 varchar(char) | 1
                -------------------
                 a   | 1
                 ab  | 1
                 abcd| 1
                 abcd| 1""");
    }

    @Test
    public void testTrimConstant() {
        this.qs("""
                SELECT TRIM(BOTH FROM '  bunch o blanks  ') = 'bunch o blanks' AS "bunch o blanks";
                 bunch o blanks
                ----------------
                 t
                (1 row)

                SELECT TRIM(LEADING FROM '  bunch o blanks  ') = 'bunch o blanks  ' AS "bunch o blanks  ";
                 bunch o blanks
                ------------------
                 t
                (1 row)

                SELECT TRIM(TRAILING FROM '  bunch o blanks  ') = '  bunch o blanks' AS "  bunch o blanks";
                   bunch o blanks
                ------------------
                 t
                (1 row)
                """);
    }

    @Test
    public void testTrim() {
        this.qs("""
                SELECT TRIM(BOTH FROM '  bunch o blanks  ') = 'bunch o blanks' AS "bunch o blanks";
                 bunch o blanks
                ----------------
                 t
                (1 row)

                SELECT TRIM(LEADING FROM '  bunch o blanks  ') = 'bunch o blanks  ' AS "bunch o blanks  ";
                 bunch o blanks
                ------------------
                 t
                (1 row)

                SELECT TRIM(TRAILING FROM '  bunch o blanks  ') = '  bunch o blanks' AS "  bunch o blanks";
                   bunch o blanks
                ------------------
                 t
                (1 row)
                """);
    }

    @Test
    public void testTrimArg() {
        this.q("""
                SELECT TRIM(BOTH 'x' FROM 'xxxxxsome Xsxxxxx') = 'some Xs' AS "some Xs";
                 some Xs
                ---------
                 t""");
    }

    @Test
    public void testSubstring() {
        this.qs("""
               SELECT SUBSTRING('1234567890' FROM 3) = '34567890' AS "34567890";
                34567890
               ----------
                t
               (1 row)

               SELECT SUBSTRING('1234567890' FROM 4 FOR 3) = '456' AS "456";
                456
               -----
                t
               (1 row)
        
               SELECT SUBSTRING('string' FROM -10 FOR 2147483646) AS "string";
                string
               --------
                string
               (1 row)
               
               SELECT SUBSTRING('string' FROM -10 FOR 5) AS "string";
                string
               --------
               \s
               (1 row)""");
    }

    @Test
    public void testSubstringOverflow() {
        this.q(
                """
                        SELECT SUBSTRING('string' FROM 2 FOR 2147483646) AS "tring";
                         tring
                        -------
                         tring""");
    }

    @Test
    public void testNegativeSubstringLength() {
        this.q("""
                SELECT SUBSTRING('string' FROM -10 FOR -2147483646) AS "error";
                error
                ------
                \s""");
        this.q("""
                SELECT SUBSTRING('string' FROM 0 FOR -2) AS "error";
                error
                ------
                \s""");
    }

    @Test
    public void testPosition() {
        this.q("""
                SELECT POSITION('4' IN '1234567890') = '4' AS "4";
                 4
                ---
                 t""");
        this.q("""
                SELECT POSITION('5' IN '1234567890') = '5' AS "5";
                 5
                ---
                 t""");
        this.q("""
                SELECT POSITION('A' IN '1234567890') = '0' AS "0";
                 5
                ---
                 t""");
    }

    // SUBSTRING ... SIMILAR syntax not supported
    // SELECT SUBSTRING('abcdefg' SIMILAR 'a#"(b_d)#"%' ESCAPE '#') AS "bcd";

    @Test
    public void testOverlay() {
        this.q("""
                SELECT OVERLAY('abcdef' PLACING '45' FROM 4) AS "abc45f";
                 abc45f
                --------
                 abc45f""");
        this.q("""
                SELECT OVERLAY('yabadoo' PLACING 'daba' FROM 5) AS "yabadaba";
                 yabadaba
                ----------
                 yabadaba""");
        this.q("""
                SELECT OVERLAY('yabadoo' PLACING 'daba' FROM 5 FOR 0) AS "yabadabadoo";
                 yabadabadoo
                -------------
                 yabadabadoo""");
        this.q("""
                SELECT OVERLAY('babosa' PLACING 'ubb' FROM 2 FOR 4) AS "bubba";
                 bubba
                -------
                 bubba""");
    }

    @Test @Ignore("Not yet implemented")
    public void testRegexpReplace() {
        this.qs("""
                SELECT regexp_replace('1112223333', E'(\\\\d{3})(\\\\d{3})(\\\\d{4})', E'(\\\\1) \\\\2-\\\\3');
                 regexp_replace
                ----------------
                 (111) 222-3333
                (1 row)

                SELECT regexp_replace('foobarrbazz', E'(.)\\\\1', E'X\\\\&Y', 'g');
                  regexp_replace
                -------------------
                 fXooYbaXrrYbaXzzY
                (1 row)

                SELECT regexp_replace('foobarrbazz', E'(.)\\\\1', E'X\\\\\\\\Y', 'g');
                 regexp_replace
                ----------------
                 fX\\YbaX\\YbaX\\Y
                (1 row)

                SELECT regexp_replace('foobarrbazz', E'(.)\\\\1', E'X\\\\Y\\\\1Z\\\\');
                 regexp_replace
                -----------------
                 fX\\YoZ\\barrbazz
                (1 row)

                SELECT regexp_replace('AAA   BBB   CCC   ', E'\\\\s+', ' ', 'g');
                 regexp_replace
                ----------------
                 AAA BBB CCC\\s
                (1 row)

                SELECT regexp_replace('AAA', '^|$', 'Z', 'g');
                 regexp_replace
                ----------------
                 ZAAAZ
                (1 row)

                SELECT regexp_replace('AAA aaa', 'A+', 'Z', 'gi');
                 regexp_replace
                ----------------
                 Z Z
                (1 row)

                SELECT regexp_replace('A PostgreSQL function', 'A|e|i|o|u', 'X', 1);
                    regexp_replace
                -----------------------
                 X PostgreSQL function
                (1 row)

                SELECT regexp_replace('A PostgreSQL function', 'A|e|i|o|u', 'X', 1, 2);
                    regexp_replace
                -----------------------
                 A PXstgreSQL function
                (1 row)

                SELECT regexp_replace('A PostgreSQL function', 'a|e|i|o|u', 'X', 1, 0, 'i');
                    regexp_replace
                -----------------------
                 X PXstgrXSQL fXnctXXn
                (1 row)

                SELECT regexp_replace('A PostgreSQL function', 'a|e|i|o|u', 'X', 1, 1, 'i');
                    regexp_replace
                -----------------------
                 X PostgreSQL function
                (1 row)

                SELECT regexp_replace('A PostgreSQL function', 'a|e|i|o|u', 'X', 1, 2, 'i');
                    regexp_replace
                -----------------------
                 A PXstgreSQL function
                (1 row)

                SELECT regexp_replace('A PostgreSQL function', 'a|e|i|o|u', 'X', 1, 3, 'i');
                    regexp_replace
                -----------------------
                 A PostgrXSQL function
                (1 row)

                SELECT regexp_replace('A PostgreSQL function', 'a|e|i|o|u', 'X', 1, 9, 'i');
                    regexp_replace
                -----------------------
                 A PostgreSQL function
                (1 row)

                SELECT regexp_replace('A PostgreSQL function', 'A|e|i|o|u', 'X', 7, 0, 'i');
                    regexp_replace
                -----------------------
                 A PostgrXSQL fXnctXXn
                (1 row)

                -- 'g' flag should be ignored when N is specified
                SELECT regexp_replace('A PostgreSQL function', 'a|e|i|o|u', 'X', 1, 1, 'g');
                    regexp_replace
                -----------------------
                 A PXstgreSQL function
                (1 row)
                """);
    }

    // TODO: regexp_count
    // TODO: regexp_like
    // TODO: regexp_instr
    // TODO: regexp_substr
    // TODO: regexp_matches
    // TODO: regexp_split_to_array
    // TODO: regexp_split_to_table

    @Test
    public void testLike2() {
        this.q("""
                SELECT 'hawkeye' LIKE 'h%' AS "true";
                 true
                ------
                 t""");
        this.q("""
                SELECT 'hawkeye' NOT LIKE 'h%' AS "false";
                 false
                -------
                 f""");
        this.q("""
                SELECT 'hawkeye' LIKE 'H%' AS "false";
                 false
                -------
                 f""");
        this.q("""
                SELECT 'hawkeye' NOT LIKE 'H%' AS "true";
                 true
                ------
                 t""");
        this.q("""
                SELECT 'hawkeye' LIKE 'indio%' AS "false";
                 false
                -------
                 f""");
        this.q("""
                SELECT 'hawkeye' NOT LIKE 'indio%' AS "true";
                 true
                ------
                 t""");
        this.q("""
                SELECT 'hawkeye' LIKE 'h%eye' AS "true";
                 true
                ------
                 t""");
        this.q("""
                SELECT 'hawkeye' NOT LIKE 'h%eye' AS "false";
                 false
                -------
                 f""");
        this.q("""
                SELECT 'indio' LIKE '_ndio' AS "true";
                 true
                ------
                 t""");
        this.q("""
                SELECT 'indio' NOT LIKE '_ndio' AS "false";
                 false
                -------
                 f""");
        this.q("""
                SELECT 'indio' LIKE 'in__o' AS "true";
                 true
                ------
                 t""");
        this.q("""
                SELECT 'indio' NOT LIKE 'in__o' AS "false";
                 false
                -------
                 f""");
        this.q("""
                SELECT 'indio' LIKE 'in_o' AS "false";
                 false
                -------
                 f""");
        this.q("""
                SELECT 'indio' NOT LIKE 'in_o' AS "true";
                 true
                ------
                 t""");
    }

    @Test
    public void testRlike() {
        // This is not a postgres operator
        this.q("""
                SELECT 'hawkeye' RLIKE 'h.*' AS "true";
                 true
                ------
                 t""");
        this.q("""
                SELECT 'hawkeye' NOT RLIKE 'h.*' AS "false";
                 false
                -------
                 f""");
        this.q("""
                SELECT 'hawkeye' RLIKE 'H.*' AS "false";
                 false
                -------
                 f""");
        this.q("""
                SELECT 'hawkeye' NOT RLIKE 'H.*' AS "true";
                 true
                ------
                 t""");
        this.q("""
                SELECT 'hawkeye' RLIKE 'indio.*' AS "false";
                 false
                -------
                 f""");
        this.q("""
                SELECT 'hawkeye' NOT RLIKE 'indio.*' AS "true";
                 true
                ------
                 t""");
        this.q("""
                SELECT 'hawkeye' RLIKE 'h.*eye' AS "true";
                 true
                ------
                 t""");
        this.q("""
                SELECT 'hawkeye' NOT RLIKE 'h.*eye' AS "false";
                 false
                -------
                 f""");
        this.q("""
                SELECT 'indio' RLIKE '.ndio' AS "true";
                 true
                ------
                 t""");
        this.q("""
                SELECT 'indio' NOT RLIKE '.ndio' AS "false";
                 false
                -------
                 f""");
        this.q("""
                SELECT 'indio' RLIKE 'in..o' AS "true";
                 true
                ------
                 t""");
        this.q("""
                SELECT 'indio' NOT RLIKE 'in..o' AS "false";
                 false
                -------
                 f""");
        this.q("""
                SELECT 'indio' RLIKE 'in.o' AS "false";
                 false
                -------
                 f""");
        this.q("""
                SELECT 'indio' NOT RLIKE 'in.o' AS "true";
                 true
                ------
                 t""");
    }

    @Test
    public void testRlike2() {
        // This is not a postgres operator
        this.q("""
                SELECT RLIKE('hawkeye', 'h.*') AS "true";
                 true
                ------
                 t""");
        this.q("""
                SELECT RLIKE('hawkeye', 'H.*') AS "false";
                 false
                -------
                 f""");
        this.q("""
                SELECT RLIKE('hawkeye', 'indio.*') AS "false";
                 false
                -------
                 f""");
        this.q("""
                SELECT RLIKE('hawkeye', 'h.*eye') AS "true";
                 true
                ------
                 t""");
        this.q("""
                SELECT RLIKE('indio', '.ndio') AS "true";
                 true
                ------
                 t""");
        this.q("""
                SELECT RLIKE('indio', 'in..o') AS "true";
                 true
                ------
                 t""");
        this.q("""
                SELECT RLIKE('indio', 'in.o') AS "false";
                 false
                -------
                 f""");
    }

    @Test
    public void testLike3() {
        this.q("""
                SELECT 'hawkeye' LIKE 'h%' ESCAPE '#' AS "true";
                 true
                ------
                 t""");
        this.q("""
                SELECT 'hawkeye' NOT LIKE 'h%' ESCAPE '#' AS "false";
                 false
                -------
                 f""");
        this.q("""
                SELECT 'indio' LIKE 'ind_o' ESCAPE '$' AS "true";
                 true
                ------
                 t""");
        this.q("""
                SELECT 'indio' NOT LIKE 'ind_o' ESCAPE '$' AS "false";
                 false
                -------
                 f""");
        this.q("""
                SELECT 'h%' LIKE 'h#%' ESCAPE '#' AS "true";
                 true
                ------
                 t""");
        this.q("""
                SELECT 'h%' NOT LIKE 'h#%' ESCAPE '#' AS "false";
                 false
                -------
                 f""");
        this.q("""
                SELECT 'h%wkeye' LIKE 'h#%' ESCAPE '#' AS "false";
                 false
                -------
                 f""");
        this.q("""
                SELECT 'h%wkeye' NOT LIKE 'h#%' ESCAPE '#' AS "true";
                 true
                ------
                 t""");
        this.q("""
                SELECT 'h%wkeye' LIKE 'h#%%' ESCAPE '#' AS "true";
                 true
                ------
                 t""");
        this.q("""
                SELECT 'h%wkeye' NOT LIKE 'h#%%' ESCAPE '#' AS "false";
                 false
                -------
                 f""");
        this.q("""
                SELECT 'h%awkeye' LIKE 'h#%a%k%e' ESCAPE '#' AS "true";
                 true
                ------
                 t""");
        this.q("""
                SELECT 'h%awkeye' NOT LIKE 'h#%a%k%e' ESCAPE '#' AS "false";
                 false
                -------
                 f""");
        this.q("""
                SELECT 'indio' LIKE '_ndio' ESCAPE '$' AS "true";
                 true
                ------
                 t""");
        this.q("""
                SELECT 'indio' NOT LIKE '_ndio' ESCAPE '$' AS "false";
                 false
                -------
                 f""");
        this.q("""
                SELECT 'i_dio' LIKE 'i$_d_o' ESCAPE '$' AS "true";
                 true
                ------
                 t""");
        this.q("""
                SELECT 'i_dio' NOT LIKE 'i$_d_o' ESCAPE '$' AS "false";
                 false
                -------
                 f""");
        this.q("""
                SELECT 'i_dio' LIKE 'i$_nd_o' ESCAPE '$' AS "false";
                 false
                -------
                 f""");
        this.q("""
                SELECT 'i_dio' NOT LIKE 'i$_nd_o' ESCAPE '$' AS "true";
                 true
                ------
                 t""");
        this.q("""
                SELECT 'i_dio' LIKE 'i$_d%o' ESCAPE '$' AS "true";
                 true
                ------
                 t""");
        this.q("""
                SELECT 'i_dio' NOT LIKE 'i$_d%o' ESCAPE '$' AS "false";
                 false
                -------
                 f""");
    }

    @Test @Ignore("We do not allow escape characters that are % or _")
    public void testLike3Pattern() {
        // -- escape character same as pattern character\n"
        this.q("""
                SELECT 'maca' LIKE 'm%aca' ESCAPE '%' AS "true";
                 true
                ------
                 t""");
        this.q("""
                SELECT 'maca' NOT LIKE 'm%aca' ESCAPE '%' AS "false";
                 false
                -------
                 f""");
        this.q("""
                SELECT 'ma%a' LIKE 'm%a%%a' ESCAPE '%' AS "true";
                 true
                ------
                 t""");
        this.q("""
                SELECT 'ma%a' NOT LIKE 'm%a%%a' ESCAPE '%' AS "false";
                 false
                -------
                 f""");
        this.q("""
                SELECT 'bear' LIKE 'b_ear' ESCAPE '_' AS "true";
                 true
                ------
                 t""");
        this.q("""
                SELECT 'bear' NOT LIKE 'b_ear' ESCAPE '_' AS "false";
                 false
                -------
                 f""");
        this.q("""
                SELECT 'be_r' LIKE 'b_e__r' ESCAPE '_' AS "true";
                 true
                ------
                 t""");
        this.q("""
                SELECT 'be_r' NOT LIKE 'b_e__r' ESCAPE '_' AS "false";
                 false
                -------
                 f""");
        this.q("""
                SELECT 'be_r' LIKE '__e__r' ESCAPE '_' AS "false";
                 false
                -------
                 f""");
        this.q("""
                SELECT 'be_r' NOT LIKE '__e__r' ESCAPE '_' AS "true";
                 true
                ------
                 t""");
    }

    @Test
    public void testILike2() {
        this.q("""
                SELECT 'hawkeye' ILIKE 'h%' AS "true";
                 true
                ------
                 t""");
        this.q("""
                SELECT 'hawkeye' NOT ILIKE 'h%' AS "false";
                 false
                -------
                 f""");
        this.q("""
                SELECT 'hawkeye' ILIKE 'H%' AS "true";
                 true
                ------
                 t""");
        this.q("""
                SELECT 'hawkeye' NOT ILIKE 'H%' AS "false";
                 false
                -------
                 f""");
        this.q("""
                SELECT 'hawkeye' ILIKE 'H%Eye' AS "true";
                 true
                ------
                 t""");
        this.q("""
                SELECT 'hawkeye' NOT ILIKE 'H%Eye' AS "false";
                 false
                -------
                 f""");
        this.q("""
                SELECT 'Hawkeye' ILIKE 'h%' AS "true";
                 true
                ------
                 t""");
        this.q("""
                SELECT 'Hawkeye' NOT ILIKE 'h%' AS "false";
                 false
                -------
                 f""");
        this.q("""
                SELECT 'ABC' ILIKE '_b_' AS "true";
                 true
                -------
                 t""");
        this.q("""
                SELECT 'ABC' NOT ILIKE '_b_' AS "false";
                 false
                ------
                 f""");
    }


    @Test
    public void testLikeCombinations() {
        this.q("""
                SELECT 'foo' LIKE '_%' as t, 'f' LIKE '_%' as t0, '' LIKE '_%' as f;
                 t | t0 | f
                ---+----+---
                 t | t  | f""");
        this.q("""
                SELECT 'foo' LIKE '%_' as t, 'f' LIKE '%_' as t0, '' LIKE '%_' as f;
                 t | t0 | f
                ---+----+---
                 t | t  | f""");
        this.q("""
                SELECT 'foo' LIKE '__%' as t, 'foo' LIKE '___%' as t0, 'foo' LIKE '____%' as f;
                 t | t0 | f
                ---+----+---
                 t | t  | f""");
        this.q("""
                SELECT 'foo' LIKE '%__' as t, 'foo' LIKE '%___' as t0, 'foo' LIKE '%____' as f;
                 t | t0 | f
                ---+----+---
                 t | t  | f""");
        this.q("""
                SELECT 'jack' LIKE '%____%' AS t;
                 t
                ---
                 t""");
    }

    @Test
    public void testLikeNull() {
        this.qs("""
                SELECT NULL LIKE '%';
                 result
                -------
                \s
                (1 row)

                SELECT 'a' LIKE NULL;
                 result
                -------
                \s
                (1 row)

                SELECT NULL LIKE NULL;
                 result
                -------
                \s
                (1 row)

                SELECT NULL NOT LIKE '%';
                 result
                -------
                \s
                (1 row)

                SELECT 'a' NOT LIKE NULL;
                 result
                -------
                \s
                (1 row)

                SELECT NULL NOT LIKE NULL;
                 result
                -------
                \s
                (1 row)

                SELECT NULL RLIKE '.*';
                 result
                -------
                \s
                (1 row)

                SELECT 'a' RLIKE NULL;
                 result
                -------
                \s
                (1 row)

                SELECT NULL RLIKE NULL;
                 result
                -------
                \s
                (1 row)
                """);
    }

    @Test
    public void testConcatConversions() {
        // In Postgres concatenation converts to text, whereas Calcite does not.
        this.q("""
                SELECT 'text'::text || ' and characters'::char(20) || 'x' AS "Concat text to char";
                 Concat text to char
                ---------------------
                 text and characters     x""");
        this.q("""
                SELECT 'unknown' || ' and unknown' AS "Concat unknown types";
                 Concat unknown types
                ----------------------
                 unknown and unknown""");
        this.q("""
                SELECT 'text'::text || ' and unknown' AS "Concat text to unknown type";
                 Concat text to unknown type
                -----------------------------
                 text and unknown""");
        this.q("""
                SELECT 'characters' ::char(20) || ' and text' AS "Concat char to unknown type";
                 Concat char to unknown type
                -----------------------------
                 characters           and text""");
        this.q("""
                SELECT 'text'::text || ' and varchar'::varchar AS "Concat text to varchar";
                 Concat text to varchar
                ------------------------
                 text and varchar""");
    }

    @Test
    public void testLength() {
        // added a few more aliases supported from other dialects
        this.qs("""
                SELECT char_length('abcdef') AS "length_6";
                 length_6
                ----------
                        6
                (1 row)
                
                SELECT length('abcdef') AS "length_6";
                 length_6
                ----------
                        6
                (1 row)

                SELECT len('abcdef') AS "length_6";
                 length_6
                ----------
                        6
                (1 row)

                SELECT character_length('abcdef') AS "length_6";
                 length_6
                ----------
                        6
                (1 row)
        
                SELECT character_length('josé') AS "length";
                 length
                --------
                       4
                (1 row)""");
    }

    @Test
    public void testStrpos() {
        // No strpos in Calcite, replace with 'position' with arguments swapped
        this.q("""
                SELECT POSITION('cd' IN 'abcdef') AS "pos_3";
                 pos_3
                -------
                     3""");
        this.q("""
                SELECT POSITION('xy' IN 'abcdef') AS "pos_0";
                 pos_0
                -------
                     0""");
        this.q("""
                SELECT POSITION('' IN 'abcdef') AS "pos_1";
                 pos_1
                -------
                     1""");
        this.q("""
                SELECT POSITION('xy' IN '') AS "pos_0";
                 pos_0
                -------
                     0""");
        this.q("""
                SELECT POSITION('' IN '') AS "pos_1";
                 pos_1
                -------
                     1""");
    }

    @Test
    public void testReplace() {
        this.q("""
                SELECT REPLACE('abcdef', 'de', '45') AS "abc45f";
                 abc45f
                --------
                 abc45f""");
        this.q("""
                SELECT replace('yabadabadoo', 'ba', '123') AS "ya123da123doo";
                 ya123da123doo
                ---------------
                 ya123da123doo""");
        this.q("""
                SELECT replace('yabadoo', 'bad', '') AS "yaoo";
                 yaoo
                ------
                 yaoo""");
    }

    @Test
    public void testSplitPart() {
        this.qs("""
            SELECT split_part('abc~@~def~@~ghi', '~@~', 1) AS result;
             result
            -------
             abc
            (1 row)
            
            SELECT split_part('abc~@~def~@~ghi', '~@~', -1) AS result;
             result
            -------
             ghi
            (1 row)

            SELECT split_part('abc~@~def~@~ghi', '~@~', 2) AS result;
             result
            -------
             def
            (1 row)
            
            SELECT split_part('abc~@~def~@~ghi', '~@~', -2) AS result;
             result
            -------
             def
            (1 row)

            SELECT split_part('abc~@~def~@~ghi', '~@~', 3) AS result;
             result
            -------
             ghi
            (1 row)
            
            SELECT split_part('abc~@~def~@~ghi', '~@~', -3) AS result;
             result
            -------
             abc
            (1 row)
            
            SELECT split_part('abc~@~def~@~ghi', '~@~', 4) AS result;
             result
            -------
            \s
            (1 row)
            
            SELECT split_part('abc~@~def~@~ghi', '~@~', -4) AS result;
             result
            -------
            \s
            (1 row)
            
            SELECT split_part('abc~@~def~@~ghi', '~@~', 0) AS result;
             result
            -------
            \s
            (1 row)
            
            SELECT split_part('abc~@~def~@~ghi', 'abc~@~def~@~ghi', 1) AS result;
             result
            -------
            \s
            (1 row)
            
            SELECT split_part('abc', 'abc', 1) AS result;
             result
            -------
            \s
            (1 row)
            
            SELECT split_part('abc', 'abc', 2) AS result;
             result
            -------
            \s
            (1 row)
            
            SELECT split_part('abc', 'n', 1) AS result;
             result
            -------
             abc
            (1 row)
            """);
    }


    // TODO: to_hex
    // TODO: sha, encode, decode

    @Test
    public void testConforming() {
        this.q("""
                select 'a\\bcd' as f1, 'a\\b''cd' as f2, 'a\\b''''cd' as f3, 'abcd\\'   as f4, 'ab\\''cd' as f5, '\\\\' as f6;
                  f1  |  f2   |   f3   | f4   |   f5  | f6
                ------+-------+--------+------+-------+----
                 a\\bcd| a\\b'cd| a\\b''cd| abcd\\| ab\\'cd| \\\\""");
    }

    // TODO: lpat, translate,

    @Test
    public void testAscii() {
        this.q("""
                SELECT ascii('x');
                 ascii
                -------
                   120""");
        this.q("""
                SELECT ascii('');
                 ascii
                -------
                     0""");
    }

    @Test
    public void testChr() {
        this.q("""
                SELECT chr(65);
                 chr
                -----
                 A""");
        this.q("""
                SELECT chr(0);
                 chr
                -----
                 \0""");
    }

    @Test
    public void testRepeat() {
        this.q("""
                SELECT repeat('Pg', 4);
                  repeat
                ----------
                 PgPgPgPg""");
        this.q("""
                SELECT repeat('Pg', -4);
                 repeat
                --------
                \s""");
    }

    @Test
    public void testLikeTable() {
        String sql = """
                CREATE TABLE example (
                    name VARCHAR
                );

                CREATE VIEW example_count AS
                SELECT COUNT(*) FROM example WHERE name LIKE 'abc%' OR name LIKE name;
                """;
        this.getCCS(sql);
    }

    // TODO: bytea computations
    // TODO unistr
}
