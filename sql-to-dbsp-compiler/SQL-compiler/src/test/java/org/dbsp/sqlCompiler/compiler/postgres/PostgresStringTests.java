package org.dbsp.sqlCompiler.compiler.postgres;

import org.dbsp.sqlCompiler.compiler.backend.DBSPCompiler;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

/**
 * https://github.com/postgres/postgres/blob/master/src/test/regress/expected/strings.out
 */
@SuppressWarnings("JavadocLinkAsPlainText")
public class PostgresStringTests extends PostgresBaseTest {
    @Override
    public void prepareData(DBSPCompiler compiler) {
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
                "  ('hi de ho neighbor');";
        compiler.compileStatements(data);
    }

    @Test
    public void continuationTest() {
        this.queryWithOutput("SELECT 'first line'\n" +
                "' - next line'\n" +
                "\t' - third line'\n" +
                "\tAS \"Three lines to one\";\n" +
                "         Three lines to one          \n" +
                "-------------------------------------\n" +
                "first line - next line - third line");
    }

    @Test
    public void illegalContinuationTest() {
        Exception exception = Assert.assertThrows(RuntimeException.class, () -> {
            // Cannot continue a string without a newline
            this.queryWithOutput("SELECT 'first line' " +
                    "' - next line'\n" +
                    "\tAS \"Illegal comment within continuation\";\n");
        });
        Assert.assertTrue(exception.getMessage().contains("String literal continued on same line"));
    }

    @Test
    public void unicodeIdentifierTest() {
        // Calcite does not support 6-digits escapes like Postgres, so
        // I have modified +000061 to just 0061
        this.queryWithOutput("SELECT U&'d\\0061t\\0061' AS U&\"d\\0061t\\0061\";\n" +
                " data \n" +
                "------\n" +
                "data");
    }

    @Test
    public void unicodeNewEscapeTest() {
        this.queryWithOutput(
                "SELECT U&'d!0061t!0061' UESCAPE '!' AS U&\"d*0061t\\0061\" UESCAPE '*';\n" +
                " data \n" +
                "------\n" +
                "data");
    }

    @Test
    public void namesWithSlashTest() {
        this.queryWithOutput("SELECT U&'a\\\\b' AS \"a\\b\";\n" +
                " a\\b \n" +
                "-----\n" +
                "a\\b");
    }

    @Test
    public void backslashWithSpacesTest() {
        this.queryWithOutput("SELECT U&' \\' UESCAPE '!' AS \"tricky\";\n" +
                "tricky \n" +
                "--------\n" +
                " \\");
    }

    @Test
    public void invalidUnicodeTest() {
        Exception exception = Assert.assertThrows(
                RuntimeException.class, () -> this.queryWithOutput("SELECT U&'wrong: \\061';\n"));
        Assert.assertTrue(exception.getMessage().contains(
                "Unicode escape sequence starting at character 7 is not exactly four hex digits"));
    }

    // Lots of other escaping tests skipped, many using the E escaping notation from Postgres

    @Test
    public void testCharN() {
        this.queryWithOutput("SELECT CAST(f1 AS text) AS \"text(char)\" FROM CHAR_TBL;\n" +
                " text(char) \n" +
                "------------\n" +
                "a\n" +
                "ab\n" +
                "abcd\n" +
                "abcd");
    }

    @Test
    public void testVarcharN() {
        this.queryWithOutput("SELECT CAST(f1 AS text) AS \"text(varchar)\" FROM VARCHAR_TBL;\n" +
                " text(varchar) \n" +
                "------------\n" +
                "a\n" +
                "ab\n" +
                "abcd\n" +
                "abcd");
    }

    @Test
    public void testVarchar() {
        this.queryWithOutput("SELECT f1 AS \"text(varchar)\" FROM UVARCHAR_TBL;\n" +
                " text(varchar) \n" +
                        "------------\n" +
                        "a\n" +
                        "ab\n" +
                        "abcd\n" +
                        "abcd");
    }

    // 'name' not supported
    // SELECT CAST(name 'namefield' AS text) AS "text(name)"

    @Test
    public void testTextTbl() {
        this.queryWithOutput("SELECT CAST(f1 AS char(10)) AS \"char(text)\" FROM TEXT_TBL;\n" +
                "char(text) \n" +
                "------------\n" +
                "doh!      \n" +
                "hi de ho n");
    }

    @Test
    public void testWiderChar() {
        this.queryWithOutput("SELECT CAST(f1 AS char(20)) AS \"char(text)\" FROM TEXT_TBL;\n" +
                "      char(text)      \n" +
                "----------------------\n" +
                "doh!                \n" +
                "hi de ho neighbor   ");
    }

    @Test
    public void testWiderVarchar() {
        this.queryWithOutput("SELECT CAST(f1 AS char(10)) AS \"char(varchar)\" FROM VARCHAR_TBL;\n" +
                " char(varchar) \n" +
                "---------------\n" +
                "a         \n" +
                "ab        \n" +
                "abcd      \n" +
                "abcd      ");
    }

    @Test
    public void testTextTbl2() {
        this.queryWithOutput("SELECT CAST(f1 AS varchar) AS \"varchar(text)\" FROM TEXT_TBL;\n" +
                "   varchar(text)   \n" +
                "-------------------\n" +
                "doh!\n" +
                "hi de ho neighbor");
    }

    @Test
    public void testCharTbl() {
        this.queryWithOutput("SELECT CAST(f1 AS varchar) AS \"varchar(char)\" FROM CHAR_TBL;\n" +
                " varchar(char) \n" +
                "---------------\n" +
                "a\n" +
                "ab\n" +
                "abcd\n" +
                "abcd");
    }

    @Test
    public void testTrimConstant() {
        this.queryWithOutput("SELECT TRIM(BOTH FROM '  bunch o blanks  ') = 'bunch o blanks' AS \"bunch o blanks\";\n" +
                " bunch o blanks \n" +
                "----------------\n" +
                " t");
        this.queryWithOutput("SELECT TRIM(LEADING FROM '  bunch o blanks  ') = 'bunch o blanks  ' AS \"bunch o blanks  \";\n" +
                " bunch o blanks   \n" +
                "------------------\n" +
                " t");
        this.queryWithOutput("SELECT TRIM(TRAILING FROM '  bunch o blanks  ') = '  bunch o blanks' AS \"  bunch o blanks\";\n" +
                "   bunch o blanks \n" +
                "------------------\n" +
                " t");
    }

    @Test
    public void testTrim() {
        this.queryWithOutput("SELECT TRIM(BOTH FROM '  bunch o blanks  ') = 'bunch o blanks' AS \"bunch o blanks\";\n" +
                " bunch o blanks \n" +
                "----------------\n" +
                " t");
        this.queryWithOutput("SELECT TRIM(LEADING FROM '  bunch o blanks  ') = 'bunch o blanks  ' AS \"bunch o blanks  \";\n" +
                " bunch o blanks   \n" +
                "------------------\n" +
                " t");
        this.queryWithOutput("SELECT TRIM(TRAILING FROM '  bunch o blanks  ') = '  bunch o blanks' AS \"  bunch o blanks\";\n" +
                "   bunch o blanks \n" +
                "------------------\n" +
                " t");
    }

    @Test
    public void testTrimArg() {
        this.queryWithOutput("SELECT TRIM(BOTH 'x' FROM 'xxxxxsome Xsxxxxx') = 'some Xs' AS \"some Xs\";\n" +
                " some Xs \n" +
                "---------\n" +
                " t");
    }

    @Test
    public void testSubstring() {
        this.queryWithOutput("SELECT SUBSTRING('1234567890' FROM 3) = '34567890' AS \"34567890\";\n" +
                " 34567890 \n" +
                "----------\n" +
                " t");
        this.queryWithOutput(
                "SELECT SUBSTRING('1234567890' FROM 4 FOR 3) = '456' AS \"456\";\n" +
                " 456 \n" +
                "-----\n" +
                " t");
        this.queryWithOutput("SELECT SUBSTRING('string' FROM -10 FOR 2147483646) AS \"string\";\n" +
                " string \n" +
                "--------\n" +
                "string");
    }

    @Test @Ignore("https://issues.apache.org/jira/browse/CALCITE-5810")
    public void testSubstringOverflow() {
        this.queryWithOutput(
                "SELECT SUBSTRING('string' FROM 2 FOR 2147483646) AS \"tring\";\n" +
                        " tring \n" +
                        "-------\n" +
                        "tring");
    }

    @Test
    public void testNegativeSubstringLength() {
        this.queryWithOutput("SELECT SUBSTRING('string' FROM -10 FOR -2147483646) AS \"error\";\n" +
                "error\n" +
                "------\n" +
                "");
        this.queryWithOutput("SELECT SUBSTRING('string' FROM 0 FOR -2) AS \"error\";\n" +
                "error\n" +
                "------\n" +
                "");
    }

    @Test
    public void testPosition() {
        this.queryWithOutput("SELECT POSITION('4' IN '1234567890') = '4' AS \"4\";\n" +
                " 4 \n" +
                "---\n" +
                " t");
        this.queryWithOutput("SELECT POSITION('5' IN '1234567890') = '5' AS \"5\";\n" +
                " 5 \n" +
                "---\n" +
                " t");
        this.queryWithOutput("SELECT POSITION('A' IN '1234567890') = '0' AS \"0\";\n" +
                " 5 \n" +
                "---\n" +
                " t");
    }

    // SUBSTRING ... SIMILAR syntax not supported
    // SELECT SUBSTRING('abcdefg' SIMILAR 'a#"(b_d)#"%' ESCAPE '#') AS "bcd";

    @Test
    public void testOverlay() {
        this.queryWithOutput("SELECT OVERLAY('abcdef' PLACING '45' FROM 4) AS \"abc45f\";\n" +
                " abc45f \n" +
                "--------\n" +
                "abc45f");
        this.queryWithOutput("SELECT OVERLAY('yabadoo' PLACING 'daba' FROM 5) AS \"yabadaba\";\n" +
                " yabadaba \n" +
                "----------\n" +
                "yabadaba");
        this.queryWithOutput("SELECT OVERLAY('yabadoo' PLACING 'daba' FROM 5 FOR 0) AS \"yabadabadoo\";\n" +
                " yabadabadoo \n" +
                "-------------\n" +
                "yabadabadoo");
        this.queryWithOutput("SELECT OVERLAY('babosa' PLACING 'ubb' FROM 2 FOR 4) AS \"bubba\";\n" +
                " bubba \n" +
                "-------\n" +
                "bubba");
    }

    // TODO: regexp_replace
    // TODO: regexp_count
    // TODO: regexp_like
    // TODO: regexp_instr
    // TODO: regexp_substr
    // TODO: regexp_matches
    // TODO: regexp_split_to_array
    // TODO: regexp_split_to_table

    @Test
    public void testLike2() {
        this.queryWithOutput("SELECT 'hawkeye' LIKE 'h%' AS \"true\";\n" +
                " true \n" +
                "------\n" +
                " t");
        this.queryWithOutput("SELECT 'hawkeye' NOT LIKE 'h%' AS \"false\";\n" +
                " false \n" +
                "-------\n" +
                " f");
        this.queryWithOutput("SELECT 'hawkeye' LIKE 'H%' AS \"false\";\n" +
                " false \n" +
                "-------\n" +
                " f");
        this.queryWithOutput("SELECT 'hawkeye' NOT LIKE 'H%' AS \"true\";\n" +
                " true \n" +
                "------\n" +
                " t");
        this.queryWithOutput("SELECT 'hawkeye' LIKE 'indio%' AS \"false\";\n" +
                " false \n" +
                "-------\n" +
                " f");
        this.queryWithOutput("SELECT 'hawkeye' NOT LIKE 'indio%' AS \"true\";\n" +
                " true \n" +
                "------\n" +
                " t");
        this.queryWithOutput("SELECT 'hawkeye' LIKE 'h%eye' AS \"true\";\n" +
                " true \n" +
                "------\n" +
                " t");
        this.queryWithOutput("SELECT 'hawkeye' NOT LIKE 'h%eye' AS \"false\";\n" +
                " false \n" +
                "-------\n" +
                " f");
        this.queryWithOutput("SELECT 'indio' LIKE '_ndio' AS \"true\";\n" +
                " true \n" +
                "------\n" +
                " t");
        this.queryWithOutput("SELECT 'indio' NOT LIKE '_ndio' AS \"false\";\n" +
                " false \n" +
                "-------\n" +
                " f");
        this.queryWithOutput("SELECT 'indio' LIKE 'in__o' AS \"true\";\n" +
                " true \n" +
                "------\n" +
                " t");
        this.queryWithOutput("SELECT 'indio' NOT LIKE 'in__o' AS \"false\";\n" +
                " false \n" +
                "-------\n" +
                " f");
        this.queryWithOutput("SELECT 'indio' LIKE 'in_o' AS \"false\";\n" +
                " false \n" +
                "-------\n" +
                " f");
        this.queryWithOutput("SELECT 'indio' NOT LIKE 'in_o' AS \"true\";\n" +
                " true \n" +
                "------\n" +
                " t");
    }

    @Test
    public void testLike3() {
        this.queryWithOutput("SELECT 'hawkeye' LIKE 'h%' ESCAPE '#' AS \"true\";\n" +
                " true \n" +
                "------\n" +
                " t");
        this.queryWithOutput("SELECT 'hawkeye' NOT LIKE 'h%' ESCAPE '#' AS \"false\";\n" +
                " false \n" +
                "-------\n" +
                " f");
        this.queryWithOutput("SELECT 'indio' LIKE 'ind_o' ESCAPE '$' AS \"true\";\n" +
                " true \n" +
                "------\n" +
                " t");
        this.queryWithOutput("SELECT 'indio' NOT LIKE 'ind_o' ESCAPE '$' AS \"false\";\n" +
                " false \n" +
                "-------\n" +
                " f");
        this.queryWithOutput("SELECT 'h%' LIKE 'h#%' ESCAPE '#' AS \"true\";\n" +
                " true \n" +
                "------\n" +
                " t");
        this.queryWithOutput("SELECT 'h%' NOT LIKE 'h#%' ESCAPE '#' AS \"false\";\n" +
                " false \n" +
                "-------\n" +
                " f");
        this.queryWithOutput("SELECT 'h%wkeye' LIKE 'h#%' ESCAPE '#' AS \"false\";\n" +
                " false \n" +
                "-------\n" +
                " f");
        this.queryWithOutput("SELECT 'h%wkeye' NOT LIKE 'h#%' ESCAPE '#' AS \"true\";\n" +
                " true \n" +
                "------\n" +
                " t");
        this.queryWithOutput("SELECT 'h%wkeye' LIKE 'h#%%' ESCAPE '#' AS \"true\";\n" +
                " true \n" +
                "------\n" +
                " t");
        this.queryWithOutput("SELECT 'h%wkeye' NOT LIKE 'h#%%' ESCAPE '#' AS \"false\";\n" +
                " false \n" +
                "-------\n" +
                " f");
        this.queryWithOutput("SELECT 'h%awkeye' LIKE 'h#%a%k%e' ESCAPE '#' AS \"true\";\n" +
                " true \n" +
                "------\n" +
                " t");
        this.queryWithOutput("SELECT 'h%awkeye' NOT LIKE 'h#%a%k%e' ESCAPE '#' AS \"false\";\n" +
                " false \n" +
                "-------\n" +
                " f");
        this.queryWithOutput("SELECT 'indio' LIKE '_ndio' ESCAPE '$' AS \"true\";\n" +
                " true \n" +
                "------\n" +
                " t");
        this.queryWithOutput("SELECT 'indio' NOT LIKE '_ndio' ESCAPE '$' AS \"false\";\n" +
                " false \n" +
                "-------\n" +
                " f");
        this.queryWithOutput("SELECT 'i_dio' LIKE 'i$_d_o' ESCAPE '$' AS \"true\";\n" +
                " true \n" +
                "------\n" +
                " t");
        this.queryWithOutput("SELECT 'i_dio' NOT LIKE 'i$_d_o' ESCAPE '$' AS \"false\";\n" +
                " false \n" +
                "-------\n" +
                " f");
        this.queryWithOutput("SELECT 'i_dio' LIKE 'i$_nd_o' ESCAPE '$' AS \"false\";\n" +
                " false \n" +
                "-------\n" +
                " f");
        this.queryWithOutput("SELECT 'i_dio' NOT LIKE 'i$_nd_o' ESCAPE '$' AS \"true\";\n" +
                " true \n" +
                "------\n" +
                " t");
        this.queryWithOutput("SELECT 'i_dio' LIKE 'i$_d%o' ESCAPE '$' AS \"true\";\n" +
                " true \n" +
                "------\n" +
                " t");
        this.queryWithOutput("SELECT 'i_dio' NOT LIKE 'i$_d%o' ESCAPE '$' AS \"false\";\n" +
                " false \n" +
                "-------\n" +
                " f");
    }

    @Test @Ignore("We do not allow escape characters that are % or _")
    public void testLike3Pattern() {
        // -- escape character same as pattern character\n"
        this.queryWithOutput("SELECT 'maca' LIKE 'm%aca' ESCAPE '%' AS \"true\";\n" +
                " true \n" +
                "------\n" +
                " t");
        this.queryWithOutput("SELECT 'maca' NOT LIKE 'm%aca' ESCAPE '%' AS \"false\";\n" +
                " false \n" +
                "-------\n" +
                " f");
        this.queryWithOutput("SELECT 'ma%a' LIKE 'm%a%%a' ESCAPE '%' AS \"true\";\n" +
                " true \n" +
                "------\n" +
                " t");
        this.queryWithOutput("SELECT 'ma%a' NOT LIKE 'm%a%%a' ESCAPE '%' AS \"false\";\n" +
                " false \n" +
                "-------\n" +
                " f");
        this.queryWithOutput("SELECT 'bear' LIKE 'b_ear' ESCAPE '_' AS \"true\";\n" +
                " true \n" +
                "------\n" +
                " t");
        this.queryWithOutput("SELECT 'bear' NOT LIKE 'b_ear' ESCAPE '_' AS \"false\";\n" +
                " false \n" +
                "-------\n" +
                " f");
        this.queryWithOutput("SELECT 'be_r' LIKE 'b_e__r' ESCAPE '_' AS \"true\";\n" +
                " true \n" +
                "------\n" +
                " t");
        this.queryWithOutput("SELECT 'be_r' NOT LIKE 'b_e__r' ESCAPE '_' AS \"false\";\n" +
                " false \n" +
                "-------\n" +
                " f");
        this.queryWithOutput("SELECT 'be_r' LIKE '__e__r' ESCAPE '_' AS \"false\";\n" +
                " false \n" +
                "-------\n" +
                " f");
        this.queryWithOutput("SELECT 'be_r' NOT LIKE '__e__r' ESCAPE '_' AS \"true\";\n" +
                " true \n" +
                "------\n" +
                " t");
    }

    // TODO ILIKE

    @Test
    public void testLikeCombinations() {
        this.queryWithOutput("SELECT 'foo' LIKE '_%' as t, 'f' LIKE '_%' as t, '' LIKE '_%' as f;\n" +
                " t | t | f \n" +
                "---+---+---\n" +
                " t | t | f");
        this.queryWithOutput("SELECT 'foo' LIKE '%_' as t, 'f' LIKE '%_' as t, '' LIKE '%_' as f;\n" +
                " t | t | f \n" +
                "---+---+---\n" +
                " t | t | f");
        this.queryWithOutput("SELECT 'foo' LIKE '__%' as t, 'foo' LIKE '___%' as t, 'foo' LIKE '____%' as f;\n" +
                " t | t | f \n" +
                "---+---+---\n" +
                " t | t | f");
        this.queryWithOutput("SELECT 'foo' LIKE '%__' as t, 'foo' LIKE '%___' as t, 'foo' LIKE '%____' as f;\n" +
                " t | t | f \n" +
                "---+---+---\n" +
                " t | t | f");
        this.queryWithOutput("SELECT 'jack' LIKE '%____%' AS t;\n" +
                " t \n" +
                "---\n" +
                " t");
    }

    @Test
    public void testConcatConversions() {
        // In Postgres concatenation converts to text, whereas Calcite does not.
        this.queryWithOutput("SELECT 'unknown' || ' and unknown' AS \"Concat unknown types\";\n" +
                " Concat unknown types \n" +
                "----------------------\n" +
                "unknown and unknown");
        this.queryWithOutput("SELECT 'text'::text || ' and unknown' AS \"Concat text to unknown type\";\n" +
                " Concat text to unknown type \n" +
                "-----------------------------\n" +
                "text and unknown");
        this.queryWithOutput("SELECT 'characters' ::char(20) || ' and text' AS \"Concat char to unknown type\";\n" +
                " Concat char to unknown type \n" +
                "-----------------------------\n" +
                "characters           and text");
        this.queryWithOutput("SELECT 'text'::text || ' and characters'::char(20) AS \"Concat text to char\";\n" +
                " Concat text to char \n" +
                "---------------------\n" +
                "text and characters     ");
        this.queryWithOutput("SELECT 'text'::text || ' and varchar'::varchar AS \"Concat text to varchar\";\n" +
                " Concat text to varchar \n" +
                "------------------------\n" +
                "text and varchar");
    }

    @Test
    public void testLength() {
        // length in postgres is equivalent to char_length
        this.queryWithOutput("SELECT char_length('abcdef') AS \"length_6\";\n" +
                " length_6 \n" +
                "----------\n" +
                "        6");
        this.queryWithOutput("SELECT character_length('abcdef') AS \"length_6\";\n" +
                " length_6 \n" +
                "----------\n" +
                "        6");
        this.queryWithOutput("SELECT character_length('josé') AS \"length\";\n" +
                " length \n" +
                "--------\n" +
                "       4");
    }

    @Test
    public void testStrpos() {
        // No strpos in Calcite, replace with 'position' with arguments swapped
        this.queryWithOutput("SELECT POSITION('cd' IN 'abcdef') AS \"pos_3\";\n" +
                " pos_3 \n" +
                "-------\n" +
                "     3");
        this.queryWithOutput("SELECT POSITION('xy' IN 'abcdef') AS \"pos_0\";\n" +
                " pos_0 \n" +
                "-------\n" +
                "     0");
        this.queryWithOutput("SELECT POSITION('' IN 'abcdef') AS \"pos_1\";\n" +
                " pos_1 \n" +
                "-------\n" +
                "     1");
        this.queryWithOutput("SELECT POSITION('xy' IN '') AS \"pos_0\";\n" +
                " pos_0 \n" +
                "-------\n" +
                "     0");
        this.queryWithOutput("SELECT POSITION('' IN '') AS \"pos_1\";\n" +
                " pos_1 \n" +
                "-------\n" +
                "     1");
    }

    @Test @Ignore("No 'replace' yet in Calcite")
    public void testReplace() {
        this.queryWithOutput("SELECT replace('abcdef', 'de', '45') AS \"abc45f\";\n" +
                " abc45f \n" +
                "--------\n" +
                " abc45f");
        this.queryWithOutput("SELECT replace('yabadabadoo', 'ba', '123') AS \"ya123da123doo\";\n" +
                " ya123da123doo \n" +
                "---------------\n" +
                " ya123da123doo");
        this.queryWithOutput("SELECT replace('yabadoo', 'bad', '') AS \"yaoo\";\n" +
                " yaoo \n" +
                "------\n" +
                " yaoo");
    }

    // TODO: replace
    // TODO: split_part
    // TODO: to_hex
    // TODO: sha, encode, decode

    @Test
    public void testConforming() {
        this.queryWithOutput("select 'a\\bcd' as f1, 'a\\b''cd' as f2, 'a\\b''''cd' as f3, 'abcd\\'   as f4, 'ab\\''cd' as f5, '\\\\' as f6;\n" +
                "  f1  |  f2   |   f3   | f4   |   f5  | f6 \n" +
                "------+-------+--------+------+-------+----\n" +
                "a\\bcd|a\\b'cd|a\\b''cd|abcd\\|ab\\'cd|\\\\");
    }

    // TODO: lpat, translate,
    
    @Test
    public void testAscii() {
        this.queryWithOutput("SELECT ascii('x');\n" +
                " ascii \n" +
                "-------\n" +
                "   120");
        this.queryWithOutput("SELECT ascii('');\n" +
                " ascii \n" +
                "-------\n" +
                "     0");
    }

    @Test
    public void testChr() {
        this.queryWithOutput("SELECT chr(65);\n" +
                " chr \n" +
                "-----\n" +
                "A");
        this.queryWithOutput("SELECT chr(0);\n" +
                " chr \n" +
                "-----\n" +
                "\0");
    }

    @Test @Ignore("https://issues.apache.org/jira/browse/CALCITE-5813")
    public void testRepeat() {
        this.queryWithOutput("SELECT repeat('Pg', 4);\n" +
                "  repeat  \n" +
                "----------\n" +
                "PgPgPgPg");
        this.queryWithOutput("SELECT repeat('Pg', -4);\n" +
                " repeat \n" +
                "--------\n" +
                "");
    }

    // TODO: bytea computations
    // TODO unistr
}
