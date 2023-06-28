package org.dbsp.sqlCompiler.compiler.postgres;

import org.dbsp.sqlCompiler.compiler.backend.DBSPCompiler;
import org.junit.Assert;
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
                "  ('abcd    ');";
        compiler.compileStatements(data);
    }

    @Test
    public void continuationTest() {
        String query = "SELECT 'first line'\n" +
                "' - next line'\n" +
                "\t' - third line'\n" +
                "\tAS \"Three lines to one\"";
        String expected = "         Three lines to one          \n" +
                "-------------------------------------\n" +
                "first line - next line - third line";
        this.compare(query, expected);
    }

    @Test
    public void illegalContinuationTest() {
        Exception exception = Assert.assertThrows(RuntimeException.class, () -> {
            // Cannot continue a string without a newline
            String query = "SELECT 'first line' " +
                    "' - next line'\n" +
                    "\tAS \"Illegal comment within continuation\"\n";
            this.compare(query, "");
        });
        Assert.assertTrue(exception.getMessage().contains("String literal continued on same line"));
    }

    @Test
    public void unicodeIdentifierTest() {
        // Calcite does not support 6-digits escapes like Postgres, so
        // I have modified +000061 to just 0061
        String query = "SELECT U&'d\\0061t\\0061' AS U&\"d\\0061t\\0061\"";
        String expected = " data \n" +
                "------\n" +
                "data";
        this.compare(query, expected);
    }

    @Test
    public void unicodeNewEscapeTest() {
        String query = "SELECT U&'d!0061t!0061' UESCAPE '!' AS U&\"d*0061t\\0061\" UESCAPE '*'";
        String expected = " data \n" +
                "------\n" +
                "data";
        this.compare(query, expected);
    }

    @Test
    public void namesWithSlashTest() {
        String query = "SELECT U&'a\\\\b' AS \"a\\b\"";
        String expected = " a\\b \n" +
                "-----\n" +
                "a\\b";
        this.compare(query, expected);
    }

    @Test
    public void backslashWithSpacesTest() {
        String query = "SELECT U&' \\' UESCAPE '!' AS \"tricky\"";
        String expected = "tricky \n" +
                "--------\n" +
                " \\";
        this.compare(query, expected);
    }

    @Test
    public void invalidUnicodeTest() {
        Exception exception = Assert.assertThrows(
                RuntimeException.class, () -> {
                    String query = "SELECT U&'wrong: \\061'";
                    this.compare(query, "");
                });
        Assert.assertTrue(exception.getMessage().contains(
                "Unicode escape sequence starting at character 7 is not exactly four hex digits"));
    }

    // Lots of other escaping tests skipped, many using the E escaping notation from Postgres

    @Test
    public void testCharN() {
        // I hope this is right, the .out file does not contain trailing spaces.
        String query = "SELECT CAST(f1 AS text) AS \"text(char)\" FROM CHAR_TBL";
        String expected =
                " text(char) \n" +
                "------------\n" +
                "a   \n" +
                "ab  \n" +
                "abcd\n" +
                "abcd";
        this.compare(query, expected);
    }

    @Test
    public void testVarcharN() {
        // I hope this is right, the .out file does not contain trailing spaces.
        String query = "SELECT CAST(f1 AS text) AS \"text(varchar)\" FROM VARCHAR_TBL";
        String expected =
                " text(char) \n" +
                        "------------\n" +
                        "a   \n" +
                        "ab  \n" +
                        "abcd\n" +
                        "abcd";
        this.compare(query, expected);
    }
}
