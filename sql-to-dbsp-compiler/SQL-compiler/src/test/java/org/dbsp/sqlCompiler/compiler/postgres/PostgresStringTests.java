package org.dbsp.sqlCompiler.compiler.postgres;

import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.compiler.BaseSQLTests;
import org.dbsp.sqlCompiler.compiler.backend.DBSPCompiler;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPStringLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPZSetLiteral;
import org.junit.Assert;
import org.junit.Test;

public class PostgresStringTests extends BaseSQLTests {
    void compare(String query, DBSPZSetLiteral.Contents expected) {
        DBSPCompiler compiler = testCompiler();
        compiler.compileStatement("CREATE VIEW VV AS " + query);
        compiler.optimize();
        DBSPCircuit circuit = getCircuit(compiler);
        InputOutputPair streams = new InputOutputPair(
                new DBSPZSetLiteral.Contents[0],
                new DBSPZSetLiteral.Contents[] { expected }
        );
        this.addRustTestCase(query, compiler, circuit, streams);
    }

    @Test
    public void continuationTest() {
        String query = "SELECT 'first line'\n" +
                "' - next line'\n" +
                "\t' - third line'\n" +
                "\tAS \"Three lines to one\"";
        this.compare(query, new DBSPZSetLiteral.Contents(new DBSPTupleExpression(
                new DBSPStringLiteral("first line - next line - third line"))));
    }

    @Test
    public void illegalContinuationTest() {
        Exception exception = Assert.assertThrows(RuntimeException.class, () -> {
            // Cannot continue a string without a newline
            String query = "SELECT 'first line' " +
                    "' - next line'\n" +
                    "\tAS \"Illegal comment within continuation\"\n";
            this.compare(query, new DBSPZSetLiteral.Contents(new DBSPTupleExpression(
                    new DBSPStringLiteral("first line - next line - third line"))));
        });
        Assert.assertTrue(exception.getMessage().contains("String literal continued on same line"));
    }

    @Test
    public void unicodeIdentifierTest() {
        // Calcite does not support 6-digits escapes like Postgres, so
        // I have modified +000061 to just 0061
        String query = "SELECT U&'d\\0061t\\0061' AS U&\"d\\0061t\\0061\"";
        this.compare(query, new DBSPZSetLiteral.Contents(new DBSPTupleExpression(
                new DBSPStringLiteral("data"))));
    }

    @Test
    public void unicodeNewEscapeTest() {
        String query = "SELECT U&'d!0061t!0061' UESCAPE '!' AS U&\"d*0061t\\0061\" UESCAPE '*'";
        this.compare(query, new DBSPZSetLiteral.Contents(new DBSPTupleExpression(
                new DBSPStringLiteral("data"))));
    }

    @Test
    public void namesWithSlashTest() {
        String query = "SELECT U&'a\\\\b' AS \"a\\b\"";
        this.compare(query, new DBSPZSetLiteral.Contents(new DBSPTupleExpression(
                new DBSPStringLiteral("a\\b"))));
    }

    @Test
    public void backslashWithSpacesTest() {
        String query = "SELECT U&' \\' UESCAPE '!' AS \"tricky\"";
        this.compare(query, new DBSPZSetLiteral.Contents(new DBSPTupleExpression(
                new DBSPStringLiteral(" \\"))));
    }

    @Test
    public void invalidUnicodeTest() {
        Exception exception = Assert.assertThrows(
                RuntimeException.class, () -> {
                    String query = "SELECT U&'wrong: \\061'";
                    this.compare(query, new DBSPZSetLiteral.Contents(new DBSPTupleExpression(
                            new DBSPStringLiteral(""))));
                });
        Assert.assertTrue(exception.getMessage().contains(
                "Unicode escape sequence starting at character 7 is not exactly four hex digits"));
    }

    //SELECT U&'wrong: \+0061';
    //ERROR:  invalid Unicode escape
    //LINE 1: SELECT U&'wrong: \+0061';
    //                         ^
    //HINT:  Unicode escapes must be \XXXX or \+XXXXXX.
    //SELECT U&'wrong: +0061' UESCAPE +;
    //ERROR:  UESCAPE must be followed by a simple string literal at or near "+"
    //LINE 1: SELECT U&'wrong: +0061' UESCAPE +;
    //                                        ^
    //SELECT U&'wrong: +0061' UESCAPE '+';
    //ERROR:  invalid Unicode escape character at or near "'+'"
    //LINE 1: SELECT U&'wrong: +0061' UESCAPE '+';
    //                                        ^
    //SELECT U&'wrong: \db99';
    //ERROR:  invalid Unicode surrogate pair
    //LINE 1: SELECT U&'wrong: \db99';
    //                              ^
    //SELECT U&'wrong: \db99xy';
    //ERROR:  invalid Unicode surrogate pair
    //LINE 1: SELECT U&'wrong: \db99xy';
    //                              ^
    //SELECT U&'wrong: \db99\\';
    //ERROR:  invalid Unicode surrogate pair
    //LINE 1: SELECT U&'wrong: \db99\\';
    //                              ^
    //SELECT U&'wrong: \db99\0061';
    //ERROR:  invalid Unicode surrogate pair
    //LINE 1: SELECT U&'wrong: \db99\0061';
    //                              ^
    //SELECT U&'wrong: \+00db99\+000061';
    //ERROR:  invalid Unicode surrogate pair
    //LINE 1: SELECT U&'wrong: \+00db99\+000061';
    //                                 ^
    //SELECT U&'wrong: \+2FFFFF';
    //ERROR:  invalid Unicode escape value
    //LINE 1: SELECT U&'wrong: \+2FFFFF';
    // Skipped a bunch more tests with various errors
}
