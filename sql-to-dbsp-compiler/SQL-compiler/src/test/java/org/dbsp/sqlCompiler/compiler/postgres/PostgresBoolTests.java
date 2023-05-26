package org.dbsp.sqlCompiler.compiler.postgres;

import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.compiler.BaseSQLTests;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.backend.DBSPCompiler;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPBoolLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPI32Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPZSetLiteral;
import org.junit.Test;

/**
 * <a href="https://github.com/postgres/postgres/blob/master/src/test/regress/expected/boolean.out">boolean.out</a>
 */
public class PostgresBoolTests extends BaseSQLTests {
    public DBSPCompiler compileQuery(String query, boolean optimize) {
        CompilerOptions options = new CompilerOptions();
        options.optimizerOptions.optimizationLevel = optimize ? 2 : 1;
        options.optimizerOptions.generateInputForEveryTable = true;
        DBSPCompiler compiler = new DBSPCompiler(options);
        compiler.compileStatement(query);
        return compiler;
    }

    void testQuery(String query, DBSPZSetLiteral.Contents expectedOutput, boolean optimize) {
        query = "CREATE VIEW V AS " + query;
        DBSPCompiler compiler = this.compileQuery(query, optimize);
        compiler.throwIfErrorsOccurred();
        DBSPCircuit circuit = getCircuit(compiler);
        InputOutputPair streams = new InputOutputPair(
                new DBSPZSetLiteral.Contents[0],
                new DBSPZSetLiteral.Contents[] { expectedOutput });
        this.addRustTestCase(compiler, circuit, streams);
    }

    @Test
    public void testOne() {
        String query = "SELECT 1 as 'one'";
        this.testQuery(query, new DBSPZSetLiteral.Contents(
                new DBSPTupleExpression(new DBSPI32Literal(1))), true);
    }

    @Test
    public void testFalse() {
        String query = "SELECT false as 'false'";
        this.testQuery(query, new DBSPZSetLiteral.Contents(
                new DBSPTupleExpression(DBSPBoolLiteral.FALSE)), true);
    }

    // SELECT bool 't' as 'true'
    // SELECT bool '   f           ' AS false
    // SELECT bool 'true' AS true
    // SELECT bool 'test' AS error;
    // ERROR:  invalid input syntax for type boolean: "test"
    // SELECT bool 'yes' AS true
    // SELECT bool 'yeah' AS error;
    // ERROR:  invalid input syntax for type boolean: "yeah"
    // SELECT bool 'no' AS false
    // SELECT bool 'on' AS true;
    // SELECT bool 'off' AS false;
    // SELECT bool 'of' AS false;
    // SELECT bool 'o' AS error;
    // ERROR:  invalid input syntax for type boolean: "o"
    // SELECT bool 'on_' AS error;
    // ERROR:  invalid input syntax for type boolean: "on_"
    // SELECT bool 'off_' AS error;
    // ERROR:  invalid input syntax for type boolean: "off_"
    // SELECT bool '1' AS true;
    // SELECT bool '11' AS error;
    // ERROR:  invalid input syntax for type boolean: "11"
    // SELECT bool '0' AS false;
    // SELECT bool '000' AS error;
    // SELECT bool '' AS error;
    // ERROR:  invalid input syntax for type boolean: ""
}
