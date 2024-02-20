package org.dbsp.sqlCompiler.compiler.sql;

import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.TableContents;
import org.dbsp.sqlCompiler.compiler.sql.simple.Change;
import org.dbsp.sqlCompiler.compiler.sql.simple.InputOutputChangeStream;
import org.dbsp.sqlCompiler.ir.type.DBSPType;

public class StreamingTest extends SqlIoTest {
    /** Compiles a SQL script composed of INSERT and DELETE statements.
     * into a Change. */
    public Change toChange(String script) {
        // Use a fresh compiler
        DBSPCompiler compiler = this.testCompiler();
        // Execute table creation statements.
        this.prepareInputs(compiler);
        compiler.compileStatements(script);
        TableContents tableContents = compiler.getTableContents();
        return new Change(tableContents);
    }

    public void addStep(
            DBSPCircuit circuit, InputOutputChangeStream stream, String script, String expected) {
        Change input = this.toChange(script);
        DBSPType outputType = circuit.getSingleOutputType();
        Change output = this.parseTable(expected, outputType);
        stream.addPair(input, output);
    }
}
