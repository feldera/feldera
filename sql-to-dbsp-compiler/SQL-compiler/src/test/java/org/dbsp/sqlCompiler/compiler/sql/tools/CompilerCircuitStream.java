package org.dbsp.sqlCompiler.compiler.sql.tools;

import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.TableContents;
import org.dbsp.sqlCompiler.ir.type.DBSPType;

import java.util.List;

/**
 * Helper class for testing.  Holds together
 * - the compiler that is used to compile a program,
 * - the circuit, and
 * - the input/output data that is used to test the circuit. */
public class CompilerCircuitStream {
    public final DBSPCompiler compiler;
    public final DBSPCircuit circuit;
    final InputOutputChangeStream stream;

    public CompilerCircuitStream(DBSPCompiler compiler) {
        this(compiler, new InputOutputChangeStream());
    }

    public CompilerCircuitStream(DBSPCompiler compiler, List<String> inputs, List<String> outputs) {
        this(compiler, new InputOutputChangeStream(inputs, outputs));
    }

    public CompilerCircuitStream(DBSPCompiler compiler, InputOutputChangeStream streams) {
        this.compiler = compiler;
        this.circuit = BaseSQLTests.getCircuit(compiler);
        this.stream = streams;
    }

    public void showErrors() {
        this.compiler.messages.show(System.err);
        this.compiler.messages.clear();
    }

    /** Compiles a SQL script composed of INSERT statements.
     * into a Change. */
    public Change toChange(String script) {
        this.compiler.clearTables();
        this.compiler.compileStatements(script);
        TableContents tableContents = this.compiler.getTableContents();
        return new Change(tableContents);
    }

    /**
     * Add a step to a change stream with many input tables but one single output view.
     * A step is described as an input-output pair.
     *
     * @param script   SQL script that describes insertions and deletions into the input tables.
     * @param expected A text representation of the output produced for this step with an extra last
     *                 column that contains weights.
     */
    public void step(String script, String expected) {
        Change input = this.toChange(script);
        DBSPType outputType = this.circuit.getSingleOutputType();
        Change output = TableParser.parseChangeTable(expected, outputType);
        this.stream.addPair(input, output);
    }

    public void addChange(InputOutputChange ioChange) {
        this.stream.addChange(ioChange);
    }

    public void addPair(Change inputChange, Change outputChange) {
        this.stream.addPair(inputChange, outputChange);
    }
}
