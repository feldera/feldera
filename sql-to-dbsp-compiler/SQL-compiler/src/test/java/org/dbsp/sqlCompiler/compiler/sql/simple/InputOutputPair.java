package org.dbsp.sqlCompiler.compiler.sql.simple;

import org.dbsp.sqlCompiler.ir.expression.literal.DBSPZSetLiteral;

/**
 * A pair of inputs and outputs used in a test.
 */
public class InputOutputPair {
    /**
     * An input value for every input table.
     */
    public final DBSPZSetLiteral[] inputs;
    /**
     * An expected output value for every output view.
     */
    public final DBSPZSetLiteral[] outputs;

    public InputOutputPair(DBSPZSetLiteral[] inputs, DBSPZSetLiteral[] outputs) {
        this.inputs = inputs;
        this.outputs = outputs;
    }

    public InputOutputPair(DBSPZSetLiteral input, DBSPZSetLiteral output) {
        this.inputs = new DBSPZSetLiteral[1];
        this.inputs[0] = input;
        this.outputs = new DBSPZSetLiteral[1];
        this.outputs[0] = output;
    }

    public DBSPZSetLiteral[] getInputs() {
        return this.inputs;
    }

    public DBSPZSetLiteral[] getOutputs() {
        return this.outputs;
    }
}
