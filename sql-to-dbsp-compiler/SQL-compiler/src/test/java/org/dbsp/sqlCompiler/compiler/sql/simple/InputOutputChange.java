package org.dbsp.sqlCompiler.compiler.sql.simple;

/** A pair of inputs and outputs used in a test. */
public class InputOutputChange {
    /** An input value for every input table. */
    public final IChange inputs;
    /** An expected output value for every output view. */
    public final IChange outputs;

    public InputOutputChange(IChange inputs, IChange outputs) {
        this.inputs = inputs;
        this.outputs = outputs;
    }

    public InputOutputChangeStream toStream() {
        return new InputOutputChangeStream().addChange(this);
    }

    public IChange getInputs() {
        return this.inputs;
    }

    public IChange getOutputs() {
        return this.outputs;
    }
}
