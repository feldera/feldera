package org.dbsp.sqlCompiler.compiler.sql.tools;

/** A pair of changes, one for inputs and the expected corresponding outputs,
 * used in a test. */
public class InputOutputChange implements IStreamCommand {
    /** An input value for every input table. */
    public final Change inputs;
    /** An expected output value for every output view. */
    public final Change outputs;

    public InputOutputChange(Change inputs, Change outputs) {
        this.inputs = inputs;
        this.outputs = outputs;
    }

    public InputOutputChangeStream toStream() {
        return new InputOutputChangeStream().addChange(this);
    }

    public Change getInputs() {
        return this.inputs;
    }

    public Change getOutputs() {
        return this.outputs;
    }

    public boolean compatible(IStreamCommand change) {
        if (change instanceof InputOutputChange otherChange) {
            return this.inputs.compatible(otherChange.inputs) &&
                    this.outputs.compatible(otherChange.outputs);
        }
        return true;
    }
}
