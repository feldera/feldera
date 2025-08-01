
package org.dbsp.simulator.operators;

import org.dbsp.simulator.types.DataType;

public abstract class BaseOperator {
    final Stream[] inputs;
    final Stream output;

    protected BaseOperator(DataType outputType, Stream... inputs) {
        this.inputs = inputs;
        this.output = new Stream(outputType, this);
    }

    /** Execute one computation step: gather data from the inputs,
     * and compute the current output. */
    public abstract void step();

    public Stream getOutput() {
        return this.output;
    }

    public int getInputCount() {
        return this.inputs.length;
    }
}
