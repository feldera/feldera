
package org.dbsp.simulator.operators;

import org.dbsp.simulator.types.CollectionType;
import org.dbsp.simulator.util.ICastable;

/** Base class for operators */
public abstract class BaseOperator implements ICastable {
    final Stream[] inputs;
    final Stream output;

    protected BaseOperator(CollectionType outputType, Stream... inputs) {
        this.inputs = inputs;
        this.output = new Stream(outputType);
    }

    /** Execute one computation step: gather data from the inputs,
     * and compute the current output. */
    public abstract void step();

    public void reset() {}

    public Stream getOutput() {
        return this.output;
    }

    public int getInputCount() {
        return this.inputs.length;
    }

    public CollectionType getOutputType() {
        return this.getOutput().getType();
    }
}
