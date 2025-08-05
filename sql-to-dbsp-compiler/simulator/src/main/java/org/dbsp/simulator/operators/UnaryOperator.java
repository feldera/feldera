package org.dbsp.simulator.operators;

import org.dbsp.simulator.types.CollectionType;

/** Operator with one single input */
public abstract class UnaryOperator extends BaseOperator {
    UnaryOperator(CollectionType outputType, Stream input) {
        super(outputType, input);
    }

    public Stream input() {
        return this.inputs[0];
    }
}
