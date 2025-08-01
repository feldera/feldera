package org.dbsp.simulator.operators;

import org.dbsp.simulator.collections.BaseCollection;
import org.dbsp.simulator.types.DataType;
import org.dbsp.simulator.types.WeightType;

public abstract class UnaryOperator extends BaseOperator {
    UnaryOperator(DataType outputType, Stream input) {
        super(outputType, input);
    }

    public Stream input() {
        return this.inputs[0];
    }
}
