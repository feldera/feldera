package org.dbsp.simulator.operators;

import org.dbsp.simulator.collections.BaseCollection;
import org.dbsp.simulator.types.WeightType;

public abstract class UnaryOperator<Weight> extends BaseOperator<Weight> {
    UnaryOperator(WeightType<Weight> weightType, BaseOperator<Weight> input) {
        super(weightType, input);
    }

    public BaseOperator<Weight> input() {
        return this.inputs[0];
    }

    public BaseCollection<Weight> getInputValue() {
        return this.input().getOutput();
    }
}
