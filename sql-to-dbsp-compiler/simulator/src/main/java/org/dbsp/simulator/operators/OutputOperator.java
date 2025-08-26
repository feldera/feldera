package org.dbsp.simulator.operators;

import org.dbsp.simulator.collections.BaseCollection;

public class OutputOperator extends UnaryOperator {
    final String name;

    public OutputOperator(String name, Stream input) {
        super(input.getType(), input);
        this.name = name;
    }

    public BaseCollection getValue() {
        return this.output.getCurrentValue();
    }

    @Override
    public void step() {
        this.output.setValue(this.input().getCurrentValue());
    }
}
