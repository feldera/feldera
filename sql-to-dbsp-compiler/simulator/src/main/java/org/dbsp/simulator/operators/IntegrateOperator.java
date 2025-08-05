package org.dbsp.simulator.operators;

import org.dbsp.simulator.collections.BaseCollection;

public class IntegrateOperator extends UnaryOperator {
    BaseCollection current;

    public IntegrateOperator(Stream input) {
        super(input.getType(), input);
        this.reset();
    }

    @Override
    public void reset() {
        this.current = this.getOutputType().zero();
    }

    @Override
    public void step() {
        BaseCollection input = this.input().getCurrentValue();
        this.current.append(input);
        this.output.setValue(this.current);
    }
}
