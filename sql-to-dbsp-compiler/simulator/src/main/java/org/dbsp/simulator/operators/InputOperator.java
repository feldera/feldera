package org.dbsp.simulator.operators;

import org.dbsp.simulator.collections.BaseCollection;
import org.dbsp.simulator.types.CollectionType;

public class InputOperator extends BaseOperator {
    final String name;

    public InputOperator(String name, CollectionType outputType) {
        super(outputType);
        this.name = name;
    }

    public void setValue(BaseCollection value) {
        this.output.setValue(value);
    }

    @Override
    public void step() {}
}
