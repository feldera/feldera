package org.dbsp.simulator.operators;

import org.dbsp.simulator.RuntimeFunction;
import org.dbsp.simulator.collections.BaseCollection;
import org.dbsp.simulator.collections.ZSet;
import org.dbsp.simulator.types.DataType;
import org.dbsp.simulator.types.WeightType;
import org.dbsp.simulator.values.SqlTuple;

import java.util.function.Predicate;

public class FilterOperator extends UnaryOperator {
    final RuntimeFunction keep;

    public FilterOperator(DataType outputType, Stream input, RuntimeFunction keep) {
        super(outputType, input);
        this.keep = keep;
    }

    @Override
    public void step() {
        BaseCollection input = this.input().getCurrentValue();
        var filtered = input.filter(this.keep);
        this.getOutput().setValue(filtered);
    }
}
