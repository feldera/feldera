package org.dbsp.simulator.operators;

import org.dbsp.simulator.collections.ZSet;
import org.dbsp.simulator.types.CollectionType;
import org.dbsp.simulator.values.BooleanSqlValue;
import org.dbsp.simulator.values.DynamicSqlValue;
import org.dbsp.simulator.values.RuntimeFunction;

@SuppressWarnings("unchecked")
public class FilterOperator extends UnaryOperator {
    final RuntimeFunction<DynamicSqlValue, BooleanSqlValue> keep;

    public FilterOperator(CollectionType outputType, Stream input, RuntimeFunction<DynamicSqlValue, BooleanSqlValue> keep) {
        super(outputType, input);
        this.keep = keep;
    }

    @Override
    public void step() {
        ZSet<DynamicSqlValue> input = (ZSet<DynamicSqlValue>) this.input().getCurrentValue();
        var filtered = input.filter(this.keep);
        this.getOutput().setValue(filtered);
    }
}
