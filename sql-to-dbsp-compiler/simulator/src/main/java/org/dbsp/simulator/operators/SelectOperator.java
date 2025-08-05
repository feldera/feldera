package org.dbsp.simulator.operators;

import org.dbsp.simulator.collections.ZSet;
import org.dbsp.simulator.types.CollectionType;
import org.dbsp.simulator.values.DynamicSqlValue;
import org.dbsp.simulator.values.RuntimeFunction;

/** Apply a function to every element of an input collection to produce the output collection */
public class SelectOperator extends UnaryOperator {
    final RuntimeFunction<DynamicSqlValue, DynamicSqlValue> tupleTransform;

    public SelectOperator(CollectionType outputType,
                          RuntimeFunction<DynamicSqlValue, DynamicSqlValue> tupleTransform,
                          Stream input) {
        super(outputType, input);
        this.tupleTransform = tupleTransform;
    }

    @Override
    public void step() {
        ZSet<DynamicSqlValue> input = (ZSet<DynamicSqlValue>) this.input().getCurrentValue();
        ZSet<DynamicSqlValue> result = input.map(this.tupleTransform.getFunction());
        this.output.setValue(result);
    }
}
