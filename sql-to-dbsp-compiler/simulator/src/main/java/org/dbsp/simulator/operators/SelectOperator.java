package org.dbsp.simulator.operators;

import org.dbsp.simulator.collections.BaseCollection;
import org.dbsp.simulator.collections.ZSet;
import org.dbsp.simulator.values.SqlTuple;
import org.dbsp.simulator.types.WeightType;

import java.util.function.Function;

public class SelectOperator<Weight> extends UnaryOperator<Weight> {
    final Function<SqlTuple, SqlTuple> tupleTransform;

    public SelectOperator(WeightType<Weight> weightType,
                          Function<SqlTuple, SqlTuple> tupleTransform,
                          BaseOperator<Weight> input) {
        super(weightType, input);
        this.tupleTransform = tupleTransform;
    }

    @Override
    public void step() {
        BaseCollection<Weight> input = this.getInputValue();
        ZSet<SqlTuple, Weight> inputZset = (ZSet<SqlTuple, Weight>) input;
        this.nextOutput = inputZset.map(this.tupleTransform);
    }
}
