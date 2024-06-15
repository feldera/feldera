package org.dbsp.simulator.operators;

import org.dbsp.simulator.collections.BaseCollection;
import org.dbsp.simulator.collections.ZSet;
import org.dbsp.simulator.types.WeightType;
import org.dbsp.simulator.values.SqlTuple;

import java.util.function.Predicate;

public class FilterOperator<Weight> extends UnaryOperator<Weight> {
    final Predicate<SqlTuple> keep;

    public FilterOperator(WeightType<Weight> weightType,
                          Predicate<SqlTuple> keep, BaseOperator<Weight> input) {
        super(weightType, input);
        this.keep = keep;
    }

    @Override
    public void step() {
        BaseCollection<Weight> input = this.getInputValue();
        ZSet<SqlTuple, Weight> inputZset = (ZSet<SqlTuple, Weight>) input;
        this.nextOutput = inputZset.filter(this.keep);
    }
}
