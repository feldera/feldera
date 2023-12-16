package org.dbsp.simulator.operators;

import org.dbsp.simulator.collections.BaseCollection;
import org.dbsp.simulator.collections.ZSet;
import org.dbsp.simulator.types.WeightType;
import org.dbsp.simulator.values.SqlTuple;

import java.util.function.Function;

public class IndexOperator<Weight> extends UnaryOperator<Weight> {
    public final Function<SqlTuple, SqlTuple> keyFunction;

    public IndexOperator(WeightType<Weight> weightType,
                         Function<SqlTuple, SqlTuple> keyFunction, BaseOperator<Weight> input) {
        super(weightType, input);
        this.keyFunction = keyFunction;
    }

    @Override
    public void step() {
        BaseCollection<Weight> input = this.getInputValue();
        ZSet<SqlTuple, Weight> zset = (ZSet<SqlTuple, Weight>) input;
        this.nextOutput = zset.index(this.keyFunction);
    }
}
