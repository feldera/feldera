package org.dbsp.simulator.operators;

import org.dbsp.simulator.collections.BaseCollection;
import org.dbsp.simulator.collections.IndexedZSet;
import org.dbsp.simulator.types.WeightType;
import org.dbsp.simulator.values.SqlTuple;

import java.util.function.BiFunction;

public class JoinOperator<Weight> extends BaseOperator<Weight> {
    final BiFunction<SqlTuple, SqlTuple, SqlTuple> combiner;

    protected JoinOperator(BiFunction<SqlTuple, SqlTuple, SqlTuple> combiner,
                           WeightType<Weight> weightType, BaseOperator<Weight>[] inputs) {
        super(weightType, inputs);
        assert inputs.length == 2;
        this.combiner = combiner;
    }

    @Override
    public void step() {
        BaseCollection<Weight> left = this.inputs[0].getOutput();
        IndexedZSet<SqlTuple, SqlTuple, Weight> leftIndex = (IndexedZSet<SqlTuple, SqlTuple, Weight> ) left;
        BaseCollection<Weight> right = this.inputs[1].getOutput();
        IndexedZSet<SqlTuple, SqlTuple, Weight> rightIndex = (IndexedZSet<SqlTuple, SqlTuple, Weight>) right;
        this.nextOutput = leftIndex.join(rightIndex, this.combiner);
    }
}
