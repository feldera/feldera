package org.dbsp.simulator.operators;

import org.dbsp.simulator.collections.BaseCollection;
import org.dbsp.simulator.collections.IndexedZSet;
import org.dbsp.simulator.types.CollectionType;
import org.dbsp.simulator.values.SqlTuple;

import java.util.function.BiFunction;

public class JoinOperator<Weight> extends BaseOperator {
    final BiFunction<SqlTuple, SqlTuple, SqlTuple> combiner;

    protected JoinOperator(BiFunction<SqlTuple, SqlTuple, SqlTuple> combiner,
                           CollectionType outputType,
                           Stream[] inputs) {
        super(outputType, inputs);
        assert inputs.length == 2;
        this.combiner = combiner;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void step() {
        BaseCollection left = this.inputs[0].getCurrentValue();
        IndexedZSet<SqlTuple, SqlTuple> leftIndex = (IndexedZSet<SqlTuple, SqlTuple> ) left;
        BaseCollection right = this.inputs[1].getCurrentValue();
        IndexedZSet<SqlTuple, SqlTuple> rightIndex = (IndexedZSet<SqlTuple, SqlTuple>) right;
        var result = leftIndex.join(rightIndex, this.combiner);
        this.getOutput().setValue(result);
    }
}
