package org.dbsp.simulator.operators;

import org.dbsp.simulator.RuntimeFunction;
import org.dbsp.simulator.collections.BaseCollection;
import org.dbsp.simulator.collections.IndexedZSet;
import org.dbsp.simulator.collections.ZSet;
import org.dbsp.simulator.types.DataType;
import org.dbsp.simulator.types.WeightType;
import org.dbsp.simulator.values.SqlTuple;

import java.util.function.Function;

public class IndexOperator<Weight> extends UnaryOperator {
    public final RuntimeFunction keyFunction;

    public IndexOperator(DataType outputType,
                         RuntimeFunction keyFunction, Stream input) {
        super(outputType, input);
        this.keyFunction = keyFunction;
    }

    @Override
    public void step() {
        BaseCollection input = this.input().getCurrentValue();
        ZSet<SqlTuple> zset = (ZSet<SqlTuple>) input;
        Function<SqlTuple, SqlTuple> indexer = (Function<SqlTuple, SqlTuple>) this.keyFunction.getFunction();
        IndexedZSet<SqlTuple, SqlTuple> result = zset.index(indexer);
    }
}
