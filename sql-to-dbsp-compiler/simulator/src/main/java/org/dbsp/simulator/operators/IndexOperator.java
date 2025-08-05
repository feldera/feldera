package org.dbsp.simulator.operators;

import org.dbsp.simulator.types.CollectionType;
import org.dbsp.simulator.types.SqlType;
import org.dbsp.simulator.values.DynamicSqlValue;
import org.dbsp.simulator.values.RuntimeFunction;
import org.dbsp.simulator.collections.BaseCollection;
import org.dbsp.simulator.collections.IndexedZSet;
import org.dbsp.simulator.collections.ZSet;
import org.dbsp.simulator.types.DataType;
import org.dbsp.simulator.values.SqlTuple;

import java.util.function.Function;

public class IndexOperator<Weight> extends UnaryOperator {
    public final RuntimeFunction<SqlTuple, SqlTuple> keyFunction;

    public IndexOperator(CollectionType outputType,
                         RuntimeFunction<SqlTuple, SqlTuple> keyFunction, Stream input) {
        super(outputType, input);
        this.keyFunction = keyFunction;
    }

    @Override
    public void step() {
        BaseCollection input = this.input().getCurrentValue();
        ZSet<SqlTuple> zset = (ZSet<SqlTuple>) input;
        IndexedZSet<SqlTuple, SqlTuple> result = zset.index(keyFunction);
        this.output.setValue(result);
    }
}
