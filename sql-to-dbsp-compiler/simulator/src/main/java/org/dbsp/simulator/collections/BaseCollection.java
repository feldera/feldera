package org.dbsp.simulator.collections;

import org.dbsp.simulator.types.DataType;
import org.dbsp.simulator.types.WeightType;
import org.dbsp.simulator.util.ICastable;
import org.dbsp.simulator.util.IndentStream;
import org.dbsp.simulator.util.ToIndentableString;

public abstract class BaseCollection implements ICastable, ToIndentableString, DataType {
    final WeightType weightType;

    protected BaseCollection(WeightType weightType) {
        this.weightType = weightType;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        IndentStream stream = new IndentStream(builder);
        this.toString(stream);
        return stream.toString();
    }

    public abstract void append(BaseCollection other);
}
