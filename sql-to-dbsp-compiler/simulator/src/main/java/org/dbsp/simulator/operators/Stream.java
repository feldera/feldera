package org.dbsp.simulator.operators;

import org.dbsp.simulator.collections.BaseCollection;
import org.dbsp.simulator.types.DataType;

import javax.annotation.Nullable;

public class Stream {
    final DataType dataType;
    final BaseOperator source;
    @Nullable
    BaseCollection currentValue;

    Stream(DataType dataType, BaseOperator source) {
        this.dataType = dataType;
        this.source = source;
        this.currentValue = null;
    }

    public void setValue(BaseCollection value) {
        this.currentValue = value;
    }

    public BaseCollection getCurrentValue() {
        return this.currentValue;
    }

    public void clearValue() {
        this.currentValue = null;
    }
}
