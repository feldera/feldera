package org.dbsp.simulator.operators;

import org.dbsp.simulator.collections.BaseCollection;
import org.dbsp.simulator.types.CollectionType;

import javax.annotation.Nullable;

public class Stream {
    final CollectionType dataType;
    @Nullable
    BaseCollection currentValue;

    Stream(CollectionType dataType) {
        this.dataType = dataType;
        this.currentValue = null;
    }

    public void setValue(BaseCollection value) {
        this.currentValue = value;
    }

    public BaseCollection getCurrentValue() {
        assert this.currentValue != null;
        return this.currentValue;
    }

    public CollectionType getType() {
        return this.dataType;
    }

    public void clearValue() {
        this.currentValue = null;
    }
}
