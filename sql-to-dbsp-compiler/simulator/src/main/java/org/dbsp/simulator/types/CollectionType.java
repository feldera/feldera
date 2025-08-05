package org.dbsp.simulator.types;

import org.dbsp.simulator.collections.BaseCollection;

public abstract class CollectionType implements SqlType {
    protected final WeightType weightType;

    protected CollectionType(WeightType weightType) {
        this.weightType = weightType;
    }

    public abstract BaseCollection zero();
}
