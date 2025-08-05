package org.dbsp.simulator.types;

import org.dbsp.simulator.collections.BaseCollection;
import org.dbsp.simulator.collections.ZSet;

public class ZSetType extends CollectionType {
    @Override
    public SqlTypeName getTypeName() {
        return SqlTypeName.ZSET;
    }

    final SqlType elementType;

    public ZSetType(SqlType elementType, WeightType weightType) {
        super(weightType);
        this.elementType = elementType;
    }

    @Override
    public BaseCollection zero() {
        return new ZSet<>(this.weightType);
    }
}
