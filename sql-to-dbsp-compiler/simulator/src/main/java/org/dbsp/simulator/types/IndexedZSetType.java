package org.dbsp.simulator.types;

import org.dbsp.simulator.collections.BaseCollection;
import org.dbsp.simulator.collections.IndexedZSet;

public class IndexedZSetType extends CollectionType {
    @Override
    public SqlTypeName getTypeName() {
        return SqlTypeName.INDEXED_ZSET;
    }

    final SqlType elementType;
    final SqlType keyType;

    public IndexedZSetType(SqlType keyType, SqlType elementType, WeightType weightType) {
        super(weightType);
        this.elementType = elementType;
        this.keyType = keyType;
    }

    @Override
    public BaseCollection zero() {
        return new IndexedZSet<>(this.weightType);
    }
}
