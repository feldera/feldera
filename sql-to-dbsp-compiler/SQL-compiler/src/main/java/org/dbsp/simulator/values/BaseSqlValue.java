package org.dbsp.simulator.values;

import org.dbsp.simulator.types.SqlType;

public abstract class BaseSqlValue implements DynamicSqlValue {
    public final SqlType type;

    public BaseSqlValue(SqlType type) {
        this.type = type;
    }

    @Override
    public SqlType getType() {
        return this.type;
    }
}
