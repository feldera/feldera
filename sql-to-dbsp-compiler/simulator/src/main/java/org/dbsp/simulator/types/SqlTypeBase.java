package org.dbsp.simulator.types;

public abstract class SqlTypeBase implements SqlType {
    public final SqlTypeName typeName;

    protected SqlTypeBase(SqlTypeName typeName) {
        this.typeName = typeName;
    }

    public SqlTypeName getTypeName() {
        return this.typeName;
    }

    @Override
    public String toString() {
        return this.typeName.text;
    }
}
