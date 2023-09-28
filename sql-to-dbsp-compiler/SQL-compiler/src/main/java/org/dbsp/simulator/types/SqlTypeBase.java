package org.dbsp.simulator.types;

public abstract class SqlTypeBase implements SqlType {
    public final SqlTypeName typeName;
    public final int precision;

    protected SqlTypeBase(SqlTypeName typeName) {
        this.typeName = typeName;
        this.precision = INFINITE_PRECISION;
    }

    protected SqlTypeBase(SqlTypeName typeName, int precision) {
        this.typeName = typeName;
        this.precision = precision;
    }

    @Override
    public int getPrecision() {
        return this.precision;
    }

    public SqlTypeName getTypeName() {
        return this.typeName;
    }

    @Override
    public String toString() {
        String result = this.typeName.text;
        if (this.precision != INFINITE_PRECISION)
            result += "(" + this.precision + ")";
        return result;
    }
}
