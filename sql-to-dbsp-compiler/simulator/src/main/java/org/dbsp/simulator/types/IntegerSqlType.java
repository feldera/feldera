package org.dbsp.simulator.types;

public class IntegerSqlType extends SqlTypeBase {
    public static final IntegerSqlType INSTANCE = new IntegerSqlType();

    protected IntegerSqlType() {
        super(SqlTypeName.INTEGER);
    }

    @Override
    public String toString() {
        return "INTEGER";
    }
}
