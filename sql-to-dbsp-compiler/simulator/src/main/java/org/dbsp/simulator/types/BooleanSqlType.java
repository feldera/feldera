package org.dbsp.simulator.types;

public class BooleanSqlType extends SqlTypeBase {
    public static final BooleanSqlType INSTANCE = new BooleanSqlType();

    protected BooleanSqlType() {
        super(SqlTypeName.BOOLEAN);
    }

    @Override
    public String toString() {
        return "BOOLEAN";
    }
}
