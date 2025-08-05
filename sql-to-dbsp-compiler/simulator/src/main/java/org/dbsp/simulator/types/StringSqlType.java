package org.dbsp.simulator.types;

public class StringSqlType extends SqlTypeBase {
    public static final StringSqlType INSTANCE = new StringSqlType();

    private StringSqlType() {
        super(SqlTypeName.VARCHAR);
    }
}
