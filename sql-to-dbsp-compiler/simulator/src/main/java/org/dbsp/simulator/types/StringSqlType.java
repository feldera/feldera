package org.dbsp.simulator.types;

public class StringSqlType extends SqlTypeBase {
    public StringSqlType(int precision) {
        super(SqlTypeName.VARCHAR, precision);
    }

    public StringSqlType() {
        super(SqlTypeName.VARCHAR);
    }

}
