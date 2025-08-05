package org.dbsp.simulator.types;

import java.util.List;

public class TupleSqlType implements SqlType {
    final List<SqlType> fieldTypes;

    public TupleSqlType(List<SqlType> fieldTypes) {
        this.fieldTypes = fieldTypes;
    }

    public TupleSqlType(SqlType... fieldTypes) {
        this.fieldTypes = List.of(fieldTypes);
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("(");
        for (int i = 0; i < this.fieldTypes.size(); i++) {
            if (i > 0)
                builder.append(", ");
            builder.append(fieldTypes.get(i).getTypeName());
        }
        builder.append(")");
        return builder.toString();
    }


    @Override
    public SqlTypeName getTypeName() {
        return SqlTypeName.TUPLE;
    }
}
