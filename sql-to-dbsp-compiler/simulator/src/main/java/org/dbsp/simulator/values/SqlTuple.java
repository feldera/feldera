package org.dbsp.simulator.values;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class SqlTuple {
    private final List<DynamicSqlValue> values;

    public SqlTuple() {
        this.values = new ArrayList<>();
    }

    public SqlTuple add(DynamicSqlValue value) {
        this.values.add(value);
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SqlTuple sqlTuple = (SqlTuple) o;
        return values.equals(sqlTuple.values);
    }

    @Override
    public int hashCode() {
        return Objects.hash(values);
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        boolean first = true;
        builder.append("[");
        for (DynamicSqlValue value: this.values) {
            if (!first)
                builder.append(", ");
            first = false;
            builder.append(value.toString());
        }
        builder.append("]");
        return builder.toString();
    }
}
