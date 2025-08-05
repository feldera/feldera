package org.dbsp.simulator.values;

import org.dbsp.simulator.types.SqlType;
import org.dbsp.simulator.types.TupleSqlType;
import org.dbsp.util.Linq;
import org.jetbrains.annotations.NotNull;

import javax.management.DynamicMBean;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class SqlTuple implements DynamicSqlValue {
    private final TupleSqlType type;
    private final List<DynamicSqlValue> values;

    public SqlTuple(TupleSqlType type) {
        this.type = type;
        this.values = new ArrayList<>();
    }

    public SqlTuple(DynamicSqlValue... values) {
        this.values = List.of(values);
        this.type = new TupleSqlType(Linq.map(values, DynamicSqlValue::getType, SqlType.class));
    }

    public DynamicSqlValue get(int index) {
        return this.values.get(index);
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

    public int size() {
        return this.values.size();
    }

    @Override
    public SqlType getType() {
        return this.type;
    }

    @Override
    public boolean isNull() {
        return false;
    }

    @Override
    public int compareTo(@NotNull DynamicSqlValue dynamicSqlValue) {
        SqlTuple other = (SqlTuple) dynamicSqlValue;
        assert this.size() == other.size();
        for (int i = 0; i < this.size(); i++) {
            int compare = this.values.get(i).compareTo(other.values.get(i));
            if (compare != 0)
                return compare;
        }
        return 0;
    }
}
