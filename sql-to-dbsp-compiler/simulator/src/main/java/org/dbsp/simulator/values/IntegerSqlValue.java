package org.dbsp.simulator.values;

import org.dbsp.simulator.types.IntegerSqlType;
import org.jetbrains.annotations.NotNull;

import javax.annotation.Nullable;
import java.util.Comparator;
import java.util.Objects;

public class IntegerSqlValue extends BaseSqlValue {
    @Nullable
    final Integer value;

    public IntegerSqlValue(@Nullable Integer value) {
        super(IntegerSqlType.INSTANCE);
        this.value = value;
    }

    @Override
    public boolean isNull() {
        return this.value == null;
    }

    @Override
    public String toString() {
        if (this.isNull())
            return "NULL";
        return Integer.toString(Objects.requireNonNull(this.value));
    }

    @Nullable
    public Integer getValue() {
        return this.value;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        IntegerSqlValue that = (IntegerSqlValue) o;
        return Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(value);
    }

    @Override
    public int compareTo(@NotNull DynamicSqlValue dynamicSqlValue) {
        IntegerSqlValue other = (IntegerSqlValue) dynamicSqlValue;
        return Comparator.comparing(
                IntegerSqlValue::getValue,
                Comparator.nullsFirst(Integer::compare))
                .compare(this, other);
    }
}
