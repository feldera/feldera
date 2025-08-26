package org.dbsp.simulator.values;

import org.dbsp.simulator.types.BooleanSqlType;
import org.jetbrains.annotations.NotNull;

import javax.annotation.Nullable;
import java.util.Comparator;
import java.util.Objects;

public class BooleanSqlValue extends BaseSqlValue {
    @Nullable
    final Boolean value;

    public BooleanSqlValue(@Nullable Boolean value) {
        super(BooleanSqlType.INSTANCE);
        this.value = value;
    }

    @Override
    public boolean isNull() {
        return this.value == null;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;

        BooleanSqlValue that = (BooleanSqlValue) o;
        return Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(value);
    }

    @Override
    public String toString() {
        if (this.isNull())
            return "NULL";
        return Boolean.toString(Objects.requireNonNull(this.value));
    }

    public boolean isTrue() {
        return this.value != null && this.value;
    }

    @Nullable
    public Boolean getValue() {
        return this.value;
    }

    @Override
    public int compareTo(@NotNull DynamicSqlValue dynamicSqlValue) {
        BooleanSqlValue other = (BooleanSqlValue) dynamicSqlValue;
        return Comparator.comparing(
                        BooleanSqlValue::getValue,
                        Comparator.nullsFirst(Boolean::compare))
                .compare(this, other);
    }
}
