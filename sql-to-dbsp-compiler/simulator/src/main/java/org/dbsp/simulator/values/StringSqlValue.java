package org.dbsp.simulator.values;

import org.dbsp.simulator.types.StringSqlType;
import org.dbsp.simulator.util.Utilities;
import org.jetbrains.annotations.NotNull;

import javax.annotation.Nullable;
import java.util.Comparator;
import java.util.Objects;

public class StringSqlValue extends BaseSqlValue {
    @Nullable
    final String value;

    public StringSqlValue(@Nullable String value) {
        super(StringSqlType.INSTANCE);
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
        return Utilities.singleQuote(Objects.requireNonNull(this.value));
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;

        StringSqlValue that = (StringSqlValue) o;
        return Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(value);
    }

    @Nullable
    public String getValue() {
        return this.value;
    }

    @Override
    public int compareTo(@NotNull DynamicSqlValue dynamicSqlValue) {
        StringSqlValue other = (StringSqlValue) dynamicSqlValue;
        return Comparator.comparing(
                        StringSqlValue::getValue,
                        Comparator.nullsFirst(String::compareTo))
                .compare(this, other);
    }
}
