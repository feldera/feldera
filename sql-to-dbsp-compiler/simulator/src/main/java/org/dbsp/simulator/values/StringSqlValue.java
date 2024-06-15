package org.dbsp.simulator.values;

import org.dbsp.simulator.types.StringSqlType;
import org.dbsp.simulator.util.Utilities;

import javax.annotation.Nullable;
import java.util.Objects;

public class StringSqlValue extends BaseSqlValue {
    @Nullable
    final String value;

    public StringSqlValue(@Nullable String value, StringSqlType type) {
        super(type);
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
}
