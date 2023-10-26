package org.dbsp.simulator.values;

import org.dbsp.simulator.types.IntegerSqlType;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.util.Objects;

public class IntegerSqlValue extends BaseSqlValue {
    @Nullable
    Integer value;

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
}
