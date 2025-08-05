package org.dbsp.simulator.values;

import org.dbsp.simulator.types.FunctionType;

import javax.annotation.Nullable;
import java.util.function.Function;

public class RuntimeFunction<S extends DynamicSqlValue, R extends DynamicSqlValue> {
    @Nullable
    final FunctionType type;
    final Function<S, R> implementation;

    public RuntimeFunction(@Nullable FunctionType type, Function<S, R> implementation) {
        assert type == null || type.getParameterCount() == 1;
        this.type = type;
        this.implementation = implementation;
    }

    public RuntimeFunction(Function<S, R> implementation) {
        this(null, implementation);
    }

    public Function<S, R> getFunction() {
        return this.implementation;
    }

    public R apply(S value) {
        R result = this.implementation.apply(value);
        if (this.type != null) {
            assert value.getType().equals(this.type.getParameterType(0));
            assert result.getType().equals(this.type.getResultType());
        }
        return result;
    }
}
