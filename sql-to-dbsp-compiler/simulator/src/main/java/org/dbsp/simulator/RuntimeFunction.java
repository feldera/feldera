package org.dbsp.simulator;

import org.dbsp.simulator.types.DataType;
import org.dbsp.simulator.types.FunctionType;

import java.util.function.Function;

public class RuntimeFunction {
    final FunctionType type;
    final Function<DataType, DataType> implementation;

    public RuntimeFunction(FunctionType type, Function<DataType, DataType> implementation) {
        this.type = type;
        this.implementation = implementation;
        assert type.getParameterCount() == 1;
    }

    public Function<DataType, DataType> getFunction() {
        return this.implementation;
    }
}
