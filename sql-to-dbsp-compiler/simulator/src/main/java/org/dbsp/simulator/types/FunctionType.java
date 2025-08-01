package org.dbsp.simulator.types;

public class FunctionType implements DataType {
    final DataType[] parameterTypes;
    final DataType resultType;

    public FunctionType(DataType[] parameterTypes, DataType resultType) {
        this.parameterTypes = parameterTypes;
        this.resultType = resultType;
    }

    public int getParameterCount() {
        return this.parameterTypes.length;
    }
}
