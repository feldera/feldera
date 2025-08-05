package org.dbsp.simulator.types;

public class FunctionType implements DataType {
    final DataType[] parameterTypes;
    final DataType resultType;

    public FunctionType(DataType resultType, DataType... parameterTypes) {
        this.parameterTypes = parameterTypes;
        this.resultType = resultType;
    }

    public int getParameterCount() {
        return this.parameterTypes.length;
    }

    public DataType getParameterType(int i) {
        return this.parameterTypes[i];
    }

    public DataType getResultType() {
        return this.resultType;
    }
}
