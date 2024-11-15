package org.dbsp.sqlCompiler.circuit.operator;

import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;

/** Base class for the many kinds of joins we have, some incremental,
 * some non-incremental. */
public abstract class DBSPJoinBaseOperator extends DBSPBinaryOperator {
    protected DBSPJoinBaseOperator(
            CalciteObject node, String operation, DBSPExpression function,
            DBSPType outputType, boolean isMultiset,
            OutputPort left, OutputPort right) {
        super(node, operation, function, outputType, isMultiset, left, right);
        this.checkParameterCount(function, 3);
    }

    public DBSPType getKeyType() {
        return left().getOutputIndexedZSetType().keyType;
    }

    public DBSPType getLeftInputValueType() {
        return this.left().getOutputIndexedZSetType().elementType;
    }

    public DBSPType getRightInputValueType() {
        return this.right().getOutputIndexedZSetType().elementType;
    }
}
