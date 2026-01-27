package org.dbsp.sqlCompiler.circuit.operator;

import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteRelNode;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeIndexedZSet;
import org.dbsp.util.Linq;
import org.dbsp.util.Utilities;

/** Base class for the many kinds of joins we have, some incremental,
 * some non-incremental. */
public abstract class DBSPJoinBaseOperator extends DBSPBinaryOperator implements IJoin {
    /** If true the join is dynamically balanced; else it is always a hash-join */
    public final boolean balanced;

    protected DBSPJoinBaseOperator(
            CalciteRelNode node, String operation, DBSPExpression function,
            DBSPType outputType, boolean isMultiset,
            OutputPort left, OutputPort right, boolean balanced) {
        super(node, operation, function, outputType, isMultiset, left, right);
        this.checkParameterCount(function, 3);
        this.balanced = balanced;
        DBSPClosureExpression closure = this.getClosureFunction();
        DBSPTypeIndexedZSet leftType = left.getOutputIndexedZSetType();
        DBSPTypeIndexedZSet rightType = right.getOutputIndexedZSetType();
        Utilities.enforce(closure.parameters[1].getType().deref().sameType(leftType.elementType),
                () -> "Type of parameter 1 of join function " + closure.parameters[1].getType() +
                        " does not match left input element type " + leftType.elementType);
        Utilities.enforce(this.is(DBSPAsofJoinOperator.class) || // Not always true
                closure.parameters[2].getType().deref().sameType(rightType.elementType),
                () -> "Type of parameter 2 of join function " + closure.parameters[2].getType().deref() +
                        " does not match right input element type " + rightType.elementType);
        Utilities.enforce(closure.parameters[0].getType().deref().sameType(leftType.keyType),
                () -> "Type of parameter 0 of join function " + closure.parameters[0].getType().deref() +
                        " does not match left input key type " + leftType.keyType);
        Utilities.enforce(closure.parameters[0].getType().deref().sameType(rightType.keyType),
                () -> "Type of parameter 0 of join function " + closure.parameters[0].getType().deref() +
                        " does not match right input key type " + rightType.keyType);
    }

    protected static String joinOperationName(String operation, boolean balanced) {
        if (balanced)
            return operation + "_balanced";
        return operation;
    }

    /** Replace inputs and function, preserve output type */
    public final DBSPJoinBaseOperator withFunctionAndInputs(
            DBSPExpression function, OutputPort left, OutputPort right) {
        return this.with(function, this.outputType, Linq.list(left, right), false)
                .to(DBSPJoinBaseOperator.class);
    }

    @Override
    public boolean equivalent(DBSPOperator other) {
        if (!other.is(DBSPJoinBaseOperator.class))
            return false;
        DBSPJoinBaseOperator o = other.to(DBSPJoinBaseOperator.class);
        return super.equivalent(other) && this.balanced == o.balanced;
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
