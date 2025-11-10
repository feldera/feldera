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
public abstract class DBSPJoinBaseOperator extends DBSPBinaryOperator {
    protected DBSPJoinBaseOperator(
            CalciteRelNode node, String operation, DBSPExpression function,
            DBSPType outputType, boolean isMultiset,
            OutputPort left, OutputPort right) {
        super(node, operation, function, outputType, isMultiset, left, right, true);
        this.checkParameterCount(function, 3);
        DBSPClosureExpression closure = this.getClosureFunction();
        DBSPTypeIndexedZSet leftType = left.getOutputIndexedZSetType();
        DBSPTypeIndexedZSet rightType = right.getOutputIndexedZSetType();
        Utilities.enforce(closure.parameters[1].getType().deref().sameType(leftType.elementType),
                () -> "Type of parameter 1 of join function " + closure.parameters[1].getType() +
                        " does not match left input element type " + leftType.elementType);
        Utilities.enforce(this.is(DBSPAsofJoinOperator.class) || // Not always true
                closure.parameters[2].getType().deref().sameType(rightType.elementType),
                () -> "Type of parameter 2 of join function " + closure.parameters[2].getType() +
                        " does not match right input element type " + rightType.elementType);
        Utilities.enforce(closure.parameters[0].getType().deref().sameType(leftType.keyType),
                () -> "Type of parameter 0 of join function " + closure.parameters[0].getType() +
                        " does not match left input key type " + leftType.keyType);
        Utilities.enforce(closure.parameters[0].getType().deref().sameType(rightType.keyType),
                () -> "Type of parameter 0 of join function " + closure.parameters[0].getType() +
                        " does not match right input key type " + rightType.keyType);
    }

    /** Replace inputs and function, preserve output type */
    public final DBSPJoinBaseOperator withFunctionAndInputs(
            DBSPExpression function, OutputPort left, OutputPort right) {
        return this.with(function, this.outputType, Linq.list(left, right), false)
                .to(DBSPJoinBaseOperator.class);
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
