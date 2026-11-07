package org.dbsp.sqlCompiler.circuit.operator.primitives;

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.circuit.operator.DBSPBinaryOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinFilterMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.circuit.operator.IPrimitive;
import org.dbsp.sqlCompiler.compiler.backend.JsonDecoder;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteEmptyRel;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteRelNode;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeIndexedZSet;
import org.dbsp.sqlCompiler.ir.type.user.StreamKind;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;

/** Models the low-level DBSP operator {@code JoinTrace}: it joins the changes of one side with
 * the integral of the other side.  One input is a snapshot of the changes in this step, the other input is a
 * snapshot of a collection; either input can hold the changes.  The output holds the changes
 * of the join.  The function receives the key, the left value, and the right value.  It returns
 * a row for a {@link DBSPJoinOperator}, a key and a value for a {@link DBSPJoinIndexOperator},
 * and an optional row for a {@link DBSPJoinFilterMapOperator}, which drops the pair when the
 * row is missing. */
public final class DBSPJoinKernelOperator extends DBSPBinaryOperator implements IPrimitive {
    public DBSPJoinKernelOperator(CalciteRelNode node, DBSPClosureExpression function, DBSPType outputType,
                                  OutputPort left, OutputPort right) {
        super(node, "join_kernel", function, outputType, left, right);
        this.checkParameterCount(function, 3);
        DBSPTypeIndexedZSet leftType = left.getOutputIndexedZSetType();
        DBSPTypeIndexedZSet rightType = right.getOutputIndexedZSetType();
        Utilities.enforce(leftType.keyType.sameType(rightType.keyType),
                () -> "Join kernel " + this + " joins keys of type " + leftType.keyType +
                        " with keys of type " + rightType.keyType);
        Utilities.enforce(function.parameters[0].getType().deref().sameType(leftType.keyType),
                () -> "Type of parameter 0 of join kernel function " + function.parameters[0].getType() +
                        " does not match the key type " + leftType.keyType);
        Utilities.enforce(function.parameters[1].getType().deref().sameType(leftType.elementType),
                () -> "Type of parameter 1 of join kernel function " + function.parameters[1].getType() +
                        " does not match the left element type " + leftType.elementType);
        Utilities.enforce(function.parameters[2].getType().deref().sameType(rightType.elementType),
                () -> "Type of parameter 2 of join kernel function " + function.parameters[2].getType() +
                        " does not match the right element type " + rightType.elementType);
    }

    /** One input holds the changes of its side, the other input the integral of its side. */
    @Override
    protected StreamKind computeOutputKind() {
        StreamKind left = this.left().kind();
        StreamKind right = this.right().kind();
        boolean leftChanges = left == StreamKind.DELTA_SNAPSHOT && right == StreamKind.COLLECTION_SNAPSHOT;
        boolean rightChanges = left == StreamKind.COLLECTION_SNAPSHOT && right == StreamKind.DELTA_SNAPSHOT;
        Utilities.enforce(leftChanges || rightChanges,
                () -> "Join kernel " + this + " reads " + left + " and " + right +
                        "; it needs one " + StreamKind.DELTA_SNAPSHOT + " and one " + StreamKind.COLLECTION_SNAPSHOT);
        return StreamKind.DELTA;
    }

    @Override
    public DBSPSimpleOperator with(
            @Nullable DBSPExpression function, DBSPType outputType,
            List<OutputPort> newInputs, boolean force) {
        if (this.mustReplace(force, function, newInputs, outputType))
            return new DBSPJoinKernelOperator(this.getRelNode(),
                    Objects.requireNonNull(function).to(DBSPClosureExpression.class), outputType,
                    newInputs.get(0), newInputs.get(1))
                    .copyAnnotations(this);
        return this;
    }

    @Override
    public void accept(CircuitVisitor visitor) {
        visitor.push(this);
        VisitDecision decision = visitor.preorder(this);
        if (!decision.stop())
            visitor.postorder(this);
        visitor.pop(this);
    }

    @SuppressWarnings("unused")
    public static DBSPJoinKernelOperator fromJson(JsonNode node, JsonDecoder decoder) {
        CommonInfo info = commonInfoFromJson(node, decoder);
        return new DBSPJoinKernelOperator(CalciteEmptyRel.INSTANCE, info.getClosureFunction(),
                info.outputType(), info.getInput(0), info.getInput(1))
                .addAnnotations(info.annotations(), DBSPJoinKernelOperator.class);
    }
}
