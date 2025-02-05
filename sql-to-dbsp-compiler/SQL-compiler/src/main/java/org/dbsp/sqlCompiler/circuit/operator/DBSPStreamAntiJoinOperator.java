package org.dbsp.sqlCompiler.circuit.operator;

import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;

import javax.annotation.Nullable;
import java.util.List;

/** Currently there is no corespondent operator in DBSP. */
public final class DBSPStreamAntiJoinOperator extends DBSPBinaryOperator {
    public DBSPStreamAntiJoinOperator(CalciteObject node, OutputPort left, OutputPort right) {
        super(node, "stream_antijoin", null, left.outputType(), left.isMultiset(), left, right);
        left.getOutputIndexedZSetType();
        right.getOutputIndexedZSetType();
        assert left.getOutputIndexedZSetType().keyType.sameType(right.getOutputIndexedZSetType().keyType) :
            "Anti join key types to not match\n" +
                left.getOutputIndexedZSetType().keyType + " and\n" +
                right.getOutputIndexedZSetType().keyType;
    }

    @Override
    public void accept(CircuitVisitor visitor) {
        visitor.push(this);
        VisitDecision decision = visitor.preorder(this);
        if (!decision.stop())
            visitor.postorder(this);
        visitor.pop(this);
    }

    @Override
    public DBSPSimpleOperator withFunction(@Nullable DBSPExpression expression, DBSPType outputType) {
        return new DBSPStreamAntiJoinOperator(
                this.getNode(), this.left(), this.right()).copyAnnotations(this);
    }

    @Override
    public DBSPSimpleOperator withInputs(List<OutputPort> newInputs, boolean force) {
        if (force || this.inputsDiffer(newInputs))
            return new DBSPStreamAntiJoinOperator(
                    this.getNode(), newInputs.get(0), newInputs.get(1))
                    .copyAnnotations(this);
        return this;
    }

    // equivalent inherited from base class
}
