package org.dbsp.sqlCompiler.circuit.operator;

import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeIndexedZSet;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;

/** A join that produces an IndexedZSet.
 * Notice that, unlike the corresponding DBSP operator join_index,
 * this operator has a function that produces a single value, and not an Iterator.
 * The code generator has to wrap this value in a Some to be able to use the DBSP operator. */
public final class DBSPJoinIndexOperator extends DBSPJoinBaseOperator {
    public DBSPJoinIndexOperator(
            CalciteObject node, DBSPTypeIndexedZSet outputType,
            DBSPExpression function, boolean isMultiset,
            DBSPOperator left, DBSPOperator right) {
        super(node, "join_index", function, outputType, isMultiset, left, right);
        assert left.getOutputIndexedZSetType().keyType.sameType(right.getOutputIndexedZSetType().keyType);
    }

    @Override
    public DBSPOperator withFunction(@Nullable DBSPExpression expression, DBSPType outputType) {
        return new DBSPJoinIndexOperator(
                this.getNode(), outputType.to(DBSPTypeIndexedZSet.class),
                Objects.requireNonNull(expression),
                this.isMultiset, this.left(), this.right()).copyAnnotations(this);
    }

    @Override
    public DBSPOperator withInputs(List<DBSPOperator> newInputs, boolean force) {
        if (force || this.inputsDiffer(newInputs))
            return new DBSPJoinIndexOperator(
                    this.getNode(), this.getOutputIndexedZSetType(),
                    this.getFunction(), this.isMultiset, newInputs.get(0), newInputs.get(1))
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

    @Override
    public boolean equivalent(DBSPOperator other) {
        if (!super.equivalent(other))
            return false;
        DBSPJoinIndexOperator otherOperator = other.as(DBSPJoinIndexOperator.class);
        if (otherOperator == null)
            return false;
        return this.function.equivalent(otherOperator.function);
    }
}
