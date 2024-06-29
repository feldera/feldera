package org.dbsp.sqlCompiler.circuit.operator;

import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeZSet;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;

/** We use join_flatmap to implement a join followed by a filter.
 * The function returns None for dropping a value, and Some(result)
 * for keeping a result. */
public final class DBSPJoinFlatmapOperator extends DBSPBinaryOperator {
    public DBSPJoinFlatmapOperator(
            CalciteObject node, DBSPTypeZSet outputType,
            DBSPExpression function, boolean isMultiset,
            DBSPOperator left, DBSPOperator right) {
        super(node, "join_flatmap", function, outputType, isMultiset, left, right);
    }

    @Override
    public DBSPOperator withFunction(@Nullable DBSPExpression expression, DBSPType outputType) {
        return new DBSPJoinFlatmapOperator(
                this.getNode(), outputType.to(DBSPTypeZSet.class),
                Objects.requireNonNull(expression),
                this.isMultiset, this.left(), this.right());
    }

    @Override
    public DBSPOperator withInputs(List<DBSPOperator> newInputs, boolean force) {
        if (force || this.inputsDiffer(newInputs))
            return new DBSPJoinFlatmapOperator(
                    this.getNode(), this.getOutputZSetType(),
                    this.getFunction(), this.isMultiset, newInputs.get(0), newInputs.get(1));
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
}
