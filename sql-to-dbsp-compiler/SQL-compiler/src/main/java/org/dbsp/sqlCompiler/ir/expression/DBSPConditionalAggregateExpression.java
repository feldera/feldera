package org.dbsp.sqlCompiler.ir.expression;

import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.EquivalenceContext;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeBool;
import org.dbsp.util.IIndentStream;

import javax.annotation.Nullable;

/** A conditional aggregate has the form
 * (accumulator, increment, predicate) -> accumulator.
 * The predicate is optional. */
public final class DBSPConditionalAggregateExpression extends DBSPExpression {
    public final DBSPOpcode opcode;
    public final DBSPExpression left;
    public final DBSPExpression right;
    @Nullable
    public final DBSPExpression condition;

    public DBSPConditionalAggregateExpression(
            CalciteObject node, DBSPOpcode opcode, DBSPType resultType, DBSPExpression left,
            DBSPExpression right, @Nullable DBSPExpression condition) {
        super(node, resultType);
        this.opcode = opcode;
        this.left = left;
        this.right = right;
        this.condition = condition;
        if (this.condition != null && !this.condition.getType().is(DBSPTypeBool.class))
            throw new InternalCompilerError("Expected a boolean condition type " + left + " got " +
                    this.left.getType(), this);
    }

    @Override
    public DBSPExpression deepCopy() {
        return new DBSPConditionalAggregateExpression(this.getNode(), this.opcode, this.getType(),
                this.left.deepCopy(), this.right.deepCopy(), DBSPExpression.nullableDeepCopy(condition));
    }

    @Override
    public boolean equivalent(EquivalenceContext context, DBSPExpression other) {
        DBSPConditionalAggregateExpression otherExpression = other.as(DBSPConditionalAggregateExpression.class);
        if (otherExpression == null)
            return false;
        return this.opcode == otherExpression.opcode &&
                context.equivalent(this.left, otherExpression.left) &&
                context.equivalent(this.right, otherExpression.right) &&
                context.equivalent(this.condition, otherExpression.condition);
    }

    @Override
    public void accept(InnerVisitor visitor) {
        VisitDecision decision = visitor.preorder(this);
        if (decision.stop()) return;
        visitor.push(this);
        visitor.property("type");
        this.type.accept(visitor);
        visitor.property("left");
        this.left.accept(visitor);
        visitor.property("right");
        this.right.accept(visitor);
        if (this.condition != null) {
            visitor.property("condition");
            this.condition.accept(visitor);
        }
        visitor.pop(this);
        visitor.postorder(this);
    }

    @Override
    public boolean sameFields(IDBSPInnerNode other) {
        DBSPConditionalAggregateExpression o = other.as(DBSPConditionalAggregateExpression.class);
        if (o == null)
            return false;
        return this.left == o.left &&
                this.right == o.right &&
                this.condition == o.condition &&
                this.hasSameType(o);
    }

    @Override
    public IIndentStream toString(IIndentStream builder) {
        builder.append(this.opcode.toString())
                .append("(")
                .append(this.left)
                .append(", ")
                .append(this.right);
        if (this.condition != null)
            builder.append(", ")
                    .append(this.condition);
        return builder.append(")");
    }
}
