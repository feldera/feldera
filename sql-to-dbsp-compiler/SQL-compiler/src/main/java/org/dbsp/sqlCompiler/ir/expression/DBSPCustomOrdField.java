package org.dbsp.sqlCompiler.ir.expression;

import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.EquivalenceContext;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.IDBSPNode;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTupleBase;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeWithCustomOrd;
import org.dbsp.util.IIndentStream;

/** An expression of the form '(*expression).0.field' or
 * Some((*expression).0.field), where
 * expression has a type of the form Option[&WithCustomOrd[T, S]]
 * The result type is always nullable */
public final class DBSPCustomOrdField extends DBSPExpression {
    public final DBSPExpression expression;
    public final int fieldNo;

    private static DBSPType getFieldType(DBSPType sourceType, int field) {
        return sourceType.deref()
                .to(DBSPTypeWithCustomOrd.class)
                .getDataType()
                .to(DBSPTypeTupleBase.class)
                .tupFields[field];
    }

    /** True if the original source field is not nullable, and thus the
     * final result needs to be wrapped in a Some(). */
    public boolean needsSome() {
        return !this.getFieldType().mayBeNull;
    }

    public DBSPCustomOrdField(DBSPExpression expression, int field) {
        super(expression.getNode(), getFieldType(expression.getType(), field).withMayBeNull(true));
        assert expression.getType().mayBeNull;
        this.expression = expression;
        this.fieldNo = field;
    }

    public DBSPType getFieldType() {
        return getFieldType(this.expression.getType(), this.fieldNo);
    }

    @Override
    public void accept(InnerVisitor visitor) {
        VisitDecision decision = visitor.preorder(this);
        if (decision.stop()) return;
        visitor.push(this);
        this.type.accept(visitor);
        this.expression.accept(visitor);
        visitor.pop(this);
        visitor.postorder(this);
    }

    @Override
    public boolean sameFields(IDBSPNode other) {
        DBSPCustomOrdField o = other.as(DBSPCustomOrdField.class);
        if (o == null)
            return false;
        return this.expression == o.expression &&
                this.fieldNo == o.fieldNo &&
                this.hasSameType(o);
    }

    @Override
    public IIndentStream toString(IIndentStream builder) {
        return builder.append("(*")
                .append(this.expression)
                .append(").get().")
                .append(this.fieldNo);
    }

    @Override
    public DBSPExpression deepCopy() {
        return new DBSPCustomOrdField(this.expression.deepCopy(), this.fieldNo);
    }

    @Override
    public boolean equivalent(EquivalenceContext context, DBSPExpression other) {
        DBSPCustomOrdField otherExpression = other.as(DBSPCustomOrdField.class);
        if (otherExpression == null)
            return false;
        return context.equivalent(this.expression, otherExpression.expression) &&
                this.fieldNo == otherExpression.fieldNo;
    }
}
