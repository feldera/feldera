package org.dbsp.sqlCompiler.ir.expression;

import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.EquivalenceContext;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.util.IIndentStream;

/** This is the dual of the UnsignedWrapExpression: it unwraps an unsigned number */
public final class DBSPUnsignedUnwrapExpression extends DBSPExpression {
    public final DBSPExpression source;
    public final DBSPUnsignedWrapExpression.TypeSequence sequence;
    public final boolean nullsLast;
    public final boolean ascending;

    public DBSPUnsignedUnwrapExpression(
            CalciteObject node, DBSPExpression source, DBSPType resultType,
            boolean ascending, boolean nullsLast) {
        super(node, resultType);
        this.source = source;
        this.sequence = new DBSPUnsignedWrapExpression.TypeSequence(resultType);
        this.nullsLast = nullsLast;
        this.ascending = ascending;
    }

    @Override
    public void accept(InnerVisitor visitor) {
        VisitDecision decision = visitor.preorder(this);
        if (decision.stop()) return;
        visitor.push(this);
        visitor.property("source");
        this.source.accept(visitor);
        visitor.pop(this);
        visitor.postorder(this);
    }

    @Override
    public boolean sameFields(IDBSPInnerNode other) {
        DBSPUnsignedUnwrapExpression o = other.as(DBSPUnsignedUnwrapExpression.class);
        if (o == null)
            return false;
        return this.source == o.source && this.type.sameType(o.type)
                && this.ascending == o.ascending && this.nullsLast == o.nullsLast;
    }

    public String getMethod() {
        return this.getType().mayBeNull ? "to_signed_option" : "to_signed";
    }

    @Override
    public IIndentStream toString(IIndentStream builder) {
        return builder.append("UnsignedWrapper")
                .append("::")
                .append(this.getMethod())
                .append("(")
                .append(source)
                .append(", ")
                .append(this.ascending)
                .append(", ")
                .append(this.nullsLast)
                .append(")");
    }

    @Override
    public DBSPExpression deepCopy() {
        return new DBSPUnsignedUnwrapExpression(
                this.getNode(), this.source.deepCopy(), this.getType(),
                this.ascending, this.nullsLast);
    }

    @Override
    public boolean equivalent(EquivalenceContext context, DBSPExpression other) {
        DBSPUnsignedUnwrapExpression otherExpression = other.as(DBSPUnsignedUnwrapExpression.class);
        if (otherExpression == null)
            return false;
        return this.ascending == otherExpression.ascending &&
                this.nullsLast == otherExpression.nullsLast &&
                context.equivalent(this.source, otherExpression.source);
    }

    public DBSPExpression replaceSource(DBSPExpression source) {
        return new DBSPUnsignedUnwrapExpression(this.getNode(), source, this.type, this.ascending, this.nullsLast);
    }
}
