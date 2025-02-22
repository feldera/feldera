package org.dbsp.sqlCompiler.ir.expression;

import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.EquivalenceContext;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.NonCoreIR;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeAny;
import org.dbsp.util.IIndentStream;

/** Describes an expression of the form e? */
@NonCoreIR
public final class DBSPQuestionExpression extends DBSPExpression {
    public final DBSPExpression source;

    DBSPQuestionExpression(DBSPExpression source) {
        super(source.getNode(), source.getType().withMayBeNull(false));
        this.source = source;
        assert source.getType().is(DBSPTypeAny.class) ||
                source.getType().mayBeNull;
    }

    @Override
    public void accept(InnerVisitor visitor) {
        VisitDecision decision = visitor.preorder(this);
        if (decision.stop()) return;
        visitor.push(this);
        visitor.property("source");
        this.source.accept(visitor);
        visitor.property("type");
        this.type.accept(visitor);
        visitor.pop(this);
        visitor.postorder(this);
    }

    @Override
    public boolean sameFields(IDBSPInnerNode other) {
        DBSPQuestionExpression o = other.as(DBSPQuestionExpression.class);
        if (o == null)
            return false;
        return this.source == o.source;
    }

    @Override
    public DBSPExpression deepCopy() {
        return this.source.deepCopy().question();
    }

    @Override
    public IIndentStream toString(IIndentStream builder) {
        return builder.append(this.source)
                .append("?");
    }

    @Override
    public boolean equivalent(EquivalenceContext context, DBSPExpression other) {
        DBSPQuestionExpression otherExpression = other.as(DBSPQuestionExpression.class);
        if (otherExpression == null)
            return false;
        return context.equivalent(this.source, otherExpression.source);
    }
}
