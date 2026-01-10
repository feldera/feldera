package org.dbsp.sqlCompiler.ir.expression;

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.compiler.backend.JsonDecoder;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.EquivalenceContext;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.NonCoreIR;
import org.dbsp.util.IIndentStream;
import org.dbsp.util.Utilities;

/** Describes an expression of the form e? where e is a cast expression
 * that will be expanded to have a SqlResult[T] type.  (In the IR the cast will have
 * a type of T only, but in the generated Rust code it will have a SqlResult type). */
@NonCoreIR
public final class DBSPResultQuestionExpression extends DBSPExpression {
    public final DBSPExpression source;

    public DBSPResultQuestionExpression(DBSPExpression source) {
        super(source.getNode(), source.getType());
        this.source = source;
        Utilities.enforce(source.is(DBSPCastExpression.class));
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
        DBSPResultQuestionExpression o = other.as(DBSPResultQuestionExpression.class);
        if (o == null)
            return false;
        return this.source == o.source;
    }

    @Override
    public DBSPExpression deepCopy() {
        return new DBSPResultQuestionExpression(this.source.deepCopy());
    }

    @Override
    public IIndentStream toString(IIndentStream builder) {
        return builder.append(this.source)
                .append("?");
    }

    @Override
    public boolean equivalent(EquivalenceContext context, DBSPExpression other) {
        DBSPResultQuestionExpression otherExpression = other.as(DBSPResultQuestionExpression.class);
        if (otherExpression == null)
            return false;
        return context.equivalent(this.source, otherExpression.source);
    }

    @SuppressWarnings("unused")
    public static DBSPResultQuestionExpression fromJson(JsonNode node, JsonDecoder decoder) {
        DBSPExpression source = fromJsonInner(node, "source", decoder, DBSPExpression.class);
        return new DBSPResultQuestionExpression(source);
    }
}
