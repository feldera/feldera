package org.dbsp.sqlCompiler.ir.expression;

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.compiler.backend.JsonDecoder;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.EquivalenceContext;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeAny;
import org.dbsp.util.IIndentStream;

/** A comparator that compares two values for equality.
 * The comparison itself is described using a {@link DBSPComparatorExpression},
 * but the implementation in Rust is different. */
public final class DBSPEqualityComparatorExpression extends DBSPExpression {
    public final DBSPComparatorExpression comparator;

    public DBSPEqualityComparatorExpression(CalciteObject node, DBSPComparatorExpression comparator) {
        super(node, DBSPTypeAny.INSTANCE);
        this.comparator = comparator;
    }

    @Override
    public void accept(InnerVisitor visitor) {
        VisitDecision decision = visitor.preorder(this);
        if (decision.stop()) return;
        visitor.push(this);
        visitor.property("source");
        this.comparator.accept(visitor);
        visitor.pop(this);
        visitor.postorder(this);
    }

    @Override
    public boolean sameFields(IDBSPInnerNode other) {
        DBSPEqualityComparatorExpression o = other.as(DBSPEqualityComparatorExpression.class);
        if (o == null)
            return false;
        return this.comparator == o.comparator;
    }

    @Override
    public IIndentStream toString(IIndentStream builder) {
        return builder.append(this.comparator);
    }

    @Override
    public DBSPExpression deepCopy() {
        return new DBSPEqualityComparatorExpression(
                this.getNode(), this.comparator.deepCopy().to(DBSPComparatorExpression.class));
    }

    @Override
    public boolean equivalent(EquivalenceContext context, DBSPExpression other) {
        DBSPEqualityComparatorExpression otherExpression = other.as(DBSPEqualityComparatorExpression.class);
        if (otherExpression == null)
            return false;
        return this.comparator.equivalent(context, otherExpression.comparator);
    }

    @SuppressWarnings("unused")
    public static DBSPEqualityComparatorExpression fromJson(JsonNode node, JsonDecoder decoder) {
        DBSPComparatorExpression comparator = fromJsonInner(node, "comparator", decoder, DBSPComparatorExpression.class);
        return new DBSPEqualityComparatorExpression(CalciteObject.EMPTY, comparator);
    }
}
