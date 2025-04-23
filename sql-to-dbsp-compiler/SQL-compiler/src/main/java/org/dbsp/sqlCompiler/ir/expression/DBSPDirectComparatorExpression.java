package org.dbsp.sqlCompiler.ir.expression;

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.compiler.backend.JsonDecoder;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.EquivalenceContext;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.user.DBSPComparatorType;
import org.dbsp.util.IIndentStream;
import org.dbsp.util.Utilities;

/** A comparator that compares two values directly.
 * It takes a direction, indicating whether the sort is ascending or descending. */
public final class DBSPDirectComparatorExpression extends DBSPComparatorExpression {
    public final DBSPComparatorExpression source;
    public final boolean ascending;

    public DBSPDirectComparatorExpression(
            CalciteObject node, DBSPComparatorExpression source, boolean ascending) {
        super(node, DBSPComparatorType.generateType(source.getComparatorType(), ascending));
        this.source = source;
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

    public DBSPType comparedValueType() {
        return this.source.comparedValueType();
    }

    @Override
    public boolean sameFields(IDBSPInnerNode other) {
        DBSPDirectComparatorExpression o = other.as(DBSPDirectComparatorExpression.class);
        if (o == null)
            return false;
        return this.source == o.source &&
                this.ascending == o.ascending;
    }

    @Override
    public IIndentStream toString(IIndentStream builder) {
        return builder.append(this.source)
                .append(".then_")
                .append(this.ascending ? "asc" : "desc")
                .append("(|t| t)");
    }

    @Override
    public DBSPExpression deepCopy() {
        return new DBSPDirectComparatorExpression(
                this.getNode(), this.source.deepCopy().to(DBSPComparatorExpression.class),
                this.ascending);
    }

    @Override
    public boolean equivalent(EquivalenceContext context, DBSPExpression other) {
        DBSPDirectComparatorExpression otherExpression = other.as(DBSPDirectComparatorExpression.class);
        if (otherExpression == null)
            return false;
        return this.ascending == otherExpression.ascending &&
                this.source.equivalent(context, otherExpression.source);
    }

    @SuppressWarnings("unused")
    public static DBSPDirectComparatorExpression fromJson(JsonNode node, JsonDecoder decoder) {
        DBSPComparatorExpression source = fromJsonInner(node, "source", decoder, DBSPComparatorExpression.class);
        boolean ascending = Utilities.getBooleanProperty(node, "ascending");
        return new DBSPDirectComparatorExpression(CalciteObject.EMPTY, source, ascending);
    }
}
