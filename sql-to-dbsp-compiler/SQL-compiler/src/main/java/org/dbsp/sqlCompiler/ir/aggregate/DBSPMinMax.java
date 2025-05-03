package org.dbsp.sqlCompiler.ir.aggregate;

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.compiler.backend.JsonDecoder;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.EquivalenceContext;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.util.IIndentStream;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;

/** Represents the built-in SQL Aggregator from DBSP. */
public class DBSPMinMax extends DBSPAggregator {
    public enum Aggregation {
        Min,
        MinSome1,
        Max
    }
    public final Aggregation aggregation;
    @Nullable
    public final DBSPClosureExpression postProcessing;

    public DBSPMinMax(CalciteObject node, DBSPType type,
                      @Nullable DBSPClosureExpression postProcessing, Aggregation aggregation) {
        super(node, type);
        this.postProcessing = postProcessing;
        this.aggregation = aggregation;
    }

    @Override
    public DBSPExpression deepCopy() {
        return this;
    }

    @Override
    public boolean equivalent(EquivalenceContext context, DBSPExpression other) {
        DBSPMinMax o = other.as(DBSPMinMax.class);
        if (o == null)
            return false;
        return this.type.sameType(o.type) &&
                context.equivalent(this.postProcessing, o.postProcessing) &&
                this.aggregation == o.aggregation;
    }

    @Override
    public void accept(InnerVisitor visitor) {
        VisitDecision decision = visitor.preorder(this);
        if (decision.stop()) return;
        visitor.push(this);
        visitor.property("type");
        this.type.accept(visitor);
        if (this.postProcessing != null) {
            visitor.property("postProcessing");
            this.postProcessing.accept(visitor);
        }
        visitor.pop(this);
        visitor.postorder(this);
    }

    @Override
    public boolean sameFields(IDBSPInnerNode other) {
        DBSPMinMax o = other.as(DBSPMinMax.class);
        if (o == null)
            return false;
        return this.type == o.type && this.postProcessing == o.postProcessing &&
                this.aggregation == o.aggregation;
    }

    @Override
    public IIndentStream toString(IIndentStream builder) {
        return builder.append(this.aggregation.toString());
    }

    @SuppressWarnings("unused")
    public static DBSPMinMax fromJson(JsonNode node, JsonDecoder decoder) {
        DBSPType type = fromJsonInner(node, "type", decoder, DBSPType.class);
        DBSPClosureExpression postProcessing =
                fromJsonInner(node, "postProcessing", decoder, DBSPClosureExpression.class);
        Aggregation aggregation = Aggregation.valueOf(Utilities.getStringProperty(node, "aggregation"));
        return new DBSPMinMax(CalciteObject.EMPTY, type, postProcessing, aggregation);
    }
}
