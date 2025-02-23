package org.dbsp.sqlCompiler.ir.aggregate;

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.compiler.backend.JsonDecoder;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.util.Utilities;

/** Representation of an aggregate that is a call to a Min or Max function.
 * In some cases this will be treated as a NonLinearAggregate, in other cases
 * it is handled specially. */
public class MinMaxAggregate extends NonLinearAggregate {
    public final boolean isMin;
    /** An expression that refers to the row variable.  The row variable is
     * the second parameter of the 'increment' closure. */
    public final DBSPExpression aggregatedValue;

    public MinMaxAggregate(CalciteObject origin, DBSPExpression zero, DBSPClosureExpression increment,
                           DBSPExpression emptySetResult, DBSPType semigroup, DBSPExpression aggregatedValue,
                           boolean isMin) {
        super(origin, zero, increment, emptySetResult, semigroup);
        this.aggregatedValue = aggregatedValue;
        this.isMin = isMin;
    }

    @Override
    public void accept(InnerVisitor visitor) {
        VisitDecision decision = visitor.preorder(this);
        if (decision.stop()) return;
        visitor.push(this);
        visitor.property("semigroup");
        this.semigroup.accept(visitor);
        visitor.property("zero");
        this.zero.accept(visitor);
        visitor.property("increment");
        this.increment.accept(visitor);
        if (this.postProcess != null) {
            visitor.property("postProcess");
            this.postProcess.accept(visitor);
        }
        visitor.property("emptySetResult");
        this.emptySetResult.accept(visitor);
        visitor.property("aggregatedValue");
        this.aggregatedValue.accept(visitor);
        visitor.pop(this);
        visitor.postorder(this);
    }

    @Override
    public boolean compatible(AggregateBase other) {
        return other.is(MinMaxAggregate.class);
    }

    @SuppressWarnings("unused")
    public static MinMaxAggregate fromJson(JsonNode node, JsonDecoder decoder) {
        DBSPExpression zero = fromJsonInner(node, "zero", decoder, DBSPExpression.class);
        DBSPClosureExpression increment = fromJsonInner(node, "increment", decoder, DBSPClosureExpression.class);
        DBSPExpression emptySetResult = fromJsonInner(node, "emptySetResult", decoder, DBSPExpression.class);
        DBSPType semigroup = fromJsonInner(node, "semigroup", decoder, DBSPType.class);
        DBSPExpression aggregatedValue = fromJsonInner(node, "aggregatedValue", decoder, DBSPExpression.class);
        boolean isMin = Utilities.getBooleanProperty(node, "isMin");
        return new MinMaxAggregate(CalciteObject.EMPTY, zero, increment, emptySetResult, semigroup, aggregatedValue, isMin);
    }
}
