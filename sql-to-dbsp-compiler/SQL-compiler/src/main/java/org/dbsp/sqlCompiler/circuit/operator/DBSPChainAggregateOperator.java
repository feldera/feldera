package org.dbsp.sqlCompiler.circuit.operator;

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.compiler.backend.JsonDecoder;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteEmptyRel;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteRelNode;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeIndexedZSet;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.util.List;

/** Corresponds to the DBSP chain_aggregate operator.
 * Used to implement min and max for append-only collections with O(1) space and time. */
public class DBSPChainAggregateOperator extends DBSPUnaryOperator {
    public final DBSPClosureExpression init;

    public DBSPChainAggregateOperator(CalciteRelNode node, DBSPClosureExpression init,
                                      DBSPClosureExpression function, DBSPType outputType, OutputPort source) {
        super(node, "chain_aggregate", function, outputType, false, source, true);
        this.init = init;
        Utilities.enforce(init.parameters.length == 2);
        Utilities.enforce(function.parameters.length == 3);
        Utilities.enforce(init.getResultType().sameType(function.getResultType()));
        Utilities.enforce(outputType.is(DBSPTypeIndexedZSet.class));
        Utilities.enforce(source.outputType().is(DBSPTypeIndexedZSet.class));
        Utilities.enforce(source.getOutputIndexedZSetType().keyType.sameType(this.getOutputIndexedZSetType().keyType));
        this.checkResultType(function, this.getOutputIndexedZSetType().elementType);
    }

    @Override
    public DBSPSimpleOperator with(
            @Nullable DBSPExpression function, DBSPType outputType,
            List<OutputPort> newInputs, boolean force) {
        if (this.mustReplace(force, function, newInputs, outputType)) {
            return new DBSPChainAggregateOperator(this.getRelNode(), this.init, this.getClosureFunction(),
                    outputType, newInputs.get(0)).copyAnnotations(this);
        }
        return this;
    }

    @Override
    public void accept(InnerVisitor visitor) {
        super.accept(visitor);
        visitor.property("init");
        this.init.accept(visitor);
    }

    @Override
    public void accept(CircuitVisitor visitor) {
        visitor.push(this);
        VisitDecision decision = visitor.preorder(this);
        if (!decision.stop())
            visitor.postorder(this);
        visitor.pop(this);
    }

    @Override
    public boolean equivalent(DBSPOperator other) {
        if (!super.equivalent(other))
            return false;
        DBSPChainAggregateOperator otherOperator = other.as(DBSPChainAggregateOperator.class);
        if (otherOperator == null)
            return false;
        return this.init.equivalent(otherOperator.init);
    }

    @SuppressWarnings("unused")
    public static DBSPChainAggregateOperator fromJson(JsonNode node, JsonDecoder decoder) {
        CommonInfo info = DBSPSimpleOperator.commonInfoFromJson(node, decoder);
        DBSPClosureExpression init = fromJsonInner(node, "init", decoder, DBSPClosureExpression.class);
        return new DBSPChainAggregateOperator(
                CalciteEmptyRel.INSTANCE, init, info.getClosureFunction(),
                info.outputType(), info.getInput(0))
                .addAnnotations(info.annotations(), DBSPChainAggregateOperator.class);
    }
}
