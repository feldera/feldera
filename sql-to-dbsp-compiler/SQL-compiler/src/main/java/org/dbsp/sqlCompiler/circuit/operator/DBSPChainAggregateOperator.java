package org.dbsp.sqlCompiler.circuit.operator;

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.compiler.backend.JsonDecoder;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeIndexedZSet;

import java.util.List;

/** Corresponds to the DBSP chain_aggregate operator.
 * Used to implement min and max for append-only collections with O(1) space and time. */
public class DBSPChainAggregateOperator extends DBSPUnaryOperator {
    public final DBSPClosureExpression init;

    public DBSPChainAggregateOperator(CalciteObject node, DBSPClosureExpression init,
                                      DBSPClosureExpression function, DBSPType outputType, OutputPort source) {
        super(node, "chain_aggregate", function, outputType, false, source);
        this.init = init;
        assert init.parameters.length == 2;
        assert function.parameters.length == 3;
        assert init.getResultType().sameType(function.getResultType());
        assert outputType.is(DBSPTypeIndexedZSet.class);
        assert source.outputType().is(DBSPTypeIndexedZSet.class);
        assert source.getOutputIndexedZSetType().keyType.sameType(this.getOutputIndexedZSetType().keyType);
        this.checkResultType(function, this.getOutputIndexedZSetType().elementType);
    }

    @Override
    public DBSPSimpleOperator withInputs(List<OutputPort> newInputs, boolean force) {
        if (force || this.inputsDiffer(newInputs))
            return new DBSPChainAggregateOperator(this.getNode(), this.init, this.getClosureFunction(),
                    this.outputType, newInputs.get(0)).copyAnnotations(this);
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
                CalciteObject.EMPTY, init, info.getClosureFunction(),
                info.outputType(), info.getInput(0))
                .addAnnotations(info.annotations(), DBSPChainAggregateOperator.class);
    }
}
