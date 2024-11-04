package org.dbsp.sqlCompiler.circuit.operator;

import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
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
                                      DBSPClosureExpression function, DBSPType outputType, OperatorPort source) {
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
    public DBSPSimpleOperator withInputs(List<OperatorPort> newInputs, boolean force) {
        if (force || this.inputsDiffer(newInputs))
            return new DBSPChainAggregateOperator(this.getNode(), this.init, this.getClosureFunction(),
                    this.outputType, newInputs.get(0)).copyAnnotations(this);
        return this;
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
}
