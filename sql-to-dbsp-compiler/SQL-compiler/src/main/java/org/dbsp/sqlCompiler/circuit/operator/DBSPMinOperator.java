package org.dbsp.sqlCompiler.circuit.operator;

import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeIndexedZSet;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeZSet;

import java.util.List;

/** Represents a min or max aggregate.
 * This is lowered into a different representation later.
 * The output type can be either a ZSet (for global aggregates)
 * or an IndexedZSet (for group-by-aggregate). */
public class DBSPMinOperator extends DBSPUnaryOperator {
    protected DBSPMinOperator(CalciteObject node, String operation, DBSPExpression function,
                              DBSPType outputType, DBSPOperator source) {
        super(node, operation, function, outputType, false, source);
        assert operation.equals("min") || operation.equals("max");
        DBSPClosureExpression closureFunction = this.getClosureFunction();
        if (source.outputType.is(DBSPTypeZSet.class)) {
            assert outputType.is(DBSPTypeZSet.class);
            assert closureFunction.getResultType().sameType(this.getOutputZSetElementType());
        } else {
            assert outputType.is(DBSPTypeIndexedZSet.class);
            assert this.getOutputIndexedZSetType().keyType.sameType(source.getOutputIndexedZSetType().keyType);
            assert closureFunction.getResultType().sameType(this.getOutputIndexedZSetType().elementType);
        }
    }

    @Override
    public DBSPOperator withInputs(List<DBSPOperator> newInputs, boolean force) {
        if (force || this.inputsDiffer(newInputs))
            return new DBSPMinOperator(
                    this.getNode(), this.operation, this.getFunction(), this.outputType, newInputs.get(0))
                    .copyAnnotations(this);
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
}
