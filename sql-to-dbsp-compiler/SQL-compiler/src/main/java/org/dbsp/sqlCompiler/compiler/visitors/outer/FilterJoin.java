package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.operator.DBSPFilterOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinFlatmapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.compiler.IErrorReporter;
import org.dbsp.sqlCompiler.compiler.visitors.inner.BetaReduction;
import org.dbsp.sqlCompiler.ir.expression.DBSPBlockExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPIfExpression;
import org.dbsp.sqlCompiler.ir.statement.DBSPLetStatement;
import org.dbsp.util.Linq;

import java.util.List;

/** Combine a Join followed by a filter into a JoinFlatmap. */
public class FilterJoin extends CircuitCloneVisitor {
    public FilterJoin(IErrorReporter reporter) {
        super(reporter, false);
    }

    @Override
    public void postorder(DBSPFilterOperator operator) {
        DBSPOperator source = this.mapped(operator.input());
        if (source.is(DBSPJoinOperator.class)) {
            DBSPClosureExpression expression = source.getFunction().to(DBSPClosureExpression.class);
            DBSPClosureExpression filter = operator.getFunction().to(DBSPClosureExpression.class);
            DBSPLetStatement let = new DBSPLetStatement("tmp", expression.body);
            DBSPExpression cond = filter.call(let.getVarReference().borrow());
            DBSPIfExpression ifexp = new DBSPIfExpression(
                    source.getNode(),
                    cond,
                    let.getVarReference().some(),
                    let.getVarReference().getType().setMayBeNull(true).none());
            DBSPBlockExpression block = new DBSPBlockExpression(Linq.list(let), ifexp);
            DBSPClosureExpression newFunction = block.closure(expression.parameters);
            newFunction = new BetaReduction(this.errorReporter).apply(newFunction).to(DBSPClosureExpression.class);
            DBSPOperator result =
                    new DBSPJoinFlatmapOperator(source.getNode(), source.getOutputZSetType(),
                            newFunction, source.isMultiset, source.inputs.get(0), source.inputs.get(1));
            this.map(operator, result);
            return;
        }
        super.postorder(operator);
    }
}
