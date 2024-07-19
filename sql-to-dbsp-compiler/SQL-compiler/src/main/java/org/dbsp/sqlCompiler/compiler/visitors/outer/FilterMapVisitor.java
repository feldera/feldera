package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.operator.DBSPFilterOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPFlatMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.compiler.IErrorReporter;
import org.dbsp.sqlCompiler.ir.expression.DBSPBlockExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPIfExpression;
import org.dbsp.sqlCompiler.ir.statement.DBSPLetStatement;
import org.dbsp.util.Linq;

/** Combine a map followed by a filter into a flatmap. */
public class FilterMapVisitor extends CircuitCloneVisitor {
    final CircuitGraph graph;

    public FilterMapVisitor(IErrorReporter reporter, CircuitGraph graph) {
        super(reporter, false);
        this.graph = graph;
    }

    @Override
    public void postorder(DBSPFilterOperator operator) {
        DBSPOperator source = this.mapped(operator.input());
        if (source.is(DBSPMapOperator.class) &&
                (graph.getFanout(operator.input()) == 1)) {
            // Generate code for the function
            // let tmp = map(...);
            // if (filter(tmp)) {
            //   Some(tmp)
            // } else {
            //   None
            // }
            DBSPClosureExpression map = source.getClosureFunction();
            DBSPClosureExpression filter = operator.getClosureFunction();
            DBSPLetStatement let = new DBSPLetStatement("tmp", map.body);
            DBSPExpression cond = filter.call(let.getVarReference().borrow()).reduce(errorReporter);
            DBSPExpression tmp = let.getVarReference();
            DBSPIfExpression ifexp = new DBSPIfExpression(
                    operator.getNode(),
                    cond,
                    tmp.some(),
                    tmp.getType().setMayBeNull(true).none());
            DBSPBlockExpression block = new DBSPBlockExpression(Linq.list(let), ifexp);
            DBSPClosureExpression function = block.closure(map.parameters);
            DBSPOperator result =
                    new DBSPFlatMapOperator(source.getNode(),
                            function, source.getOutputZSetType(),
                            operator.isMultiset, source.inputs.get(0));
            this.map(operator, result);
            return;
        }
        super.postorder(operator);
    }
}
