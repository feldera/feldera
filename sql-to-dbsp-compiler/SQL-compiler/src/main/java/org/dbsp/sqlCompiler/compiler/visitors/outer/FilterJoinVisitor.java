package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.operator.DBSPFilterOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinFilterMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.compiler.IErrorReporter;

/** Combine a Join followed by a filter into a JoinFilterMap. */
public class FilterJoinVisitor extends CircuitCloneVisitor {
    final CircuitGraph graph;

    public FilterJoinVisitor(IErrorReporter reporter, CircuitGraph graph) {
        super(reporter, false);
        this.graph = graph;
    }

    @Override
    public void postorder(DBSPFilterOperator operator) {
        DBSPOperator source = this.mapped(operator.input());
        if (source.is(DBSPJoinOperator.class) &&
                (this.graph.getFanout(operator.input()) == 1)) {
            DBSPOperator result =
                    new DBSPJoinFilterMapOperator(source.getNode(), source.getOutputZSetType(),
                            source.getFunction(), operator.getFunction(), null,
                            source.isMultiset, source.inputs.get(0), source.inputs.get(1))
                            .copyAnnotations(operator);
            this.map(operator, result);
            return;
        }
        super.postorder(operator);
    }
}
