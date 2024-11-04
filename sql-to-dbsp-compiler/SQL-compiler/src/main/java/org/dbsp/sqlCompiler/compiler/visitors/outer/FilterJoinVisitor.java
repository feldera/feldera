package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.operator.DBSPFilterOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinFilterMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.circuit.operator.OperatorPort;
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
        OperatorPort source = this.mapped(operator.input());
        if (source.node().is(DBSPJoinOperator.class) &&
                (this.graph.getFanout(operator.input().node()) == 1)) {
            DBSPJoinOperator join = source.node().to(DBSPJoinOperator.class);
            DBSPSimpleOperator result =
                    new DBSPJoinFilterMapOperator(join.getNode(), source.getOutputZSetType(),
                            join.getFunction(), operator.getFunction(), null,
                            join.isMultiset, join.inputs.get(0), join.inputs.get(1))
                            .copyAnnotations(operator);
            this.map(operator, result);
            return;
        }
        super.postorder(operator);
    }
}
