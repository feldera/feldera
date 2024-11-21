package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.operator.DBSPFilterOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinFilterMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;

/** Combine a Join followed by a filter into a JoinFilterMap. */
public class FilterJoinVisitor extends CircuitCloneWithGraphsVisitor {
    public FilterJoinVisitor(DBSPCompiler compiler, CircuitGraphs graphs) {
        super(compiler, graphs, false);
    }

    @Override
    public void postorder(DBSPFilterOperator operator) {
        OutputPort source = this.mapped(operator.input());
        if (source.node().is(DBSPJoinOperator.class) &&
                (this.getGraph().getFanout(operator.input().node()) == 1)) {
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
