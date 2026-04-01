package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.operator.DBSPFilterOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinFilterMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPLeftJoinFilterMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPLeftJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStarJoinFilterMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStarJoinOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteRelNode;

/** Combine a {@link DBSPJoinOperator} followed by a {@link DBSPFilterOperator} into a
 * {@link DBSPJoinFilterMapOperator}.  Similar expansion for {@link DBSPLeftJoinOperator}. */
public class FilterJoinVisitor extends CircuitCloneWithGraphsVisitor {
    public FilterJoinVisitor(DBSPCompiler compiler, CircuitGraphs graphs) {
        super(compiler, graphs, false);
    }

    @Override
    public void postorder(DBSPFilterOperator operator) {
        OutputPort source = this.mapped(operator.input());
        int inputFanout = this.getGraph().getFanout(operator.input().node());
        if (source.node().is(DBSPJoinOperator.class) && (inputFanout == 1)) {
            DBSPJoinOperator join = source.node().to(DBSPJoinOperator.class);
            CalciteRelNode node = join.getRelNode().after(operator.getRelNode());
            DBSPSimpleOperator result =
                    new DBSPJoinFilterMapOperator(node, source.getOutputZSetType(),
                            join.getFunction(), operator.getClosureFunction(), null,
                            join.isMultiset, join.inputs.get(0), join.inputs.get(1), join.balanced)
                            .copyAnnotations(operator);
            this.map(operator, result);
            return;
        } else if (source.node().is(DBSPLeftJoinOperator.class) && (inputFanout == 1)) {
            DBSPLeftJoinOperator join = source.node().to(DBSPLeftJoinOperator.class);
            CalciteRelNode node = join.getRelNode().after(operator.getRelNode());
            DBSPSimpleOperator result =
                    new DBSPLeftJoinFilterMapOperator(node, source.getOutputZSetType(),
                            join.getFunction(), operator.getClosureFunction(), null,
                            join.isMultiset, join.inputs.get(0), join.inputs.get(1), join.balanced)
                            .copyAnnotations(operator);
            this.map(operator, result);
            return;
        } else if (source.node().is(DBSPStarJoinOperator.class) && (inputFanout == 1)) {
            DBSPStarJoinOperator join = source.node().to(DBSPStarJoinOperator.class);
            CalciteRelNode node = join.getRelNode().after(operator.getRelNode());
            DBSPSimpleOperator result =
                    new DBSPStarJoinFilterMapOperator(node, source.getOutputZSetType(),
                            join.getClosureFunction(), operator.getClosureFunction(), null,
                            join.isMultiset, join.inputs)
                            .copyAnnotations(operator);
            this.map(operator, result);
            return;
        }
        super.postorder(operator);
    }
}
