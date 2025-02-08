package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.util.Maybe;

/** Map \circ MapIndex = Map */
public class ComposeIndexWithMap extends CircuitCloneWithGraphsVisitor {
    public ComposeIndexWithMap(DBSPCompiler compiler, CircuitGraphs graphs) {
        super(compiler, graphs, false);
    }

    @Override
    public void postorder(DBSPMapOperator operator) {
        int inputFanout = this.getGraph().getFanout(operator.input().node());
        OutputPort source = this.mapped(operator.input());
        if (inputFanout == 1 && source.node().is(DBSPMapIndexOperator.class)) {
            DBSPMapIndexOperator mx = source.node().to(DBSPMapIndexOperator.class);
            DBSPClosureExpression expression = mx.getClosureFunction();
            DBSPClosureExpression newFunction = operator.getClosureFunction()
                    .applyAfterMapIndex(this.compiler(), expression, Maybe.MAYBE);
            DBSPSimpleOperator result = new DBSPMapOperator(
                    operator.getNode(), newFunction, operator.getOutputZSetType(), mx.input());
            this.map(operator, result);
            return;
        }
        super.postorder(operator);
    }
}
