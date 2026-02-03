package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDifferentiateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;

import java.util.List;

/** Applied late during compilation, moves Differential operators before their sources if possible
 * This is mostly useful when they get moved after Constants */
public class PushDifferentialsUp extends CircuitCloneWithGraphsVisitor {
    public PushDifferentialsUp(DBSPCompiler compiler, CircuitGraphs graphs) {
        super(compiler, graphs, false);
    }

    @Override
    public void postorder(DBSPDifferentiateOperator operator) {
        OutputPort source = this.mapped(operator.input());
        int inputFanout = this.getGraph().getFanout(operator.input().node());
        if (inputFanout != 1) {
            super.postorder(operator);
            return;
        }
        if (source.node().is(DBSPMapIndexOperator.class) ||
            source.node().is(DBSPMapOperator.class)) {
            DBSPDifferentiateOperator diff = new DBSPDifferentiateOperator(
                    operator.getRelNode(), source.node().inputs.get(0));
            this.addOperator(diff);
            DBSPSimpleOperator sourceClone = source.node().withInputs(List.of(diff.outputPort()), false)
                    .to(DBSPSimpleOperator.class);
            this.map(operator, sourceClone);
        } else {
            super.postorder(operator);
        }
    }
}
