package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.operator.DBSPViewOperator;
import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;

/** Remove DBSPViewOperator operators. */
public class RemoveViewOperators extends CircuitCloneVisitor {
    /** If false remove only non-recursive views */
    final boolean all;

    public RemoveViewOperators(DBSPCompiler compiler, boolean all) {
        super(compiler, false);
        this.all = all;
    }

    @Override
    public void postorder(DBSPViewOperator operator) {
        if (!this.all && operator.metadata.recursive) {
            // preserve the view.
            // Views have to be preserved if we build the CircuitGraph.
            super.postorder(operator);
            return;
        }
        OutputPort input = this.mapped(operator.input());
        this.map(operator.outputPort(), input, false);
    }
}
