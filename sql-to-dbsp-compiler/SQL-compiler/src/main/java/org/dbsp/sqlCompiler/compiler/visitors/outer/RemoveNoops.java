package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.operator.DBSPNoopOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.GCOperator;
import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.compiler.IErrorReporter;
import org.dbsp.util.Linq;
import org.dbsp.util.graph.Port;

import java.util.List;

/** Removes noops that are not followed by a GC operator */
public class RemoveNoops extends CircuitCloneWithGraphsVisitor {
    public RemoveNoops(IErrorReporter reporter, CircuitGraphs graphs) {
        super(reporter, graphs,  false);
    }

    @Override
    public void postorder(DBSPNoopOperator operator) {
        List<Port<DBSPOperator>> destinations = this.getGraph().getSuccessors(operator);
        boolean keep = Linq.any(destinations, d -> d.node().is(GCOperator.class));
        if (keep) {
            super.postorder(operator);
        } else {
            OutputPort input = this.mapped(operator.input());
            this.map(operator.outputPort(), input, false);
        }
    }
}
