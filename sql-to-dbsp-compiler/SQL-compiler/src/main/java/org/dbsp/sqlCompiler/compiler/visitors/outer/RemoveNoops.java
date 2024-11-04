package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.operator.DBSPNoopOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.circuit.operator.GCOperator;
import org.dbsp.sqlCompiler.circuit.operator.OperatorPort;
import org.dbsp.sqlCompiler.compiler.IErrorReporter;
import org.dbsp.util.Linq;
import org.dbsp.util.graph.Port;

import java.util.List;

/** Removes noops that are not followed by a GC operator */
public class RemoveNoops extends CircuitCloneVisitor {
    private final CircuitGraph graph;

    public RemoveNoops(IErrorReporter reporter, CircuitGraph graph) {
        super(reporter, false);
        this.graph = graph;
    }

    @Override
    public void postorder(DBSPNoopOperator operator) {
        List<Port<DBSPOperator>> destinations = this.graph.getSuccessors(operator);
        boolean keep = Linq.any(destinations, d -> d.node().is(GCOperator.class));
        if (keep) {
            super.postorder(operator);
        } else {
            OperatorPort input = this.mapped(operator.input());
            this.map(operator.getOutput(), input, false);
        }
    }
}
