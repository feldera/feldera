package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.operator.DBSPNoopOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.GCOperator;
import org.dbsp.sqlCompiler.compiler.IErrorReporter;
import org.dbsp.util.Linq;

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
        List<DBSPOperator> destinations = this.graph.getDestinations(operator);
        boolean keep = Linq.any(destinations, d -> d.is(GCOperator.class));
        if (keep) {
            super.postorder(operator);
        } else {
            DBSPOperator input = this.mapped(operator.input());
            this.map(operator, input, false);
        }
    }
}
