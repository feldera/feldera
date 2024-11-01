package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDeltaOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.compiler.IErrorReporter;
import org.dbsp.sqlCompiler.ir.IDBSPOuterNode;
import org.dbsp.util.Linq;
import org.dbsp.util.Logger;
import org.dbsp.util.Utilities;
import org.dbsp.util.graph.SCC;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

public class RecursiveComponents extends CircuitCloneVisitor {
    final CircuitGraph graph;
    @Nullable
    SCC<DBSPOperator> scc = null;

    public RecursiveComponents(IErrorReporter reporter, CircuitGraph graph) {
        super(reporter, false);
        this.graph = graph;
    }

    @Override
    public void replace(DBSPOperator operator) {
        // Check if operator is in a larger connected component
        assert this.scc != null;
        int myComponent = Utilities.getExists(this.scc.componentId, operator);
        List<DBSPOperator> component = Utilities.getExists(this.scc.component, myComponent);
        if (component.size() == 1) {
            super.replace(operator);
            return;
        }
        // Check if any inputs of the operator are in a different component
        // If they are, insert a delta operator in front.
        List<DBSPOperator> sources = new ArrayList<>();
        for (DBSPOperator input: operator.inputs) {
            DBSPOperator source = this.mapped(input);
            int sourceComp = Utilities.getExists(this.scc.componentId, input);
            if (sourceComp != myComponent) {
                DBSPDeltaOperator delta = new DBSPDeltaOperator(operator.getNode(), source);
                this.addOperator(delta);
                sources.add(delta);
            } else {
                sources.add(source);
            }
        }

        if (!Linq.same(sources, operator.inputs)) {
            Logger.INSTANCE.belowLevel(this, 2)
                    .append(this.toString())
                    .append(" replacing inputs of ")
                    .increase()
                    .append(operator.toString())
                    .append(":")
                    .join(", ", Linq.map(operator.inputs, DBSPOperator::toString))
                    .newline()
                    .append("with:")
                    .join(", ", Linq.map(sources, DBSPOperator::toString))
                    .newline()
                    .decrease();
        }
        DBSPOperator result = operator.withInputs(sources, this.force);
        result.setDerivedFrom(operator.id);
        this.map(operator, result);
    }

    @Override
    public void startVisit(IDBSPOuterNode circuit) {
        super.startVisit(circuit);
        DBSPCircuit c = circuit.to(DBSPCircuit.class);
        this.scc = new SCC<>(c, this.graph);
    }
}
