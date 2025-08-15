package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.circuit.ICircuit;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDeltaOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPInputMapWithWaterlineOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPNestedOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperatorWithError;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPViewDeclarationOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPViewOperator;
import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.ir.IDBSPOuterNode;
import org.dbsp.util.Logger;
import org.dbsp.util.Utilities;

import java.util.HashMap;
import java.util.Map;

/** Build a {@link CircuitGraphs} structure from a {@link DBSPCircuit} */
public class Graph extends CircuitVisitor {
    public final CircuitGraphs graphs = new CircuitGraphs();

    /** Map each source view operator to its parent */
    final Map<DBSPViewDeclarationOperator, ICircuit> delayed = new HashMap<>();

    public Graph(DBSPCompiler compiler) {
        super(compiler);
    }

    @Override
    public Token startVisit(IDBSPOuterNode node) {
        this.graphs.clear();
        return super.startVisit(node);
    }

    CircuitGraph currentGraph() {
        return this.graphs.getGraph(this.getParent());
    }

    public CircuitGraphs getGraphs() {
        return this.graphs;
    }

    void addOperator(DBSPOperator operator) {
        CircuitGraph graph = this.currentGraph();
        graph.addNode(operator);
        int index = 0;
        if (operator.is(DBSPDeltaOperator.class))
            // Pretend that deltas have no inputs:
            // these cross between graph layers.
            return;
        for (OutputPort source: operator.inputs) {
            graph.addEdge(source.node(), operator, index++);
        }
    }

    @Override
    public void postorder(DBSPSimpleOperator operator) {
        this.addOperator(operator);
    }

    @Override
    public void postorder(DBSPInputMapWithWaterlineOperator operator) {
        this.addOperator(operator);
    }

    @Override
    public void postorder(DBSPOperatorWithError operator) {
        this.addOperator(operator);
    }

    @Override
    public VisitDecision preorder(DBSPCircuit circuit) {
        this.graphs.newCircuit(circuit);
        return super.preorder(circuit);
    }

    @Override
    public VisitDecision preorder(DBSPNestedOperator circuit) {
        this.graphs.newCircuit(circuit);
        this.addOperator(circuit);
        return super.preorder(circuit);
    }

    void postCircuit(ICircuit circuit) {
        CircuitGraph graph = this.graphs.getGraph(circuit);
        for (var opAndParent: this.delayed.entrySet()) {
            DBSPViewOperator view = opAndParent.getKey().getCorrespondingView(opAndParent.getValue());
            if (view != null)
                // View can be missing if it is not defined
                graph.addEdge(view, opAndParent.getKey(), 0);
        }
        this.delayed.clear();
    }

    @Override
    public void postorder(DBSPNestedOperator circuit) {
        this.postCircuit(circuit);
        super.postorder(circuit);
    }

    @Override
    public void postorder(DBSPCircuit circuit) {
        this.postCircuit(circuit);
        super.postorder(circuit);
        Logger.INSTANCE.belowLevel(this, 1)
                .appendSupplier(this.graphs::toString)
                .newline();
    }

    @Override
    public void postorder(DBSPViewDeclarationOperator operator) {
        CircuitGraph graph = this.currentGraph();
        graph.addNode(operator);
        // Process the input edges at the end
        Utilities.putNew(this.delayed, operator, this.getParent());
    }
}
