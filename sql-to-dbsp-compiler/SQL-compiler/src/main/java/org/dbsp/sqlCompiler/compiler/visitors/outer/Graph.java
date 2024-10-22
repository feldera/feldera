package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.compiler.IErrorReporter;
import org.dbsp.sqlCompiler.ir.IDBSPOuterNode;

/** Build a graph data structure from a Circuit */
public class Graph extends CircuitVisitor {
    public final CircuitGraph graph = new CircuitGraph();

    public Graph(IErrorReporter errorReporter) {
        super(errorReporter);
    }

    @Override
    public void startVisit(IDBSPOuterNode node) {
        this.graph.clear();
    }

    @Override
    public void postorder(DBSPOperator operator) {
        this.graph.addNode(operator);
        int index = 0;
        for (DBSPOperator source: operator.inputs) {
            this.graph.addEdge(source, operator, index++);
        }
    }
}
