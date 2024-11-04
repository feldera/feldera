package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSourceViewDeclarationOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPViewOperator;
import org.dbsp.sqlCompiler.circuit.operator.OperatorPort;
import org.dbsp.sqlCompiler.compiler.IErrorReporter;
import org.dbsp.sqlCompiler.ir.IDBSPOuterNode;

import java.util.ArrayList;
import java.util.List;

/** Build a graph data structure from a Circuit */
public class Graph extends CircuitVisitor {
    public final CircuitGraph graph = new CircuitGraph();
    final List<DBSPSourceViewDeclarationOperator> delayed = new ArrayList<>();

    public Graph(IErrorReporter errorReporter) {
        super(errorReporter);
    }

    @Override
    public void startVisit(IDBSPOuterNode node) {
        super.startVisit(node);
        this.graph.clear();
    }

    @Override
    public void postorder(DBSPSimpleOperator operator) {
        this.graph.addNode(operator);
        int index = 0;
        for (OperatorPort source: operator.inputs) {
            this.graph.addEdge(source.node(), operator, index++);
        }
    }

    @Override
    public void endVisit() {
        assert this.circuit != null;
        for (DBSPSourceViewDeclarationOperator operator: this.delayed) {
            DBSPViewOperator view = operator.getCorrespondingView(this.circuit.circuit);
            this.graph.addEdge(view, operator, 0);
        }
        super.endVisit();
    }

    @Override
    public void postorder(DBSPSourceViewDeclarationOperator operator) {
        this.graph.addNode(operator);
        // Add the input edges at the end.
        this.delayed.add(operator);
    }
}
