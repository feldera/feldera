package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.ICircuit;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSourceViewDeclarationOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPViewOperator;
import org.dbsp.sqlCompiler.circuit.operator.OperatorPort;
import org.dbsp.sqlCompiler.compiler.IErrorReporter;
import org.dbsp.sqlCompiler.ir.IDBSPOuterNode;
import org.dbsp.util.Utilities;

import java.util.HashMap;
import java.util.Map;

/** Build a graph data structure from a Circuit */
public class Graph extends CircuitVisitor {
    public final CircuitGraph graph = new CircuitGraph();
    /** Map each source view operator to its parent */
    final Map<DBSPSourceViewDeclarationOperator, ICircuit> delayed = new HashMap<>();

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
        for (var opAndParent: this.delayed.entrySet()) {
            DBSPViewOperator view = opAndParent.getKey().getCorrespondingView(opAndParent.getValue());
            this.graph.addEdge(view, opAndParent.getKey(), 0);
        }
        super.endVisit();
    }

    @Override
    public void postorder(DBSPSourceViewDeclarationOperator operator) {
        this.graph.addNode(operator);
        // Process the input edges at the end
        Utilities.putNew(this.delayed, operator, this.getParent());
    }
}
