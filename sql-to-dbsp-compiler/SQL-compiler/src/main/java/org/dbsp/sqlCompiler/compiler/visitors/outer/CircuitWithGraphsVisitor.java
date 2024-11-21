package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.compiler.DBSPCompiler;

/** A CircuitVisitor which contains the {@link CircuitGraphs} */
public class CircuitWithGraphsVisitor extends CircuitVisitor {
    protected final CircuitGraphs graphs;

    protected CircuitGraph getGraph() {
        return this.graphs.getGraph(this.getParent());
    }

    protected CircuitWithGraphsVisitor(DBSPCompiler compiler, CircuitGraphs graphs) {
        super(compiler);
        this.graphs = graphs;
    }
}
