package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.compiler.IErrorReporter;

/** A CircuitVisitor which contains the {@link CircuitGraphs} */
public class CircuitWithGraphsVisitor extends CircuitVisitor {
    protected final CircuitGraphs graphs;

    protected CircuitGraph getGraph() {
        return this.graphs.getGraph(this.getParent());
    }

    protected CircuitWithGraphsVisitor(IErrorReporter reporter, CircuitGraphs graphs) {
        super(reporter);
        this.graphs = graphs;
    }
}
