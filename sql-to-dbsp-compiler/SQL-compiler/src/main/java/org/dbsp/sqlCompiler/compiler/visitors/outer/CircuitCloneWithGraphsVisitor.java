package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.compiler.DBSPCompiler;

/** Class extending {@link CircuitCloneVisitor} that provides access to the {@link CircuitGraphs} */
public abstract class CircuitCloneWithGraphsVisitor extends CircuitCloneVisitor {
    protected final CircuitGraphs graphs;

    protected CircuitGraph getGraph() {
        return this.graphs.getGraph(this.getParent());
    }

    protected CircuitCloneWithGraphsVisitor(DBSPCompiler compiler, CircuitGraphs graphs, boolean force) {
        super(compiler, force);
        this.graphs = graphs;
    }
}
