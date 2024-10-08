package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.compiler.IErrorReporter;

public class RecursiveComponents extends CircuitCloneVisitor {
    final CircuitGraph graph;

    public RecursiveComponents(IErrorReporter reporter, CircuitGraph graph) {
        super(reporter, false);
        this.graph = graph;
    }
}
