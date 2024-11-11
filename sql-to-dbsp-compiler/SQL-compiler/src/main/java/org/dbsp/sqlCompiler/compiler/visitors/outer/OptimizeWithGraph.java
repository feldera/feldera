package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.compiler.IErrorReporter;

import java.util.function.Function;

/** Applies another optimization visitor that needs the graph structure
 * repeatedly, until convergence. */
public class OptimizeWithGraph extends Repeat {
    static CircuitTransform createOnePass(
            IErrorReporter reporter,
            Function<CircuitGraph, CircuitTransform> optimizerFactory) {
        Passes result = new Passes(reporter);
        Graph graph = new Graph(reporter);
        result.add(graph);
        CircuitTransform optimizer = optimizerFactory.apply(graph.graph);
        result.add(optimizer);
        result.add(new DeadCode(reporter, true, false));
        return result;
    }

    public OptimizeWithGraph(IErrorReporter reporter,
                             Function<CircuitGraph, CircuitTransform> optimizerFactory) {
        super(reporter, createOnePass(reporter, optimizerFactory));
    }
}
