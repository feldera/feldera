package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.compiler.IErrorReporter;

import java.util.function.Function;

/** Applies another optimization visitor that needs the graph structure
 * repeatedly, until convergence. */
public class OptimizeWithGraph extends Repeat {
    static CircuitTransform createOnePass(
            IErrorReporter reporter,
            Function<CircuitGraphs, CircuitTransform> optimizerFactory) {
        Graph graph = new Graph(reporter);
        CircuitTransform optimizer = optimizerFactory.apply(graph.getGraphs());
        Passes result = new Passes(optimizer.getName(), reporter);
        result.add(graph);
        result.add(optimizer);
        result.add(new DeadCode(reporter, true, false));
        return result;
    }

    public OptimizeWithGraph(IErrorReporter reporter,
                             Function<CircuitGraphs, CircuitTransform> optimizerFactory) {
        super(reporter, createOnePass(reporter, optimizerFactory));
    }
}
