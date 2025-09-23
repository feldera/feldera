package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.compiler.DBSPCompiler;

import java.util.function.Function;

/** Applies another optimization visitor that needs the graph structure
 * repeatedly, until convergence. */
public class OptimizeWithGraph extends Repeat {
    static CircuitTransform createOnePass(
            DBSPCompiler compiler,
            Function<CircuitGraphs, CircuitTransform> optimizerFactory) {
        Graph graph = new Graph(compiler);
        CircuitTransform optimizer = optimizerFactory.apply(graph.getGraphs());
        Passes result = new Passes(optimizer.getName(), compiler);
        result.add(graph);
        result.add(optimizer);
        result.add(new DeadCode(compiler, true));
        return result;
    }

    public OptimizeWithGraph(DBSPCompiler compiler,
                             Function<CircuitGraphs, CircuitTransform> optimizerFactory, int maxRepeats) {
        super(compiler, createOnePass(compiler, optimizerFactory), maxRepeats);
    }

    public OptimizeWithGraph(DBSPCompiler compiler,
                             Function<CircuitGraphs, CircuitTransform> optimizerFactory) {
        this(compiler, optimizerFactory, Integer.MAX_VALUE);
    }
}
