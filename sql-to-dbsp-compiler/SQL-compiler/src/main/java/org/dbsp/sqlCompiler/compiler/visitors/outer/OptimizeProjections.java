package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.compiler.IErrorReporter;

/** Optimizes projections in a circuit until convergence is achieved. */
public class OptimizeProjections extends Repeat {
    static CircuitTransform createOnePass(IErrorReporter reporter) {
        Passes result = new Passes(reporter);
        FanoutVisitor fanout = new FanoutVisitor(reporter);
        result.add(fanout);
        result.add(new OptimizeProjectionVisitor(reporter,
                // Do not remove joins with fanout > 1
                op -> fanout.getFanout(op) == 1));
        result.add(new DeadCode(reporter, true, false));
        return result;
    }

    public OptimizeProjections(IErrorReporter reporter) {
        super(reporter, createOnePass(reporter));
    }
}
