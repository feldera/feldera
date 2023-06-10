package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.compiler.IErrorReporter;

/**
 * Optimizes projections in a circuit until covergence is achieved.
 */
public class OptimizeProjections extends RepeatVisitor {
    static CircuitVisitor createOnePass(IErrorReporter reporter) {
        PassesVisitor result = new PassesVisitor(reporter);
        FanoutVisitor fanout = new FanoutVisitor(reporter);
        result.add(fanout);
        result.add(new OptimizeProjectionVisitor(reporter,
                // Do not remove joins with fanout > 1
                op -> fanout.getFanout(op) == 1));
        result.add(new DeadCode(reporter, false));
        return result;
    };

    public OptimizeProjections(IErrorReporter reporter) {
        super(reporter, createOnePass(reporter));
    }
}
