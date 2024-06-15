package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.compiler.IErrorReporter;
import org.dbsp.sqlCompiler.compiler.backend.ToDotVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.outer.expansion.ExpandOperators;
import org.dbsp.util.IWritesLogs;

/** Implements a dataflow analysis for detecting values that change monotonically,
 * and inserts nodes that prune the internal circuit state where possible. */
public class MonotoneAnalyzer implements CircuitTransform, IWritesLogs {
    final IErrorReporter reporter;

    public MonotoneAnalyzer(IErrorReporter reporter) {
        this.reporter = reporter;
    }

    @Override
    public DBSPCircuit apply(DBSPCircuit circuit) {
        final boolean debug = this.getDebugLevel() >= 2;
        final boolean details = this.getDebugLevel() >= 3;

        if (debug)
            ToDotVisitor.toDot(this.reporter, "original.png", details, "png", circuit);
        ExpandOperators expander = new ExpandOperators(this.reporter, 1);
        Repeat repeat = new Repeat(this.reporter, expander);
        DBSPCircuit expanded = repeat.apply(circuit);
        if (debug)
            ToDotVisitor.toDot(reporter, "expanded.png", false, "png", expanded);

        Monotonicity monotonicity = new Monotonicity(this.reporter);
        expanded = monotonicity.apply(expanded);

        InsertLimiters limiters = new InsertLimiters(
                this.reporter, expanded, monotonicity.monotonicity, expander.expansion);
        // Notice that we apply the limiters to the original circuit, not to the expanded circuit!
        DBSPCircuit result = limiters.apply(circuit);

        if (debug)
            ToDotVisitor.toDot(reporter, "limited.png", details, "png", result);
        return result;
    }

    @Override
    public String toString() {
        return "MonotoneAnalyzer";
    }
}
