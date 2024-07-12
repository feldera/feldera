package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.compiler.IErrorReporter;
import org.dbsp.sqlCompiler.compiler.backend.ToDotVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.inner.monotone.MonotoneExpression;
import org.dbsp.sqlCompiler.compiler.visitors.outer.expansion.ExpandOperators;
import org.dbsp.util.IWritesLogs;
import org.dbsp.util.IndentStream;

/** Implements a dataflow analysis for detecting values that change monotonically,
 * and inserts nodes that prune the internal circuit state where possible. */
public class MonotoneAnalyzer implements CircuitTransform, IWritesLogs {
    final IErrorReporter reporter;

    /** Extension of ToDot which shows monotone types.
     * They are shown as M(type) on the edges. */
    static class MonotoneDot extends ToDotVisitor {
        final Monotonicity.MonotonicityInformation info;

        public MonotoneDot(IErrorReporter reporter, IndentStream stream, int details,
                           Monotonicity.MonotonicityInformation info) {
            super(reporter, stream, details);
            this.info = info;
        }

        @Override
        public String getEdgeLabel(DBSPOperator source) {
            String result = source.getOutputRowType().toString();
            MonotoneExpression expr = info.get(source);
            if (expr != null) {
                result = source.getIdString() + " " + Monotonicity.getBodyType(expr);
            }
            return result;
        }
    }

    public MonotoneAnalyzer(IErrorReporter reporter) {
        this.reporter = reporter;
    }

    @Override
    public DBSPCircuit apply(DBSPCircuit circuit) {
        final boolean debug = this.getDebugLevel() >= 1;
        final int details = this.getDebugLevel();

        if (debug)
            ToDotVisitor.toDot(this.reporter, "original.png", details, "png", circuit);
        ExpandOperators expander = new ExpandOperators(this.reporter);
        DBSPCircuit expanded = expander.apply(circuit);

        Monotonicity monotonicity = new Monotonicity(this.reporter);
        expanded = monotonicity.apply(expanded);
        if (debug) {
            MonotoneDot.toDot("expanded.png", details, "png", expanded,
                    stream -> new MonotoneDot(reporter, stream, details, monotonicity.info));
        }

        InsertLimiters limiters = new InsertLimiters(
                this.reporter, expanded, monotonicity.info, expander.expansion);
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
