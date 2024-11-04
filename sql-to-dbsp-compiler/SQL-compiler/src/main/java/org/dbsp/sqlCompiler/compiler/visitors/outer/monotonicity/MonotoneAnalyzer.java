package org.dbsp.sqlCompiler.compiler.visitors.outer.monotonicity;

import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.circuit.operator.OperatorPort;
import org.dbsp.sqlCompiler.compiler.IErrorReporter;
import org.dbsp.sqlCompiler.compiler.backend.ToDotVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.inner.monotone.MonotoneExpression;
import org.dbsp.sqlCompiler.compiler.visitors.outer.AppendOnly;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitTransform;
import org.dbsp.sqlCompiler.compiler.visitors.outer.Graph;
import org.dbsp.sqlCompiler.compiler.visitors.outer.OptimizeWithGraph;
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
        public String getEdgeLabel(OperatorPort source) {
            MonotoneExpression expr = this.info.get(source.simpleNode());
            if (expr != null) {
                return source.node().id + " " + Monotonicity.getBodyType(expr);
            } else {
                return source.node().id + " " + super.getEdgeLabel(source);
            }
        }
    }

    public MonotoneAnalyzer(IErrorReporter reporter) {
        this.reporter = reporter;
    }

    @Override
    public DBSPCircuit apply(DBSPCircuit circuit) {
        final boolean debug = this.getDebugLevel() >= 1;
        final int details = this.getDebugLevel();

        // Insert noops between consecutive integrators
        Graph graph = new Graph(this.reporter);
        graph.apply(circuit);
        SeparateIntegrators separate = new SeparateIntegrators(this.reporter, graph.graph);
        circuit = separate.apply(circuit);
        // Find relations which are append-only
        AppendOnly appendOnly = new AppendOnly(this.reporter);
        appendOnly.apply(circuit);
        // Identify uses of primary and foreign keys
        KeyPropagation keyPropagation = new KeyPropagation(this.reporter);
        keyPropagation.apply(circuit);

        if (debug)
            ToDotVisitor.toDot(this.reporter, "original.png", details, "png", circuit);

        ExpandOperators expander = new ExpandOperators(
                this.reporter,
                appendOnly.appendOnly::contains,
                keyPropagation.joins::get);
        DBSPCircuit expanded = expander.apply(circuit);

        Monotonicity monotonicity = new Monotonicity(this.reporter);
        expanded = monotonicity.apply(expanded);
        if (debug)
            MonotoneDot.toDot("expanded.png", "png", expanded,
                    stream -> new MonotoneDot(reporter, stream, details, monotonicity.info));

        InsertLimiters limiters = new InsertLimiters(
                this.reporter, expanded, monotonicity.info, expander.expansion,
                keyPropagation.joins::get);
        // Notice that we apply the limiters to the original circuit, not to the expanded circuit!
        DBSPCircuit result = limiters.apply(circuit);
        if (debug)
            ToDotVisitor.toDot(reporter, "limited.png", details, "png", result);

        CircuitTransform merger = new OptimizeWithGraph(this.reporter, g -> new MergeGC(this.reporter, g));
        result = merger.apply(result);

        graph.apply(result);
        CheckRetain check = new CheckRetain(this.reporter, graph.graph);
        check.apply(result);
        return result;
    }

    @Override
    public String toString() {
        return "MonotoneAnalyzer";
    }
}
