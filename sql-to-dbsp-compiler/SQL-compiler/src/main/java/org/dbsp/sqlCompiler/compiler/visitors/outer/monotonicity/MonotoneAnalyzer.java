package org.dbsp.sqlCompiler.compiler.visitors.outer.monotonicity;

import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.backend.dot.ToDotEdgesVisitor;
import org.dbsp.sqlCompiler.compiler.backend.dot.ToDot;
import org.dbsp.sqlCompiler.compiler.backend.dot.ToDotNodesVisitor;
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
    final DBSPCompiler compiler;

    @Override
    public String getName() {
        return "MonotoneAnalyzer";
    }

    /** Extension of ToDot which shows monotone types.
     * They are shown as M(type) on the edges. */
    static class MonotoneDot extends ToDotEdgesVisitor {
        final Monotonicity.MonotonicityInformation info;

        public MonotoneDot(DBSPCompiler compiler, IndentStream stream, int details,
                           Monotonicity.MonotonicityInformation info) {
            super(compiler, stream, details);
            this.info = info;
        }

        @Override
        public String getEdgeLabel(OutputPort source) {
            MonotoneExpression expr = this.info.get(source);
            if (expr != null) {
                return source.node().id + " " + Monotonicity.getBodyType(expr);
            } else {
                return source.node().id + " " + super.getEdgeLabel(source);
            }
        }
    }

    public MonotoneAnalyzer(DBSPCompiler compiler) {
        this.compiler = compiler;
    }

    @Override
    public DBSPCircuit apply(DBSPCircuit circuit) {
        final boolean debug = this.getDebugLevel() >= 1;
        final int details = this.getDebugLevel();

        // Insert noops between consecutive integrators
        Graph graph = new Graph(this.compiler);
        graph.apply(circuit);
        if (this.compiler.options.languageOptions.incrementalize) {
            SeparateIntegrators separate = new SeparateIntegrators(this.compiler, graph.getGraphs());
            circuit = separate.apply(circuit);
        }
        // Find relations which are append-only
        AppendOnly appendOnly = new AppendOnly(this.compiler);
        appendOnly.apply(circuit);
        // Identify uses of primary and foreign keys
        KeyPropagation keyPropagation = new KeyPropagation(this.compiler);
        keyPropagation.apply(circuit);

        if (debug)
            ToDot.dump(this.compiler, "original.png", details, "png", circuit);

        ExpandOperators expander = new ExpandOperators(
                this.compiler,
                appendOnly.appendOnly::contains,
                keyPropagation.joins::get);
        DBSPCircuit expanded = expander.apply(circuit);
        // ToDot.dump(this.reporter, "blowup.png", details, "png", expanded);

        Monotonicity monotonicity = new Monotonicity(this.compiler);
        expanded = monotonicity.apply(expanded);
        if (debug)
            ToDot.dump("expanded.png", "png", expanded,
                    stream -> new ToDotNodesVisitor(compiler, stream, details),
                    stream -> new MonotoneDot(compiler, stream, details, monotonicity.info));

        InsertLimiters limiters = new InsertLimiters(
                this.compiler, expanded, monotonicity.info, expander.expansion,
                keyPropagation.joins::get);
        // Notice that we apply the limiters to the original circuit, not to the expanded circuit!
        DBSPCircuit result = limiters.apply(circuit);
        if (debug)
            ToDot.dump(compiler, "limited.png", details, "png", result);

        CircuitTransform merger = new OptimizeWithGraph(this.compiler, g -> new MergeGC(this.compiler, g));
        result = merger.apply(result);
        graph.apply(result);
        CheckRetain check = new CheckRetain(this.compiler, graph.getGraphs());
        check.apply(result);
        return result;
    }

    @Override
    public String toString() {
        return "MonotoneAnalyzer";
    }
}
