package org.dbsp.sqlCompiler.compiler.visitors.outer.monotonicity;

import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.IInputOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.backend.dot.ToDotEdgesVisitor;
import org.dbsp.sqlCompiler.compiler.backend.dot.ToDot;
import org.dbsp.sqlCompiler.compiler.backend.dot.ToDotNodesVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.inner.IRTransform;
import org.dbsp.sqlCompiler.compiler.visitors.monotone.MonotoneExpression;
import org.dbsp.sqlCompiler.compiler.visitors.outer.AppendOnly;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitGraph;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitRewriter;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitTransform;
import org.dbsp.sqlCompiler.compiler.visitors.outer.Graph;
import org.dbsp.sqlCompiler.compiler.visitors.outer.OptimizeWithGraph;
import org.dbsp.sqlCompiler.compiler.visitors.outer.expansion.ExpandOperators;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.util.IWritesLogs;
import org.dbsp.util.IndentStream;
import org.dbsp.util.graph.Port;

import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;
import java.util.function.Function;

/** Implements a dataflow analysis for detecting values that change monotonically,
 * and inserts nodes that prune the internal circuit state where possible.
 * It also hooks up all operators that produce lateness errors to the error view. */
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

    /** Compute and return the set of operators that receive an input directly or
     * indirectly from the ERROR_TABLE_NAME (which only feeds the error view at this point). */
    private Set<DBSPOperator> reachableFromError(DBSPCircuit circuit, CircuitGraph graph) {
        LinkedList<DBSPOperator> queue = new LinkedList<>();
        IInputOperator errorTable = circuit.getInput(DBSPCompiler.ERROR_TABLE_NAME);
        if (errorTable != null)
            queue.add(errorTable.asOperator());

        Set<DBSPOperator> result = new HashSet<>();
        while (!queue.isEmpty()) {
            DBSPOperator operator = queue.removeFirst();
            if (result.contains(operator))
                continue;
            result.add(operator);
            for (Port<DBSPOperator> successor : graph.getSuccessors(operator)) {
                queue.add(successor.node());
            }
        }

        return result;
    }

    @Override
    public DBSPCircuit apply(DBSPCircuit circuit) {
        final boolean debug = this.getDebugLevel() >= 1;
        final int details = this.getDebugLevel();

        IRTransform transform = new IRTransform() {
            @Override
            public void setOperatorContext(DBSPOperator operator) {}

            @Override
            public IDBSPInnerNode apply(IDBSPInnerNode e) {
                if (e.isExpression())
                    return e.to(DBSPExpression.class).ensureTree(compiler);
                return e;
            }

            @Override
            public String toString() {
                return "EnsuresTree";
            }
        };
        CircuitRewriter toTree = new CircuitRewriter(this.compiler, transform, false);
        circuit = toTree.apply(circuit);

        // Insert noops between consecutive integrators
        Graph graph = new Graph(this.compiler);
        graph.apply(circuit);
        if (this.compiler.options.languageOptions.incrementalize) {
            SeparateIntegrators separate = new SeparateIntegrators(this.compiler, graph.getGraphs());
            circuit = separate.apply(circuit);
            // Recompute graph
            graph.apply(circuit);
        }

        // Find relations which are append-only
        AppendOnly appendOnly = new AppendOnly(this.compiler);
        appendOnly.apply(circuit);
        // Identify uses of primary and foreign keys
        KeyPropagation keyPropagation = new KeyPropagation(this.compiler);
        keyPropagation.apply(circuit);

        CircuitGraph outerGraph = graph.getGraphs().getGraph(circuit);
        Set<DBSPOperator> reachableFromError = this.reachableFromError(circuit, outerGraph);
        // Sort the nodes in the internal array so that the nodes that depend on
        // the error view come after all other nodes.
        Function<Boolean, Integer> bi = b -> b ? 1 : -1;
        circuit.sortOperators(Comparator.comparing(o -> bi.apply(reachableFromError.contains(o))));

        if (debug)
            ToDot.dump(this.compiler, "original.png", details, "png", circuit);

        ExpandOperators expander = new ExpandOperators(
                this.compiler,
                appendOnly.appendOnly::contains,
                keyPropagation.joins::get);
        DBSPCircuit expanded = expander.apply(circuit);

        Monotonicity monotonicity = new Monotonicity(this.compiler);
        expanded = monotonicity.apply(expanded);
        if (debug)
            ToDot.customDump("expanded.png", "png", expanded,
                    stream -> new ToDotNodesVisitor(compiler, stream, details),
                    stream -> new MonotoneDot(compiler, stream, details, monotonicity.info));

        InsertLimiters limiters = new InsertLimiters(
                this.compiler, expanded, monotonicity.info, expander.expansion,
                keyPropagation.joins::get, reachableFromError);

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
