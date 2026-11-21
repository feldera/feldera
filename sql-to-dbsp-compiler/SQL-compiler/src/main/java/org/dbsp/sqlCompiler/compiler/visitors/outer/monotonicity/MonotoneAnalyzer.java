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
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitGraph;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitGraphs;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitRewriter;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitTransform;
import org.dbsp.sqlCompiler.compiler.visitors.outer.Graph;
import org.dbsp.sqlCompiler.compiler.visitors.outer.expansion.DeltaExpandOperators;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.util.IWritesLogs;
import org.dbsp.util.IndentStream;
import org.dbsp.util.graph.Port;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;
import java.util.function.UnaryOperator;

/** Implements a dataflow analysis for detecting values that change monotonically,
 * and inserts nodes that prune the internal circuit state where possible.
 * It also hooks up all operators that produce lateness errors to the error view. */
public class MonotoneAnalyzer implements CircuitTransform, IWritesLogs {
    final DBSPCompiler compiler;
    private final UnaryOperator<CircuitTransform> select;

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
                return super.getEdgeLabel(source);
            }
        }
    }

    public MonotoneAnalyzer(DBSPCompiler compiler) {
        this(compiler, UnaryOperator.identity());
    }

    public MonotoneAnalyzer(DBSPCompiler compiler, UnaryOperator<CircuitTransform> select) {
        this.select = select;
        this.compiler = compiler;
    }

    protected CircuitTransform separateIntegrators(CircuitGraphs graphs) {
        return this.select.apply(new SeparateIntegrators(this.compiler, graphs));
    }

    /** Compute and return the set of operators that receive an input directly or
     * indirectly from the ERROR_TABLE_NAME (which only feeds the error view at this point). */
    protected Set<DBSPOperator> reachableFromError(DBSPCircuit circuit, CircuitGraph graph) {
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

    protected CircuitTransform ensureExpressionTrees() {
        IRTransform transform = new IRTransform() {
            @Override
            public void setOperatorContext(DBSPOperator operator) {}

            @Override
            public void setCircuitContext(DBSPCircuit circuit) {}

            @Override
            public IDBSPInnerNode apply(IDBSPInnerNode e) {
                if (e.isExpression()) {
                    return e.to(DBSPExpression.class).ensureTree(compiler);
                }
                return e;
            }

            @Override
            public String getName() {
                return "EnsureExpressionTrees";
            }

            @Override
            public String toString() {
                return "EnsuresTree";
            }
        };
        return this.select.apply(new CircuitRewriter(this.compiler, transform, false));
    }

    protected CircuitTransform checkRetain(CircuitGraphs graphs) {
        return this.select.apply(new CheckRetain(this.compiler, graphs));
    }

    protected CircuitTransform mergeGC() {
        return this.select.apply(new MergeGC(this.compiler));
    }

    protected DeltaExpandOperators deltaExpander() {
        return new DeltaExpandOperators(this.compiler);
    }

    protected CircuitTransform insertLimiters(CircuitTransform original) {
        return original;
    }

    protected CircuitTransform analyzeMonotonicity(CircuitTransform original, DBSPCircuit originalCircuit) {
        return original;
    }

    @Override
    public DBSPCircuit apply(DBSPCircuit circuit) {
        final boolean debug = this.getDebugLevel() >= 1;
        final int details = this.getDebugLevel();

        CircuitTransform toTree = this.ensureExpressionTrees();
        circuit = this.compiler.applyObservedPass(this.getName(), 0, 0, toTree, circuit);

        // Insert noops between consecutive integrators
        Graph graph = new Graph(this.compiler);
        CircuitTransform buildGraph = this.select.apply(graph);
        this.compiler.applyObservedPass(this.getName(), 1, 0, buildGraph, circuit);
        if (this.compiler.options.languageOptions.incrementalize) {
            CircuitTransform separate = this.separateIntegrators(graph.getGraphs());
            circuit = this.compiler.applyObservedPass(this.getName(), 2, 0, separate, circuit);
            // Recompute graph
            this.compiler.applyObservedPass(this.getName(), 3, 0, buildGraph, circuit);
        }

        CircuitGraph outerGraph = graph.getGraphs().getGraph(circuit);
        var order = new OrderErrorConsumers(c -> this.reachableFromError(c, outerGraph));
        circuit = this.compiler.applyObservedPass(this.getName(), 4, 0, this.select.apply(order), circuit);
        Set<DBSPOperator> reachableFromError = order.reachable();

        if (debug)
            ToDot.dump(this.compiler, "original.png", details, "png", circuit);

        DeltaExpandOperators expander = this.deltaExpander();
        DBSPCircuit expanded = this.compiler.applyObservedPass(this.getName(), 5, 0, expander, circuit);
        // ToDot.dump(this.compiler, "expanded-before.png", details, "png", expanded);

        Monotonicity monotonicity = new Monotonicity(this.compiler);
        CircuitTransform analysis = debug ? monotonicity : this.analyzeMonotonicity(monotonicity, circuit);
        expanded = this.compiler.applyObservedPass(this.getName(), 6, 0, analysis, expanded);
        if (debug)
            ToDot.customDump("expanded.png", "png", expanded,
                    stream -> new ToDotNodesVisitor(compiler, stream, details),
                    stream -> new MonotoneDot(compiler, stream, details, monotonicity.info));

        InsertLimiters limiters = new InsertLimiters(
                this.compiler, expanded, monotonicity.info, expander.expansion, reachableFromError);

        // Notice that we apply the limiters to the original circuit, not to the expanded circuit!
        DBSPCircuit result = this.compiler.applyObservedPass(this.getName(), 7, 0,
                this.insertLimiters(limiters), circuit);
        if (debug)
            ToDot.dump(compiler, "limited.png", details, "png", result);

        CircuitTransform merger = this.mergeGC();
        result = this.compiler.applyObservedPass(this.getName(), 8, 0, merger, result);
        this.compiler.applyObservedPass(this.getName(), 9, 0, buildGraph, result);
        CircuitTransform check = this.checkRetain(graph.getGraphs());
        this.compiler.applyObservedPass(this.getName(), 10, 0, check, result);
        return result;
    }

    @Override
    public String toString() {
        return "MonotoneAnalyzer";
    }
}
