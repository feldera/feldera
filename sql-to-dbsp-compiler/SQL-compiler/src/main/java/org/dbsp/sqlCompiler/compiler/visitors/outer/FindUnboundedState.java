package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.rel.core.Intersect;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Minus;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.core.Window;
import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.circuit.ICircuit;
import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.circuit.operator.DBSPAggregateLinearPostprocessRetainKeysOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPAggregateOperatorBase;
import org.dbsp.sqlCompiler.circuit.operator.DBSPBinaryDistinctOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPBinaryOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPConstantOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDelayOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDifferentiateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDistinctOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIndexedTopKOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIntegrateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPLagOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPNestedOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPPartitionedRollingAggregateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPPartitionedRollingAggregateWithWaterlineOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPPositiveOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPRankOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPRowNumberOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSinkOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSourceTableOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamDistinctOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPUpsertFeedbackOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPViewBaseOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPViewDeclarationOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPWaterlineOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPWindowOperator;
import org.dbsp.sqlCompiler.circuit.operator.IGCOperator;
import org.dbsp.sqlCompiler.circuit.operator.IInputOperator;
import org.dbsp.sqlCompiler.circuit.operator.IJoin;
import org.dbsp.sqlCompiler.circuit.operator.ILinearAggregate;
import org.dbsp.sqlCompiler.circuit.operator.ILinear;
import org.dbsp.sqlCompiler.circuit.operator.INonLinearAggregate;
import org.dbsp.sqlCompiler.circuit.operator.IStateful;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.ViewOrigins;
import org.dbsp.sqlCompiler.compiler.errors.SourcePositionRange;
import org.dbsp.sqlCompiler.compiler.errors.SourcePositionRanges;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTupleBase;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeBool;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeIndexedZSet;
import org.dbsp.util.Linq;
import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** Find operators whose state may grow without bound.
 *
 * <p>The analysis computes two properties:
 * <ul>
 * <li>"bounded", a property of streams: the integral of
 * the stream is bounded.</li>
 * <li>"bounded state", a property of operators.  An operator may internally
 * contain multiple integrators.</li>
 * </ul>
 * Stateful operators with unbounded state are collected in {@link #unbounded}.
 *
 * <p>Only stream-processing programs receive a warning for each unbounded operator. */
public class FindUnboundedState extends Passes {
    /** Error type shared by all warnings emitted by this pass */
    public static final String WARNING = "Unbounded state";
    /** Continuation of the first warning of a compilation; tells the user how to silence all of them
     * and where they are documented */
    public static final String HINT = "Silence these warnings with SET " +
            DBSPCompiler.silencingVariable(WARNING) + " = ON\n" +
            "See https://docs.feldera.com/sql/streaming#unbounded-state-warnings";

    /**
     * An operator whose state may grow without bound.
     *
     * @param operator        The operator holding the state.
     * @param circuit         The circuit that contains the operator.
     * @param unboundedInputs Indexes of the operator inputs that are not bounded.
     */
    public record UnboundedOperator(DBSPOperator operator, ICircuit circuit, List<Integer> unboundedInputs) { }

    /** Streams whose integral is bounded */
    final Set<OutputPort> bounded = new HashSet<>();
    /** Streams whose trace is pruned by a GC operator */
    final Set<OutputPort> gcedStreams = new HashSet<>();
    /** Operators whose state may grow without bound */
    public final List<UnboundedOperator> unbounded = new ArrayList<>();
    /** True if the program declares LATENESS or append_only tables, or uses a temporal filter */
    boolean streaming = false;

    public FindUnboundedState(DBSPCompiler compiler) {
        super("FindUnboundedState", compiler);
        this.add(new DetectStreamingOperations(compiler));
        this.add(new FindGCedStreams(compiler));
        FindBounded findBounded = new FindBounded(compiler);
        this.add(findBounded);
        // Second run for recursive circuits
        this.add(findBounded);
        this.add(new CollectUnbounded(compiler));
        Graph graph = new Graph(compiler);
        this.add(graph);
        this.add(new ReportUnbounded(graph.getGraphs()));
    }

    @Override
    public DBSPCircuit apply(DBSPCircuit circuit) {
        this.bounded.clear();
        this.gcedStreams.clear();
        this.unbounded.clear();
        this.streaming = false;
        return super.apply(circuit);
    }

    /** True if the integral of the stream is bounded */
    boolean isBounded(OutputPort port) {
        return this.bounded.contains(port) || this.gcedStreams.contains(port);
    }

    /** True if some output stream of the operator has its stream pruned by a GC operator */
    boolean hasGCedOutput(DBSPOperator operator) {
        for (OutputPort port : this.gcedStreams)
            if (port.operator == operator)
                return true;
        return false;
    }

    /** True for the operators whose output is bounded when all their inputs are bounded. */
    static boolean propagatesBounded(DBSPOperator operator) {
        if (operator.is(ILinear.class))
            return !operator.is(DBSPIntegrateOperator.class);
        return operator.is(INonLinearAggregate.class)
                || operator.is(DBSPDistinctOperator.class)
                || operator.is(DBSPStreamDistinctOperator.class)
                || operator.is(DBSPBinaryDistinctOperator.class)
                || operator.is(DBSPPositiveOperator.class)
                || operator.is(DBSPUpsertFeedbackOperator.class)
                || operator.is(DBSPIndexedTopKOperator.class)
                || operator.is(DBSPRankOperator.class)
                || operator.is(DBSPRowNumberOperator.class)
                || operator.is(IJoin.class);
    }

    /** True for the aggregates that produce one row per group; rolling aggregates
     * also group their input, but produce one row per input row. */
    static boolean isGroupByAggregate(DBSPOperator operator) {
        boolean aggregate = operator.is(ILinearAggregate.class)
                || operator.is(INonLinearAggregate.class)
                || operator.is(DBSPAggregateOperatorBase.class);
        boolean rolling = operator.is(DBSPPartitionedRollingAggregateOperator.class)
                || operator.is(DBSPPartitionedRollingAggregateWithWaterlineOperator.class);
        return aggregate && !rolling
                && operator.inputs.get(0).outputType().is(DBSPTypeIndexedZSet.class);
    }

    /** A group-by key has a bounded number of values when it has at most this many */
    static final long MAX_KEY_VALUES = 1024;

    /** True if the group-by aggregate has a bounded number of groups: its key is empty,
     * or all its fields are booleans and they admit at most {@link #MAX_KEY_VALUES}
     * combinations (2 values for a boolean and 3 for a nullable one). */
    static boolean hasBoundedKey(DBSPOperator aggregate) {
        DBSPTypeTupleBase key = aggregate.inputs.get(0).getOutputIndexedZSetType()
                .keyType.to(DBSPTypeTupleBase.class);
        long values = 1;
        for (DBSPType field : key.tupFields) {
            if (!field.is(DBSPTypeBool.class))
                return false;
            values *= field.mayBeNull ? 3 : 2;
            if (values > MAX_KEY_VALUES)
                return false;
        }
        return true;
    }

    boolean allInputsBounded(DBSPOperator operator) {
        for (OutputPort input : operator.inputs)
            if (!this.isBounded(input))
                return false;
        return true;
    }

    /** Detects whether the program declares that it processes unbounded streams.
     * NOW() counts only when it feeds a window operator, i.e., in a temporal filter. */
    class DetectStreamingOperations extends CircuitVisitor {
        DetectStreamingOperations(DBSPCompiler compiler) {
            super(compiler);
        }

        @Override
        public void postorder(DBSPSourceTableOperator node) {
            boolean lateness = Linq.any(node.metadata.getColumns(), column -> column.lateness != null);
            if (lateness || node.metadata.isAppendOnly())
                FindUnboundedState.this.streaming = true;
        }

        @Override
        public void postorder(DBSPViewBaseOperator node) {
            if (node.metadata.hasLateness())
                FindUnboundedState.this.streaming = true;
        }

        @Override
        public void postorder(DBSPWindowOperator node) {
            FindUnboundedState.this.streaming = true;
        }
    }

    /**
     * Record the streams whose trace is pruned by a GC operator.
     */
    class FindGCedStreams extends CircuitVisitor {
        FindGCedStreams(DBSPCompiler compiler) {
            super(compiler);
        }

        @Override
        public void postorder(DBSPOperator node) {
            if (node.is(IGCOperator.class))
                FindUnboundedState.this.gcedStreams.add(node.to(DBSPBinaryOperator.class).left());
        }
    }

    /** Compute the "bounded" stream property. */
    class FindBounded extends CircuitVisitor {
        FindBounded(DBSPCompiler compiler) {
            super(compiler);
        }

        /** True if all output streams of the operator are bounded */
        boolean hasBoundedOutput(DBSPOperator node) {
            if (node.is(DBSPWindowOperator.class))
                return !node.to(DBSPWindowOperator.class).lowerUnbounded;
            // A waterline is a single value
            if (node.is(DBSPWaterlineOperator.class) || node.is(DBSPConstantOperator.class))
                return true;
            // Prunes its state and its output using its waterline input
            if (node.is(DBSPPartitionedRollingAggregateWithWaterlineOperator.class))
                return true;
            // The NOW system table always contains exactly one row
            if (node.is(IInputOperator.class) &&
                    node.to(IInputOperator.class).getTableName().equals(DBSPCompiler.NOW_TABLE_NAME))
                return true;
            // An aggregate over a bounded key produces a bounded number of rows
            if (isGroupByAggregate(node) && hasBoundedKey(node))
                return true;

            return propagatesBounded(node) && FindUnboundedState.this.allInputsBounded(node);
        }

        @Override
        public void postorder(DBSPOperator node) {
            if (node.is(IGCOperator.class))
                return;
            if (this.hasBoundedOutput(node))
                for (int i = 0; i < node.outputCount(); i++)
                    FindUnboundedState.this.bounded.add(node.getOutput(i));
        }

        /** The declaration of a recursive view is the feedback input of the recursive circuit;
         * nothing else is assumed about its size. */
        @Override
        public void postorder(DBSPViewDeclarationOperator node) {
            ICircuit parent = this.getParent();
            if (!parent.is(DBSPNestedOperator.class))
                return;
            OutputPort port = parent.to(DBSPNestedOperator.class).outputForDeclaration(node);
            if (port != null && FindUnboundedState.this.isBounded(port))
                FindUnboundedState.this.bounded.add(node.outputPort());
        }

        @Override
        public void postorder(DBSPSourceTableOperator node) {
            if (node.metadata.expectedSize != null)
                FindUnboundedState.this.bounded.add(node.outputPort());
            else
                super.postorder(node);
        }

        @Override
        public void postorder(DBSPNestedOperator node) {
            for (int i = 0; i < node.outputCount(); i++) {
                OutputPort internal = node.internalOutputs.get(i);
                if (internal != null && FindUnboundedState.this.isBounded(internal))
                    FindUnboundedState.this.bounded.add(node.getOutput(i));
            }
        }
    }

    /**
     * Collect the stateful operators without bounded state; runs after the
     * stream properties have been computed.
     */
    class CollectUnbounded extends CircuitVisitor {
        CollectUnbounded(DBSPCompiler compiler) {
            super(compiler);
        }

        boolean insideRecursive() {
            return this.getParent().is(DBSPNestedOperator.class);
        }

        @Override
        public void postorder(DBSPDelayOperator node) {
            if (this.insideRecursive())
                super.postorder(node);
        }

        @Override
        public void postorder(DBSPDifferentiateOperator node) {
            if (this.insideRecursive())
                super.postorder(node);
        }

        @Override
        public void postorder(DBSPWaterlineOperator node) {
            if (this.insideRecursive())
                super.postorder(node);
        }

        @Override
        public void postorder(DBSPAggregateLinearPostprocessRetainKeysOperator node) {
            if (this.insideRecursive())
                super.postorder(node);
        }

        @Override
        public void postorder(DBSPPartitionedRollingAggregateWithWaterlineOperator node) {
            if (this.insideRecursive())
                super.postorder(node);
        }

        @Override
        public void postorder(DBSPWindowOperator node) {
            if (node.lowerUnbounded || this.insideRecursive())
                super.postorder(node);
        }

        void markUnbounded(DBSPOperator operator, List<Integer> inputs) {
            var ub = new UnboundedOperator(operator, this.getParent(), inputs);
            FindUnboundedState.this.unbounded.add(ub);
        }

        @Override
        public void postorder(DBSPOperator node) {
            if (!node.is(IStateful.class))
                return;
            // Operators whose output trace is pruned by a GC operator have bounded state
            if (FindUnboundedState.this.hasGCedOutput(node))
                return;
            // A linear aggregate keeps one accumulator per group, so a bounded key bounds its state
            if (node.is(ILinearAggregate.class) && isGroupByAggregate(node) && hasBoundedKey(node))
                return;
            List<Integer> unbounded = new ArrayList<>();
            for (int input = 0; input < node.inputs.size(); input++) {
                if (!FindUnboundedState.this.isBounded(node.inputs.get(input)))
                    unbounded.add(input);
            }
            if (node.inputs.isEmpty()) {
                // A stateful input operator stores the table; the table is bounded
                // only when the declaration promises a bounded size
                if (!FindUnboundedState.this.isBounded(node.getOutput(0)))
                    this.markUnbounded(node, unbounded);
                return;
            }
            if (!unbounded.isEmpty())
                this.markUnbounded(node, unbounded);
        }
    }

    /**
     * Warn about each unbounded operator of a stream-processing program;
     * runs after {@link CollectUnbounded} and {@link Graph}.
     */
    class ReportUnbounded implements CircuitTransform {
        final CircuitGraphs graphs;

        ReportUnbounded(CircuitGraphs graphs) {
            this.graphs = graphs;
        }

        /** SQL-level name, with its article, of the relational operator {@code rel} when it
         * holds state; null for stateless relational operators such as projections and filters. */
        @Nullable
        static String sqlName(RelNode rel) {
            if (rel instanceof Join || rel instanceof Correlate)
                return "a JOIN";
            if (rel instanceof Aggregate aggregate)
                return aggregate.getAggCallList().isEmpty() ? "a DISTINCT" : "an aggregate";
            if (rel instanceof Window)
                return "a window function";
            if (rel instanceof Sort sort)
                return (sort.fetch != null || sort.offset != null) ? "an ORDER BY with LIMIT" : null;
            if (rel instanceof Intersect intersect)
                return intersect.all ? "an INTERSECT ALL" : "an INTERSECT";
            if (rel instanceof Minus minus)
                return minus.all ? "an EXCEPT ALL" : "an EXCEPT";
            if (rel instanceof Union union)
                return union.all ? null : "a UNION";
            return null;
        }

        /** SQL-level name, with its article, of the construct whose state the operator holds:
         * the first stateful relational operator that the operator implements; the kind of
         * operator decides for operators that the compiler synthesizes without one. */
        static String sqlName(DBSPOperator operator) {
            for (RelNode rel : operator.getRelNode().getRelNodes()) {
                String name = sqlName(rel);
                if (name != null)
                    return name;
            }
            if (operator.is(IJoin.class))
                return "a JOIN";
            if (operator.is(DBSPWindowOperator.class))
                return "a temporal filter";
            if (operator.is(DBSPPositiveOperator.class))
                return "an EXCEPT ALL";
            if (operator.is(DBSPDistinctOperator.class)
                    || operator.is(DBSPBinaryDistinctOperator.class)
                    || operator.is(DBSPStreamDistinctOperator.class))
                return "a DISTINCT";
            if (operator.is(DBSPPartitionedRollingAggregateOperator.class)
                    || operator.is(DBSPPartitionedRollingAggregateWithWaterlineOperator.class)
                    || operator.is(DBSPRankOperator.class)
                    || operator.is(DBSPRowNumberOperator.class)
                    || operator.is(DBSPLagOperator.class))
                return "a window function";
            if (operator.is(ILinearAggregate.class)
                    || operator.is(INonLinearAggregate.class)
                    || operator.is(DBSPAggregateOperatorBase.class))
                return "an aggregate";
            String internal = operator.is(DBSPSimpleOperator.class) ?
                    operator.to(DBSPSimpleOperator.class).operation :
                    operator.getClass().getSimpleName();
            return "the " + internal + " operator";
        }

        /** The view whose definition contains the operator, obtained by inspecting
         * the attached Calcite Rel tree. */
        @Nullable
        ViewOrigins.ViewSourcePosition viewOf(UnboundedOperator ub) {
            for (RelNode rel : ub.operator().getRelNode().getRelNodes()) {
                ViewOrigins.ViewSourcePosition origin = FindUnboundedState.this.compiler.viewOrigins.get(rel);
                if (origin != null)
                    return origin;
            }
            DBSPOperator sink = this.graphs.closestDownstream(
                    ub.circuit(), ub.operator(), operator -> operator.is(DBSPSinkOperator.class));
            if (sink == null)
                return null;
            return new ViewOrigins.ViewSourcePosition(sink.to(DBSPSinkOperator.class).viewName, this.ownPosition(sink));
        }

        /** The SQL construct that produced the operator when known,
         * otherwise the first expression the operator evaluates. */
        SourcePositionRange ownPosition(DBSPOperator operator) {
            SourcePositionRanges own = new SourcePositionRanges(operator.getSourcePositions());
            if (!own.positions.isEmpty())
                return own.positions.get(0);
            SourcePositionRanges all = FindSourcePositions.getPositions(
                    FindUnboundedState.this.compiler, operator);
            if (!all.positions.isEmpty())
                return all.positions.get(0);
            return SourcePositionRange.INVALID;
        }

        /** The operator's own position when it has one; the compiler synthesizes
         * many operators without positions, and the view statement narrows the position. */
        SourcePositionRange positionOf(DBSPOperator operator, @Nullable ViewOrigins.ViewSourcePosition view) {
            SourcePositionRange own = this.ownPosition(operator);
            if (own.isValid() || view == null)
                return own;
            return view.position();
        }

        String describe(DBSPOperator operator, @Nullable ViewOrigins.ViewSourcePosition view) {
            if (operator.is(IInputOperator.class)) {
                String table = operator.to(IInputOperator.class).getTableName().singleQuote();
                return "The index of table " + table + " may grow without bound";
            }
            String where = view == null ? "" :
                    " in the code implementing view " + view.view().singleQuote();
            return "The state of " + sqlName(operator) + where + " may grow without bound";
        }

        @Override
        public DBSPCircuit apply(DBSPCircuit circuit) {
            if (!FindUnboundedState.this.streaming)
                return circuit;
            boolean first = true;
            for (UnboundedOperator ub : FindUnboundedState.this.unbounded) {
                ViewOrigins.ViewSourcePosition view = this.viewOf(ub);
                FindUnboundedState.this.compiler.reportWarning(
                        this.positionOf(ub.operator(), view), WARNING, this.describe(ub.operator(), view));
                if (first)
                    FindUnboundedState.this.compiler.reportWarning(SourcePositionRange.INVALID, WARNING, HINT, true);
                first = false;
            }
            return circuit;
        }

        @Override
        public String getName() {
            return "ReportUnbounded";
        }

        @Override
        public String toString() {
            return this.getName();
        }
    }
}
