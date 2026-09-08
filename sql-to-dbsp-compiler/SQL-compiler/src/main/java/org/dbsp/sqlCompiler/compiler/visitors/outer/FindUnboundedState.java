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
import org.dbsp.sqlCompiler.circuit.operator.DBSPAntiJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPAsofJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPBinaryDistinctOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPBinaryOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPConcreteAsofJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPConstantOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPPrimitiveAggregateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPChainAggregateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPAggregateLinearPostprocessOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDelayOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDifferentiateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDistinctOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIndexedTopKOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIntegrateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinBaseOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPLagOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPLeftJoinFilterMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPLeftJoinIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPLeftJoinOperator;
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
import org.dbsp.sqlCompiler.circuit.operator.DBSPStarJoinBaseOperator;
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
import org.dbsp.sqlCompiler.compiler.Documentation;
import org.dbsp.sqlCompiler.compiler.ViewOrigins;
import org.dbsp.sqlCompiler.compiler.errors.SourcePositionRange;
import org.dbsp.sqlCompiler.compiler.errors.SourcePositionRanges;
import org.dbsp.sqlCompiler.compiler.visitors.outer.keys.KeyAnalysis;
import org.dbsp.sqlCompiler.compiler.visitors.outer.keys.LosslessCastKeyAnalysis;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPUSizeLiteral;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTupleBase;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeBool;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeIndexedZSet;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeZSet;
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
 * Note that a join of a bounded stream with an unbounded table over a key of the
 * table has a bounded output, at most one row per row of the stream, while its state
 * is unbounded.
 * Stateful operators with unbounded state are collected in {@link #unbounded}.
 *
 * <p>Only stream-processing programs receive a warning for each unbounded operator. */
public class FindUnboundedState extends Passes {
    /** Error type shared by all warnings emitted by this pass */
    public static final String WARNING = "Unbounded state";
    public static final Documentation.Link DOCUMENTATION =
            new Documentation.Link("sql/streaming", "unbounded-state-warnings");
    /** Continuation of the first warning of a compilation; tells the user how to silence all of them
     * and where they are documented */
    public static final String HINT = "Silence these warnings with SET " +
            DBSPCompiler.silencingVariable(WARNING) + " = ON\n" + DOCUMENTATION.citation();

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
    /** The keys of every collection; a join over a key of one input has as many rows as the other */
    final KeyAnalysis keys;

    public FindUnboundedState(DBSPCompiler compiler) {
        super("FindUnboundedState", compiler);
        this.add(new DetectStreamingOperations(compiler));
        this.add(new FindGCedStreams(compiler));
        this.keys = new LosslessCastKeyAnalysis(compiler);
        this.add(this.keys);
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

    /** A group-by key has a bounded number of values when it has at most this many */
    static final long MAX_KEY_VALUES = 1024;

    /** True if the tuple type has a bounded number of values: it has no fields, or all its
     * fields are booleans and they admit at most {@link #MAX_KEY_VALUES} combinations
     * (2 values for a boolean and 3 for a nullable one). */
    static boolean hasBoundedDomain(DBSPTypeTupleBase tuple) {
        long values = 1;
        for (DBSPType field : tuple.tupFields) {
            if (!field.is(DBSPTypeBool.class))
                return false;
            values *= field.mayBeNull ? 3 : 2;
            if (values > MAX_KEY_VALUES)
                return false;
        }
        return true;
    }

    /** True if the group-by aggregate has a bounded number of groups: the key of its
     * indexed input has a bounded domain. */
    static boolean hasBoundedKey(DBSPOperator aggregate) {
        OutputPort input = aggregate.inputs.get(0);
        if (!input.outputType().is(DBSPTypeIndexedZSet.class))
            return false;
        return hasBoundedDomain(input.getOutputIndexedZSetType().keyType.to(DBSPTypeTupleBase.class));
    }

    /** True if the rows of the Z-set on {@code port} have a bounded domain */
    static boolean hasBoundedRows(OutputPort port) {
        if (!port.outputType().is(DBSPTypeZSet.class))
            return false;
        DBSPType element = port.getOutputZSetElementType();
        return element.is(DBSPTypeTupleBase.class) && hasBoundedDomain(element.to(DBSPTypeTupleBase.class));
    }

    boolean allInputsBounded(DBSPOperator operator) {
        for (OutputPort input : operator.inputs)
            if (!this.isBounded(input))
                return false;
        return true;
    }

    /** True if the index of the indexed collection {@code port} is a key of it: each index
     * value occurs in at most one row. */
    boolean indexIsKey(OutputPort port) {
        return this.keys.getKeys(port).hasKeyWithinIndex();
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

        void markBounded(DBSPOperator node) {
            for (int i = 0; i < node.outputCount(); i++)
                FindUnboundedState.this.bounded.add(node.getOutput(i));
        }

        boolean allInputsBounded(DBSPOperator node) {
            return FindUnboundedState.this.allInputsBounded(node);
        }

        boolean isBounded(OutputPort port) {
            return FindUnboundedState.this.isBounded(port);
        }

        boolean indexIsKey(OutputPort port) {
            return FindUnboundedState.this.indexIsKey(port);
        }

        /** Operators without a rule of their own propagate boundedness from all their inputs */
        @Override
        public void postorder(DBSPOperator node) {
            if (node.is(IGCOperator.class))
                return;
            if (propagatesBounded(node) && this.allInputsBounded(node))
                this.markBounded(node);
        }

        /** A window with a lower bound retains a bounded interval of its input */
        @Override
        public void postorder(DBSPWindowOperator node) {
            if (!node.lowerUnbounded)
                this.markBounded(node);
        }

        /** A waterline is a single value */
        @Override
        public void postorder(DBSPWaterlineOperator node) {
            this.markBounded(node);
        }

        @Override
        public void postorder(DBSPConstantOperator node) {
            this.markBounded(node);
        }

        /** Prunes its state and its output using its waterline input */
        @Override
        public void postorder(DBSPPartitionedRollingAggregateWithWaterlineOperator node) {
            this.markBounded(node);
        }

        /** A table is bounded when its declaration says so; the NOW system table holds one row */
        @Override
        public void postorder(DBSPSourceTableOperator node) {
            if (node.metadata.expectedSize != null || node.tableName.equals(DBSPCompiler.NOW_TABLE_NAME))
                this.markBounded(node);
        }

        /** A group-by aggregate produces one row per group */
        void aggregate(DBSPOperator node) {
            if (hasBoundedKey(node) || this.allInputsBounded(node))
                this.markBounded(node);
        }

        @Override
        public void postorder(DBSPAggregateOperatorBase node) {
            this.aggregate(node);
        }

        /** A rolling aggregate produces one row per input row */
        @Override
        public void postorder(DBSPPartitionedRollingAggregateOperator node) {
            this.postorder((DBSPOperator) node);
        }

        @Override
        public void postorder(DBSPAggregateLinearPostprocessOperator node) {
            this.aggregate(node);
        }

        @Override
        public void postorder(DBSPAggregateLinearPostprocessRetainKeysOperator node) {
            this.aggregate(node);
        }

        @Override
        public void postorder(DBSPChainAggregateOperator node) {
            this.aggregate(node);
        }

        @Override
        public void postorder(DBSPPrimitiveAggregateOperator node) {
            this.aggregate(node);
        }

        /** A distinct produces at most one row per value of its row type */
        @Override
        public void postorder(DBSPDistinctOperator node) {
            if (hasBoundedRows(node.input()) || this.allInputsBounded(node))
                this.markBounded(node);
        }

        @Override
        public void postorder(DBSPBinaryDistinctOperator node) {
            if (hasBoundedRows(node.left()) || this.allInputsBounded(node))
                this.markBounded(node);
        }

        /** A top-k produces at most 'limit' rows per group */
        @Override
        public void postorder(DBSPIndexedTopKOperator node) {
            boolean perGroup = node.limit.is(DBSPUSizeLiteral.class) && hasBoundedKey(node);
            if (perGroup || this.allInputsBounded(node))
                this.markBounded(node);
        }

        /** A join produces at most one row per row of a bounded input when every such row
         * matches at most one row of the other input */
        @Override
        public void postorder(DBSPJoinBaseOperator node) {
            OutputPort left = node.left();
            OutputPort right = node.right();
            if (this.allInputsBounded(node)
                    || (this.isBounded(left) && this.indexIsKey(right))
                    || (this.isBounded(right) && this.indexIsKey(left)))
                this.markBounded(node);
        }

        /** A left join also outputs every unmatched left row */
        void leftJoin(DBSPJoinBaseOperator node) {
            if (this.isBounded(node.left()) && (this.isBounded(node.right()) || this.indexIsKey(node.right())))
                this.markBounded(node);
        }

        @Override
        public void postorder(DBSPLeftJoinOperator node) {
            this.leftJoin(node);
        }

        @Override
        public void postorder(DBSPLeftJoinIndexOperator node) {
            this.leftJoin(node);
        }

        @Override
        public void postorder(DBSPLeftJoinFilterMapOperator node) {
            this.leftJoin(node);
        }

        /** An ASOF join outputs at most one row per left row */
        @Override
        public void postorder(DBSPAsofJoinOperator node) {
            if (this.isBounded(node.left()))
                this.markBounded(node);
        }

        @Override
        public void postorder(DBSPConcreteAsofJoinOperator node) {
            if (this.isBounded(node.left()))
                this.markBounded(node);
        }

        /** An anti join outputs a subset of its left input */
        @Override
        public void postorder(DBSPAntiJoinOperator node) {
            if (this.isBounded(node.left()))
                this.markBounded(node);
        }

        /** All inputs of a star join share the index.  At most one input may lack a key within
         * it, and that input must be bounded; if every input has one, any bounded input suffices. */
        @Override
        public void postorder(DBSPStarJoinBaseOperator node) {
            OutputPort unkeyed = null;
            boolean anyBounded = false;
            for (OutputPort input : node.inputs) {
                anyBounded |= this.isBounded(input);
                if (!this.indexIsKey(input)) {
                    if (unkeyed != null)
                        return;
                    unkeyed = input;
                }
            }
            if (unkeyed == null ? anyBounded : this.isBounded(unkeyed))
                this.markBounded(node);
        }

        /** The declaration of a recursive view is the feedback input of the recursive circuit;
         * nothing else is assumed about its size. */
        @Override
        public void postorder(DBSPViewDeclarationOperator node) {
            ICircuit parent = this.getParent();
            if (!parent.is(DBSPNestedOperator.class))
                return;
            OutputPort port = parent.to(DBSPNestedOperator.class).outputForDeclaration(node);
            if (port != null && this.isBounded(port))
                FindUnboundedState.this.bounded.add(node.outputPort());
        }

        @Override
        public void postorder(DBSPNestedOperator node) {
            for (int i = 0; i < node.outputCount(); i++) {
                OutputPort internal = node.internalOutputs.get(i);
                if (internal != null && this.isBounded(internal))
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

        /** A distinct keeps one entry per distinct row, so a bounded row domain bounds its state */
        @Override
        public void postorder(DBSPDistinctOperator node) {
            if (!hasBoundedRows(node.input()))
                super.postorder(node);
        }

        @Override
        public void postorder(DBSPBinaryDistinctOperator node) {
            if (!hasBoundedRows(node.left()))
                super.postorder(node);
        }

        /** A linear aggregate keeps one accumulator per group, so a bounded key bounds its state */
        void linearAggregate(DBSPOperator node) {
            if (!hasBoundedKey(node))
                this.postorder((DBSPOperator) node);
        }

        @Override
        public void postorder(DBSPAggregateLinearPostprocessOperator node) {
            this.linearAggregate(node);
        }

        @Override
        public void postorder(DBSPChainAggregateOperator node) {
            this.linearAggregate(node);
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
