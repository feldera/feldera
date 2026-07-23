package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.circuit.ICircuit;
import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.circuit.operator.DBSPAggregateLinearPostprocessRetainKeysOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPBinaryDistinctOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPBinaryOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPConstantOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDelayOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDifferentiateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDistinctOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIndexedTopKOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIntegrateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPNestedOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPPartitionedRollingAggregateWithWaterlineOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPPositiveOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPRankOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPRowNumberOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamDistinctOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPUpsertFeedbackOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPViewDeclarationOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPWaterlineOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPWindowOperator;
import org.dbsp.sqlCompiler.circuit.operator.IGCOperator;
import org.dbsp.sqlCompiler.circuit.operator.IInputOperator;
import org.dbsp.sqlCompiler.circuit.operator.IJoin;
import org.dbsp.sqlCompiler.circuit.operator.ILinear;
import org.dbsp.sqlCompiler.circuit.operator.INonLinearAggregate;
import org.dbsp.sqlCompiler.circuit.operator.IStateful;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.errors.SourcePositionRanges;
import org.dbsp.util.Logger;

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
 * <p>Currently no one consumes the output produced by this pass, but it can be obtained
 * by turning logging up using the compiler option -TFindUnboundedState=1 */
public class FindUnboundedState extends Passes {
    /**
     * An operator whose state may grow without bound.
     *
     * @param operator        The operator holding the state.
     * @param unboundedInputs Indexes of the operator inputs that are not bounded.
     */
    public record UnboundedOperator(DBSPOperator operator, List<Integer> unboundedInputs) { }

    /** Streams whose integral is bounded */
    final Set<OutputPort> bounded = new HashSet<>();
    /** Streams whose trace is pruned by a GC operator */
    final Set<OutputPort> gcedStreams = new HashSet<>();
    /** Operators whose state may grow without bound */
    public final List<UnboundedOperator> unbounded = new ArrayList<>();

    public FindUnboundedState(DBSPCompiler compiler) {
        super("FindUnboundedState", compiler);
        this.add(new FindGCedStreams(compiler));
        FindBounded findBounded = new FindBounded(compiler);
        this.add(findBounded);
        // Second run for recursive circuits
        this.add(findBounded);
        this.add(new CollectUnbounded(compiler));
    }

    @Override
    public DBSPCircuit apply(DBSPCircuit circuit) {
        this.bounded.clear();
        this.gcedStreams.clear();
        this.unbounded.clear();
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

    boolean allInputsBounded(DBSPOperator operator) {
        for (OutputPort input : operator.inputs)
            if (!this.isBounded(input))
                return false;
        return true;
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
            var ub = new UnboundedOperator(operator, inputs);
            FindUnboundedState.this.unbounded.add(ub);

            SourcePositionRanges pos = FindSourcePositions.getPositions(this.compiler, operator);
            String sources = this.compiler.sources.getFragments(pos);
            if (!sources.isEmpty())
                sources += "\n";
            Logger.INSTANCE.belowLevel(FindUnboundedState.class, 1)
                    .append("Potentially unbounded memory in operator ")
                    .append(operator.getClass().getSimpleName())
                    .newline()
                    .append(sources);
        }

        @Override
        public void postorder(DBSPOperator node) {
            if (!node.is(IStateful.class))
                return;
            // Operators inside recursive circuits store a history of deltas,
            // which can grow without bound
            if (this.getParent().is(DBSPNestedOperator.class)) {
                List<Integer> all = new ArrayList<>();
                for (int input = 0; input < node.inputs.size(); input++)
                    all.add(input);
                this.markUnbounded(node, all);
                return;
            }
            // Operators whose output trace is pruned by a GC operator have bounded state
            if (FindUnboundedState.this.hasGCedOutput(node))
                return;
            List<Integer> unbounded = new ArrayList<>();
            for (int input = 0; input < node.inputs.size(); input++) {
                if (!FindUnboundedState.this.isBounded(node.inputs.get(input)))
                    unbounded.add(input);
            }
            if (node.inputs.isEmpty())
                // Stateful non-GCed input operator
                this.markUnbounded(node, unbounded);
            if (!unbounded.isEmpty())
                this.markUnbounded(node, unbounded);
        }
    }
}
