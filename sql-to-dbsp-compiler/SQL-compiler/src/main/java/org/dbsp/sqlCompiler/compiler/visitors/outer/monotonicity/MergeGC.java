package org.dbsp.sqlCompiler.compiler.visitors.outer.monotonicity;

import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.circuit.operator.DBSPApplyNOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIntegrateTraceRetainKeysOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIntegrateTraceRetainNValuesOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIntegrateTraceRetainValuesOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPNoopOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.circuit.operator.IGCOperator;
import org.dbsp.sqlCompiler.circuit.operator.IHasInputIntegrator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.ExpressionCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteRelNode;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CSE;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitCloneVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitGraphs;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitWithGraphsVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.outer.Graph;
import org.dbsp.sqlCompiler.compiler.visitors.outer.Passes;
import org.dbsp.sqlCompiler.ir.IDBSPOuterNode;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPOpcode;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.util.Linq;
import org.dbsp.util.Logger;
import org.dbsp.util.Utilities;
import org.dbsp.util.graph.Port;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Replace the following pattern, where every consumer of the noops keeps a full integral of
 * its input, so that the consumers can share one integral:
 * source -> noop -> retain1
 *        -> noop -> retain2
 * ... with source -> noop -> retain1
 *                         -> retain2
 * The two retain operators are then merged by the patterns below.
 *
 * <p>Replace the following pattern, where all noop consumers keep full integrals:
 * source -> noop -> retain
 *        -> noop
 * ... with source -> noop: the consumer without a retain keeps a copy of the source.
 *
 * <p>Window operators are not optimized in this way.
 *
 * <p>Replace the following pattern
 * source -> retainKey1
 *        -> retainKey2
 * With source -> retainKey (min of control inputs).
 *
 * <p>Replace the following pattern
 * source -> retainValues1
 *        -> retainValues2
 * With source -> retainValues, which keeps a value when (retainValues1 OR retainValues2) keeps it.
 */
public class MergeGC extends Passes {
    /** This is a modified form of {@link CSE.FindCSE} */
    static class FindEquivalentNoops extends CircuitWithGraphsVisitor {
        /** Maps each operator to its canonical representative */
        public final Map<DBSPOperator, DBSPOperator> canonical;
        /** GC operators to remove: the shared integral has no retention of their kind */
        public final Set<DBSPOperator> removed;

        public FindEquivalentNoops(DBSPCompiler compiler, CircuitGraphs graphs) {
            super(compiler, graphs);
            this.canonical = new HashMap<>();
            this.removed = new HashSet<>();
        }

        @Override
        public Token startVisit(IDBSPOuterNode node) {
            this.canonical.clear();
            this.removed.clear();
            return super.startVisit(node);
        }

        /** @return True if every consumer of {@code noop} that is not a GC operator keeps a full
         * integral of its input, so that the consumers can share one integral.  A consumer that
         * truncates its integral with a bound of its own keeps no full copy, and a shared integral
         * would lose either the retain operators or the bound. */
        boolean consumersCanShareIntegral(DBSPOperator noop) {
            for (Port<DBSPOperator> succ: this.getGraph().getSuccessors(noop)) {
                DBSPOperator consumer = succ.node();
                if (consumer.is(IGCOperator.class))
                    continue;
                if (!SeparateIntegrators.hasPreIntegrator(consumer, succ.port()))
                    return false;
                if (consumer.is(IHasInputIntegrator.class) &&
                        consumer.to(IHasInputIntegrator.class).garbageCollectsInput(succ.port()))
                    return false;
            }
            return true;
        }

        /** @return The GC operator of {@code noop} with retention of kind {@code kind}, or null if there is none. */
        @Nullable
        IGCOperator gcOperator(DBSPOperator noop, CheckRetain.RetentionKind kind) {
            for (Port<DBSPOperator> succ: this.getGraph().getSuccessors(noop)) {
                DBSPOperator node = succ.node();
                if (node.is(IGCOperator.class) && node.to(IGCOperator.class).garbageCollects(noop) &&
                        CheckRetain.slot(node) == kind)
                    return node.to(IGCOperator.class);
            }
            return null;
        }

        /** Make the first noop of {@code group} of noops the "canonical" representative
         * of the group when:
         * - they all read one port;
         * - their consumers can share one integral;
         * - for every kind of retention that every noop has, their GC operators can be merged.
         * A kind of retention that some noop lacks is removed from the other noops: the consumers
         * of that noop keep a full copy of the input anyway, and one such copy replaces it and the
         * garbage collected copies. */
        void findMergeableNoops(List<DBSPOperator> group) {
            List<IGCOperator> toRemove = new ArrayList<>();
            for (CheckRetain.RetentionKind kind : CheckRetain.RetentionKind.values()) {
                List<IGCOperator> ofKind = new ArrayList<>();
                for (DBSPOperator noop : group) {
                    IGCOperator gc = this.gcOperator(noop, kind);
                    if (gc != null)
                        ofKind.add(gc);
                }
                if (ofKind.size() < group.size()) {
                    toRemove.addAll(ofKind);
                    continue;
                }
                for (IGCOperator gc : ofKind)
                    if (!FindMultipleRetains.canMerge(ofKind.get(0), gc))
                        return;
            }
            for (IGCOperator gc : toRemove) {
                Logger.INSTANCE.belowLevel(this, 1)
                        .append("MergeGC removing ")
                        .appendSupplier(gc::toString)
                        .newline();
                this.removed.add(gc.asOperator());
            }
            DBSPOperator first = group.get(0);
            for (DBSPOperator noop : group) {
                if (noop == first)
                    continue;
                Logger.INSTANCE.belowLevel(this, 1)
                        .append("MergeGC ")
                        .appendSupplier(noop::toString)
                        .append(" -> ")
                        .appendSupplier(first::toString)
                        .newline();
                this.canonical.put(noop, first);
            }
        }

        /** True if {@code left} and {@code right} carry the same values: they are the same port, or
         * ports of operators whose inputs carry the same values and that are equivalent once one
         * is rebuilt on the inputs of the other.  InsertLimiters builds a separate chain of
         * operators computing the bounds of each retain operator.  An operator without inputs,
         * such as a source, is only the same as itself. */
        static boolean sameStream(OutputPort left, OutputPort right) {
            if (left.equals(right))
                return true;
            if (left.port() != right.port())
                return false;
            DBSPOperator leftNode = left.node();
            DBSPOperator rightNode = right.node();
            if (leftNode.inputs.isEmpty() || leftNode.inputs.size() != rightNode.inputs.size())
                return false;
            for (int i = 0; i < leftNode.inputs.size(); i++)
                if (!sameStream(leftNode.inputs.get(i), rightNode.inputs.get(i)))
                    return false;
            return leftNode.withInputs(rightNode.inputs, false).equivalent(rightNode);
        }

        /** True if {@code gc0} and {@code gc1} retain the same data in the same way: their bounds carry
         * the same values, and the operators are equivalent once {@code gc1} is rebuilt on the inputs
         * of {@code gc0}. */
        static boolean samePolicy(IGCOperator gc0, IGCOperator gc1) {
            DBSPSimpleOperator op0 = gc0.asOperator();
            DBSPSimpleOperator op1 = gc1.asOperator();
            return sameStream(op0.inputs.get(1), op1.inputs.get(1)) &&
                    op0.equivalent(op1.withInputs(op0.inputs, false));
        }

        /** Group the noops that read {@code operator} by the output port they read from, keeping
         * only noops whose consumers can share one integral. Record which noops of each group to
         * merge and which GC operators to remove. */
        @Override
        public void postorder(DBSPSimpleOperator operator) {
            // Key: an output port of the operator.  Value: the noops reading it whose consumers can share one integral.
            Map<OutputPort, List<DBSPOperator>> groups = new HashMap<>();
            for (Port<DBSPOperator> destination : this.getGraph().getSuccessors(operator)) {
                DBSPOperator noop = destination.node();
                if (!noop.is(DBSPNoopOperator.class) || !this.consumersCanShareIntegral(noop))
                    continue;
                groups.computeIfAbsent(noop.inputs.get(0), p -> new ArrayList<>()).add(noop);
            }
            for (List<DBSPOperator> group : groups.values())
                if (group.size() > 1)
                    this.findMergeableNoops(group);
        }
    }

    /** Find multiple GC operators that read the same data which can be replaced by a single operator.
     * All the operators of a group have the same class: {@link DBSPIntegrateTraceRetainKeysOperator}s
     * with the same comparison, {@link DBSPIntegrateTraceRetainValuesOperator}s, or
     * {@link DBSPIntegrateTraceRetainNValuesOperator}s with identical policies. */
    static class FindMultipleRetains extends CircuitWithGraphsVisitor {
        /** Groups of at least two GC operators of the same class that read the same data; MergeRetain
         * replaces each group with one operator. */
        final List<List<IGCOperator>> shareLeftInput;
        /** The GC operators already placed in a group. */
        final Set<IGCOperator> grouped;

        FindMultipleRetains(DBSPCompiler compiler, CircuitGraphs graphs) {
            super(compiler, graphs);
            this.shareLeftInput = new ArrayList<>();
            this.grouped = new HashSet<>();
        }

        /** True if a single operator can replace {@code a} and {@code b} when they read the same data.
         * A retain-keys operator driven by the minimum of two bounds replaces two retain-keys operators
         * with the same comparison.  A retain-values operator that keeps a value when either keeps it
         * replaces any two retain-values operators.  Any other pair must have the same policy. */
        static boolean canMerge(IGCOperator a, IGCOperator b) {
            DBSPIntegrateTraceRetainKeysOperator keysA = a.as(DBSPIntegrateTraceRetainKeysOperator.class);
            DBSPIntegrateTraceRetainKeysOperator keysB = b.as(DBSPIntegrateTraceRetainKeysOperator.class);
            if (keysA != null && keysB != null)
                return keysA.accumulate == keysB.accumulate &&
                        keysA.getFunction().equivalent(keysB.getFunction());
            if (a.is(DBSPIntegrateTraceRetainValuesOperator.class) &&
                    b.is(DBSPIntegrateTraceRetainValuesOperator.class))
                return true;
            return FindEquivalentNoops.samePolicy(a, b);
        }

        /** Group {@code retain} with the GC operators of its class that read the same data and that
         * can be merged with it; a group with more than one member is recorded in
         * {@code shareLeftInput}. */
        void collect(IGCOperator retain) {
            if (this.grouped.contains(retain))
                return;
            List<IGCOperator> common = new ArrayList<>();
            common.add(retain);
            this.grouped.add(retain);
            OutputPort data = retain.data();
            for (var succ: this.getGraph().getSuccessors(data.node())) {
                if (!succ.node().is(IGCOperator.class))
                    continue;
                IGCOperator other = succ.node().to(IGCOperator.class);
                if (other == retain || this.grouped.contains(other))
                    continue;
                if (!other.data().equals(data) || !canMerge(retain, other))
                    continue;
                common.add(other);
                this.grouped.add(other);
            }
            if (common.size() > 1) {
                this.shareLeftInput.add(common);
            }
        }

        @Override
        public void postorder(DBSPIntegrateTraceRetainKeysOperator retain) {
            this.collect(retain);
        }

        @Override
        public void postorder(DBSPIntegrateTraceRetainValuesOperator retain) {
            this.collect(retain);
        }

        @Override
        public void postorder(DBSPIntegrateTraceRetainNValuesOperator retain) {
            this.collect(retain);
        }
    }

    /** Merge the groups of GC operators found by {@link FindMultipleRetains} */
    static class MergeRetain extends CircuitCloneVisitor {
        /** Keep a counter and a list; offer a method to decrement counter */
        static class ListCounter<T> {
            int counter;
            public final List<T> list;

            ListCounter(List<T> data) {
                this.counter = data.size();
                this.list = data;
            }

            boolean decrement() {
                this.counter--;
                return this.counter == 0;
            }
        }

        /** Key: a GC operator to merge.  Value: its group, with a counter of the group members not yet visited. */
        final Map<IGCOperator, ListCounter<IGCOperator>> toMerge;
        final FindMultipleRetains fmr;

        /** One retain-keys operator driven by the minimum of the bounds of {@code operators}. */
        DBSPIntegrateTraceRetainKeysOperator mergeRetainKeys(
                List<DBSPIntegrateTraceRetainKeysOperator> operators) {
            DBSPIntegrateTraceRetainKeysOperator first = operators.get(0);
            OutputPort left = this.mapped(first.left());
            List<OutputPort> rights = Linq.map(operators, o -> this.mapped(o.right()));
            OutputPort apply = InsertLimiters.createMinBound(this.compiler, rights);
            this.addOperator(apply.node());
            return new DBSPIntegrateTraceRetainKeysOperator(
                    first.getRelNode(), first.getClosureFunction(), left, apply, first.accumulate);
        }

        /** One retain-values operator that keeps a value when any of {@code operators} keeps it.
         * Its control is the tuple of the controls of {@code operators}. */
        DBSPIntegrateTraceRetainValuesOperator mergeRetainValues(
            List<DBSPIntegrateTraceRetainValuesOperator> operators) {
            DBSPIntegrateTraceRetainValuesOperator first = operators.get(0);
            CalciteRelNode node = first.getRelNode();
            OutputPort data = this.mapped(first.left());
            List<OutputPort> controls = Linq.map(operators, o -> this.mapped(o.right()));

            List<DBSPVariablePath> controlVars = Linq.map(controls, c -> c.outputType().ref().var());
            List<DBSPExpression> controlFields = Linq.map(controlVars, v -> v.deref().applyCloneIfNeeded());
            DBSPClosureExpression tuple = new DBSPTupleExpression(controlFields.toArray(new DBSPExpression[0]))
                    .closure(controlVars.toArray(new DBSPVariablePath[0]));
            DBSPApplyNOperator control = new DBSPApplyNOperator(node, tuple, controls);
            this.addOperator(control);

            DBSPType valueType = data.getOutputIndexedZSetType().elementType;
            DBSPVariablePath value = valueType.ref().var();
            DBSPVariablePath bounds = control.outputType().ref().var();
            List<DBSPExpression> keeps = new ArrayList<>();
            for (int i = 0; i < operators.size(); i++) {
                DBSPClosureExpression keep = operators.get(i).getClosureFunction();
                DBSPExpression bound = bounds.deepCopy().deref().field(i).borrow();
                keeps.add(keep.call(value.deepCopy(), bound).reduce(this.compiler));
            }
            DBSPExpression any = ExpressionCompiler.makeBinaryExpressions(
                    node, keeps.get(0).getType(), DBSPOpcode.OR, keeps);
            return new DBSPIntegrateTraceRetainValuesOperator(
                    node, any.closure(value, bounds), data, control.outputPort());
        }

        /** Drop the operators of {@code operators} whose policy repeats an earlier one.
         * @param operators  GC operators that read the same data.
         * @return  The first operator of each retention policy, in the order of {@code operators}; an
         *          operator that retains the same data with the same bound as an earlier one is
         *          left out. */
        static List<IGCOperator> keepOperatorsWithDistinctPolicies(List<IGCOperator> operators) {
            List<IGCOperator> result = new ArrayList<>();
            for (IGCOperator operator : operators) {
                if (!Linq.any(result, kept -> FindEquivalentNoops.samePolicy(kept, operator)))
                    result.add(operator);
            }
            return result;
        }

        /** Create one GC operator that replaces {@code operators}; all operators read the same data.
         * @param operators  A group found by {@link FindMultipleRetains}: at least two GC operators
         *                   of the same class, all {@link DBSPIntegrateTraceRetainKeysOperator}s,
         *                   all {@link DBSPIntegrateTraceRetainValuesOperator}s, or all
         *                   {@link DBSPIntegrateTraceRetainNValuesOperator}s, that read the same data.
         * @return  The first operator rebuilt on the mapped inputs, when all the policies are the
         *          same; otherwise a retain-keys operator driven by the minimum of the distinct bounds,
         *          or a retain-values operator that keeps a value when any distinct policy keeps it. */
        DBSPSimpleOperator merge(List<IGCOperator> operators) {
            Utilities.enforce(operators.size() > 1);
            Class<?> kind = operators.get(0).getClass();
            Utilities.enforce(Linq.all(operators, o -> o.getClass() == kind),
                    () -> "GC operators of different kinds in one group: " + operators);
            List<IGCOperator> distinct = keepOperatorsWithDistinctPolicies(operators);
            if (distinct.size() == 1) {
                // Always the case for RetainNValues operators, which are grouped only when identical
                DBSPSimpleOperator first = distinct.get(0).asOperator();
                return first.withInputs(Linq.map(first.inputs, this::mapped), false).to(DBSPSimpleOperator.class);
            }
            if (distinct.get(0).is(DBSPIntegrateTraceRetainKeysOperator.class))
                return this.mergeRetainKeys(Linq.map(distinct, o -> o.to(DBSPIntegrateTraceRetainKeysOperator.class)));
            Utilities.enforce(distinct.get(0).is(DBSPIntegrateTraceRetainValuesOperator.class),
                    () -> "Cannot merge " + distinct);
            return this.mergeRetainValues(Linq.map(distinct, o -> o.to(DBSPIntegrateTraceRetainValuesOperator.class)));
        }

        public MergeRetain(DBSPCompiler compiler, FindMultipleRetains fmr) {
            super(compiler, false);
            this.toMerge = new HashMap<>();
            this.fmr = fmr;
        }

        @Override
        public Token startVisit(IDBSPOuterNode circuit) {
            for (var l: this.fmr.shareLeftInput) {
                ListCounter<IGCOperator> list = new ListCounter<>(l);
                for (var e: l) {
                    Utilities.putNew(this.toMerge, e, list);
                }
            }
            return super.startVisit(circuit);
        }

        void process(IGCOperator op) {
            var listCounter = Utilities.getExists(this.toMerge, op);
            boolean done = listCounter.decrement();
            if (!done) {
                // DO NOT PROCESS, it will be deleted
                return;
            }

            // Create the replacement only when the last element in the group has been processed.
            // This ensures that all their inputs have been processed as well.
            DBSPSimpleOperator merge = this.merge(listCounter.list);
            this.map(op.asOperator(), merge);
        }

        @Override
        public void postorder(DBSPIntegrateTraceRetainKeysOperator op) {
            if (this.toMerge.containsKey(op))
                this.process(op);
            else
                super.postorder(op);
        }

        @Override
        public void postorder(DBSPIntegrateTraceRetainValuesOperator op) {
            if (this.toMerge.containsKey(op))
                this.process(op);
            else
                super.postorder(op);
        }

        @Override
        public void postorder(DBSPIntegrateTraceRetainNValuesOperator op) {
            if (this.toMerge.containsKey(op))
                this.process(op);
            else
                super.postorder(op);
        }
    }

    /** Replace each noop with its canonical representative and remove the GC operators
     * whose shared integral is not garbage collected. */
    static class MergeNoops extends CSE.RemoveCSE {
        final Set<DBSPOperator> removed;

        MergeNoops(DBSPCompiler compiler, Map<DBSPOperator, DBSPOperator> canonical, Set<DBSPOperator> removed) {
            super(compiler, canonical);
            this.removed = removed;
        }

        @Override
        public void replace(DBSPSimpleOperator operator) {
            if (this.removed.contains(operator))
                return;
            super.replace(operator);
        }
    }

    public MergeGC(DBSPCompiler compiler) {
        super("MergeGC", compiler);
        // Merge the noops whose consumers can share one integral
        Graph graphs = new Graph(compiler);
        this.add(graphs);
        FindEquivalentNoops find = new FindEquivalentNoops(compiler, graphs.getGraphs());
        this.add(find);
        this.add(new MergeNoops(compiler, find.canonical, find.removed));

        // Merge retainKey and retainValues
        Graph graphs1 = new Graph(compiler);
        this.add(graphs1);
        FindMultipleRetains findRetain = new FindMultipleRetains(compiler, graphs1.getGraphs());
        this.add(findRetain);
        this.add(new MergeRetain(compiler, findRetain));
    }
}
