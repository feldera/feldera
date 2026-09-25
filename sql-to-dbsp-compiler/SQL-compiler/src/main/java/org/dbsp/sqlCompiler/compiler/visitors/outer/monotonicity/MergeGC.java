package org.dbsp.sqlCompiler.compiler.visitors.outer.monotonicity;

import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIntegrateTraceRetainKeysOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPNoopOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.circuit.operator.IGCOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CSE;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitCloneVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitGraphs;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitWithGraphsVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.outer.Graph;
import org.dbsp.sqlCompiler.compiler.visitors.outer.Passes;
import org.dbsp.sqlCompiler.ir.IDBSPOuterNode;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTuple;
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

/** Look for the following pattern:
 * source -> noop -> retain
 *        -> noop -> retain
 * Where the two retain operators are equivalent.
 * Replace this with a single chain.
 *
 * <p>Replace the following pattern
 * source -> retainKey1
 *        -> retainKey2
 * With source -> retainKey (min of control inputs)
 */
public class MergeGC extends Passes {
    /** This is a modified form of {@link CSE.FindCSE} */
    static class FindEquivalentNoops extends CircuitWithGraphsVisitor {
        /** Maps each operator to its canonical representative */
        public final Map<DBSPOperator, DBSPOperator> canonical;

        public FindEquivalentNoops(DBSPCompiler compiler, CircuitGraphs graphs) {
            super(compiler, graphs);
            this.canonical = new HashMap<>();
        }

        @Override
        public Token startVisit(IDBSPOuterNode node) {
            this.canonical.clear();
            return super.startVisit(node);
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

        @Nullable
        DBSPSimpleOperator getSingleGcSuccessor(DBSPOperator operator) {
            if (!operator.is(DBSPNoopOperator.class))
                return null;
            List<Port<DBSPOperator>> baseDests = Linq.where(
                    this.getGraph().getSuccessors(operator),
                    p -> (p.node().is(IGCOperator.class) && p.port() == 0));
            if (baseDests.size() != 1)
                return null;
            Port<DBSPOperator> port = baseDests.get(0);
            return port.node().to(DBSPSimpleOperator.class);
        }

        @Override
        public void postorder(DBSPSimpleOperator operator) {
            List<Port<DBSPOperator>> destinations = this.getGraph().getSuccessors(operator);
            // Compare every pair of destinations
            for (int i = 0; i < destinations.size(); i++) {
                DBSPOperator base = destinations.get(i).node();
                DBSPSimpleOperator gc0 = this.getSingleGcSuccessor(base);
                if (gc0 == null)
                    continue;

                for (int j = i + 1; j < destinations.size(); j++) {
                    DBSPOperator compare = destinations.get(j).node();
                    if (this.canonical.containsKey(compare))
                        // Already found a canonical representative
                        continue;
                    if (!base.equivalent(compare))
                        continue;

                    DBSPSimpleOperator gc1 = this.getSingleGcSuccessor(compare);
                    if (gc1 == null)
                        continue;

                    // The data inputs of gc0 and gc1 are different noops, so compare gc0 with gc1
                    // rebuilt on the inputs of gc0, after checking that the bounds are the same:
                    // the merged trace keeps gc0 only.
                    if (sameStream(gc0.inputs.get(1), gc1.inputs.get(1)) &&
                            gc0.equivalent(gc1.withInputs(gc0.inputs, false))) {
                        Logger.INSTANCE.belowLevel(this, 1)
                                .append("MergeGC ")
                                .appendSupplier(compare::toString)
                                .append(" -> ")
                                .appendSupplier(base::toString)
                                .newline()
                                .appendSupplier(gc1::toString)
                                .append(" -> ")
                                .appendSupplier(gc0::toString);
                        this.canonical.put(compare, base);
                        this.canonical.put(gc1, gc0);
                    }
                }
            }
        }
    }

    /** Find multiple {@link DBSPIntegrateTraceRetainKeysOperator} that apply the same retention
     * policy to the same data. */
    static class FindMultipleRetainKeys extends CircuitWithGraphsVisitor {
        final List<List<DBSPIntegrateTraceRetainKeysOperator>> shareLeftInput;
        final Set<DBSPIntegrateTraceRetainKeysOperator> visited;

        FindMultipleRetainKeys(DBSPCompiler compiler, CircuitGraphs graphs) {
            super(compiler, graphs);
            this.shareLeftInput = new ArrayList<>();
            this.visited = new HashSet<>();
        }

        /** A single operator driven by the minimum of two bounds can replace two retain-keys
         * operators only when they retain the same data with the same comparison. */
        static boolean samePolicy(DBSPIntegrateTraceRetainKeysOperator a, DBSPIntegrateTraceRetainKeysOperator b) {
            return a.left().equals(b.left()) &&
                    a.accumulate == b.accumulate &&
                    a.getFunction().equivalent(b.getFunction());
        }

        @Override
        public void postorder(DBSPIntegrateTraceRetainKeysOperator retain) {
            if (this.visited.contains(retain))
                return;
            List<DBSPIntegrateTraceRetainKeysOperator> common = new ArrayList<>();
            common.add(retain);
            this.visited.add(retain);
            for (var succ: this.getGraph().getSuccessors(retain.left().node())) {
                var other = succ.node().as(DBSPIntegrateTraceRetainKeysOperator.class);
                if (other == null || other == retain || this.visited.contains(other))
                    continue;
                if (!samePolicy(retain, other))
                    continue;
                common.add(other);
                this.visited.add(other);
            }
            if (common.size() > 1) {
                this.shareLeftInput.add(common);
            }
        }
    }

    /** Merge multiple {@link DBSPIntegrateTraceRetainKeysOperator}s which share their left input */
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

        final Map<DBSPIntegrateTraceRetainKeysOperator, ListCounter<DBSPIntegrateTraceRetainKeysOperator>> toMerge;
        final FindMultipleRetainKeys fmk;

        DBSPIntegrateTraceRetainKeysOperator merge(List<DBSPIntegrateTraceRetainKeysOperator> operators) {
            // The shared operators must all have the same left input.
            Utilities.enforce(operators.size() > 1);
            DBSPIntegrateTraceRetainKeysOperator first = operators.get(0);
            OutputPort left = this.mapped(first.left());
            List<OutputPort> rights = Linq.map(operators, o -> this.mapped(o.right()));

            List<DBSPVariablePath> variables = new ArrayList<>();
            for (var r: rights)
                variables.add(r.outputType().to(DBSPTypeTuple.class).getFieldType(1).ref().var());

            List<DBSPExpression> dataFields = Linq.map(variables, DBSPExpression::deref);
            DBSPVariablePath[] vars = variables.toArray(new DBSPVariablePath[0]);
            DBSPClosureExpression min = InsertLimiters.combineMin(dataFields).closure(vars);
            OutputPort apply = InsertLimiters.createApplyN(this.compiler, rights, min);
            this.addOperator(apply.node());
            return new DBSPIntegrateTraceRetainKeysOperator(
                    first.getRelNode(), first.getClosureFunction(), left, apply, first.accumulate);
        }

        public MergeRetain(DBSPCompiler compiler, FindMultipleRetainKeys fmk) {
            super(compiler, false);
            this.toMerge = new HashMap<>();
            this.fmk = fmk;
        }

        @Override
        public Token startVisit(IDBSPOuterNode circuit) {
            for (var l: this.fmk.shareLeftInput) {
                ListCounter<DBSPIntegrateTraceRetainKeysOperator> list = new ListCounter<>(l);
                for (var e: l) {
                    Utilities.putNew(this.toMerge, e, list);
                }
            }
            return super.startVisit(circuit);
        }

        @Override
        public void postorder(DBSPIntegrateTraceRetainKeysOperator op) {
            if (!this.toMerge.containsKey(op)) {
                super.postorder(op);
                return;
            }
            var listCounter = Utilities.getExists(this.toMerge, op);
            boolean done = listCounter.decrement();
            if (!done) {
                // DO NOT PROCESS, it will be deleted
                return;
            }

            // Create the replacement only when the last element in the list of equivalent
            // retain-key operators has been processed.  This ensures that all their
            // inputs have been processed as well.
            DBSPIntegrateTraceRetainKeysOperator merge = this.merge(listCounter.list);
            this.map(op, merge);
        }
    }

    public MergeGC(DBSPCompiler compiler) {
        super("MergeGC", compiler);
        // Remove redundant noops
        Graph graphs = new Graph(compiler);
        this.add(graphs);
        FindEquivalentNoops find = new FindEquivalentNoops(compiler, graphs.getGraphs());
        this.add(find);
        this.add(new CSE.RemoveCSE(compiler, find.canonical));

        // Merge retainKey
        Graph graphs1 = new Graph(compiler);
        this.add(graphs1);
        FindMultipleRetainKeys findRetain = new FindMultipleRetainKeys(compiler, graphs1.getGraphs());
        this.add(findRetain);
        this.add(new MergeRetain(compiler, findRetain));
    }
}
