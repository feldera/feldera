package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.ICircuit;
import org.dbsp.sqlCompiler.circuit.operator.IMultiOutput;
import org.dbsp.sqlCompiler.circuit.operator.DBSPConstantOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDeltaOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPNestedOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.circuit.operator.IGCOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIncrementalConstantOperator;
import org.dbsp.sqlCompiler.circuit.operator.ISplitInput;
import org.dbsp.sqlCompiler.circuit.operator.ISplitOutput;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPZSetExpression;
import org.dbsp.util.Linq;
import org.dbsp.util.Logger;
import org.dbsp.util.graph.Port;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Common-subexpression elimination at the level of circuit operators */
public class CSE extends Repeat {
    public CSE(DBSPCompiler compiler) {
        super(compiler, new OneCSEPass(compiler));
    }

    /** One CSE pass:
     * - build circuit graph,
     * - find common subexpressions,
     * - remove them. */
    static class OneCSEPass extends Passes {
        /** Maps each operator to its canonical representative */
        final Map<DBSPOperator, DBSPOperator> canonical = new HashMap<>();

        OneCSEPass(DBSPCompiler compiler) {
            super("CSE", compiler);
            Graph graph = new Graph(compiler);
            this.add(graph);
            this.add(new FindCSE(compiler, graph.graphs, this.canonical));
            this.add(new RemoveCSE(compiler, this.canonical));
        }
    }

    /** Find common subexpressions, write them into the 'canonical' map */
    public static class FindCSE extends CircuitWithGraphsVisitor {
        /** Maps each operator to its canonical representative */
        final Map<DBSPOperator, DBSPOperator> canonical;
        /** Constants and incremental constants: sources, which no common input links */
        final List<DBSPSimpleOperator> constants;

        public FindCSE(DBSPCompiler compiler, CircuitGraphs graphs,
                       Map<DBSPOperator, DBSPOperator> canonical) {
            super(compiler, graphs);
            this.canonical = canonical;
            this.constants = new ArrayList<>();
        }

        /** True if two constants have the same type and the same values, compared as expressions,
         * with the same weights and in the same order. */
        static boolean sameRows(DBSPSimpleOperator left, DBSPSimpleOperator right) {
            if (!left.outputType.sameType(right.outputType))
                return false;
            DBSPZSetExpression leftRows = left.getFunction().as(DBSPZSetExpression.class);
            DBSPZSetExpression rightRows = right.getFunction().as(DBSPZSetExpression.class);
            if (leftRows == null || rightRows == null || leftRows.size() != rightRows.size())
                return false;
            var rightIterator = rightRows.data.entrySet().iterator();
            for (Map.Entry<DBSPExpression, Long> leftEntry: leftRows.data.entrySet()) {
                Map.Entry<DBSPExpression, Long> rightEntry = rightIterator.next();
                if (!leftEntry.getValue().equals(rightEntry.getValue()) ||
                        !leftEntry.getKey().equivalent(rightEntry.getKey()))
                    return false;
            }
            return true;
        }

        /** Maps 'operator' to an earlier constant of the same class with the same rows */
        void constant(DBSPSimpleOperator operator) {
            for (DBSPSimpleOperator op: this.constants) {
                if (op.getClass() == operator.getClass() && (op.equivalent(operator) || sameRows(op, operator))) {
                    this.setCanonical(operator, op);
                    return;
                }
            }
            this.constants.add(operator);
            postorder(operator.to(DBSPOperator.class));
        }

        @Override
        public void postorder(DBSPConstantOperator operator) {
            this.constant(operator);
        }

        @Override
        public void postorder(DBSPIncrementalConstantOperator operator) {
            this.constant(operator);
        }

        /** The GC operators that garbage-collect the integral after {@code port}. */
        List<DBSPOperator> gcSuccessors(OutputPort port) {
            List<DBSPOperator> result = new ArrayList<>();
            for (Port<DBSPOperator> succ: this.getGraph().getSuccessors(port.node())) {
                DBSPOperator node = succ.node();
                // A GC operator that reads 'port' on several inputs is a successor once per input
                if (node.is(IGCOperator.class) && node.to(IGCOperator.class).garbageCollects(port)
                        && !result.contains(node))
                    result.add(node);
            }
            return result;
        }

        /** For each output of {@code operator}, by output number, the GC operators that
         * garbage-collect the integral after that output. */
        List<List<DBSPOperator>> outputGcSuccessors(DBSPOperator operator) {
            List<List<DBSPOperator>> result = new ArrayList<>(operator.outputCount());
            for (int i = 0; i < operator.outputCount(); i++)
                result.add(this.gcSuccessors(operator.getOutput(i)));
            return result;
        }

        /** True if {@code gc} would be equivalent to {@code other} if {@code other} garbage-collected
         * the same data as {@code gc}. */
        static boolean equivalentGc(DBSPOperator gc, DBSPOperator other) {
            OutputPort data = gc.to(IGCOperator.class).data();
            OutputPort otherData = other.to(IGCOperator.class).data();
            List<OutputPort> inputs = new ArrayList<>(other.inputs);
            inputs.set(inputs.indexOf(otherData), data);
            DBSPOperator moved = other.withInputs(inputs, false);
            return gc.equivalent(moved);
        }

        /** True if each GC operator in {@code left} has an equivalent GC operator in {@code right}, and the
         * other way around. */
        private boolean sameGc(List<DBSPOperator> left, List<DBSPOperator> right) {
            if (left.size() != right.size())
                return false;
            List<DBSPOperator> unmatched = new ArrayList<>(right);
            for (DBSPOperator gc: left) {
                DBSPOperator match = null;
                for (DBSPOperator candidate: unmatched) {
                    if (equivalentGc(gc, candidate)) {
                        match = candidate;
                        break;
                    }
                }
                if (match == null)
                    return false;
                unmatched.remove(match);
            }
            return true;
        }

        /** True if each output of one operator is garbage-collected in the same way as the output
         * with the same number of the other operator.  Two equivalent operators can share their
         * traces only if they are garbage-collected in the same way for both.
         * @param left   Result of {@link #outputGcSuccessors} for one operator.
         * @param right  Result of {@link #outputGcSuccessors} for the other operator. */
        private boolean sameOutputGc(List<List<DBSPOperator>> left, List<List<DBSPOperator>> right) {
            if (left.size() != right.size())
                return false;
            return Linq.all(Linq.zipSameLength(left, right, this::sameGc));
        }

        @Override
        public void postorder(DBSPDeltaOperator operator) {
            // Compare with all other deltas in the same circuit
            ICircuit parent = this.getParent();
            DBSPNestedOperator nested = parent.to(DBSPNestedOperator.class);
            for (var delta: nested.deltaInputs) {
                if (operator == delta)
                    // Compare only with the previous ones
                    break;
                if (operator.input().equals(delta.input())) {
                    this.setCanonical(operator, delta);
                }
            }
        }

        void setCanonical(DBSPOperator operator, DBSPOperator canonical) {
            Logger.INSTANCE.belowLevel(this, 1)
                    .append("CSE ")
                    .appendSupplier(operator::toString)
                    .append(" -> ")
                    .appendSupplier(canonical::toString)
                    .newline();
            this.canonical.put(operator, canonical);
            ISplitInput split = operator.as(ISplitInput.class);
            if (split != null) {
                DBSPOperator outputHalf = split.splitOutput().outputPort().node();
                DBSPOperator canonicalOutputHalf = canonical.to(ISplitInput.class).splitOutput().outputPort().node();
                this.setCanonical(outputHalf, canonicalOutputHalf);
            }
        }

        @Override
        public void postorder(DBSPOperator operator) {
            List<Port<DBSPOperator>> destinations = this.getGraph().getSuccessors(operator);
            // Compare every pair of destinations
            for (int i = 0; i < destinations.size(); i++) {
                DBSPOperator base = destinations.get(i).node();
                List<List<DBSPOperator>> baseGc = this.outputGcSuccessors(base);
                for (int j = i + 1; j < destinations.size(); j++) {
                    DBSPOperator compare = destinations.get(j).node();
                    if (this.canonical.containsKey(compare))
                        // Already found a canonical representative
                        continue;
                    if (compare == base)
                        // E.g., a join where both inputs come from the same source
                        continue;
                    // The next round will CSE the GC operators as well
                    if (base.equivalent(compare) && this.sameOutputGc(baseGc, this.outputGcSuccessors(compare))) {
                        this.setCanonical(compare, base);
                    }
                }
            }
        }
    }

    /** Remove common subexpressions */
    public static class RemoveCSE extends CircuitCloneVisitor {
        /** Maps each operator to its canonical representative */
        final Map<DBSPOperator, DBSPOperator> canonical;

        public RemoveCSE(DBSPCompiler compiler, Map<DBSPOperator, DBSPOperator> canonical) {
            super(compiler, false);
            this.canonical = canonical;
        }

        @Override
        public void replace(DBSPSimpleOperator operator) {
            if (this.remap.containsKey(operator.outputPort()))
                // An output half that moved up to the position of an output half that it replaces
                return;
            DBSPOperator replacement = this.canonical.get(operator);
            if (replacement == null || replacement == operator) {
                super.replace(operator);
                return;
            }
            OutputPort kept = replacement.to(DBSPSimpleOperator.class).outputPort();
            if (kept.node().is(ISplitOutput.class) && !this.remap.containsKey(kept))
                // An output half has no inputs, so the kept one can move up to the position of
                // the one it replaces, which can precede it
                this.map(kept, kept, true);
            this.map(operator.outputPort(), this.mapped(kept), false);
        }

        @Override
        public void replaceMultiOutput(IMultiOutput operator) {
            DBSPOperator replacement = this.canonical.get(operator.asOperator());
            if (replacement == null) {
                super.replaceMultiOutput(operator);
                return;
            }
            for (int i = 0; i < operator.outputCount(); i++)
                this.map(operator.getOutput(i), this.mapped(replacement.getOutput(i)), false);
        }
    }
}
