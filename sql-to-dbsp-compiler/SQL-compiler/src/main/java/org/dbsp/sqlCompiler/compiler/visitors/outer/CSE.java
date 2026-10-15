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
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPZSetExpression;
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
        final List<DBSPConstantOperator> constants;

        public FindCSE(DBSPCompiler compiler, CircuitGraphs graphs,
                       Map<DBSPOperator, DBSPOperator> canonical) {
            super(compiler, graphs);
            this.canonical = canonical;
            this.constants = new ArrayList<>();
        }

        /** True if two constants have the same type and the same values, compared as expressions,
         * with the same weights and in the same order. */
        static boolean sameRows(DBSPConstantOperator left, DBSPConstantOperator right) {
            if (left.isMultiset != right.isMultiset || !left.outputType.sameType(right.outputType))
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

        @Override
        public void postorder(DBSPConstantOperator operator) {
            for (DBSPConstantOperator op: this.constants) {
                if (op.equivalent(operator) || sameRows(op, operator)) {
                    this.setCanonical(operator, op);
                    return;
                }
            }
            this.constants.add(operator);
            postorder(operator.to(DBSPOperator.class));
        }

        /** The GC operators that read {@code operator} on input 0; only that input affects the GC. */
        List<DBSPOperator> gcSuccessors(DBSPOperator operator) {
            List<DBSPOperator> result = new ArrayList<>();
            for (Port<DBSPOperator> succ: this.getGraph().getSuccessors(operator)) {
                if (succ.node().is(IGCOperator.class) && succ.port() == 0)
                    result.add(succ.node());
            }
            return result;
        }

        /** True if {@code gc} would be equivalent to {@code other} if {@code other} read input 0 of {@code gc}. */
        static boolean equivalentGc(DBSPOperator gc, DBSPOperator other) {
            List<OutputPort> inputs = new ArrayList<>(other.inputs);
            inputs.set(0, gc.inputs.get(0));
            DBSPOperator moved = other.withInputs(inputs, false);
            return gc.equivalent(moved);
        }

        /** True if each GC operator in {@code left} has an equivalent GC operator in {@code right}, and the
         * other way around.  Two equivalent operators can share a trace only if it is garbage-collected
         * in the same way for both. */
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
        }

        @Override
        public void postorder(DBSPOperator operator) {
            List<Port<DBSPOperator>> destinations = this.getGraph().getSuccessors(operator);
            // Compare every pair of destinations
            for (int i = 0; i < destinations.size(); i++) {
                DBSPOperator base = destinations.get(i).node();
                List<DBSPOperator> baseGc = this.gcSuccessors(base);
                for (int j = i + 1; j < destinations.size(); j++) {
                    DBSPOperator compare = destinations.get(j).node();
                    if (this.canonical.containsKey(compare))
                        // Already found a canonical representative
                        continue;
                    if (compare == base)
                        // E.g., a join where both inputs come from the same source
                        continue;
                    // The next round will CSE the GC operators as well
                    if (base.equivalent(compare) && this.sameGc(baseGc, this.gcSuccessors(compare))) {
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
            DBSPOperator replacement = this.canonical.get(operator);
            if (replacement == null || replacement == operator) {
                super.replace(operator);
                return;
            }
            OutputPort newReplacement = this.mapped(replacement.to(DBSPSimpleOperator.class).outputPort());
            this.map(operator.outputPort(), newReplacement, false);
        }

        @Override
        public void replaceMultiOutput(IMultiOutput operator) {
            DBSPOperator replacement = this.canonical.get(operator.asOperator());
            if (replacement == null) {
                super.replaceMultiOutput(operator);
                return;
            }

            IMultiOutput we = replacement.to(IMultiOutput.class);
            this.map(operator, we, true);
        }
    }
}
