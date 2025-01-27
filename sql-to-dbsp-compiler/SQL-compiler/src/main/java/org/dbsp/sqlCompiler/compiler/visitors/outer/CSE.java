package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.ICircuit;
import org.dbsp.sqlCompiler.circuit.operator.DBSPConstantOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDeltaOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIntegrateTraceRetainKeysOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIntegrateTraceRetainValuesOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPNestedOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperatorWithError;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.util.Logger;
import org.dbsp.util.graph.Port;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Common-subexpression elimination */
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
        final Set<DBSPConstantOperator> constants;

        public FindCSE(DBSPCompiler compiler, CircuitGraphs graphs,
                       Map<DBSPOperator, DBSPOperator> canonical) {
            super(compiler, graphs);
            this.canonical = canonical;
            this.constants = new HashSet<>();
        }

        @Override
        public void postorder(DBSPConstantOperator operator) {
            for (DBSPConstantOperator op: this.constants) {
                if (op.equivalent(operator)) {
                    this.setCanonical(operator, op);
                } else {
                    this.constants.add(op);
                }
            }
        }

        boolean hasGcSuccessor(DBSPOperator operator) {
            for (Port<DBSPOperator> succ: this.getGraph().getSuccessors(operator)) {
                if (succ.node().is(DBSPIntegrateTraceRetainKeysOperator.class) ||
                        succ.node().is(DBSPIntegrateTraceRetainValuesOperator.class))
                    // only input 0 of these operators affects the GC
                    return succ.port() == 0;
            }
            return false;
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
                if (hasGcSuccessor(base))
                    continue;
                for (int j = i + 1; j < destinations.size(); j++) {
                    DBSPOperator compare = destinations.get(j).node();
                    if (this.canonical.containsKey(compare))
                        // Already found a canonical representative
                        continue;
                    if (compare == base)
                        // E.g., a join where both inputs come from the same source
                        continue;
                    if (hasGcSuccessor(compare))
                        continue;
                    // Do not CSE something which is followed by a GC operator
                    if (base.equivalent(compare)) {
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
            if (replacement == null) {
                super.replace(operator);
                return;
            }
            OutputPort newReplacement = this.mapped(replacement.to(DBSPSimpleOperator.class).outputPort());
            this.map(operator.outputPort(), newReplacement, false);
        }

        @Override
        public void replace(DBSPOperatorWithError operator) {
            DBSPOperator replacement = this.canonical.get(operator);
            if (replacement == null) {
                super.replace(operator);
                return;
            }

            DBSPOperatorWithError we = replacement.to(DBSPOperatorWithError.class);
            this.map(operator, we, true);
        }
    }
}
