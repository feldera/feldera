package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.operator.DBSPConstantOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIntegrateTraceRetainKeysOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIntegrateTraceRetainValuesOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.compiler.IErrorReporter;
import org.dbsp.util.Logger;
import org.dbsp.util.graph.Port;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Common-subexpression elimination */
public class CSE extends Repeat {
    public CSE(IErrorReporter reporter) {
        super(reporter, new OneCSEPass(reporter));
    }

    /** One CSE pass:
     * - build circuit graph,
     * - find common subexpressions,
     * - remove them. */
    static class OneCSEPass extends Passes {
        /** Maps each operator to its canonical representative */
        final Map<DBSPOperator, DBSPOperator> canonical = new HashMap<>();

        OneCSEPass(IErrorReporter reporter) {
            super(reporter);
            Graph graph = new Graph(reporter);
            this.add(graph);
            this.add(new FindCSE(reporter, graph.graph, this.canonical));
            this.add(new RemoveCSE(reporter, this.canonical));
        }
    }

    /** Find common subexpressions, write them into the 'canonical' map */
    public static class FindCSE extends CircuitVisitor {
        /** Maps each operator to its canonical representative */
        final Map<DBSPOperator, DBSPOperator> canonical;
        final CircuitGraph graph;
        final Set<DBSPConstantOperator> constants;

        public FindCSE(IErrorReporter errorReporter, CircuitGraph graph,
                       Map<DBSPOperator, DBSPOperator> canonical) {
            super(errorReporter);
            this.graph = graph;
            this.canonical = canonical;
            this.constants = new HashSet<>();
        }

        @Override
        public void postorder(DBSPConstantOperator operator) {
            for (DBSPConstantOperator op: this.constants) {
                if (op.equivalent(operator)) {
                    this.canonical.put(operator, op);
                } else {
                    this.constants.add(op);
                }
            }
        }

        boolean hasGcSuccessor(DBSPOperator operator) {
            for (Port<DBSPOperator> succ: this.graph.getSuccessors(operator)) {
                if (succ.node.is(DBSPIntegrateTraceRetainKeysOperator.class) ||
                        succ.node.is(DBSPIntegrateTraceRetainValuesOperator.class))
                    // only input 0 of these operators affects the GC
                        return succ.port == 0;
            }
            return false;
        }

        @Override
        public void postorder(DBSPOperator operator) {
            List<Port<DBSPOperator>> destinations = this.graph.getSuccessors(operator);
            // Compare every pair of destinations
            for (int i = 0; i < destinations.size(); i++) {
                DBSPOperator base = destinations.get(i).node;
                if (hasGcSuccessor(base))
                    continue;
                for (int j = i + 1; j < destinations.size(); j++) {
                    DBSPOperator compare = destinations.get(j).node;
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
                        Logger.INSTANCE.belowLevel(this, 1)
                                .append("CSE ")
                                .append(compare.toString())
                                .append(" -> ")
                                .append(base.toString())
                                .newline();
                        this.canonical.put(compare, base);
                    }
                }
            }
        }
    }

    /** Remove common subexpressions */
    public static class RemoveCSE extends CircuitCloneVisitor {
        /** Maps each operator to its canonical representative */
        final Map<DBSPOperator, DBSPOperator> canonical;

        public RemoveCSE(IErrorReporter reporter, Map<DBSPOperator, DBSPOperator> canonical) {
            super(reporter, false);
            this.canonical = canonical;
        }

        @Override
        public void replace(DBSPOperator operator) {
            DBSPOperator replacement = this.canonical.get(operator);
            if (replacement == null) {
                super.replace(operator);
                return;
            }
            DBSPOperator newReplacement = this.mapped(replacement);
            this.map(operator, newReplacement, false);
        }
    }
}
