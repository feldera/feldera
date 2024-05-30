package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.operator.DBSPConstantOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.compiler.IErrorReporter;
import org.dbsp.util.Logger;

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
     * - remove them */
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
        final Graph.CircuitGraph graph;
        final Set<DBSPConstantOperator> constants;

        public FindCSE(IErrorReporter errorReporter, Graph.CircuitGraph graph,
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

        @Override
        public void postorder(DBSPOperator operator) {
            List<DBSPOperator> destinations = this.graph.edges.get(operator);
            // Compare every pair of destinations
            for (int i = 0; i < destinations.size(); i++) {
                DBSPOperator base = destinations.get(i);
                for (int j = i + 1; j < destinations.size(); j++) {
                    DBSPOperator compare = destinations.get(j);
                    if (this.canonical.containsKey(compare))
                        // Already found a canonical representative
                        continue;
                    if (compare == base)
                        // E.g., a join where both inputs come from the same source
                        continue;
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
    static class RemoveCSE extends CircuitCloneVisitor {
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
            this.map(operator, replacement, false);
        }
    }
}
