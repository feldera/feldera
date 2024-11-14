package org.dbsp.sqlCompiler.compiler.visitors.outer.monotonicity;

import org.dbsp.sqlCompiler.circuit.operator.DBSPNoopOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.circuit.operator.GCOperator;
import org.dbsp.sqlCompiler.compiler.IErrorReporter;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CSE;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitGraph;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitGraphs;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitWithGraphsVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.outer.Passes;
import org.dbsp.sqlCompiler.ir.IDBSPOuterNode;
import org.dbsp.util.Linq;
import org.dbsp.util.Logger;
import org.dbsp.util.graph.Port;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Look for the following pattern:
 * source -> noop -> retain
 *        -> noop -> retain
 * Where the two retain operators are equivalent.
 * Replace this with a single chain.
 */
public class MergeGC extends Passes {
    /** This is a modified form of {@link CSE.FindCSE} */
    static class FindEquivalentNoops extends CircuitWithGraphsVisitor {
        /** Maps each operator to its canonical representative */
        public final Map<DBSPOperator, DBSPOperator> canonical;

        public FindEquivalentNoops(IErrorReporter errorReporter, CircuitGraphs graphs) {
            super(errorReporter, graphs);
            this.canonical = new HashMap<>();
        }

        @Override
        public Token startVisit(IDBSPOuterNode node) {
            this.canonical.clear();
            return super.startVisit(node);
        }

        @Nullable
        DBSPSimpleOperator getSingleGcSuccessor(DBSPOperator operator) {
            if (!operator.is(DBSPNoopOperator.class))
                return null;
            List<Port<DBSPOperator>> baseDests = Linq.where(
                    this.getGraph().getSuccessors(operator),
                    p -> (p.node().is(GCOperator.class) && p.port() == 0));
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

                    // Cannot call directly gc0.equivalent(gc1),
                    // since that requires them to already have the same inputs.
                    if (gc0.getFunction().equivalent(gc1.getFunction())) {
                        Logger.INSTANCE.belowLevel(this, 1)
                                .append("MergeGC ")
                                .append(compare.toString())
                                .append(" -> ")
                                .append(base.toString())
                                .newline()
                                .append(gc1.toString())
                                .append(" -> ")
                                .append(gc0.toString());
                        this.canonical.put(compare, base);
                        this.canonical.put(gc1, gc0);
                    }
                }
            }
        }
    }

    public MergeGC(IErrorReporter reporter, CircuitGraphs graphs) {
        super("MergeGC", reporter);
        FindEquivalentNoops find = new FindEquivalentNoops(reporter, graphs);
        this.add(find);
        this.add(new CSE.RemoveCSE(reporter, find.canonical));
    }
}
