package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.IGCOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.ir.IDBSPOuterNode;

import java.util.function.Predicate;

/** Debugging helper.
 * Dumps the circuit topology as text, one line per operator, in visit order:
 * <pre>
 * === at X ===
 *   748 JoinIndex &lt;- [747:0, 731:0]
 *   749 IntegrateTraceRetainNValues &lt;- [748:0, 745:0]  GC
 * </pre>
 * Insert it between passes to inspect the graph without generating images:
 * <pre>
 * this.add(new DumpTopology(compiler, "at X"));
 * </pre>
 */
public class DumpTopology extends CircuitVisitor {
    private final String label;
    /** Restrict dump to operators selected by this predicate */
    private final Predicate<DBSPOperator> filter;

    public DumpTopology(DBSPCompiler compiler, String label) {
        this(compiler, label, op -> true);
    }

    public DumpTopology(DBSPCompiler compiler, String label, Predicate<DBSPOperator> filter) {
        super(compiler);
        this.label = label;
        this.filter = filter;
    }

    /** Dumps only the garbage-collection operators and their sources. */
    public static DumpTopology gcOnly(DBSPCompiler compiler, String label) {
        return new DumpTopology(compiler, label, op -> op.is(IGCOperator.class));
    }

    @Override
    public Token startVisit(IDBSPOuterNode node) {
        System.out.println("=== " + this.label + " ===");
        return super.startVisit(node);
    }

    @Override
    public void postorder(DBSPOperator operator) {
        if (!this.filter.test(operator))
            return;
        StringBuilder line = new StringBuilder();
        line.append("  ").append(operator.id).append(" ")
                .append(operator.getClass().getSimpleName().replace("DBSP", "").replace("Operator", ""))
                .append(" <- [");
        boolean first = true;
        for (OutputPort input : operator.inputs) {
            if (!first)
                line.append(", ");
            first = false;
            line.append(input.node().id).append(":").append(input.port());
        }
        line.append("]");
        if (operator.is(IGCOperator.class))
            line.append("  GC");
        System.out.println(line);
    }

    @Override
    public String toString() {
        return "DumpTopology(" + this.label + ")";
    }
}
