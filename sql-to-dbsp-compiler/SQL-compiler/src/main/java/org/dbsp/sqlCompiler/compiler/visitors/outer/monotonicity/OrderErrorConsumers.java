package org.dbsp.sqlCompiler.compiler.visitors.outer.monotonicity;

import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitTransform;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

/** Schedule error consumers last so the limiter pass has every late-row stream
 * when it reaches the error view. Both partitions preserve topological order. */
public final class OrderErrorConsumers implements CircuitTransform {
    private final Function<DBSPCircuit, Set<DBSPOperator>> analyze;
    private Set<DBSPOperator> reachable = Set.of();

    OrderErrorConsumers(Function<DBSPCircuit, Set<DBSPOperator>> analyze) {
        this.analyze = analyze;
    }

    public Set<DBSPOperator> reachable() {
        return this.reachable;
    }

    @Override
    public String getName() {
        return "OrderErrorConsumers";
    }

    @Override
    public DBSPCircuit apply(DBSPCircuit circuit) {
        this.reachable = this.analyze.apply(circuit);
        circuit.sortOperators(Comparator.comparing(this.reachable::contains));
        return circuit;
    }

    /** Resolve the Rust schedule to the same live operators used by InsertLimiters. */
    public void applyOrder(DBSPCircuit circuit, long[] order, int firstError) {
        if (order.length != circuit.allOperators.size() || firstError < 0 || firstError > order.length)
            throw new IllegalStateException("Invalid error-consumer partition");
        var byId = new HashMap<Long, DBSPOperator>();
        circuit.allOperators.forEach(op -> byId.put(op.id, op));
        var ordered = new ArrayList<DBSPOperator>(order.length);
        for (long id : order)
            ordered.add(Objects.requireNonNull(byId.remove(id), "Unknown or repeated error consumer"));
        this.reachable = new HashSet<>(ordered.subList(firstError, order.length));
        circuit.allOperators.clear();
        circuit.allOperators.addAll(ordered);
    }
}
