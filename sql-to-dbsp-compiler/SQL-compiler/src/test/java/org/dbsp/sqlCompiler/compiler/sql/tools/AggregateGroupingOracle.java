package org.dbsp.sqlCompiler.compiler.sql.tools;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.circuit.operator.DBSPNestedOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamAggregateOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.backend.RustCircuitPass;
import org.dbsp.sqlCompiler.compiler.visitors.outer.AppendOnly;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitTransform;
import org.dbsp.sqlCompiler.ir.aggregate.IAggregate;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/** Compare the native grouping with each implementation's Java compatibility predicate
 * at the point where expansion consumes the completed append-only analysis. */
public final class AggregateGroupingOracle extends FrozenCircuitObserver {
    private int lists;
    private int mixed;
    private int appendOnlyLists;

    private AggregateGroupingOracle(DBSPCompiler compiler, ObjectNode record) {
        super(compiler, record);
    }

    private void compare(Iterable<DBSPOperator> operators, AppendOnly analysis) {
        for (var operator : operators) {
            if (operator instanceof DBSPNestedOperator nested)
                this.compare(nested.getAllOperators(), analysis);
            if (!(operator instanceof DBSPStreamAggregateOperator stream) || stream.aggregateList == null)
                continue;
            List<IAggregate> parts = stream.aggregateList.aggregates;
            if (parts.size() < 2)
                continue;
            boolean appendOnly = analysis.isAppendOnly(stream.input());
            List<List<Integer>> expected = new ArrayList<>();
            for (int i = 0; i < parts.size(); i++) {
                final var part = parts.get(i);
                var group = expected.stream().filter(g -> parts.get(g.get(g.size() - 1)).compatible(part, appendOnly))
                        .findFirst().orElseGet(() -> {
                            List<Integer> fresh = new ArrayList<>();
                            expected.add(fresh);
                            return fresh;
                        });
                group.add(i);
            }
            var actual = RustCircuitPass.aggregateGroups(parts, appendOnly);
            if (!actual.equals(expected))
                throw new AssertionError("Aggregate groups at " + stream.id + ": " + actual + " vs " + expected);
            this.lists++;
            if (expected.size() > 1)
                this.mixed++;
            if (appendOnly)
                this.appendOnlyLists++;
        }
    }

    @Override
    public void before(String container, int index, int iteration, CircuitTransform pass, DBSPCircuit circuit) { }

    @Override
    public void after(String container, int index, int iteration, CircuitTransform pass,
                      DBSPCircuit circuit, boolean completed) {
        if (pass instanceof AppendOnly analysis && container.equals("ExpandAggregates") && completed)
            this.compare(circuit.getAllOperators(), analysis);
    }

    @Override
    void finish() {
        this.observation.put("lists", this.lists).put("multiple_groups", this.mixed)
                .put("append_only_lists", this.appendOnlyLists);
    }

    public static void main(String[] args) throws IOException {
        run(Path.of(args[0]), AggregateGroupingOracle::new);
    }
}
