package org.dbsp.sqlCompiler.compiler.visitors.outer.indexSharing;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitTransform;

import java.util.Comparator;
import java.util.IdentityHashMap;

/** Capture the narrow inputs, the groups that survived widening and the
 * positions at which the shared value supplies each original column. */
public final class SharedIndexOracle {
    private SharedIndexOracle() {}

    public static void before(CircuitTransform step, ObjectNode state) {
        var pass = (ReplaceSharedIndexes) step;
        var candidates = state.putArray("candidate_indexes");
        for (var candidate : pass.finder.candidates) {
            var group = candidates.addArray();
            for (var member : candidate.members())
                group.addObject().put("index", member.index().id).put("fixed", member.requiresFixedIndex());
        }
    }

    public static void after(CircuitTransform step, ObjectNode state) {
        var pass = (ReplaceSharedIndexes) step;
        var members = new IdentityHashMap<WideMapIndexBuilder, Long[]>();
        pass.sharedInputs.forEach((consumer, inputs) -> inputs.byInput.forEach((slot, source) -> {
            var ids = members.computeIfAbsent(source.builder(), b -> new Long[b.outputIndexes.size()]);
            ids[source.consumerIndex()] = consumer.inputs.get(slot).operator.id;
        }));
        var layouts = state.putArray("wide_indexes");
        members.entrySet().stream().sorted(Comparator.comparingLong(entry -> entry.getValue()[0]))
                .forEach(entry -> {
                    var layout = layouts.addObject();
                    var ids = layout.putArray("indexes");
                    for (long id : entry.getValue())
                        ids.add(id);
                    var columns = layout.putArray("columns");
                    for (var row : entry.getKey().outputIndexes) {
                        var values = columns.addArray();
                        row.forEach(values::add);
                    }
                });
    }
}
