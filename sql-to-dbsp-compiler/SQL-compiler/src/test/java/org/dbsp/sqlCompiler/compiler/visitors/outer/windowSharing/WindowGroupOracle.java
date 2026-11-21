package org.dbsp.sqlCompiler.compiler.visitors.outer.windowSharing;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.circuit.operator.DBSPWindowOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.inner.CanonicalForm;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;

import java.util.LinkedHashSet;

/** Observe the analysis state that ImplementWindowGroups reads at the next boundary. */
public final class WindowGroupOracle {
    private WindowGroupOracle() {}

    public static void capture(DBSPCompiler compiler, DBSPCircuit circuit, FindWindowGroups analysis, ObjectNode record) {
        record.put("threshold", compiler.metadata.windowSharingDisabled() ? 0 : compiler.metadata.windowSharingThreshold());
        var candidates = record.putArray("window_candidates");
        new CircuitVisitor(compiler) {
            @Override
            public void postorder(DBSPWindowOperator window) {
                var candidate = analysis.analyze(window);
                if (candidate == null)
                    return;
                var entry = candidates.addObject().put("window", window.id)
                        .put("key", candidate.inputFingerprint(compiler));
                var fields = entry.putArray("fields");
                var identities = entry.putArray("expressions");
                if (analysis.groups.containsKey(window))
                    for (var field : candidate.valueFields()) {
                        fields.add(CanonicalForm.asString(compiler, field));
                        identities.addArray().add(field.body.id).add(field.parameters[0].id);
                    }
            }
        }.apply(circuit);
        var groups = record.putArray("window_groups");
        for (var group : new LinkedHashSet<>(analysis.groups.values())) {
            var entry = groups.addObject();
            var shared = entry.putArray("expressions");
            for (var field : group.leftValueFields)
                shared.addArray().add(field.body.id).add(field.parameters[0].id);
            var members = entry.putArray("members");
            for (var member : group.members) {
                var projection = members.addObject().put("window", member.window().id).putArray("fields");
                for (var index : member.fieldsUsed())
                    projection.add(index);
            }
        }
    }
}
