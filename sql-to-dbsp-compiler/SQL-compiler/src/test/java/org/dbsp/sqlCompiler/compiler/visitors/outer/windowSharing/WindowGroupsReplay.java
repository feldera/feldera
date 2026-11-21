package org.dbsp.sqlCompiler.compiler.visitors.outer.windowSharing;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.dbsp.sqlCompiler.circuit.operator.DBSPWindowOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.inner.CanonicalForm;
import org.dbsp.sqlCompiler.ir.IDBSPOuterNode;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.util.Utilities;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.function.Function;

/** Use Rust's grouping and field order, retaining Java's typed closure construction. */
public final class WindowGroupsReplay extends FindWindowGroups {
    private final LinkedHashMap<Long, Candidate> candidates = new LinkedHashMap<>();
    private final Function<ObjectNode, JsonNode> select;

    public WindowGroupsReplay(DBSPCompiler compiler, Function<ObjectNode, JsonNode> select) {
        super(compiler);
        this.select = select;
    }

    @Override
    public Token startVisit(IDBSPOuterNode node) {
        this.candidates.clear();
        this.groups.clear();
        return super.startVisit(node);
    }

    @Override
    public void postorder(DBSPWindowOperator window) {
        var candidate = this.analyze(window);
        if (candidate != null)
            Utilities.putNew(this.candidates, window.id, candidate);
    }

    @Override
    public void endVisit() {
        int threshold = this.compiler().metadata.windowSharingDisabled()
                ? 0 : this.compiler().metadata.windowSharingThreshold();
        var request = Utilities.deterministicObjectMapper().createObjectNode().put("threshold", threshold);
        var wire = request.putArray("candidates");
        for (var candidate: this.candidates.values()) {
            ArrayNode fields = wire.addObject().put("window", candidate.window().id)
                    .put("input", candidate.inputFingerprint(this.compiler()))
                    .putArray("fields");
            for (var field: candidate.valueFields())
                fields.add(CanonicalForm.asString(this.compiler(), field));
        }
        JsonNode response = this.select.apply(request);
        if (!response.isArray())
            throw new IllegalArgumentException("Invalid window grouping result");
        for (var entry: response) {
            List<WindowGroup.Member> members = new ArrayList<>();
            List<DBSPClosureExpression> shared = new ArrayList<>();
            for (var field: entry.required("shared"))
                shared.add(this.candidate(field).valueFields().get(field.required("ordinal").intValue()));
            Candidate first = null;
            for (var member: entry.required("members")) {
                Candidate candidate = this.candidate(member);
                if (first == null)
                    first = candidate;
                var indices = member.required("fields");
                int[] fields = new int[indices.size()];
                for (int i = 0; i < fields.length; i++)
                    fields[i] = indices.get(i).intValue();
                members.add(new WindowGroup.Member(candidate.window(), fields));
            }
            if (first == null || threshold == 0 || members.size() < threshold)
                throw new IllegalArgumentException("Window group does not meet its threshold");
            var result = new WindowGroup(first.port(), first.closure(), shared, members);
            for (var member: members)
                Utilities.putNew(this.groups, member.window(), result);
        }
        super.endVisit();
    }

    private Candidate candidate(JsonNode field) {
        return Utilities.getExists(this.candidates, field.required("window").longValue());
    }
}
