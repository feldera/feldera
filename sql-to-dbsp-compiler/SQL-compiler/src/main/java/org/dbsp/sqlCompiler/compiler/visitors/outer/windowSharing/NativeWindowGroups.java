package org.dbsp.sqlCompiler.compiler.visitors.outer.windowSharing;

import org.dbsp.sqlCompiler.circuit.operator.DBSPWindowOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.inner.CanonicalForm;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.util.Utilities;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Attach the Rust-selected groups to the live operators used by ImplementWindowGroups. */
public final class NativeWindowGroups {
    private NativeWindowGroups() {}

    public record Fields(int[][] sources, int[][] projections) {}

    public record Plan(long[] windows, Fields fields) {}

    public static void installPlans(FindWindowGroups analysis, Map<Long, DBSPWindowOperator> windows,
                                    List<Plan> plans) {
        for (var plan : plans) {
            var members = new ArrayList<FindWindowGroups.Candidate>(plan.windows.length);
            for (long id : plan.windows) {
                var window = Utilities.getExists(windows, id);
                members.add(Objects.requireNonNull(analysis.analyze(window), "Rust selected an ineligible window"));
            }
            installGroup(analysis, members, plan.fields);
        }
    }

    public interface Selection {
        int[][] group(List<String> inputs, int threshold) throws IOException;
        Fields share(List<List<String>> fields) throws IOException;
    }

    public static void install(DBSPCompiler compiler, FindWindowGroups analysis,
                               List<DBSPWindowOperator> windows, Selection select) throws IOException {
        var candidates = windows.stream().map(window ->
                Objects.requireNonNull(analysis.analyze(window), "Rust selected an ineligible window")).toList();
        var keys = candidates.stream().map(candidate -> candidate.inputFingerprint(compiler)).toList();
        for (int[] group : select.group(keys, compiler.metadata.windowSharingThreshold())) {
            var members = new ArrayList<FindWindowGroups.Candidate>(group.length);
            for (int index : group)
                members.add(candidates.get(index));
            var fingerprints = members.stream().map(candidate -> candidate.valueFields().stream()
                    .map(field -> CanonicalForm.asString(compiler, field)).toList()).toList();
            installGroup(analysis, members, select.share(fingerprints));
        }
    }

    private static void installGroup(FindWindowGroups analysis, List<FindWindowGroups.Candidate> members, Fields fields) {
        var shared = new ArrayList<DBSPClosureExpression>(fields.sources.length);
        for (int[] source : fields.sources)
            shared.add(members.get(source[0]).valueFields().get(source[1]));
        var projections = new ArrayList<WindowGroup.Member>(members.size());
        for (int i = 0; i < members.size(); i++)
            projections.add(new WindowGroup.Member(members.get(i).window(), fields.projections[i]));
        var first = members.get(0);
        var result = new WindowGroup(first.port(), first.closure(), shared, projections);
        for (var member : projections)
            Utilities.putNew(analysis.groups, member.window(), result);
    }
}
