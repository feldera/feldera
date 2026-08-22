package org.dbsp.sqlCompiler.compiler.visitors.outer.windowSharing;

import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPWindowOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.inner.CanonicalForm;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.outer.temporal.ReorderTemporalFilters;
import org.dbsp.sqlCompiler.ir.DBSPParameter;
import org.dbsp.sqlCompiler.ir.expression.DBSPBaseTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPRawTupleExpression;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Finds groups {@link DBSPWindowOperator}s that can share their left integral.
 *
 * <p>Windows belong to one group when the {@link DBSPMapIndexOperator}s feeding them compute the
 * same key over the same common stream, filtered by the same predicate. */
/* We search for trees shaped like:
 * stream -> map_index -> window
 *        -> map_index -> window
 * where `stream` is one output port read by several indexes. */
public class FindWindowGroups extends CircuitVisitor {
    /** A window and the stream it reads, with the index function feeding it taken apart.
     *
     * @param port    Left input of the window.
     * @param closure Function computing timestamp.
     * @param valueFields For each output value field a function extracting it from the indexed input. */
    private record Candidate(DBSPWindowOperator window, OutputPort port,
                             DBSPClosureExpression closure,
                             List<DBSPClosureExpression> valueFields) implements
            ReorderTemporalFilters.PortAndClosureFingerprint
    { }

    /** Candidates for window groups, grouped by their {@link Candidate#inputFingerprint}. */
    private final Map<String, List<Candidate>> windowGroupCandidates;
    /** Result of the analysis: the group each window belongs to.  Insertion-ordered, so that
     * a consumer iterating it builds the same circuit on every run. */
    public final Map<DBSPWindowOperator, WindowGroup> groups;

    public FindWindowGroups(DBSPCompiler compiler) {
        super(compiler);
        this.windowGroupCandidates = new LinkedHashMap<>();
        this.groups = new LinkedHashMap<>();
    }

    @Nullable
    Candidate analyze(DBSPWindowOperator window) {
        // An unbounded window should not be shared with bounded windows
        if (window.lowerUnbounded)
            return null;
        if (!window.left().node().is(DBSPMapIndexOperator.class))
            return null;
        DBSPMapIndexOperator index = window.left().node().to(DBSPMapIndexOperator.class);
        if (!index.getFunction().is(DBSPClosureExpression.class))
            return null;
        DBSPClosureExpression function = index.getClosureFunction();
        if (function.parameters.length != 1)
            return null;
        if (!function.body.is(DBSPRawTupleExpression.class))
            return null;
        DBSPRawTupleExpression keyValue = function.body.to(DBSPRawTupleExpression.class);
        if (keyValue.size() != 2)
            return null;
        if (!keyValue.get(1).is(DBSPBaseTupleExpression.class))
            return null;
        DBSPBaseTupleExpression value = keyValue.get(1).to(DBSPBaseTupleExpression.class);
        if (value.fields == null)
            return null;

        DBSPParameter param = function.parameters[0];
        List<DBSPClosureExpression> valueFields = new ArrayList<>();
        for (DBSPExpression field : value.fields)
            valueFields.add(field.closure(param));
        return new Candidate(window, index.input(), keyValue.get(0).closure(param), valueFields);
    }

    @Override
    public void postorder(DBSPWindowOperator window) {
        Candidate candidate = this.analyze(window);
        if (candidate == null)
            return;
        this.windowGroupCandidates.computeIfAbsent(
                candidate.inputFingerprint(this.compiler()), k -> new ArrayList<>())
                .add(candidate);
    }

    /** Find the windows that can be shared and build this.groups. */
    @Override
    public void endVisit() {
        // A shared input keeps every field its members need, so a small group pays that width
        // for little reuse.  The same threshold decides whether filters are reordered, and a
        // threshold of 0 turns both off.
        if (this.compiler().metadata.windowSharingDisabled()) {
            super.endVisit();
            return;
        }
        int threshold = this.compiler().metadata.windowSharingThreshold();
        for (List<Candidate> group : this.windowGroupCandidates.values()) {
            if (group.size() <= threshold)
                continue;

            // We have a group large enough
            Candidate first = group.get(0);
            List<DBSPClosureExpression> shared = new ArrayList<>();
            // Maps a value field computation fingerprint to the position which will compute it
            Map<String, Integer> fingerprintToPosition = new LinkedHashMap<>();
            List<WindowGroup.Member> members = new ArrayList<>();
            for (Candidate candidate : group) {
                // The indexes of the new fields to be used
                int[] fieldsUsed = new int[candidate.valueFields.size()];
                for (int i = 0; i < fieldsUsed.length; i++) {
                    DBSPClosureExpression field = candidate.valueFields.get(i);
                    String fingerprint = CanonicalForm.asString(this.compiler(), field);
                    Integer at = fingerprintToPosition.get(fingerprint);
                    if (at == null) {
                        // A new field
                        at = shared.size();
                        fingerprintToPosition.put(fingerprint, at);
                        shared.add(field);
                    }
                    fieldsUsed[i] = at;
                }
                members.add(new WindowGroup.Member(candidate.window, fieldsUsed));
            }

            WindowGroup windowGroup = new WindowGroup(first.port, first.closure, shared, members);
            for (WindowGroup.Member member : members)
                Utilities.putNew(this.groups, member.window(), windowGroup);
        }
        super.endVisit();
    }
}
