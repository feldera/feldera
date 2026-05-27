package org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.hint.Hintable;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.dbsp.sqlCompiler.compiler.IErrorReporter;
import org.dbsp.sqlCompiler.compiler.errors.SourcePositionRange;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Rewrites a Join annotation into a lower-level form.
 * Must be run before optimizations.
 *
 * <p>Initially join annotations specify table names: e.g.
 * SELECT /*+ broadcast(T) * / T JOIN S ON ... ;
 * These are carried as Broadcast hints in the Rel plan.
 * Also, when converting SqlToRel, the two inputs of a join will be tagged with ALIAS annotations
 * that will contain the table names: e.g.,
 * LogicalJoin(condition=[=($0, $1)], joinType=[inner])[[broadcast inheritPath:[0, 0] options:[t]]
 *   LogicalTableScan(table=[[schema, t]])[[alias inheritPath:[] options:[t]]]
 *   LogicalTableScan(table=[[schema, s]])[[alias inheritPath:[] options:[s]]]
 * The optimizer later will move things around, and the ALIAS annotations may vanish if they are on subqueries.
 * So before optimizing we replace BROADCAST(S) with BROADCAST(inputNo, S):
 * LogicalJoin(condition=[=($0, $1)], joinType=[inner])[[broadcast inheritPath:[0, 0] options:[0, T]]
 *
 * <p>Note that all annotations inserted here will have an inheritPath: [] (empty).
 * This will be useful later: all annotations with a non-empty path were inserted by the optimizer.
 */
public class RewriteJoinAnnotations extends RelShuttleImpl {
    private final IErrorReporter reporter;

    static class HintAndCount {
        public final RelHint hint;
        public final List<SourcePositionRange> from;

        HintAndCount(RelHint hint) {
            this.hint = hint;
            this.from = new ArrayList<>();
        }

        void increment(SourcePositionRange referenced) {
            if (referenced.isValid())
                this.from.add(referenced);
        }
    }

    final Map<SourcePositionRange, HintAndCount> hintsUsed;

    RewriteJoinAnnotations(IErrorReporter reporter) {
        this.reporter = reporter;
        this.hintsUsed = new HashMap<>();
    }

    @Override
    public RelNode visit(LogicalProject project) {
        // Remove join hints from projects, where they are inserted initially.
        // This will prevent them from being propagated by other query optimization passes
        project = (LogicalProject) super.visit(project);
        List<RelHint> newHints = new ArrayList<>();

        boolean changes = false;
        for (var hint: project.getHints()) {
            var newHint = hint;
            if (SqlToRelCompiler.isJoinHint(hint))
                newHint = null;
            changes = changes || (newHint != hint);
            if (newHint != null)
                newHints.add(newHint);
        }

        if (changes)
            return project.withHints(newHints);
        return project;
    }

    /** Called after the visit of a Rel tree is completed.
     * Reports errors for hints not used or used multiple times. */
    public void done() {
        for (var entry: this.hintsUsed.entrySet()) {
            var hc = entry.getValue();
            if (hc.from.size() == 1) continue;
            var hint = hc.hint;
            var tableName = hint.listOptions.get(0);
            if (hc.from.isEmpty()) {
                this.reporter.reportError(new SourcePositionRange(hint.pos), "Cannot apply hint",
                        "Hint " + Utilities.singleQuote(hint.hintName + "(" + tableName + ")") +
                                " specifies an input which cannot be identified among the join inputs");
            } else {
                this.reporter.reportError(new SourcePositionRange(hint.pos), "Ambiguous hint",
                        "Hint " + Utilities.singleQuote(hint.hintName + "(" + tableName + ")") +
                                " matches " + hc.from.size() + " different collections");
                for (SourcePositionRange pos: hc.from) {
                    this.reporter.reportError(pos, "Ambiguous hint", "Matching table", true);
                }
            }
        }
    }

    /** Called when a join hint is used in some table */
    void referenced(RelHint hint, SourcePositionRange reference) {
        SourcePositionRange range = new SourcePositionRange(hint.pos);
        if (!range.isValid())
            return;
        if (!this.hintsUsed.containsKey(range))
            this.hintsUsed.put(range, new HintAndCount(hint));
        this.hintsUsed.get(range).increment(reference);
    }

    /** Given an alias, figure out which side of the join it's coming from; rewrite the hint.
     * Return null if this is a join hint which cannot be resolved.
     * Report an error in this case.
     * Return the original hint if it is not a join hint. */
    @Nullable
    RelHint rewriteJoinInputHint(RelHint hint, LogicalJoin join) {
        if (!SqlToRelCompiler.isJoinHint(hint))
            return hint;

        // Validation should ensure that the  exists.
        String tableName = hint.listOptions.get(0);
        var inputs = join.getInputs();
        for (int i = 0; i < inputs.size(); i++) {
            RelNode node = inputs.get(i);
            if (node instanceof Hintable hintable) {
                var inputHints = hintable.getHints();
                for (var inputHint: inputHints) {
                    if (inputHint.hintName.equals(SqlToRelCompiler.HINT_ALIAS)) {
                        String collectionAlias = inputHint.listOptions.get(0);
                        if (collectionAlias.equals(tableName)) {
                            // Notice that we discard the inheritPath of the original hint
                            this.referenced(hint, new SourcePositionRange(inputHint.pos));
                            return RelHint.builder(hint.hintName)
                                    .hintOption(Integer.toString(i))
                                    .hintOption(tableName)
                                    .position(hint.pos)
                                    .build();
                        }
                    }
                }
            }
        }
        // The hint is not referenced by this join, but maybe there is another one which uses it.
        this.referenced(hint, SourcePositionRange.INVALID);
        return null;
    }

    @Override
    public RelNode visit(LogicalJoin join) {
        join = (LogicalJoin) super.visit(join);
        // Use a set to eliminate duplicates
        // Sometimes Calcite will create multiple versions of a hint with different inheritPaths
        Set<SourcePositionRange> seen = new HashSet<>();
        List<RelHint> newHints = new ArrayList<>();

        boolean changes = false;
        for (var hint: join.getHints()) {
            SourcePositionRange range = new SourcePositionRange(hint.pos);
            if (range.isValid() && seen.contains(range)) {
                // Duplicate version of the same hint
                changes = true;
                continue;
            }
            seen.add(range);
            RelHint newHint = this.rewriteJoinInputHint(hint, join);
            changes = changes || (newHint != hint);
            if (newHint != null)
                newHints.add(newHint);
        }

        if (changes)
            return join.withHints(newHints);
        return join;
    }
}
