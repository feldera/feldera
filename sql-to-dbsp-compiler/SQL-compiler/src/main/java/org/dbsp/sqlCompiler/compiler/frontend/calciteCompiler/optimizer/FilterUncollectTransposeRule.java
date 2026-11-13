package org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.optimizer;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Uncollect;
import org.apache.calcite.rel.rules.TransformationRule;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.ArrayList;
import java.util.List;

/**
 * Rule that pushes the conjuncts of a Filter that only read passthrough
 * fields of an {@link Uncollect} below the Uncollect.
 *
 * <pre>
 * LogicalFilter(condition=[AND(=($4, 4), IS NOT NULL($6))])
 *   Uncollect(passthrough=[{0, 1, 2, 3, 4}], collectionFields=[{5}])
 *     LogicalProject(..., $f5=[CAST(parse_json($2)):VARIANT ARRAY])
 * </pre>
 * becomes
 * <pre>
 * LogicalFilter(condition=[IS NOT NULL($6)])
 *   Uncollect(passthrough=[{0, 1, 2, 3, 4}], collectionFields=[{5}])
 *     LogicalProject(..., $f5=[CAST(parse_json($2)):VARIANT ARRAY])
 *       LogicalFilter(condition=[=($4, 4)])
 * </pre>
 *
 * <p>All output rows of one input row have the same passthrough values,
 * so the rewrite does not change the result.  When the input of the
 * Uncollect is a Project that copies the fields that the filter reads,
 * the filter also moves below the Project.  Then an expression that
 * computes the collection does not see rows that the filter drops.
 *
 * <p>The rewrite must not add runtime errors.  An inner UNNEST drops an input
 * row whose collection is empty or NULL, so the original filter never sees that
 * row, but the pushed filter does.  For an inner UNNEST the rule only pushes
 * conjuncts that {@link RexSafety#neverThrows} accepts.  An outer UNNEST keeps
 * every input row, so the rule pushes any deterministic conjunct.
 */
public class FilterUncollectTransposeRule
        extends RelRule<DefaultOptRuleConfig<FilterUncollectTransposeRule>>
        implements TransformationRule {
    public FilterUncollectTransposeRule() {
        super(CONFIG);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final Filter filter = call.rel(0);
        final Uncollect uncollect = call.rel(1);
        if (!filter.getVariablesSet().isEmpty())
            return;
        // Output field i < passthrough.length is input field passthrough[i]
        final int[] passthrough = uncollect.getPassthroughFieldIndices().toArray();
        if (passthrough.length == 0)
            return;

        List<RexNode> pushed = new ArrayList<>();
        List<RexNode> kept = new ArrayList<>();
        ImmutableBitSet passthroughOutputs = ImmutableBitSet.range(passthrough.length);
        for (RexNode conjunct : RelOptUtil.conjunctions(filter.getCondition())) {
            boolean safe = uncollect.isOuter || RexSafety.neverThrows(conjunct);
            if (safe && RexUtil.isDeterministic(conjunct)
                    && passthroughOutputs.contains(RelOptUtil.InputFinder.bits(conjunct)))
                pushed.add(conjunct);
            else
                kept.add(conjunct);
        }
        if (pushed.isEmpty())
            return;

        RexShuttle toInput = new RexShuttle() {
            @Override
            public RexNode visitInputRef(RexInputRef ref) {
                return new RexInputRef(passthrough[ref.getIndex()], ref.getType());
            }
        };
        List<RexNode> below = toInput.apply(pushed);
        // Also go below the Projects that copy the fields the filter reads:
        // one of them can compute the collection
        List<Project> projects = new ArrayList<>();
        RelNode input = uncollect.getInput();
        // The planner wraps inputs, see RelNode.stripped()
        while (input.stripped() instanceof Project project && readsOnlyColumns(below, project)) {
            List<RexNode> belowProject = new ArrayList<>();
            for (RexNode conjunct : below)
                belowProject.add(RelOptUtil.pushPastProject(conjunct, project));
            below = belowProject;
            projects.add(project);
            input = project.getInput();
        }
        final RelBuilder builder = call.builder();
        RelNode newInput = builder.push(input).filter(below).build();
        for (int i = projects.size() - 1; i >= 0; i--) {
            Project project = projects.get(i);
            newInput = project.copy(project.getTraitSet(), newInput, project.getProjects(), project.getRowType());
        }
        RelNode newUncollect = uncollect.copy(uncollect.getTraitSet(), newInput);
        builder.push(newUncollect)
                .filter(kept);
        call.transformTo(builder.build());
    }

    /** True if each field of 'project' that 'conditions' read is a column of the project input */
    static boolean readsOnlyColumns(List<RexNode> conditions, Project project) {
        for (int field : RelOptUtil.InputFinder.bits(conditions, null)) {
            if (!(project.getProjects().get(field) instanceof RexInputRef))
                return false;
        }
        return true;
    }

    public static final DefaultOptRuleConfig<FilterUncollectTransposeRule> CONFIG =
            DefaultOptRuleConfig.<FilterUncollectTransposeRule>create()
                    .withOperandSupplier(
                            b0 -> b0.operand(Filter.class)
                                    .oneInput(b1 -> b1.operand(Uncollect.class).anyInputs()));
}
