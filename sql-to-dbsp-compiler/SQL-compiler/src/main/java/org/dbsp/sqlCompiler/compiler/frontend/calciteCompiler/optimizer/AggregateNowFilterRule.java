package org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.optimizer;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.rules.TransformationRule;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.Util;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

/**
 * Rule that extracts an aggregate FILTER condition that calls NOW()
 * into a Filter child of the aggregate.
 *
 * <p>Rewrite (one FILTER condition per application; calls with structurally
 * equal conditions move together, even from different filter columns;
 * repeated application by the HEP planner handles the remaining conditions).
 * Here $f3 and $f4 hold equal conditions, so SUM and the filtered COUNT
 * move together, behind a single join:
 * <pre>
 * LogicalAggregate(group=[{0}], s=[SUM($2) FILTER $3], c=[COUNT() FILTER $4], t=[COUNT()])
 *   LogicalProject([$0, ..., $f3=[>=($1, -(NOW(), 86400000:INTERVAL))],
 *                            $f4=[>=($1, -(NOW(), 86400000:INTERVAL))]])
 * </pre>
 * becomes
 * <pre>
 * LogicalProject([$0, s=[$3], c=[COALESCE($4, 0)], t=[$1]])
 *   LogicalJoin(left, IS NOT DISTINCT FROM($0, $2))
 *     LogicalAggregate(group=[{0}], t=[COUNT()])                 "anchor"
 *       LogicalProject(...)
 *     LogicalAggregate(group=[{0}], s=[SUM($2)], c=[COUNT()])    "filtered"
 *       LogicalProject(...)
 *         LogicalFilter([>=($1, -(NOW(), 86400000:INTERVAL))])
 * </pre>
 * COUNT over a group with no qualifying rows must be 0, not NULL, hence
 * the COALESCE on the column coming from the left join's filtered side.
 *
 * <p>The join is needed because grouping keys come from the unfiltered
 * input: a group whose rows all fail the condition must still be output.
 * The left join produces NULL for the filtered aggregates of groups absent
 * from the filtered side.  Only calls whose value over an empty set is
 * NULL (see {@link #emptyGroupValueKnown}) or COUNT may be moved; any
 * other filtered call prevents the rewrite.
 *
 * <p>Without GROUP BY both sides always produce exactly one row, so the
 * left join degenerates to a cross join.  When there are no unfiltered
 * calls at all no join is needed.
 */
public class AggregateNowFilterRule
        extends RelRule<DefaultOptRuleConfig<AggregateNowFilterRule>>
        implements TransformationRule {
    public AggregateNowFilterRule() {
        super(CONFIG);
    }

    /** True if the expression contains a call of the niladic NOW() function. */
    static boolean containsNow(RexNode node) {
        try {
            node.accept(new RexVisitorImpl<Void>(true) {
                @Override
                public Void visitCall(RexCall call) {
                    if (call.getOperands().isEmpty()
                            && call.getOperator().getName().equalsIgnoreCase("NOW"))
                        throw Util.FoundOne.NULL;
                    return super.visitCall(call);
                }
            });
            return false;
        } catch (Util.FoundOne e) {
            return true;
        }
    }

    /** True if the rewrite can produce this call's value for a group with
     * no qualifying rows, where the left join manufactures NULL.  The
     * listed kinds return NULL over an empty set (AggregateCompiler defines
     * each empty-set result), and COUNT, which returns 0, is repaired in
     * the final projection.  Any other call, e.g. ARRAY_AGG, whose
     * empty-set value is an empty array, must prevent the rewrite. */
    static boolean emptyGroupValueKnown(AggregateCall call) {
        SqlKind kind = call.getAggregation().getKind();
        if (kind == SqlKind.COUNT)
            return true;
        if (!call.getType().isNullable())
            // NULL from the join would violate the declared type
            return false;
        return switch (kind) {
            case SUM, AVG, MIN, MAX,
                 STDDEV_POP, STDDEV_SAMP, VAR_POP, VAR_SAMP,
                 BIT_AND, BIT_OR, BIT_XOR -> true;
            default -> false;
        };
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final Aggregate aggregate = call.rel(0);
        final Project project = call.rel(1);
        if (aggregate.getGroupType() != Aggregate.Group.SIMPLE)
            return;

        final List<RexNode> projects = project.getProjects();
        // The column containing the filter with NOW()
        int filterColumn = -1;
        for (AggregateCall aggCall : aggregate.getAggCallList()) {
            if (aggCall.filterArg >= 0 && containsNow(projects.get(aggCall.filterArg))) {
                filterColumn = aggCall.filterArg;
                break;
            }
        }
        if (filterColumn < 0)
            return;

        final int groupCount = aggregate.getGroupCount();
        // Calls move together when their filter columns hold structurally
        // equal expressions.
        final RexNode condition = projects.get(filterColumn);
        final List<AggregateCall> aggCalls = aggregate.getAggCallList();
        final boolean[] isMoved = new boolean[aggCalls.size()];
        final List<AggregateCall> moved = new ArrayList<>();
        final List<AggregateCall> kept = new ArrayList<>();
        for (int i = 0; i < aggCalls.size(); i++) {
            AggregateCall aggCall = aggCalls.get(i);
            if (aggCall.filterArg >= 0 && projects.get(aggCall.filterArg).equals(condition)) {
                if (groupCount > 0 && !emptyGroupValueKnown(aggCall))
                    return;
                isMoved[i] = true;
                moved.add(aggCall.withFilter(-1));
            } else {
                kept.add(aggCall);
            }
        }

        final RelBuilder builder = call.builder();
        final RexBuilder rexBuilder = aggregate.getCluster().getRexBuilder();

        // The NOW() condition becomes a Filter child of the projection
        builder.push(project.getInput())
                .filter(condition)
                .project(projects);
        RelBuilder.GroupKey groupKey =
                builder.groupKey(aggregate.getGroupSet(), aggregate.getGroupSets());
        builder.aggregate(groupKey, moved);
        final RelNode filtered = builder.build();

        if (kept.isEmpty() && groupCount == 0) {
            // Single row, all calls moved: the filtered aggregate is the result.
            call.transformTo(filtered);
            // No-op in the HEP planner, needed if this ever runs under Volcano
            call.getPlanner().prune(aggregate);
            return;
        }

        // Kept aggregates
        builder.push(project.getInput()).project(projects);
        groupKey = builder.groupKey(aggregate.getGroupSet(), aggregate.getGroupSets());
        builder.aggregate(groupKey, kept);
        final RelNode anchor = builder.build();

        builder.push(anchor).push(filtered);
        if (groupCount == 0) {
            // Both sides produce exactly one row
            builder.join(JoinRelType.INNER, builder.literal(true));
        } else {
            List<RexNode> conditions = new ArrayList<>();
            for (int i = 0; i < groupCount; i++) {
                // Use IS_NOT_DISTINCT_FROM for the JOIN condition to treat NULL keys as equal
                conditions.add(rexBuilder.makeCall(SqlStdOperatorTable.IS_NOT_DISTINCT_FROM,
                        builder.field(2, 0, i), builder.field(2, 1, i)));
            }
            builder.join(JoinRelType.LEFT, builder.and(conditions));
        }

        // Restore the original column order:
        // join row = anchor [keys, kept...] ++ filtered [keys, moved...]
        final int anchorFieldCount = groupCount + kept.size();
        final List<RexNode> resultFields = new ArrayList<>();
        for (int i = 0; i < groupCount; i++)
            resultFields.add(builder.field(i));
        int keptIndex = 0;
        int movedIndex = 0;
        for (int i = 0; i < aggCalls.size(); i++) {
            AggregateCall aggCall = aggCalls.get(i);
            if (isMoved[i]) {
                RexNode field = builder.field(anchorFieldCount + groupCount + movedIndex);
                if (groupCount > 0 && aggCall.getAggregation().getKind() == SqlKind.COUNT) {
                    // COUNT over a group with no qualifying rows is 0, but
                    // such groups are absent from the filtered side, so the
                    // left join produced NULL; correct it to a 0 using COALESCE.
                    RexNode zero = rexBuilder.makeExactLiteral(
                            BigDecimal.ZERO, aggCall.getType());
                    field = rexBuilder.makeCall(SqlStdOperatorTable.COALESCE, field, zero);
                }
                resultFields.add(field);
                movedIndex++;
            } else {
                resultFields.add(builder.field(groupCount + keptIndex));
                keptIndex++;
            }
        }
        builder.project(resultFields)
                .convert(aggregate.getRowType(), false);

        call.transformTo(builder.build());
        // No-op in the HEP planner, needed if this ever runs under Volcano
        call.getPlanner().prune(aggregate);
    }

    public static final DefaultOptRuleConfig<AggregateNowFilterRule> CONFIG =
            DefaultOptRuleConfig.<AggregateNowFilterRule>create()
                    .withOperandSupplier(
                            b0 -> b0.operand(Aggregate.class)
                                    .oneInput(b1 -> b1.operand(Project.class)
                                            .anyInputs()));
}
