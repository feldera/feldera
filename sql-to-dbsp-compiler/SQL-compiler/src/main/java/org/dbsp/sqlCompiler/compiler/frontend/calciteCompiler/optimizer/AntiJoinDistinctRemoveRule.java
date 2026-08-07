package org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.optimizer;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.rules.TransformationRule;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;

/**
 * Rule that removes a DISTINCT (an Aggregate with no aggregate calls grouping
 * by all its columns) from the right input of a LEFT JOIN, when a filter above
 * the join requires a non-nullable right column to be NULL.
 * This is the SQL idiom for an anti-join.
 *
 * <pre>
 * LogicalFilter(condition=[IS NULL($r)])   $r a right column, non-nullable below
 *   LogicalJoin(joinType=[left])
 *     any(left)
 *     LogicalAggregate(group=[{0..n}])     no aggregate calls
 * </pre>
 * becomes
 * <pre>
 * LogicalFilter(condition=[IS NULL($r)])
 *   LogicalJoin(joinType=[left])
 *     any(left)
 *     input of the LogicalAggregate
 * </pre>
 *
 * <p>The plans are equivalent because the filter output is identical:
 * a left row with matches only produces join rows where $r is not NULL,
 * so the IS NULL conjunct deletes all of them, deduplicated or not;
 * a left row without matches produces exactly one NULL-padded row,
 * and deduplication cannot change which left rows have no matches.
 *
 * <p>It does not hold for FULL joins: the unmatched rows of the
 * deduplicated input are themselves part of the output. */
public class AntiJoinDistinctRemoveRule
        extends RelRule<DefaultOptRuleConfig<AntiJoinDistinctRemoveRule>>
        implements TransformationRule {
    protected AntiJoinDistinctRemoveRule() {
        super(CONFIG);
    }

    /** True if the aggregate only deduplicates its input:
     * it groups by all its columns and computes nothing */
    static boolean isDistinct(Aggregate aggregate) {
        return aggregate.getGroupType() == Aggregate.Group.SIMPLE
                && aggregate.getAggCallList().isEmpty()
                && aggregate.getGroupSet().cardinality()
                        == aggregate.getInput().getRowType().getFieldCount();
    }

    /** True if some conjunct has the form IS NULL(column), where column is
     * a right-side column of the join that cannot be NULL below the join.
     * Such a conjunct only accepts left rows with no matches. */
    static boolean requiresNoMatch(Filter filter, Join join, Aggregate right) {
        int leftCount = join.getLeft().getRowType().getFieldCount();
        for (RexNode conjunct : RelOptUtil.conjunctions(filter.getCondition())) {
            if (conjunct.getKind() != SqlKind.IS_NULL)
                continue;
            RexNode operand = ((RexCall) conjunct).getOperands().get(0);
            if (!(operand instanceof RexInputRef ref))
                continue;
            int index = ref.getIndex() - leftCount;
            if (index < 0)
                continue;
            // The join row type declares right columns nullable because the
            // join pads them; the nullability below the join is what matters
            if (!right.getRowType().getFieldList().get(index).getType().isNullable())
                return true;
        }
        return false;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final Filter filter = call.rel(0);
        final Join join = call.rel(1);
        final Aggregate aggregate = call.rel(3);
        if (join.getJoinType() != JoinRelType.LEFT)
            return;
        if (!isDistinct(aggregate))
            return;
        if (!requiresNoMatch(filter, join, aggregate))
            return;

        Join newJoin = join.copy(join.getTraitSet(), join.getCondition(),
                join.getLeft(), aggregate.getInput(), join.getJoinType(), join.isSemiJoinDone());
        Filter newFilter = filter.copy(filter.getTraitSet(), newJoin, filter.getCondition());
        call.transformTo(newFilter);
        call.getPlanner().prune(filter);
    }

    public static final DefaultOptRuleConfig<AntiJoinDistinctRemoveRule> CONFIG =
            DefaultOptRuleConfig.<AntiJoinDistinctRemoveRule>create()
                    .withOperandSupplier(
                            b0 -> b0.operand(Filter.class)
                                    .oneInput(b1 -> b1.operand(Join.class)
                                            .inputs(
                                                    b2 -> b2.operand(RelNode.class).anyInputs(),
                                                    b3 -> b3.operand(Aggregate.class).anyInputs())));
}
