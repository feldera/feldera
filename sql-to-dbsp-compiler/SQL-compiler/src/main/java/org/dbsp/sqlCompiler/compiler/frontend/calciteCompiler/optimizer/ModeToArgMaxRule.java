package org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.optimizer;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.rules.TransformationRule;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;
import org.dbsp.util.Utilities;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Rule that rewrites the MODE aggregate using COUNT and ARG_MAX,
 * so that the backend never needs to implement MODE directly.
 *
 * <pre>
 * LogicalAggregate(group=[{0}], m=[MODE($1)], s=[SUM($2)])
 * </pre>
 * becomes
 * <pre>
 * LogicalProject(k=[$0], m=[$3], s=[$1])
 *   LogicalJoin(condition=[IS NOT DISTINCT FROM($0, $2)], joinType=[inner])
 *     LogicalAggregate(group=[{0}], s=[SUM($2)])
 *     LogicalAggregate(group=[{0}], m=[ARG_MAX($1, $2)])
 *       LogicalAggregate(group=[{0, 1}], cnt=[COUNT($1)])
 * </pre>
 *
 * <p>The inner aggregate counts, for each group, how many times each value
 * occurs; the outer ARG_MAX picks the value with the highest count.
 * COUNT(value) gives the group of NULL values a count of 0, so NULL can be the result
 * only when it is the sole candidate; this matches the SQL convention that
 * aggregates ignore NULLs, and that MODE of an all-NULL group is NULL.
 *
 * <p>MODE with a FILTER wraps the ARG_MAX value in
 * CASE(cnt &gt; 0, value, NULL): a group whose rows are all filtered out must
 * produce NULL, but the group itself must survive.
 *
 * <p>MODE with DISTINCT is rejected -- it does not really make sense.
 *
 * <p>MODE calls with the same argument and filter share one join.
 * Aggregates with GROUPING SETS are not rewritten.
 */
public class ModeToArgMaxRule
        extends RelRule<DefaultOptRuleConfig<ModeToArgMaxRule>>
        implements TransformationRule {
    public ModeToArgMaxRule() {
        super(CONFIG);
    }

    /** Argument column and filter of a MODE call; calls with equal specs share a branch. */
    record ModeSpec(int arg, int filterArg) {
        static ModeSpec of(AggregateCall call) {
            Utilities.enforce(call.getArgList().size() == 1,
                    () -> "MODE expects 1 argument, got " + call.getArgList().size());
            return new ModeSpec(call.getArgList().get(0), call.filterArg);
        }
    }

    static boolean isMode(AggregateCall call) {
        return call.getAggregation().getKind() == SqlKind.MODE;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final Aggregate aggregate = call.rel(0);
        if (aggregate.getGroupType() != Aggregate.Group.SIMPLE)
            return;
        final List<AggregateCall> aggCalls = aggregate.getAggCallList();
        if (aggCalls.stream().noneMatch(ModeToArgMaxRule::isMode))
            return;
        for (AggregateCall agg : aggCalls) {
            // DISTINCT is rejected after SqlToRel conversion; other shapes
            // should not occur, but leave them to fail in the backend.
            if (isMode(agg) &&
                    (agg.isDistinct() || !agg.rexList.isEmpty() || agg.distinctKeys != null))
                return;
        }

        final RelNode input = aggregate.getInput();
        final int groupCount = aggregate.getGroupCount();

        // Branch 0 computes the non-MODE aggregates, if any;
        // each distinct MODE spec gets its own branch.
        final List<RelNode> branches = new ArrayList<>();
        final List<AggregateCall> rest = new ArrayList<>();
        for (AggregateCall agg : aggCalls)
            if (!isMode(agg))
                rest.add(agg);

        if (!rest.isEmpty()) {
            RelBuilder b = call.builder();
            b.push(input);
            b.aggregate(b.groupKey(aggregate.getGroupSet()), rest);
            branches.add(b.build());
        }
        final Map<ModeSpec, Integer> branchOfSpec = new LinkedHashMap<>();
        for (AggregateCall agg : aggCalls) {
            if (!isMode(agg))
                continue;
            ModeSpec spec = ModeSpec.of(agg);
            if (branchOfSpec.containsKey(spec))
                continue;
            branchOfSpec.put(spec, branches.size());
            branches.add(buildModeBranch(call.builder(), input, aggregate, agg, spec));
        }

        // Join all branches on the group keys.  Every branch aggregates the
        // same input over the same groups, so the join is 1:1 on the key set.
        final RelBuilder b = call.builder();
        b.push(branches.get(0));
        for (int i = 1; i < branches.size(); i++) {
            b.push(branches.get(i));
            List<RexNode> conditions = new ArrayList<>();
            for (int k = 0; k < groupCount; k++)
                conditions.add(b.call(SqlStdOperatorTable.IS_NOT_DISTINCT_FROM,
                        b.field(2, 0, k), b.field(2, 1, k)));
            b.join(JoinRelType.INNER, b.and(conditions));
        }

        // Field offset of each branch in the join output.
        final int[] offset = new int[branches.size()];
        for (int i = 1; i < branches.size(); i++)
            offset[i] = offset[i - 1] + branches.get(i - 1).getRowType().getFieldCount();

        // Restore the original column order.
        final List<RexNode> outputs = new ArrayList<>();
        for (int k = 0; k < groupCount; k++)
            outputs.add(b.field(k));
        int restIndex = 0;
        for (AggregateCall agg : aggCalls) {
            if (isMode(agg)) {
                int branch = branchOfSpec.get(ModeSpec.of(agg));
                outputs.add(b.field(offset[branch] + groupCount));
            } else {
                outputs.add(b.field(groupCount + restIndex));
                restIndex++;
            }
        }
        b.project(outputs, aggregate.getRowType().getFieldNames());
        b.convert(aggregate.getRowType(), false);

        call.transformTo(b.build());
        call.getPlanner().prune(aggregate);
    }

    /** Builds Aggregate(G, ARG_MAX(value, cnt)) over Aggregate(G + {arg}, cnt=COUNT(arg)). */
    private static RelNode buildModeBranch(
            RelBuilder b, RelNode input, Aggregate aggregate, AggregateCall mode, ModeSpec spec) {
        final SqlParserPos pos = mode.getParserPosition();
        final RelDataType bigInt = b.getTypeFactory().createSqlType(SqlTypeName.BIGINT);

        b.push(input);
        final ImmutableBitSet innerGroup =
                aggregate.getGroupSet().union(ImmutableBitSet.of(spec.arg()));
        final AggregateCall count = AggregateCall.create(
                pos, SqlStdOperatorTable.COUNT, false, false, false,
                ImmutableList.of(), ImmutableList.of(spec.arg()), spec.filterArg(),
                null, RelCollations.EMPTY, bigInt, null);
        b.aggregate(b.groupKey(innerGroup), List.of(count));

        // The inner aggregate emits the keys of innerGroup in ascending
        // column order, followed by the count.
        final List<Integer> innerFields = innerGroup.toList();
        final int argPos = innerFields.indexOf(spec.arg());
        final int countPos = innerFields.size();

        final List<RexNode> projects = new ArrayList<>();
        for (int key : aggregate.getGroupSet())
            projects.add(b.field(innerFields.indexOf(key)));
        RexNode value = b.field(argPos);
        RelDataType resultType = mode.getType();
        if (spec.filterArg() >= 0) {
            // A group whose rows all fail the filter has only zero counts;
            // its mode must be NULL, so hide the values of such rows.
            resultType = b.getTypeFactory().createTypeWithNullability(resultType, true);
            value = b.call(SqlStdOperatorTable.CASE,
                    b.call(SqlStdOperatorTable.GREATER_THAN, b.field(countPos), b.literal(0)),
                    value,
                    b.getRexBuilder().makeNullLiteral(
                            b.getTypeFactory().createTypeWithNullability(value.getType(), true)));
        }
        projects.add(value);
        projects.add(b.field(countPos));
        b.project(projects);

        // Projected layout: [group keys, value, cnt].
        final int groupCount = aggregate.getGroupCount();
        final AggregateCall argMax = AggregateCall.create(
                pos, SqlStdOperatorTable.ARG_MAX, false, false, false,
                ImmutableList.of(), ImmutableList.of(groupCount, groupCount + 1), -1,
                null, RelCollations.EMPTY, resultType, mode.getName());
        b.aggregate(b.groupKey(ImmutableBitSet.range(groupCount)), List.of(argMax));
        return b.build();
    }

    public static final DefaultOptRuleConfig<ModeToArgMaxRule> CONFIG =
            DefaultOptRuleConfig.<ModeToArgMaxRule>create()
                    .withOperandSupplier(
                            b0 -> b0.operand(Aggregate.class).anyInputs());
}
