package org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.optimizer;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.rules.TransformationRule;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlPostfixOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

/**
 * Rule that converts MAX(CASE ... THEN 1 ELSE 0 END)
 * or MAX(CASE ... THEN 1 ELSE NULL END) (equivalently, no ELSE clause)
 * into COUNT(...) FILTER (WHERE ...).
 *
 * <p>Rewrite for ELSE 0:
 * <pre>
 * LogicalAggregate(group=[{0}], [MAX($1)]
 *   LogicalProject([$0], $f1=[CASE(=(...), 1, 0)])
 * </pre>
 * becomes
 * <pre>
 * LogicalProject([$0], [CASE(>($1, 0), 1, 0)])
 *   LogicalAggregate(group=[{0}], agg#0=[COUNT(*) FILTER $1])
 *     LogicalProject([$0], $f1=[IS TRUE(...)])
 * </pre>
 *
 * <p>Rewrite for ELSE NULL (or no ELSE):
 * <pre>
 * LogicalAggregate(group=[{0}], [MAX($1)]
 *   LogicalProject([$0], $f1=[CASE(=(...), 1, null)])
 * </pre>
 * becomes
 * <pre>
 * LogicalProject([$0], [CASE(>($1, 0), 1, null)])
 *   LogicalAggregate(group=[{0}], agg#0=[COUNT(*) FILTER $1])
 *     LogicalProject([$0], $f1=[IS TRUE(...)])
 * </pre>
 *
 * <p>This works only if there is a GROUP BY.
 */
public class MaxCaseToCountRule
        extends RelRule<DefaultOptRuleConfig<MaxCaseToCountRule>>
        implements TransformationRule {
    protected MaxCaseToCountRule() {
        super(CONFIG);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final Aggregate aggregate = call.rel(0);
        final Project project = call.rel(1);
        if (aggregate.getGroupCount() == 0) {
            return;
        }

        RelDataType resultType = aggregate.getRowType();
        final List<AggregateCall> newAggregates =
                new ArrayList<>(aggregate.getAggCallList().size());
        final List<RexNode> newProjects = new ArrayList<>(project.getProjects());
        final List<RexNode> postProjects = new ArrayList<>();
        final RelOptCluster cluster = aggregate.getCluster();
        final RexBuilder rexBuilder = cluster.getRexBuilder();

        RelDataType bigInt = cluster.getTypeFactory().createSqlType(SqlTypeName.BIGINT);
        bigInt = cluster.getTypeFactory().createTypeWithNullability(bigInt, true);

        for (int i = 0; i < aggregate.getGroupCount(); i++)
            // Preserve the keys in the postProjects
            postProjects.add(new RexInputRef(i, resultType.getFieldList().get(i).getType()));

        final int groupCount = aggregate.getGroupCount();
        for (AggregateCall aggregateCall : aggregate.getAggCallList()) {
            AggregateCall newCall = transform(aggregateCall, project, newProjects, postProjects, bigInt);
            if (newCall == null) {
                // Aggregate output: [group_keys (0..groupCount-1), agg_0, agg_1, ...]
                postProjects.add(rexBuilder.makeInputRef(
                        aggregateCall.getType(), groupCount + newAggregates.size()));
                newAggregates.add(aggregateCall);
            } else {
                newAggregates.add(newCall);
            }
        }

        if (newAggregates.equals(aggregate.getAggCallList())) {
            return;
        }

        final RelBuilder relBuilder = call.builder()
                .push(project.getInput())
                .project(newProjects);
        final RelBuilder.GroupKey groupKey =
                relBuilder.groupKey(aggregate.getGroupSet(), aggregate.getGroupSets());
        relBuilder.aggregate(groupKey, newAggregates);
        relBuilder.project(postProjects)
                .convert(resultType, false);

        RelNode result = relBuilder.build();
        call.transformTo(result);
        call.getPlanner().prune(aggregate);
    }

    @Nullable
    public static Long getLiteralValue(RexNode node) {
        if (node instanceof RexLiteral lit) {
            if (SqlTypeName.INT_TYPES.contains(lit.getType().getSqlTypeName())) {
                return lit.getValueAs(Long.class);
            }
        }
        return null;
    }

    /** Converts an aggregate call that matches to its final form.
     *
     * @param call         Call to analyze and convert.
     * @param project      Input relation for the aggregate.
     * @param newProjects  Append here new input fields needed by the synthesized aggregate.
     * @param postProjects Append here a projection that post-processes the aggregate;
     *                     contains already projections of the group by keys.
     * @param bigInt       BIGINT NULL data type
     * @return             null if the call is not changed.
     */
    private static @Nullable AggregateCall transform(
            AggregateCall call, Project project,
            List<RexNode> newProjects,
            List<RexNode> postProjects,
            RelDataType bigInt) {
        final int singleArg = soleArgument(call);
        if (singleArg < 0)
            return null;
        if (call.getAggregation().kind != SqlKind.MAX)
            return null;
        if (call.isDistinct())
            return null;

        final RexNode rexNode = project.getProjects().get(singleArg);
        if (!isThreeArgCase(rexNode))
            return null;

        final RelOptCluster cluster = project.getCluster();
        final RexBuilder rexBuilder = cluster.getRexBuilder();
        final RexCall caseCall = (RexCall) rexNode;

        RexNode op1 = caseCall.getOperands().get(1);
        RexNode op2 = caseCall.getOperands().get(2);
        Long op1Value = getLiteralValue(op1);
        if (op1Value == null)
            return null;
        Long op2Value = getLiteralValue(op2);
        if (op2Value == null && !(op2 instanceof RexLiteral))
            return null;
        boolean elseIsNull = op2Value == null;

        final boolean flip;
        if (elseIsNull) {
            // Only THEN 1 ELSE NULL is supported: MAX = 1 if any row matches, NULL otherwise.
            if (op1Value != 1L)
                return null;
            flip = false;
        } else if (op1Value == 0L && op2Value == 1L) {
            flip = true;
        } else if (op1Value == 1L && op2Value == 0L) {
            flip = false;
        } else {
            return null;
        }

        final SqlPostfixOperator op =
                flip ? SqlStdOperatorTable.IS_NOT_TRUE : SqlStdOperatorTable.IS_TRUE;
        final RexNode filterFromCase =
                rexBuilder.makeCall(op, caseCall.operands.get(0));

        final RexNode filter;
        if (call.filterArg >= 0) {
            filter = rexBuilder.makeCall(SqlStdOperatorTable.AND,
                    project.getProjects().get(call.filterArg), filterFromCase);
        } else {
            filter = filterFromCase;
        }

        final SqlParserPos pos = call.getParserPosition();
        newProjects.add(filter);

        AggregateCall result = AggregateCall.create(pos, SqlStdOperatorTable.COUNT, false, false,
                false, call.rexList, ImmutableList.of(), newProjects.size() - 1, null,
                RelCollations.EMPTY, bigInt, call.getName());

        RexInputRef ref = rexBuilder.makeInputRef(bigInt, postProjects.size());
        List<RexNode> postOperands = new ArrayList<>();
        RelDataType nullable = cluster.getTypeFactory().createTypeWithNullability(
                op1.getType(), call.type.isNullable());
        RexLiteral zero = rexBuilder.makeLiteral(BigDecimal.ZERO, bigInt);
        final RexNode postCondition;
        if (elseIsNull) {
            // CASE WHEN count > 0 THEN 1 ELSE NULL END
            postOperands.add(rexBuilder.makeCall(pos, SqlStdOperatorTable.GREATER_THAN, ref, zero));
            postOperands.add(rexBuilder.makeLiteral(BigDecimal.ONE, nullable, true));
            postOperands.add(rexBuilder.makeNullLiteral(nullable));
        } else {
            // CASE WHEN count IS NULL THEN NULL WHEN count = 0 THEN 0 ELSE 1 END
            postOperands.add(rexBuilder.makeCall(pos, SqlStdOperatorTable.IS_NULL, ref));
            postOperands.add(rexBuilder.makeNullLiteral(nullable));
            postOperands.add(rexBuilder.makeCall(pos, SqlStdOperatorTable.EQUALS, ref, zero));
            postOperands.add(rexBuilder.makeLiteral(BigDecimal.ZERO, nullable, true));
            postOperands.add(rexBuilder.makeLiteral(BigDecimal.ONE, nullable, true));
        }
        postCondition = rexBuilder.makeCall(pos, SqlStdOperatorTable.CASE, postOperands);
        postProjects.add(postCondition);
        return result;
    }

    /**
     * Returns the argument, if an aggregate call has a single argument,
     * otherwise -1.
     */
    private static int soleArgument(AggregateCall aggregateCall) {
        return aggregateCall.getArgList().size() == 1
                ? aggregateCall.getArgList().get(0)
                : -1;
    }

    private static boolean isThreeArgCase(final RexNode rexNode) {
        return rexNode.getKind() == SqlKind.CASE
                && ((RexCall) rexNode).operands.size() == 3;
    }

    public static final DefaultOptRuleConfig<MaxCaseToCountRule> CONFIG =
            DefaultOptRuleConfig.<MaxCaseToCountRule>create()
                    .withOperandSupplier(
                            b0 -> b0.operand(Aggregate.class)
                                    .oneInput(b1 -> b1.operand(Project.class)
                                            .anyInputs()));
}
