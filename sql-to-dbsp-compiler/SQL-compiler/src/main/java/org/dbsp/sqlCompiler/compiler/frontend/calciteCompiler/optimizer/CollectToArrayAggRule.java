package org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.optimizer;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Collect;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.rules.TransformationRule;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.functions.SqlArrayAggFunction;
import org.dbsp.util.Utilities;

import java.util.List;
import java.util.Set;

/**
 * Rewrites a Collect that builds an ARRAY from an input that uses correlation variables
 * into an ARRAY_AGG aggregate.  The Calcite decorrelators cannot decorrelate a Collect:
 * https://issues.apache.org/jira/browse/CALCITE-7975
 *
 * <p>The plan of an {@code ARRAY(subquery)} contains a Collect.
 * A Collect emits exactly one row with one ARRAY field:
 * <ul>
 *     <li>the array holds one element for each input row;</li>
 *     <li>for an empty input the array is empty, not NULL;</li>
 *     <li>for an input with one field the element is the field, otherwise the element
 *     is a ROW with all the input fields;</li>
 *     <li>for an input that is a Sort the elements are in the Sort order.</li>
 * </ul>
 *
 * <pre>
 * Collect(field=[x])
 *   LogicalProject(v=[$2])
 *     LogicalFilter(condition=[=($1, $cor0.id)])
 * </pre>
 * becomes
 * <pre>
 * LogicalProject(x=[COALESCE($0, ARRAY())])
 *   LogicalAggregate(group=[{}], x=[ARRAY_AGG($0)])
 *     LogicalProject(v=[$2])
 *       LogicalFilter(condition=[=($1, $cor0.id)])
 * </pre>
 *
 * <p>Decorrelation gives NULL for the ARRAY_AGG of a correlation value without rows;
 * COALESCE replaces the NULL with an empty array. */
public class CollectToArrayAggRule
        extends RelRule<DefaultOptRuleConfig<CollectToArrayAggRule>>
        implements TransformationRule {
    public CollectToArrayAggRule() {
        super(CONFIG);
    }

    /** True if the plan contains a Sort with sort keys */
    private boolean hasSortWithKeys(RelNode node) {
        if (node instanceof Sort sort && !sort.getCollation().getFieldCollations().isEmpty())
            return true;
        return node.getInputs().stream().anyMatch(this::hasSortWithKeys);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final Collect collect = call.rel(0);
        final RelNode input = CalciteOptimizer.stripRecursively(collect.getInput());
        // The variables that the input uses, but does not define
        final Set<CorrelationId> variables = RelOptUtil.getVariablesUsed(input);
        if (variables.isEmpty())
            return;
        // Decorrelation does not remap the sort keys of an ARRAY_AGG, so the array would lose its order.
        // The Sort of an ORDER BY on a column that is not selected is under a Project.
        // https://issues.apache.org/jira/browse/CALCITE-7974
        if (this.hasSortWithKeys(input))
            return;

        RelNode aggregateInput = collect.getInput().stripped();

        // RelBuilder would simplify away the COALESCE when the ARRAY_AGG type is NOT NULL
        final RexBuilder rexBuilder = collect.getCluster().getRexBuilder();
        final RelDataType arrayType = collect.getRowType().getFieldList().get(0).getType();
        final RelDataType elementType = arrayType.getComponentType();
        Utilities.enforce(elementType != null);

        final String fieldName = collect.getFieldName();
        if (aggregateInput.getRowType().getFieldCount() > 1) {
            // The element is the ROW of all input fields
            final List<RexNode> fields = rexBuilder.identityProjects(aggregateInput.getRowType());
            final RexNode row = rexBuilder.makeCall(elementType, SqlStdOperatorTable.ROW, fields);
            aggregateInput = LogicalProject.create(aggregateInput, List.of(), List.of(row), List.of(fieldName), Set.of());
        }

        final AggregateCall arrayAgg = AggregateCall.create(
                SqlParserPos.ZERO, SqlArrayAggFunction.INSTANCE, false, false, false,
                ImmutableList.of(), ImmutableList.of(0), -1,
                null, RelCollations.EMPTY, true, aggregateInput, null, fieldName);
        final RelNode aggregate = LogicalAggregate.create(
                aggregateInput, List.of(), ImmutableBitSet.of(), null, List.of(arrayAgg));

        final RexNode emptyArray = rexBuilder.makeCall(
                arrayType, SqlStdOperatorTable.ARRAY_VALUE_CONSTRUCTOR, List.of());
        final RexNode array = rexBuilder.makeCall(
                arrayType, SqlStdOperatorTable.COALESCE, List.of(rexBuilder.makeInputRef(aggregate, 0), emptyArray));
        final RelNode collected = LogicalProject.create(aggregate, List.of(), List.of(array), collect.getRowType(), Set.of());

        call.transformTo(collected);
    }

    private static final DefaultOptRuleConfig<CollectToArrayAggRule> CONFIG =
            DefaultOptRuleConfig.<CollectToArrayAggRule>create()
                    .withOperandSupplier(
                            b0 -> b0.operand(Collect.class)
                                    .predicate(c -> c.getCollectionType() == SqlTypeName.ARRAY)
                                    .anyInputs());
}
