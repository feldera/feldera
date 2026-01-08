package org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.optimizer;

import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.rules.PruneEmptyRules;
import org.apache.calcite.rel.rules.TransformationRule;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.util.Util;
import org.apache.calcite.util.trace.CalciteTrace;

import com.google.common.collect.ImmutableList;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

import static java.util.Objects.requireNonNull;

/**
 * Clone of Calcite rule {@link org.apache.calcite.rel.rules.ValuesReduceRule}
 * which uses our own version of reduction.
 *
 * <p>Planner rule that folds projections and filters into an underlying
 * {@link org.apache.calcite.rel.logical.LogicalValues}.
 *
 * <p>Returns a simplified {@code Values}, perhaps containing zero tuples
 * if all rows are filtered away.
 *
 * <p>For example,
 *
 * <blockquote><code>select a - b from (values (1, 2), (3, 5), (7, 11)) as t (a,
 * b) where a + b &gt; 4</code></blockquote>
 *
 * <p>becomes
 *
 * <blockquote><code>select x from (values (-2), (-4))</code></blockquote>
 *
 * <p>Ignores an empty {@code Values}; this is better dealt with by
 * {@link PruneEmptyRules}.
 *
 * @see CoreRules#FILTER_VALUES_MERGE
 * @see CoreRules#PROJECT_VALUES_MERGE
 * @see CoreRules#PROJECT_FILTER_VALUES_MERGE
 */
public class ValuesReduceRule
        extends RelRule<DefaultOptRuleConfig<ValuesReduceRule>>
        implements TransformationRule {

    private static final Logger LOGGER = CalciteTrace.getPlannerTracer();

    /** Creates a ValuesReduceRule. */
    protected ValuesReduceRule(DefaultOptRuleConfig<ValuesReduceRule> config) {
        super(config);
        Util.discard(LOGGER);
    }

    private static void matchProjectFilter(ValuesReduceRule rule,
                                           RelOptRuleCall call) {
        LogicalProject project = call.rel(0);
        LogicalFilter filter = call.rel(1);
        LogicalValues values = call.rel(2);
        rule.apply(call, project, filter, values);
    }

    private static void matchProject(ValuesReduceRule rule, RelOptRuleCall call) {
        LogicalProject project = call.rel(0);
        LogicalValues values = call.rel(1);
        rule.apply(call, project, null, values);
    }

    private static void matchFilter(ValuesReduceRule rule, RelOptRuleCall call) {
        LogicalFilter filter = call.rel(0);
        LogicalValues values = call.rel(1);
        rule.apply(call, null, filter, values);
    }

    @Override public void onMatch(RelOptRuleCall call) {
        config.matchHandler().accept(this, call);
    }

    /**
     * Does the work.
     *
     * @param call    Rule call
     * @param project Project, may be null
     * @param filter  Filter, may be null
     * @param values  Values rel to be reduced
     */
    protected void apply(RelOptRuleCall call, @Nullable LogicalProject project,
                         @Nullable LogicalFilter filter, LogicalValues values) {
        requireNonNull(values, "values");
        checkArgument(filter != null || project != null);
        final RexNode conditionExpr =
                (filter == null) ? null : filter.getCondition();
        final List<RexNode> projectExprs =
                (project == null) ? null : project.getProjects();
        RexBuilder rexBuilder = values.getCluster().getRexBuilder();

        // Find reducible expressions.
        final List<RexNode> reducibleExps = new ArrayList<>();
        final ValuesReduceRule.MyRexShuttle shuttle = new ValuesReduceRule.MyRexShuttle();
        for (final List<RexLiteral> literalList : values.getTuples()) {
            shuttle.literalList = literalList;
            if (conditionExpr != null) {
                RexNode c = conditionExpr.accept(shuttle);
                reducibleExps.add(c);
            }
            if (projectExprs != null) {
                requireNonNull(project, "project");
                int k = -1;
                for (RexNode projectExpr : projectExprs) {
                    ++k;
                    RexNode e = projectExpr.accept(shuttle);
                    if (RexLiteral.isNullLiteral(e)) {
                        RelDataType type =
                                project.getRowType().getFieldList().get(k).getType();
                        e = rexBuilder.makeAbstractCast(type, e, false);
                    }
                    reducibleExps.add(e);
                }
            }
        }
        int fieldsPerRow =
                ((conditionExpr == null) ? 0 : 1)
                        + ((projectExprs == null) ? 0 : projectExprs.size());
        assert fieldsPerRow > 0;
        assert reducibleExps.size() == (values.getTuples().size() * fieldsPerRow);

        // Compute the values they reduce to.
        final RelOptPredicateList predicates = RelOptPredicateList.EMPTY;
        ReduceExpressionsRule.reduceExpressions(values, reducibleExps, predicates,
                false, true, false);

        int changeCount = 0;
        final ImmutableList.Builder<ImmutableList<RexLiteral>> tuplesBuilder =
                ImmutableList.builder();
        for (int row = 0; row < values.getTuples().size(); ++row) {
            int i = 0;
            if (conditionExpr != null) {
                final RexNode reducedValue =
                        reducibleExps.get((row * fieldsPerRow) + i);
                ++i;
                if (!reducedValue.isAlwaysTrue()) {
                    ++changeCount;
                    continue;
                }
            }

            final ImmutableList<RexLiteral> valuesList;
            if (projectExprs != null) {
                ++changeCount;
                final ImmutableList.Builder<RexLiteral> tupleBuilder =
                        ImmutableList.builder();
                for (; i < fieldsPerRow; ++i) {
                    final RexNode reducedValue =
                            reducibleExps.get((row * fieldsPerRow) + i);
                    if (reducedValue instanceof RexLiteral) {
                        tupleBuilder.add((RexLiteral) reducedValue);
                    } else if (RexUtil.isNullLiteral(reducedValue, true)) {
                        tupleBuilder.add(rexBuilder.makeNullLiteral(reducedValue.getType()));
                    } else {
                        return;
                    }
                }
                valuesList = tupleBuilder.build();
            } else {
                valuesList = values.getTuples().get(row);
            }
            tuplesBuilder.add(valuesList);
        }

        if (changeCount > 0) {
            final RelDataType rowType;
            if (projectExprs != null) {
                rowType = requireNonNull(project, "project").getRowType();
            } else {
                rowType = values.getRowType();
            }
            final RelNode newRel =
                    LogicalValues.create(values.getCluster(), rowType,
                            tuplesBuilder.build());
            call.transformTo(newRel);
        } else {
            // Filter had no effect, so we can say that Filter(Values) ==
            // Values.
            call.transformTo(values);
        }

        // New plan is absolutely better than old plan. (Moreover, if
        // changeCount == 0, we've proved that the filter was trivial, and that
        // can send the volcano planner into a loop; see dtbug 2070.)
        if (filter != null) {
            call.getPlanner().prune(filter);
        }
    }

    //~ Inner Classes ----------------------------------------------------------

    /** Shuttle that converts inputs to literals. */
    private static class MyRexShuttle extends RexShuttle {
        private @Nullable List<RexLiteral> literalList;

        @Override public RexNode visitInputRef(RexInputRef inputRef) {
            requireNonNull(literalList, "literalList");
            return literalList.get(inputRef.getIndex());
        }
    }

    static final DefaultOptRuleConfig<ValuesReduceRule> FILTER = DefaultOptRuleConfig.<ValuesReduceRule>create()
            .withOperandSupplier(b0 ->
                    b0.operand(LogicalFilter.class).oneInput(b1 ->
                            b1.operand(LogicalValues.class)
                                    .predicate(Values::isNotEmpty).noInputs()))
            .withMatchHandler(ValuesReduceRule::matchFilter);

    static final DefaultOptRuleConfig<ValuesReduceRule> PROJECT = DefaultOptRuleConfig.<ValuesReduceRule>create()
            .withDescription("ValuesReduceRule(Project)")
            .withOperandSupplier(b0 ->
                    b0.operand(LogicalProject.class).oneInput(b1 ->
                            b1.operand(LogicalValues.class)
                                    .predicate(Values::isNotEmpty).noInputs()))
            .withMatchHandler(ValuesReduceRule::matchProject);

    static final DefaultOptRuleConfig<ValuesReduceRule> PROJECT_FILTER = DefaultOptRuleConfig.<ValuesReduceRule>create()
            .withDescription("ValuesReduceRule(Project-Filter)")
            .withOperandSupplier(b0 ->
                    b0.operand(LogicalProject.class).oneInput(b1 ->
                            b1.operand(LogicalFilter.class).oneInput(b2 ->
                                    b2.operand(LogicalValues.class)
                                            .predicate(Values::isNotEmpty).noInputs())))
            .withMatchHandler(ValuesReduceRule::matchProjectFilter);

    /** Rule that merges a {@link Filter} onto an underlying
     * {@link LogicalValues},
     * resulting in a {@code Values} with potentially fewer rows. */
    public static final ValuesReduceRule FILTER_VALUES_MERGE =
            new ValuesReduceRule(FILTER);

    /** Rule that merges a {@link Project} onto an underlying
     * {@link LogicalValues},
     * resulting in a {@code Values} with different columns. */
    public static final ValuesReduceRule PROJECT_VALUES_MERGE =
            new ValuesReduceRule(PROJECT);

    /** Rule that merges a {@link Project}
     * on top of a {@link Filter} onto an underlying
     * {@link LogicalValues},
     * resulting in a {@code Values} with different columns
     * and potentially fewer rows. */
    public static final ValuesReduceRule PROJECT_FILTER_VALUES_MERGE =
            new ValuesReduceRule(PROJECT_FILTER);
}

