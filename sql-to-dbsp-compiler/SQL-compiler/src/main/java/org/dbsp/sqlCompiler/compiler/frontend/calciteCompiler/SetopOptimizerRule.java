package org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Intersect;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.SetOp;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.rules.TransformationRule;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.Pair;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** Adapts SetOpToFilterRule for the case when all filters are projected with a constant.
 * That rule does not work in this case, since that requires filters as direct inputs to the union.
 *
 * <p>Plan before:
 * LogicalUnion(all=[false]), id = 127
 *  LogicalProject(EXPR$0=[true]), id = 121
 *    LogicalFilter(condition=[=($cor0.arg1, $1)]), id = 82
 *      LogicalTableScan(table=[[schema, fact_2]]), id = 81
 *  LogicalProject(EXPR$0=[true]), id = 123
 *    LogicalFilter(condition=[AND(=($1, 'admin'), =($cor0.arg1, 'member'))]), id = 96
 *      LogicalTableScan(table=[[schema, fact_2]]), id = 85
 *  LogicalProject(EXPR$0=[true]), id = 126
 *    LogicalFilter(condition=[AND(=($1, 'eng'), =($cor0.arg1, 'member'))]), id = 100
 *      LogicalTableScan(table=[[schema, fact_2]]), id = 90
 *
 * <p>Plan after:
 * LogicalAggregate(group=[{0}]), id = 306
 *   LogicalProject($f0=[true]), id = 304
 *     LogicalFilter(condition=[OR(=($cor0.arg1, $1), AND(=($1, 'admin'), =($cor0.arg1, 'member')), AND(=($1, 'eng'), =($cor0.arg1, 'member')))]), id = 302
 *       LogicalTableScan(table=[[schema, fact_2]]), id = 76
 */
public class SetopOptimizerRule
        extends RelRule<SetopOptimizerRule.Config>
        implements TransformationRule {

    /** Creates an SetOpToFilterRule. */
    protected SetopOptimizerRule(Config config) {
        super(config);
    }

    //~ Methods ----------------------------------------------------------------

    @Override public void onMatch(RelOptRuleCall call) {
        config.matchHandler().accept(this, call);
    }

    private void match(RelOptRuleCall call) {
        final SetOp setOp = call.rel(0);
        final List<RelNode> inputs = setOp.getInputs();
        if (setOp.all || inputs.size() < 2) {
            return;
        }

        final RelBuilder builder = call.builder();
        RexLiteral literal = null;
        for (int i = 0; i < inputs.size(); i++) {
            RelNode input = inputs.get(i).stripped();
            Project proj = (Project) input;
            if (i == 0) {
                literal = (RexLiteral) proj.getProjects().get(0);
            } else {
                RexLiteral nextLiteral = (RexLiteral) proj.getProjects().get(0);
                if (!literal.equals(nextLiteral))
                    return;
            }
        }

        Pair<RelNode, @Nullable RexNode> first = extractSourceAndCond(inputs.get(0).stripped().getInput(0).stripped());

        // Groups conditions by their source relational node and input position.
        // - Key: Pair of (sourceRelNode, inputPosition)
        //   - inputPosition is null for mergeable conditions
        //   - inputPosition contains original index for non-mergeable inputs
        // - Value: List of conditions
        //
        // For invalid conditions (non-deterministic expressions or containing subqueries),
        // positions are tagged with their input indices to skip unmergeable inputs
        // during map-based grouping. Other positions are set to null.
        Map<Pair<RelNode, @Nullable Integer>, List<@Nullable RexNode>> sourceToConds =
                new LinkedHashMap<>();

        RelNode firstSource = first.left;
        sourceToConds.computeIfAbsent(Pair.of(firstSource, null),
                k -> new ArrayList<>()).add(first.right);

        for (int i = 1; i < inputs.size(); i++) {
            final RelNode input = inputs.get(i).stripped().getInput(0).stripped();
            final Pair<RelNode, @Nullable RexNode> pair = extractSourceAndCond(input);
            sourceToConds.computeIfAbsent(Pair.of(pair.left, pair.right != null ? null : i),
                    k -> new ArrayList<>()).add(pair.right);
        }

        if (sourceToConds.size() == inputs.size()) {
            return;
        }

        int branchCount = 0;
        for (Map.Entry<Pair<RelNode, @Nullable Integer>, List<@Nullable RexNode>> entry
                : sourceToConds.entrySet()) {
            Pair<RelNode, @Nullable Integer> left = entry.getKey();
            List<@Nullable RexNode> conds = entry.getValue();
            // Single null condition indicates pass-through branch,
            // directly add its corresponding input to the new inputs list.
            if (conds.size() == 1 && conds.get(0) == null) {
                builder.push(left.left);
                branchCount++;
                continue;
            }

            List<RexNode> condsNonNull = conds.stream().map(e -> {
                assert e != null;
                return e;
            }).collect(Collectors.toList());

            RexNode combinedCond = combineConditions(builder, condsNonNull, setOp);
            builder.push(left.left)
                    .filter(combinedCond)
                    .project(literal);
            branchCount++;
        }

        // RelBuilder will not create 1-input SetOp
        // and remove the distinct after a SetOp
        buildSetOp(builder, branchCount, setOp)
                .distinct();
        call.transformTo(builder.build());
    }

    private static RelBuilder buildSetOp(RelBuilder builder, int count, RelNode setOp) {
        if (setOp instanceof Union) {
            return builder.union(false, count);
        } else if (setOp instanceof Intersect) {
            return builder.intersect(false, count);
        }
        // unreachable
        throw new IllegalStateException("unreachable code");
    }

    private static Pair<RelNode, @Nullable RexNode> extractSourceAndCond(RelNode input) {
        if (input instanceof Filter filter) {
            if (!RexUtil.isDeterministic(filter.getCondition())
                    || RexUtil.SubQueryFinder.containsSubQuery(filter)) {
                // Skip non-deterministic conditions or those containing subqueries
                return Pair.of(input, null);
            }
            return Pair.of(filter.getInput().stripped(), filter.getCondition());
        }
        // For non-filter inputs, use TRUE literal as default condition.
        return Pair.of(input.stripped(),
                input.getCluster().getRexBuilder().makeLiteral(true));
    }

    /**
     * Combines conditions according to set operation:
     * UNION: OR combination
     * INTERSECT: AND combination
     */
    private RexNode combineConditions(RelBuilder builder, List<RexNode> conds,
                                      SetOp setOp) {
        if (setOp instanceof Union) {
            return builder.or(conds);
        } else if (setOp instanceof Intersect) {
            return builder.and(conds);
        }
        // unreachable
        throw new IllegalStateException("unreachable code");
    }

    /** Rule configuration. */
    public interface Config extends RelRule.Config {
        Config UNION = ImmutableSetopOptimizerRule.Config.builder()
                .withMatchHandler(SetopOptimizerRule::match)
                .build()
                .withOperandSupplier(
                        b0 -> b0.operand(Union.class)
                                .inputs(b1 -> b1.operand(Project.class)
                                        .predicate(p -> p.getProjects().size() == 1 &&
                                                p.getProjects().get(0).isA(SqlKind.LITERAL))
                                        .anyInputs()))
                .as(Config.class);

        Config INTERSECT = ImmutableSetopOptimizerRule.Config.builder()
                .withMatchHandler(SetopOptimizerRule::match)
                .build()
                .withOperandSupplier(
                        b0 -> b0.operand(Intersect.class)
                                .inputs(b1 -> b1.operand(Project.class)
                                        .predicate(p -> p.getProjects().size() == 1 &&
                                                p.getProjects().get(0).isA(SqlKind.LITERAL))
                                        .anyInputs()))
                .as(Config.class);

        @Override default SetopOptimizerRule toRule() {
            return new SetopOptimizerRule(this);
        }

        /** Forwards a call to {@link #onMatch(RelOptRuleCall)}. */
        MatchHandler<SetopOptimizerRule> matchHandler();

        /** Sets {@link #matchHandler()}. */
        Config withMatchHandler(MatchHandler<SetopOptimizerRule> matchHandler);
    }
}
