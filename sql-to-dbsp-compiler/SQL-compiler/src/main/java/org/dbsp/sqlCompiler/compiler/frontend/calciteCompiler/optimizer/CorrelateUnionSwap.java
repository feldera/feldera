package org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.optimizer;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.rules.ProjectCorrelateTransposeRule;
import org.apache.calcite.rel.rules.TransformationRule;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.tools.RelBuilder;
import org.dbsp.util.Utilities;

import java.util.List;
import java.util.stream.Collectors;

/** Swaps a Correlate call with an inner Union if used to implement an EXISTS pattern.
 *
 * <p>Plan before:
 * LogicalCorrelate(correlation=[$cor1], joinType=[left], requiredColumns=[{...}])
 *   LeftInput
 *   LogicalAggregate(group=[{}], agg#0=[MIN($0)]
 *     LogicalProject($f0=[true])
 *       LogicalUnion(all=[false])
 *         LogicalProject(EXPR$0=[true])
 *           Input0
 *         LogicalProject(EXPR$0=[true])
 *           Input1
 *         ...
 *
 * <p>Plan after:
 * LogicalUnion(all=[false])
 *   LogicalCorrelate(correlation=[$cor1], joinType=[left], requiredColumns=[{...}])
 *     LeftInput
 *     LogicalAggregate(group=[{}], agg#0=[MIN($0)]
 *       LogicalProject($f0=[true])
 *         Input0
 *   LogicalCorrelate(correlation=[$cor2], joinType=[left], requiredColumns=[{...}])
 *     LeftInput
 *     LogicalAggregate(group=[{}], agg#0=[MIN($0)]
 *       LogicalProject($f0=[true])
 *         Input1
 */
public class CorrelateUnionSwap
        extends RelRule<DefaultOptRuleConfig<CorrelateUnionSwap>>
        implements TransformationRule {

    protected CorrelateUnionSwap() {
        super(CONFIG);
    }

    public static class RexFieldAccessReplacer extends RexShuttle {
        private final RexBuilder builder;
        private final CorrelationId rexCorrelVariableToReplace;
        private final RexCorrelVariable rexCorrelVariable;

        public RexFieldAccessReplacer(
                CorrelationId rexCorrelVariableToReplace,
                RexCorrelVariable rexCorrelVariable,
                RexBuilder builder) {
            this.rexCorrelVariableToReplace = rexCorrelVariableToReplace;
            this.rexCorrelVariable = rexCorrelVariable;
            this.builder = builder;
        }

        @Override public RexNode visitCorrelVariable(RexCorrelVariable variable) {
            if (variable.id.equals(rexCorrelVariableToReplace)) {
                return rexCorrelVariable;
            }
            return variable;
        }

        @Override public RexNode visitFieldAccess(RexFieldAccess fieldAccess) {
            RexNode refExpr = fieldAccess.getReferenceExpr().accept(this);
            if (refExpr == this.rexCorrelVariable) {
                int fieldIndex = fieldAccess.getField().getIndex();
                return this.builder.makeFieldAccess(
                        refExpr,
                        fieldIndex);
            }
            return super.visitFieldAccess(fieldAccess);
        }
    }

    @Override public void onMatch(RelOptRuleCall call) {
        Correlate corr = call.rel(0);
        RelNode leftInput = call.rel(1);
        Aggregate aggregate = call.rel(2);
        Project project = call.rel(3);
        Union union = call.rel(4);
        RelBuilder builder = call.builder();
        for (RelNode input: union.getInputs()) {
            input = input.stripped();
            builder.push(leftInput);
            Utilities.enforce(input instanceof Project);
            Project proj = (Project) input;
            RelNode projInput = proj.getInput().stripped();
            CorrelationId corrId = builder.getCluster().createCorrel();
            RexCorrelVariable rexCorrel = (RexCorrelVariable) builder.getRexBuilder().makeCorrel(corr.getRowType(), corrId);
            projInput = projInput.accept(
                    new ProjectCorrelateTransposeRule.RelNodesExprsHandler(
                            new RexFieldAccessReplacer(corr.getCorrelationId(), rexCorrel, builder.getRexBuilder())));

            builder.push(projInput);
            builder.project(project.getProjects());
            builder.aggregate(builder.groupKey(), aggregate.getAggCallList());
            List<RexNode> requiredNodes =
                    corr.getRequiredColumns().asList().stream()
                            .map(ord -> builder.getRexBuilder().makeInputRef(corr.getRowType(), ord))
                            .collect(Collectors.toList());
            builder.correlate(corr.getJoinType(), corrId, requiredNodes);
        }
        builder.union(union.all);
        call.transformTo(builder.build());
    }

    static boolean projectsToTrue(Project project) {
        return project.getProjects().size() == 1 &&
                project.getProjects().get(0).isAlwaysTrue();
    }

    static boolean allInputsProjectToTrue(Union u) {
        for (RelNode input: u.getInputs()) {
            input = input.stripped();
            if (!(input instanceof Project proj))
                return false;
            if (!projectsToTrue(proj))
                return false;
        }
        return true;
    }

    /** Rule configuration. */
    private static final DefaultOptRuleConfig<CorrelateUnionSwap> CONFIG =
            DefaultOptRuleConfig.<CorrelateUnionSwap>create()
                .withOperandSupplier(
                        b0 -> b0.operand(Correlate.class)
                                .inputs(b1 -> b1.operand(RelNode.class).anyInputs(),
                                        b2 -> b2.operand(Aggregate.class)
                                                .predicate(a -> a.getGroupCount() == 0 &&
                                                        a.getAggCallList().size() == 1 &&
                                                        a.getAggCallList().get(0).getAggregation().getKind() == SqlKind.MIN)
                                                .oneInput(b3 -> b3.operand(Project.class)
                                                        .predicate(CorrelateUnionSwap::projectsToTrue)
                                                        .oneInput(b4 -> b4.operand(Union.class)
                                                                .predicate(CorrelateUnionSwap::allInputsProjectToTrue)
                                                                .anyInputs()))));
}
