package org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.optimizer;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Uncollect;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.rules.TransformationRule;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.List;

/** Convert representations of Unnest that use LogicalCorrelate into
 * simple Unnest representations.
 *
 * <p>
 * LogicalProject // only uses rightmost columns of correlate
 *   LogicalCorrelate(correlation=[$cor0], joinType=[inner], requiredColumns=[{...}])
 *     LeftSubquery (arbitrary)
 *     Uncollect
 *       LogicalProject(COL=[$cor0.ARRAY])
 *         LogicalValues(tuples=[[{ 0 }]])
 *
 * <p>is converted to
 *
 * <p>
 * LogicalProject
 *   Uncollect
 *     LogicalProject
 *        LeftSubquery
 */
public class DecorrelateUnnest
        extends RelRule<DefaultOptRuleConfig<DecorrelateUnnest>>
        implements TransformationRule {

    protected DecorrelateUnnest() {
        super(DecorrelateUnnest.CONFIG);
    }

    @Override public void onMatch(RelOptRuleCall call) {
        Project outer = call.rel(0);
        Correlate cor = call.rel(1);
        CorrelationId corId = cor.getCorrelationId();

        // The outer project must keep only fields from the RHS
        RelNode left = call.rel(2);
        int leftCount = left.getRowType().getFieldCount();
        ImmutableBitSet used = RelOptUtil.InputFinder.bits(outer.getProjects(), null);
        for (int ref: used)
            if (ref < leftCount)
                return;

        Uncollect uncollect = call.rel(3);
        Project project = call.rel(4);
        List<RexNode> projects = project.getProjects();
        if (projects.size() != 1)
            return;

        RexNode projected = projects.get(0);
        if (projected instanceof RexFieldAccess fa) {
            RexNode referenceExpr = fa.getReferenceExpr();
            if (referenceExpr instanceof RexCorrelVariable cv) {
                if (cv.id != corId)
                    return;

                RelBuilder builder = call.builder();
                builder.push(left);

                RexInputRef field = builder.field(fa.getField().getName());
                builder.project(field);
                builder.uncollect(uncollect.getItemAliases(), uncollect.withOrdinality);
                List<RexNode> shifted = RexUtil.shift(outer.getProjects(), -leftCount);
                builder.project(shifted);
                RelNode result = builder.build();
                call.transformTo(result);
            }
        }
    }

    private static final DefaultOptRuleConfig<DecorrelateUnnest> CONFIG =
            DefaultOptRuleConfig.<DecorrelateUnnest>create()
                    .withOperandSupplier(b0 -> b0.operand(Project.class)
                            .oneInput(b1 -> b1.operand(Correlate.class)
                                    .inputs(b2 -> b2.operand(RelNode.class).anyInputs(),
                                            b3 -> b3.operand(Uncollect.class)
                                .oneInput(b4 -> b4.operand(Project.class)
                                        .oneInput(b5 -> b5.operand(LogicalValues.class).anyInputs())))));
}
