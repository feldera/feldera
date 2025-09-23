package org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.optimizer;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.rel.rules.TransformationRule;
import org.apache.calcite.sql2rel.RelDecorrelator;

/** The Calcite decorrelator is all or nothing: if it fails to decorrelate a query, it does nothing
 * for subqueries.  This optimization pass finds trees rooted at a Correlate node without any
 * Correlate children and tries to decorrelate them independently. */
public class InnerDecorrelator
        extends RelRule<DefaultOptRuleConfig<InnerDecorrelator>>
        implements TransformationRule {

    protected InnerDecorrelator() {
        super(InnerDecorrelator.CONFIG);
    }

    @Override public void onMatch(RelOptRuleCall call) {
        Correlate cor = call.rel(0);
        RelNode stripped = CalciteOptimizer.stripRecursively(cor);
        for (RelNode input: stripped.getInputs()) {
            if (CalciteOptimizer.containsCorrelate(input))
                // Give up
                return;
        }

        RelNode rel = RelDecorrelator.decorrelateQuery(stripped, call.builder());
        if (rel != null
                && !CalciteOptimizer.containsCorrelate(rel)
                // This is necessary due to https://issues.apache.org/jira/browse/CALCITE-7024
                && cor.getRowType().equals(rel.getRowType())) {
            call.transformTo(rel);
        }
    }

    private static final DefaultOptRuleConfig<InnerDecorrelator> CONFIG =
            DefaultOptRuleConfig.<InnerDecorrelator>create()
                .withOperandSupplier(b0 -> b0.operand(Correlate.class)
                        .anyInputs());
}
