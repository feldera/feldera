package org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.optimizer;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Minus;
import org.apache.calcite.rel.rules.TransformationRule;
import org.apache.calcite.tools.RelBuilder;

import java.util.List;

/**
 * Rewrites an n-ary {@code EXCEPT ALL} (n &gt; 2 inputs) into a binary one by
 * collapsing inputs 1..n-1 into a single {@code UNION ALL}.
 *
 * <p>The transformation is:
 * <pre>{@code
 * A EXCEPT ALL B1 EXCEPT ALL ... EXCEPT ALL Bn
 *   =>
 * A EXCEPT ALL (B1 UNION ALL ... UNION ALL Bn)
 * }</pre>
 *
 * <p>This is semantically correct because the number of copies of value {@code v}
 * retained by EXCEPT ALL is {@code max(0, count_A(v) - count_B1(v) - ... - count_Bn(v))},
 * which equals the result obtained by subtracting the combined right-side multiplicity
 * {@code count_B1(v) + ... + count_Bn(v)} in a single step.
 */
public class ExceptAllFoldRule
        extends RelRule<DefaultOptRuleConfig<ExceptAllFoldRule>>
        implements TransformationRule {

    protected ExceptAllFoldRule() {
        super(CONFIG);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        Minus minus = call.rel(0);
        assert minus.all;

        List<RelNode> inputs = minus.getInputs();
        assert inputs.size() > 2;

        RelBuilder builder = call.builder();

        // Push left side (input[0]).
        builder.push(inputs.get(0));

        // UNION ALL inputs[1..n-1] into a single right-side relation.
        builder.push(inputs.get(1));
        for (int i = 2; i < inputs.size(); i++) {
            builder.push(inputs.get(i));
            builder.union(true);  // UNION ALL
        }

        // Produce a binary EXCEPT ALL(input[0], union_all).
        builder.minus(true);  // EXCEPT ALL

        call.transformTo(builder.build());
    }

    public static final DefaultOptRuleConfig<ExceptAllFoldRule> CONFIG =
            DefaultOptRuleConfig.<ExceptAllFoldRule>create()
                    .withOperandSupplier(b ->
                            b.operand(Minus.class)
                             .predicate(m -> m.all && m.getInputs().size() > 2)
                             .anyInputs());
}
