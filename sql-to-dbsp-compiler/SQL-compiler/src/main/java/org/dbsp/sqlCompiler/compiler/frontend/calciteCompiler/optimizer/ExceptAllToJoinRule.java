package org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.optimizer;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Minus;
import org.apache.calcite.rel.rules.TransformationRule;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.dbsp.util.Utilities;

import java.util.ArrayList;
import java.util.List;

/**
 * Rewrites a binary {@code EXCEPT ALL} into a left join on ROW_NUMBER window functions.
 *
 * <p>For inputs A and B with columns c0, c1, ..., cn the transformation produces:
 * <pre>{@code
 * SELECT a.c0, a.c1, ..., a.cn
 * FROM (SELECT c0,...,cn,
 *              ROW_NUMBER() OVER (PARTITION BY c0,...,cn ORDER BY c0,...,cn) AS $rn
 *       FROM A) AS a
 * LEFT JOIN (SELECT c0,...,cn,
 *                   ROW_NUMBER() OVER (PARTITION BY c0,...,cn ORDER BY c0,...,cn) AS $rn
 *            FROM B) AS b
 * ON  a.c0 IS NOT DISTINCT FROM b.c0
 * AND ...
 * AND a.cn IS NOT DISTINCT FROM b.cn
 * AND a.$rn = b.$rn
 * WHERE b.$rn IS NULL
 * }</pre>
 *
 * <p>ROW_NUMBER partitioned by all data columns assigns each group of equal values its
 * own sequential counter starting at 1.  A row {@code (v, k)} from A survives when
 * there is no row in B with the same value {@code v} and counter {@code k}, i.e. when
 * {@code k} exceeds the count of {@code v} in B.  The surviving rows are exactly those
 * numbered {@code count_B(v)+1} through {@code count_A(v)}, giving
 * {@code max(0, count_A(v) - count_B(v))} copies of {@code v} — the multiset-difference
 * (EXCEPT ALL) semantics.
 */
public class ExceptAllToJoinRule
        extends RelRule<DefaultOptRuleConfig<ExceptAllToJoinRule>>
        implements TransformationRule {

    protected ExceptAllToJoinRule() {
        super(CONFIG);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        Minus minus = call.rel(0);
        Utilities.enforce(minus.all);

        List<RelNode> inputs = minus.getInputs();
        Utilities.enforce(inputs.size() == 2);
        int fieldCount = minus.getRowType().getFieldCount();

        RelBuilder builder = call.builder();

        // Build the augmented projection (data cols + ROW_NUMBER) for each input.
        List<RelNode> augmented = new ArrayList<>(2);
        for (RelNode input : inputs) {
            augmented.add(IntersectAllToJoinRule.augmentWithRowNumber(builder, input));
        }

        // LEFT JOIN augmented[0] (A) with augmented[1] (B).
        // After the join the field layout is:
        //   positions 0..fieldCount      : a.c0,...,a.cn, a.$rn
        //   positions fieldCount+1..2*fieldCount+1 : b.c0,...,b.cn, b.$rn
        builder.push(augmented.get(0));
        builder.push(augmented.get(1));

        // Join condition: a.ci IS NOT DISTINCT FROM b.ci for all columns including $rn.
        List<RexNode> conditions = new ArrayList<>(fieldCount + 1);
        for (int i = 0; i <= fieldCount; i++) {
            conditions.add(builder.call(
                    SqlStdOperatorTable.IS_NOT_DISTINCT_FROM,
                    builder.field(2, 0, i),
                    builder.field(2, 1, i)));
        }

        builder.join(JoinRelType.LEFT, conditions);

        // Keep only rows from A with no matching pair in B (b.$rn IS NULL).
        // b.$rn is at position 2*fieldCount+1 in the joined row.
        builder.filter(builder.call(
                SqlStdOperatorTable.IS_NULL,
                builder.field(2 * fieldCount + 1)));

        // Final project: drop the $rn and b.* columns, keep only the original data columns.
        List<String> outputNames = minus.getRowType().getFieldNames();
        List<RexNode> output = new ArrayList<>(fieldCount);
        for (int i = 0; i < fieldCount; i++) {
            output.add(builder.field(i));
        }
        builder.project(output, outputNames);

        call.transformTo(builder.build());
    }

    public static final DefaultOptRuleConfig<ExceptAllToJoinRule> CONFIG =
            DefaultOptRuleConfig.<ExceptAllToJoinRule>create()
                    .withOperandSupplier(b ->
                            b.operand(Minus.class)
                             .predicate(m -> m.all && m.getInputs().size() == 2)
                             .anyInputs());
}
