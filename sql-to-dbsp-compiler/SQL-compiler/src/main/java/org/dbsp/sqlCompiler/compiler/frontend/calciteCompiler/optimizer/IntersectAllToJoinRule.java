package org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.optimizer;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Intersect;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.rules.TransformationRule;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexFieldCollation;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexWindowBound;
import org.apache.calcite.rex.RexWindowBounds;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;

import java.util.ArrayList;
import java.util.List;

/**
 * Rewrites {@code INTERSECT ALL} into an inner join on ROW_NUMBER window functions.
 *
 * <p>For two inputs A and B with columns c0, c1, ..., cn the transformation produces:
 * <pre>{@code
 * SELECT a.c0, a.c1, ..., a.cn
 * FROM (SELECT c0,...,cn,
 *              ROW_NUMBER() OVER (PARTITION BY c0,...,cn ORDER BY c0,...,cn) AS $rn
 *       FROM A) AS a
 * JOIN (SELECT c0,...,cn,
 *              ROW_NUMBER() OVER (PARTITION BY c0,...,cn ORDER BY c0,...,cn) AS $rn
 *       FROM B) AS b
 * ON  a.c0 IS NOT DISTINCT FROM b.c0
 * AND ...
 * AND a.cn IS NOT DISTINCT FROM b.cn
 * AND a.$rn = b.$rn
 * }</pre>
 *
 * <p>Partitioning by all data columns gives each group of duplicate values its own
 * sequential counter.  Joining on both the data columns and the counter selects
 * exactly {@code min(count_A(v), count_B(v))} copies of every value {@code v},
 * which is the multiset-intersection (INTERSECT ALL) semantics.
 *
 * <p>{@code IS NOT DISTINCT FROM} is used for the data-column equalities so that
 * {@code NULL} values are matched correctly (standard {@code =} would reject them).
 *
 * <p>For more than two inputs the rule folds left-to-right:
 * it first intersects inputs 0 and 1, keeps the running {@code $rn} column,
 * and joins each subsequent input in turn.
 */
public class IntersectAllToJoinRule
        extends RelRule<DefaultOptRuleConfig<IntersectAllToJoinRule>>
        implements TransformationRule {

    protected IntersectAllToJoinRule() {
        super(CONFIG);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        Intersect intersect = call.rel(0);
        assert intersect.all;

        List<RelNode> inputs = intersect.getInputs();
        int fieldCount = intersect.getRowType().getFieldCount();

        RelBuilder builder = call.builder();
        RexBuilder rexBuilder = intersect.getCluster().getRexBuilder();
        RelDataTypeFactory typeFactory = intersect.getCluster().getTypeFactory();
        RelDataType bigintType = typeFactory.createSqlType(SqlTypeName.BIGINT);

        // Build the augmented projection (data cols + ROW_NUMBER) for each input.
        List<RelNode> augmented = new ArrayList<>(inputs.size());
        for (RelNode input : inputs) {
            List<RelDataTypeField> fields = input.getRowType().getFieldList();

            // PARTITION BY c0, c1, ..., cn
            List<RexNode> partitionKeys = new ArrayList<>(fieldCount);
            for (int i = 0; i < fieldCount; i++) {
                partitionKeys.add(rexBuilder.makeInputRef(input, i));
            }

            // ORDER BY c0 ASC, c1 ASC, ..., cn ASC
            // Within a partition all rows have identical values, so the order is arbitrary;
            // we order by the data columns to produce a deterministic plan.
            ImmutableList.Builder<RexFieldCollation> orderKeyBuilder = ImmutableList.builder();
            for (int i = 0; i < fieldCount; i++) {
                orderKeyBuilder.add(new RexFieldCollation(
                        rexBuilder.makeInputRef(input, i),
                        ImmutableSet.of())); // ASC
            }
            ImmutableList<RexFieldCollation> orderKeys = orderKeyBuilder.build();

            RexWindowBound lowerBound = RexWindowBounds.UNBOUNDED_PRECEDING;
            RexWindowBound upperBound = RexWindowBounds.CURRENT_ROW;

            RexNode rowNum = rexBuilder.makeOver(
                    bigintType,
                    SqlStdOperatorTable.ROW_NUMBER,
                    ImmutableList.of(),  // ROW_NUMBER takes no arguments
                    partitionKeys,
                    orderKeys,
                    lowerBound,
                    upperBound,
                    true,   // allowPartial
                    true,   // isRows
                    false,  // nullWhenCountZero
                    false,  // distinct
                    false); // ignoreNulls

            // Project: c0, c1, ..., cn, $rn
            List<RexNode> projects = new ArrayList<>(fieldCount + 1);
            List<String> names = new ArrayList<>(fieldCount + 1);
            for (int i = 0; i < fieldCount; i++) {
                projects.add(rexBuilder.makeInputRef(input, i));
                names.add(fields.get(i).getName());
            }
            projects.add(rowNum);
            names.add("$rn");

            builder.push(input).project(projects, names);
            augmented.add(builder.build());
        }

        // Fold left-to-right over all augmented inputs.
        // After each join the running relation has: c0,...,cn,$rn (fieldCount+1 fields).
        builder.push(augmented.get(0));

        for (int side = 1; side < augmented.size(); side++) {
            RelNode right = augmented.get(side);
            builder.push(right);

            // Join condition: a.ci IS NOT DISTINCT FROM b.ci  (for i < fieldCount)
            //               AND a.$rn = b.$rn
            List<RexNode> conditions = new ArrayList<>(fieldCount + 1);
            for (int i = 0; i < fieldCount; i++) {
                conditions.add(builder.call(
                        SqlStdOperatorTable.IS_NOT_DISTINCT_FROM,
                        builder.field(2, 0, i),
                        builder.field(2, 1, i)));
            }
            // $rn is always non-null (BIGINT), so plain equality is correct.
            conditions.add(builder.call(
                    SqlStdOperatorTable.EQUALS,
                    builder.field(2, 0, fieldCount),
                    builder.field(2, 1, fieldCount)));

            builder.join(JoinRelType.INNER, conditions);

            // After the join the row type is [left_fields..., right_fields...].
            // Project back to [c0,...,cn, $rn] (from the left side) so the next
            // iteration can join against the next input without shifting offsets.
            if (side < augmented.size() - 1) {
                List<RexNode> keepCols = new ArrayList<>(fieldCount + 1);
                for (int i = 0; i <= fieldCount; i++) {
                    keepCols.add(builder.field(i));
                }
                builder.project(keepCols);
            }
        }

        // Final project: drop the $rn column, keep only the original data columns.
        List<String> outputNames = intersect.getRowType().getFieldNames();
        List<RexNode> output = new ArrayList<>(fieldCount);
        for (int i = 0; i < fieldCount; i++) {
            output.add(builder.field(i));
        }
        builder.project(output, outputNames);

        call.transformTo(builder.build());
    }

    public static final DefaultOptRuleConfig<IntersectAllToJoinRule> CONFIG =
            DefaultOptRuleConfig.<IntersectAllToJoinRule>create()
                    .withOperandSupplier(b ->
                            b.operand(Intersect.class)
                             .predicate(i -> i.all)
                             .anyInputs());
}
