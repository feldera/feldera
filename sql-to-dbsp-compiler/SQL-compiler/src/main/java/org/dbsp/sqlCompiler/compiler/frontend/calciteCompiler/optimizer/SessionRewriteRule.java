package org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.optimizer;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalTableFunctionScan;
import org.apache.calcite.rel.rules.TransformationRule;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexWindowBounds;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlWindowTableFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

/**
 * Rewrites the SESSION window table function into standard relational
 * operators.
 *
 * <p>{@code SESSION(TABLE t, DESCRIPTOR(ts), DESCRIPTOR(k), gap)} returns
 * the rows of t with two extra columns.  Rows with the same key k whose
 * timestamps are less than gap apart belong to the same session.  For every
 * row, window_start is the session's first timestamp and window_end is its
 * last timestamp plus gap.  The key descriptor is optional; without it all
 * rows share one session timeline.  Rows with a NULL timestamp are dropped.
 *
 * <pre>
 * LogicalTableFunctionScan(SESSION(DESCRIPTOR($ts), DESCRIPTOR($k), gap))
 *   Input($0..$n-1)
 * </pre>
 * becomes ("sessionized" appears twice but is built once):
 * <pre>
 * LogicalProject($0..$n-1, window_start=[$min], window_end=[$max + gap])
 *   LogicalJoin(INNER, $k IS NOT DISTINCT FROM $k', $sid IS NOT DISTINCT FROM $sid')
 *     sessionized
 *     LogicalAggregate(group=[{$k, $sid}], min=[MIN($ts)], max=[MAX($ts)])
 *       sessionized
 * </pre>
 * where "sessionized" numbers each row's session within its key:
 * <pre>
 * LogicalProject($0..$n-1, sid=[SUM($brk) OVER w])
 *   LogicalProject($0..$n-1, brk=[CASE($prev IS NULL OR $ts >= $prev + gap, 1, 0)])
 *     LogicalProject($0..$n-1, prev=[LAG($ts) OVER w])
 *       LogicalFilter($ts IS NOT NULL)      (only for a nullable column)
 *         Input($0..$n-1)
 * </pre>
 * with w = PARTITION BY $k ORDER BY $ts RANGE UNBOUNDED PRECEDING.
 * A row starts a new session (brk = 1) when it is the first of its key or
 * follows its predecessor by gap or more, so the running sum of brk
 * identifies the row's session.
 *
 * <p>With ties in $ts the RANGE frame sums brk over all peers, which
 * assigns tied rows the same session; this matches the SESSION semantics,
 * since rows with equal timestamps always share a session.
 */
public class SessionRewriteRule
        extends RelRule<DefaultOptRuleConfig<SessionRewriteRule>>
        implements TransformationRule {
    public SessionRewriteRule() {
        super(CONFIG);
    }

    /** Column indexes of a DESCRIPTOR call, or null if 'node' is not one. */
    @Nullable
    static List<Integer> descriptorColumns(RexNode node) {
        if (!(node instanceof RexCall call) || call.getKind() != SqlKind.DESCRIPTOR)
            return null;
        List<Integer> result = new ArrayList<>();
        for (RexNode operand : call.getOperands()) {
            if (!(operand instanceof RexInputRef ref))
                return null;
            result.add(ref.getIndex());
        }
        return result;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final LogicalTableFunctionScan scan = call.rel(0);
        if (!(scan.getCall() instanceof RexCall invocation))
            return;
        if (!(invocation.getOperator() instanceof SqlWindowTableFunction)
                || !invocation.getOperator().getName().equals("SESSION"))
            return;
        if (scan.getInputs().size() != 1)
            return;

        // Operands after the TABLE argument: DESCRIPTOR(ts) [, DESCRIPTOR(k)], gap.
        // A call with named arguments may hold DEFAULT for the omitted key.
        final List<RexNode> operands = invocation.getOperands();
        if (operands.size() != 2 && operands.size() != 3)
            return;
        final List<Integer> tsColumns = descriptorColumns(operands.get(0));
        if (tsColumns == null || tsColumns.size() != 1)
            return;
        final int tsIndex = tsColumns.get(0);
        ImmutableBitSet keys = ImmutableBitSet.of();
        if (operands.size() == 3 && operands.get(1).getKind() != SqlKind.DEFAULT) {
            final List<Integer> keyColumns = descriptorColumns(operands.get(1));
            if (keyColumns == null)
                return;
            keys = ImmutableBitSet.of(keyColumns);
        }
        final RexNode gap = operands.get(operands.size() - 1);
        if (!SqlTypeUtil.isInterval(gap.getType()) || RexUtil.containsInputRef(gap))
            return;

        final RelBuilder b = call.builder();
        final RelNode input = scan.getInput(0);
        b.push(input);
        final int n = input.getRowType().getFieldCount();
        if (input.getRowType().getFieldList().get(tsIndex).getType().isNullable())
            b.filter(b.isNotNull(b.field(tsIndex)));

        // prev = LAG(ts) OVER w, appended as field $n
        final List<Integer> keyList = keys.toList();
        b.projectPlus(
                b.aggregateCall(SqlStdOperatorTable.LAG, b.field(tsIndex))
                        .over()
                        .partitionBy(b.fields(keyList))
                        .orderBy(b.field(tsIndex))
                        .rangeTo(RexWindowBounds.CURRENT_ROW)
                        .toRex());

        // brk = CASE(prev IS NULL OR ts >= prev + gap, 1, 0), replaces prev as field $n
        final RexNode prev = b.field(n);
        b.project(replaceLast(b.fields(), n,
                b.call(SqlStdOperatorTable.CASE,
                        b.or(b.isNull(prev),
                                b.call(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
                                        b.field(tsIndex),
                                        b.call(SqlStdOperatorTable.DATETIME_PLUS, prev, gap))),
                        b.literal(1), b.literal(0))));

        // sid = SUM(brk) OVER w, replaces brk as field $n
        b.project(replaceLast(b.fields(), n,
                b.aggregateCall(SqlStdOperatorTable.SUM, b.field(n))
                        .over()
                        .partitionBy(b.fields(keyList))
                        .orderBy(b.field(tsIndex))
                        .rangeTo(RexWindowBounds.CURRENT_ROW)
                        .toRex()));
        final RelNode sessionized = b.build();

        // bounds = [k..., sid, MIN(ts), MAX(ts)] grouped by (k, sid)
        final ImmutableBitSet group = keys.union(ImmutableBitSet.of(n));
        b.push(sessionized)
                .aggregate(b.groupKey(group),
                        b.min(b.field(tsIndex)), b.max(b.field(tsIndex)));
        final RelNode bounds = b.build();

        // Attach each session's bounds to each of its rows.  The join keys
        // use IS NOT DISTINCT FROM because the key columns may hold NULL.
        b.push(sessionized).push(bounds);
        final int groupCount = group.cardinality();
        final List<RexNode> conditions = new ArrayList<>();
        for (int i = 0; i < keyList.size(); i++)
            conditions.add(b.call(SqlStdOperatorTable.IS_NOT_DISTINCT_FROM,
                    b.field(2, 0, keyList.get(i)), b.field(2, 1, i)));
        conditions.add(b.call(SqlStdOperatorTable.IS_NOT_DISTINCT_FROM,
                b.field(2, 0, n), b.field(2, 1, groupCount - 1)));
        b.join(JoinRelType.INNER, b.and(conditions));

        // Final project: sessionized $0..$n, bounds keys and sid, min, max
        final int boundsBase = (n + 1) + groupCount;
        final List<RexNode> results = new ArrayList<>(b.fields().subList(0, n));
        results.add(b.field(boundsBase));
        results.add(b.call(SqlStdOperatorTable.DATETIME_PLUS, b.field(boundsBase + 1), gap));
        b.project(results, scan.getRowType().getFieldNames())
                .convert(scan.getRowType(), false);

        call.transformTo(b.build());
        // prune if this ever runs under Volcano
        call.getPlanner().prune(scan);
    }

    /** The first 'keep' fields, followed by 'last'. */
    static List<RexNode> replaceLast(List<RexNode> fields, int keep, RexNode last) {
        List<RexNode> result = new ArrayList<>(fields.subList(0, keep));
        result.add(last);
        return result;
    }

    public static final DefaultOptRuleConfig<SessionRewriteRule> CONFIG =
            DefaultOptRuleConfig.<SessionRewriteRule>create()
                    .withOperandSupplier(
                            b -> b.operand(LogicalTableFunctionScan.class).anyInputs());
}
