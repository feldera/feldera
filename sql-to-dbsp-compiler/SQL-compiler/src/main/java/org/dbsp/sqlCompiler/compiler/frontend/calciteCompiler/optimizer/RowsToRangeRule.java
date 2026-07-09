package org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.optimizer;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.logical.LogicalWindow;
import org.apache.calcite.rel.rules.TransformationRule;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexWindowBound;
import org.apache.calcite.rex.RexWindowBounds;
import org.apache.calcite.rex.RexWindowExclusion;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.util.ImmutableBitSet;
import org.dbsp.util.Utilities;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Rule that rewrites window aggregates using ROWS frames into aggregates
 * using RANGE frames over an intermediate ROW_NUMBER() value.
 *
 * <p>Within a partition, ROW_NUMBER() assigns consecutive integers following
 * the ORDER BY of the frame.  A physical frame such as
 * {@code ROWS BETWEEN 2 PRECEDING AND CURRENT ROW} therefore selects exactly
 * the rows whose row number lies within {@code [rn - 2, rn]}, which is the
 * logical frame {@code RANGE BETWEEN 2 PRECEDING AND CURRENT ROW} evaluated
 * over the row number.  (Row numbers have no duplicates, so the peer rows
 * that make RANGE differ from ROWS cannot occur.)
 *
 * <p>Rewrite of a window over an input with fields $0..$n-1.  SUM is
 * frame-sensitive and is rewritten; ROW_NUMBER and RANK ignore the frame
 * and keep the original partitioning and ordering.  The quoted names are
 * the variables in {@link #onMatch} that hold each plan node:
 * <pre>
 * LogicalWindow(window#0=[window(partition {p} order by [o]    -- "window"
 *                          rows between 2 PRECEDING and 3 FOLLOWING
 *                          aggs [SUM($1), ROW_NUMBER(), RANK()])])
 *                                                 -- fields $n, $n+1, $n+2
 *   Input(fields $0..$n-1)                                     -- "input"
 * </pre>
 * becomes
 * <pre>
 * LogicalProject($0, ..., $n-1, $n+2, $n, $n+1)                -- "result"
 *   LogicalWindow(                                             -- "outer"
 *       window#0=[window(partition {p} order by [o]
 *                  rows between 2 PRECEDING and 3 FOLLOWING
 *                  aggs [RANK()])],                            -- field $n+1
 *       window#1=[window(partition {p} order by [$n]
 *                  range between 2 PRECEDING and 3 FOLLOWING
 *                  aggs [SUM($1)])])                           -- field $n+2
 *     LogicalWindow(window#0=[window(partition {p} order by [o]  -- "outerInput"
 *                              aggs [ROW_NUMBER()])])          -- field $n
 *       Input(fields $0..$n-1)                                 -- "input"
 * </pre>
 * The projection restores the original aggregate order.
 * The bottom window synthesizes the row number that orders SUM's RANGE
 * frame; it carries the default frame, which ROW_NUMBER ignores.  The
 * user's ROW_NUMBER has the same partitioning and ordering, so it computes
 * the same value as field $n, and the projection reuses that field instead
 * of keeping the call.  RANK is tie-sensitive, so it keeps the original
 * ORDER BY in its own group; that group remains a ROWS group, which is
 * harmless because RANK ignores the frame.
 *
 * <p>Window functions that ignore the frame (their result depends only on
 * the partition and the ORDER BY) keep the original group: the ROWS flag
 * has no effect on their result, and ordering by the row number instead of
 * the original columns would corrupt the tie-sensitive ones (RANK and
 * friends).  If such calls share a group with rewritten calls, the group
 * is split in two and a final projection restores the original column
 * order.
 */
public class RowsToRangeRule
        extends RelRule<DefaultOptRuleConfig<RowsToRangeRule>>
        implements TransformationRule {
    protected RowsToRangeRule() {
        super(CONFIG);
    }

    /** List of window functions whose result depends only on the partition and the
     * ORDER BY, never on the frame.  SQL semantics alone determines this
     * set.  Groups holding only such calls need no rewrite, because the
     * ROWS flag has no effect on their result.  Moreover, RANK, DENSE_RANK,
     * PERCENT_RANK, and CUME_DIST are tie-sensitive: ordering by the row
     * number would break ties and corrupt their result. */
    private static final EnumSet<SqlKind> FRAME_INSENSITIVE = EnumSet.of(
            SqlKind.RANK, SqlKind.DENSE_RANK, SqlKind.PERCENT_RANK, SqlKind.CUME_DIST,
            SqlKind.ROW_NUMBER, SqlKind.NTILE,
            SqlKind.LAG, SqlKind.LEAD);

    /** What the rule does to one window group. */
    private enum Action {
        /** Leave the group unchanged. */
        NONE,
        /** The frame covers the whole partition: RANGE selects the same rows
         * as ROWS, so implement always using RANGE. */
        USE_RANGE,
        /** Order by an intermediate ROW_NUMBER() and use a RANGE frame. */
        REWRITE
    }

    /** Partitioning and ordering of a ROW_NUMBER() computed by the inner window. */
    private record OrderSpec(ImmutableBitSet keys, RelCollation orderKeys) {
        static OrderSpec of(Window.Group group) {
            return new OrderSpec(group.keys, group.orderKeys);
        }
    }

    /** True when the call's result does not depend on the window frame. */
    static boolean ignoresFrame(Window.RexWinAggCall aggCall) {
        return FRAME_INSENSITIVE.contains(aggCall.getOperator().getKind());
    }

    /** Decide what the rule does to one window group.
     *
     * <p>A group is left unchanged when it does not use a ROWS frame, or
     * when every call in it ignores the frame (the ROWS flag then has no
     * effect on any result), or when a peer-dependent exclusion (EXCLUDE
     * GROUP, EXCLUDE TIES) makes the rewrite unsound: peers are defined by
     * the ORDER BY columns, and the row number ordering that the rewrite
     * introduces has no peers.
     *
     * <p>A ROWS frame that spans the whole partition selects the same rows
     * as the corresponding RANGE frame, so such a group only needs its ROWS
     * flag cleared; its ORDER BY, and therefore any EXCLUDE clause, keeps
     * its meaning.
     *
     * <p>Every other group is rewritten to a RANGE frame ordered by an
     * intermediate ROW_NUMBER().  EXCLUDE CURRENT ROW is peer-independent,
     * so it survives this rewrite.
     *
     * @param group A group of aggregate calls sharing one window frame.
     * @return      The action to apply to the group.
     */
    static Action classify(Window.Group group) {
        if (!group.isRows)
            return Action.NONE;
        if (group.aggCalls.stream().allMatch(RowsToRangeRule::ignoresFrame))
            // The ROWS flag has no effect on any result.
            return Action.NONE;
        if (group.lowerBound.isUnboundedPreceding()
                && group.upperBound.isUnboundedFollowing())
            // The frame is the whole partition whether it is interpreted
            // physically or logically.  The ORDER BY is unchanged, so any
            // EXCLUDE clause keeps its meaning.
            return Action.USE_RANGE;
        if (group.exclude == RexWindowExclusion.EXCLUDE_GROUP
                || group.exclude == RexWindowExclusion.EXCLUDE_TIES)
            // These exclusions remove peers of the current row; the row
            // number ordering has no peers, which would change the excluded
            // set; a rewrite would be unsound.
            // (EXCLUDE CURRENT ROW is peer-independent and survives the rewrite.)
            return Action.NONE;
        return Action.REWRITE;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final LogicalWindow window = call.rel(0);
        // How to rewrite each group
        final List<Action> actions = new ArrayList<>(window.groups.size());
        final List<OrderSpec> rowNumberSpecs = new ArrayList<>();
        for (Window.Group group : window.groups) {
            Action action = classify(group);
            actions.add(action);
            if (action == Action.REWRITE && !rowNumberSpecs.contains(OrderSpec.of(group)))
                rowNumberSpecs.add(OrderSpec.of(group));
        }
        if (actions.stream().allMatch(a -> a == Action.NONE))
            return;

        final RelNode input = window.getInput();
        final int inputFieldCount = input.getRowType().getFieldCount();
        final RelDataTypeFactory typeFactory = window.getCluster().getTypeFactory();
        final RexBuilder rexBuilder = window.getCluster().getRexBuilder();
        final int rowNumberQueriesInserted = rowNumberSpecs.size();

        // Inner window: one ROW_NUMBER() per distinct (partition, order),
        // appended after the input fields.
        final RelNode outerInput;
        if (rowNumberQueriesInserted == 0) {
            outerInput = input;
        } else {
            // The type ROW_NUMBER has under this type system.
            RelDataType rowNumberType =
                    typeFactory.getTypeSystem().deriveRankType(typeFactory);
            List<Window.Group> rowNumberGroups = new ArrayList<>(rowNumberQueriesInserted);
            RelDataTypeFactory.Builder innerType = typeFactory.builder();
            innerType.addAll(input.getRowType().getFieldList());
            Set<String> usedNames = new HashSet<>(input.getRowType().getFieldNames());
            usedNames.addAll(window.getRowType().getFieldNames());
            for (int i = 0; i < rowNumberQueriesInserted; i++) {
                Window.RexWinAggCall rowNumber = new Window.RexWinAggCall(
                        SqlParserPos.ZERO, SqlStdOperatorTable.ROW_NUMBER, rowNumberType,
                        ImmutableList.of(), i, false, false);
                rowNumberGroups.add(new Window.Group(
                        rowNumberSpecs.get(i).keys(), false,
                        // ROW_NUMBER ignores the window frame, so use the default RANGE frame
                        RexWindowBounds.UNBOUNDED_PRECEDING, RexWindowBounds.CURRENT_ROW,
                        RexWindowExclusion.EXCLUDE_NO_OTHER, rowNumberSpecs.get(i).orderKeys(),
                        ImmutableList.of(rowNumber)));
                String name = SqlValidatorUtil.uniquify(
                        "$row_number" + i, usedNames, SqlValidatorUtil.ATTEMPT_SUGGESTER);
                innerType.add(name, rowNumberType);
            }
            outerInput = LogicalWindow.create(window.getTraitSet(), ImmutableList.of(),
                    input, ImmutableList.of(), innerType.build(), rowNumberGroups);
        }

        // In a Window, a RexInputRef with index >= inputFieldCount denotes a
        // constant; the row number columns push the constants up by rowNumberQueriesInserted.
        final RexShuttle shiftConstants = new RexShuttle() {
            @Override public RexNode visitInputRef(RexInputRef ref) {
                if (ref.getIndex() >= inputFieldCount)
                    return new RexInputRef(ref.getIndex() + rowNumberQueriesInserted, ref.getType());
                return ref;
            }
        };

        // Rebuild the groups
        final List<Window.Group> newGroups = new ArrayList<>();
        // Maps the position of each aggregate in the new window to its position in the original one.
        final List<Integer> origPositions = new ArrayList<>();
        final Map<Integer, Integer> reusedRowNumbers = new HashMap<>();
        int origPosition = 0;
        for (int g = 0; g < window.groups.size(); g++) {
            Window.Group group = window.groups.get(g);
            List<Window.RexWinAggCall> keep = new ArrayList<>();
            List<Window.RexWinAggCall> move = new ArrayList<>();
            List<Integer> keepPositions = new ArrayList<>();
            List<Integer> movePositions = new ArrayList<>();
            Action action = actions.get(g);
            int specIndex = rowNumberSpecs.indexOf(OrderSpec.of(group));
            for (Window.RexWinAggCall aggCall : group.aggCalls) {
                if (specIndex >= 0
                        && aggCall.getOperator().getKind() == SqlKind.ROW_NUMBER
                        && !aggCall.distinct) {
                    // A ROW_NUMBER over the same (partition, order) as a synthesized row
                    // number computes the same value, so it is dropped here and the
                    // final projection reads the inner window's column instead.
                    reusedRowNumbers.put(origPosition, specIndex);
                } else {
                    boolean moves = action == Action.REWRITE && !ignoresFrame(aggCall);
                    (moves ? move : keep).add(aggCall);
                    (moves ? movePositions : keepPositions).add(origPosition);
                }
                origPosition++;
            }
            RexWindowBound lowerBound = group.lowerBound.accept(shiftConstants);
            RexWindowBound upperBound = group.upperBound.accept(shiftConstants);
            if (!keep.isEmpty()) {
                boolean isRows = action != Action.USE_RANGE && group.isRows;
                newGroups.add(new Window.Group(group.keys, isRows,
                        lowerBound, upperBound,
                        group.exclude, group.orderKeys,
                        rebuildCalls(keep, keepPositions, shiftConstants, origPositions)));
            }
            if (!move.isEmpty()) {
                RelCollation rowNumberOrder = RelCollations.of(
                        new RelFieldCollation(inputFieldCount + specIndex));
                newGroups.add(new Window.Group(group.keys, false,
                        lowerBound, upperBound,
                        group.exclude, rowNumberOrder,
                        rebuildCalls(move, movePositions, shiftConstants, origPositions)));
            }
        }

        final RelDataTypeFactory.Builder outerType = typeFactory.builder();
        outerType.addAll(outerInput.getRowType().getFieldList());
        for (int position : origPositions) {
            RelDataTypeField field =
                    window.getRowType().getFieldList().get(inputFieldCount + position);
            outerType.add(field.getName(), field.getType());
        }
        final LogicalWindow outer = LogicalWindow.create(window.getTraitSet(),
                window.getHints(), outerInput, window.constants, outerType.build(), newGroups);
        if (rowNumberQueriesInserted == 0) {
            // No columns were added and no group was split; the aggregates
            // kept their positions.
            call.transformTo(outer);
            return;
        }

        // Final projection: restore the original aggregate order.  Reused
        // ROW_NUMBERs read the inner window's row number column; the other
        // row number columns are dropped.
        final int aggregateCount = window.getRowType().getFieldCount() - inputFieldCount;
        Utilities.enforce(origPositions.size() + reusedRowNumbers.size() == aggregateCount);
        final int[] newPositions = new int[aggregateCount];
        for (int newPosition = 0; newPosition < origPositions.size(); newPosition++)
            newPositions[origPositions.get(newPosition)] = newPosition;
        final List<RexNode> projects = new ArrayList<>();
        for (int i = 0; i < inputFieldCount; i++)
            projects.add(rexBuilder.makeInputRef(outer, i));
        for (int position = 0; position < aggregateCount; position++) {
            Integer specIndex = reusedRowNumbers.get(position);
            projects.add(rexBuilder.makeInputRef(outer,
                    specIndex != null
                            ? inputFieldCount + specIndex
                            : inputFieldCount + rowNumberQueriesInserted + newPositions[position]));
        }

        RelNode result = call.builder()
                .push(outer)
                .project(projects, window.getRowType().getFieldNames())
                .build();
        call.transformTo(result);
    }

    /** Copy the calls with constant references shifted and with ordinals that
     * match their position in the rebuilt window; extends origPositions with
     * the calls' original positions. */
    private static List<Window.RexWinAggCall> rebuildCalls(
            List<Window.RexWinAggCall> calls, List<Integer> positions,
            RexShuttle shiftConstants, List<Integer> origPositions) {
        List<Window.RexWinAggCall> result = new ArrayList<>(calls.size());
        for (int i = 0; i < calls.size(); i++) {
            Window.RexWinAggCall aggCall = calls.get(i);
            result.add(new Window.RexWinAggCall(
                    aggCall.getParserPosition(),
                    (SqlAggFunction) aggCall.getOperator(),
                    aggCall.getType(),
                    shiftConstants.visitList(aggCall.getOperands()),
                    origPositions.size(),
                    aggCall.distinct,
                    aggCall.ignoreNulls));
            origPositions.add(positions.get(i));
        }
        return result;
    }

    public static final DefaultOptRuleConfig<RowsToRangeRule> CONFIG =
            DefaultOptRuleConfig.<RowsToRangeRule>create()
                    .withOperandSupplier(
                            b -> b.operand(LogicalWindow.class).anyInputs());
}
