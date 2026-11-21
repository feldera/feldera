package org.dbsp.sqlCompiler.compiler.sql.tools;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalAsofJoin;
import org.apache.calcite.rel.logical.LogicalSnapshot;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.TimestampString;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

public class S1RelExpressionsTests {
    private static RelOptCluster cluster() {
        return RelOptCluster.create(new VolcanoPlanner(), new RexBuilder(new S1TypeTable.Factory()));
    }

    private static final class Literals extends RexShuttle {
        final List<RexLiteral> values = new ArrayList<>();
        @Override public RexNode visitLiteral(RexLiteral literal) {
            this.values.add(literal);
            return literal;
        }
    }

    @Test public void asofMatchConditionIsVisitedIndependentlyOfOrdinaryCondition() {
        var cluster = cluster();
        var builder = cluster.getRexBuilder();
        var input = LogicalValues.createOneRow(cluster);
        var sentinel = builder.makeExactLiteral(new BigDecimal("17011"));
        var match = builder.makeCall(SqlStdOperatorTable.GREATER_THAN,
                builder.makeInputRef(sentinel.getType(), 0), sentinel);
        var asof = LogicalAsofJoin.create(input, input, List.of(), builder.makeLiteral(true),
                match, JoinRelType.ASOF, ImmutableList.of());
        var visitor = new Literals();
        asof.accept(visitor);
        Assert.assertFalse(visitor.values.contains(sentinel));
        S1RelExpressions.visit(asof, visitor, ignored -> { });
        Assert.assertTrue(visitor.values.contains(sentinel));
    }

    @Test public void aggregateDirectArgumentsAndReturnTypeAreVisited() {
        var cluster = cluster();
        var builder = cluster.getRexBuilder();
        var input = LogicalValues.createOneRow(cluster);
        var sentinel = builder.makeExactLiteral(new BigDecimal("0.314159"));
        var resultType = input.getRowType().getFieldList().get(0).getType();
        // The direct-argument field is distinct from the input field ordinals.
        var call = AggregateCall.create(SqlStdOperatorTable.PERCENTILE_CONT, false, false, false,
                List.of(sentinel), List.of(0), -1, null, RelCollations.of(0), resultType, "p");
        var aggregate = LogicalAggregate.create(input, List.of(), ImmutableBitSet.of(),
                List.of(ImmutableBitSet.of()), List.of(call));
        var visitor = new Literals();
        var types = new ArrayList<org.apache.calcite.rel.type.RelDataType>();
        aggregate.accept(visitor);
        Assert.assertFalse(visitor.values.contains(sentinel));
        S1RelExpressions.visit(aggregate, visitor, types::add);
        Assert.assertTrue(visitor.values.contains(sentinel));
        Assert.assertTrue(types.stream().anyMatch(type -> type == resultType));
    }

    @Test public void unauditedRelationClassesFailInsteadOfDroppingFields() {
        var cluster = cluster();
        var input = LogicalValues.createOneRow(cluster);
        var period = cluster.getRexBuilder().makeTimestampLiteral(new TimestampString("2026-01-01 00:00:00"), 0);
        var snapshot = LogicalSnapshot.create(input, period);
        Assert.assertThrows(IllegalArgumentException.class,
                () -> S1RelExpressions.visit(snapshot, new Literals(), ignored -> { }));
    }
}
