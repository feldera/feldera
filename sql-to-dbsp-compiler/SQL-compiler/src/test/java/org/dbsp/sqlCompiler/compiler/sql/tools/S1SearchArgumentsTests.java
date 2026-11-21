package org.dbsp.sqlCompiler.compiler.sql.tools;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.Range;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexUnknownAs;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Sarg;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.List;

public class S1SearchArgumentsTests {
    @Test public void temporalEndpointsRetainCalciteStorage() throws IOException {
        var builder = new RexBuilder(new S1TypeTable.Factory());
        assertPoint(builder, SqlTypeName.DATE, new org.apache.calcite.util.DateString("1994-01-01"));
        assertPoint(builder, SqlTypeName.TIME, new org.apache.calcite.util.TimeString("12:34:56"));
        assertPoint(builder, SqlTypeName.TIMESTAMP, new org.apache.calcite.util.TimestampString("1994-01-01 12:34:56"));
    }

    private static <C extends Comparable<C>> void assertPoint(RexBuilder builder, SqlTypeName name, C endpoint) throws IOException {
        var type = builder.getTypeFactory().createSqlType(name);
        var literal = builder.makeSearchArgumentLiteral(Sarg.of(RexUnknownAs.UNKNOWN,
                ImmutableRangeSet.of(Range.singleton(endpoint))), type);
        var serialized = org.dbsp.util.Utilities.deterministicObjectMapper().readTree(S1SearchArguments.encode(literal).toString());
        var restored = S1SearchArguments.decode(serialized, type, 0);
        Assert.assertEquals(literal.getValue(), restored.getValue());
        var actual = restored.getValueAs(Sarg.class).rangeSet.span().lowerEndpoint();
        Assert.assertEquals(endpoint.getClass(), actual.getClass());
        Assert.assertEquals(endpoint, actual);
    }

    @Test public void rangesRetainNullPolicyOpenBoundsAndEmptyOrUniversalSets() throws IOException {
        var builder = new RexBuilder(new S1TypeTable.Factory());
        var type = builder.getTypeFactory().createSqlType(SqlTypeName.INTEGER);
        for (var ranges: List.of(ImmutableRangeSet.<BigDecimal>of(), ImmutableRangeSet.of(Range.<BigDecimal>all()),
                ImmutableRangeSet.of(Range.closed(BigDecimal.ONE, BigDecimal.TEN)),
                ImmutableRangeSet.<BigDecimal>builder().add(Range.lessThan(BigDecimal.ZERO)).add(Range.greaterThan(BigDecimal.TEN)).build())) {
            for (var policy: RexUnknownAs.values()) {
                var literal = builder.makeSearchArgumentLiteral(Sarg.of(policy, ranges), type);
                var wire = S1SearchArguments.encode(literal);
                var restored = S1SearchArguments.decode(wire, type, 0);
                Assert.assertEquals(literal.getValue(), restored.getValue());
                Assert.assertSame(type, restored.getType());
                Assert.assertEquals(SqlTypeName.SARG, restored.getTypeName());
                Assert.assertEquals(wire, S1SearchArguments.encode(restored));
            }
        }
    }

    @Test public void invalidOrReorderedRangesFailBeforeLowering() throws IOException {
        var builder = new RexBuilder(new S1TypeTable.Factory());
        var type = builder.getTypeFactory().createSqlType(SqlTypeName.INTEGER);
        var ranges = ImmutableRangeSet.<BigDecimal>builder().add(Range.closed(BigDecimal.ZERO, BigDecimal.ONE))
                .add(Range.closed(BigDecimal.TEN, BigDecimal.valueOf(20))).build();
        var wire = S1SearchArguments.encode(builder.makeSearchArgumentLiteral(Sarg.of(RexUnknownAs.UNKNOWN, ranges), type));
        var reversed = wire.deepCopy();
        var array = reversed.withArray("ranges"); var first = array.get(0); array.set(0, array.get(1)); array.set(1, first);
        Assert.assertThrows(IOException.class, () -> S1SearchArguments.decode(reversed, type, 0));
        var invalid = wire.deepCopy(); ((ObjectNode) invalid.at("/ranges/0/upper/literal/value")).put("value", "-1");
        Assert.assertThrows(IOException.class, () -> S1SearchArguments.decode(invalid, type, 0));
        var policy = wire.deepCopy(); policy.put("null_as", "ignore");
        Assert.assertThrows(IOException.class, () -> S1SearchArguments.decode(policy, type, 0));
    }
}
