package org.dbsp.sqlCompiler.compiler.sql.tools;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.fun.SqlSumEmptyIsZeroAggFunction;
import org.apache.calcite.util.ImmutableBitSet;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;

public class S1RelationPropertiesTests {
    @Test public void freshAggregateOperatorsRemainDistinctFromEachOtherAndTheSingleton() throws Exception {
        var registry = new S1BuiltinOperatorRegistry();
        var sources = S1ExpressionSources.of(null);
        var objects = new S1OperatorObjects(registry, sources);
        var first = new SqlSumEmptyIsZeroAggFunction();
        var second = new SqlSumEmptyIsZeroAggFunction();
        Assert.assertEquals(0, objects.add(first));
        Assert.assertEquals(1, objects.add(second));
        Assert.assertEquals(0, objects.add(first));
        Assert.assertEquals(2, objects.add(SqlStdOperatorTable.SUM0));
        var wire = objects.encode(Map.of(), Map.of());
        var decoded = S1OperatorObjects.decode(wire, List.of(), sources, registry);
        objects.verify(decoded.objects(), Map.of(), List.of(), Map.of(), List.of());
        Assert.assertNotSame(decoded.objects().get(0), decoded.objects().get(1));
        Assert.assertNotSame(decoded.objects().get(0), SqlStdOperatorTable.SUM0);
        Assert.assertSame(decoded.objects().get(2), SqlStdOperatorTable.SUM0);
        ((com.fasterxml.jackson.databind.node.ObjectNode) wire.get(0)).put("extra", true);
        Assert.assertThrows(java.io.IOException.class, () -> S1OperatorObjects.decode(wire, List.of(), sources, registry));
    }

    @Test public void uncollectCanRetainAndExpandTheSameCollection() throws Exception {
        var cluster = RelOptCluster.create(new VolcanoPlanner(), new RexBuilder(new S1TypeTable.Factory()));
        var factory = cluster.getTypeFactory();
        var array = factory.createArrayType(factory.createSqlType(org.apache.calcite.sql.type.SqlTypeName.INTEGER), -1);
        var row = factory.builder().add("items", array).build();
        var input = LogicalValues.createEmpty(cluster, row);
        var uncollect = org.apache.calcite.rel.core.Uncollect.create(input.getTraitSet(), input, false,
                List.of(), ImmutableBitSet.of(0), ImmutableBitSet.of(0), true, false);
        Assert.assertEquals(2, uncollect.getRowType().getFieldCount());
        Assert.assertEquals(array, uncollect.getRowType().getFieldList().get(0).getType());
        var captured = S1ExpressionSharingProbe.collect(List.of(uncollect));
        var types = S1InputUsesTable.encode(captured);
        var properties = S1RelationProperties.encode(captured, types, new S1BuiltinOperatorRegistry());
        Assert.assertEquals(properties.at("/relations/0/fields/passthrough"), properties.at("/relations/0/fields/collections"));
    }

    @Test public void aggregateDescriptorsKeepDirectArgumentsFlagsAndOrdering() throws Exception {
        var cluster = RelOptCluster.create(new VolcanoPlanner(), new RexBuilder(new S1TypeTable.Factory()));
        var input = LogicalValues.createOneRow(cluster);
        var direct = cluster.getRexBuilder().makeExactLiteral(new java.math.BigDecimal("0.75"));
        var ordering = RelCollations.of(new RelFieldCollation(0,
                RelFieldCollation.Direction.DESCENDING, RelFieldCollation.NullDirection.FIRST));
        var call = AggregateCall.create(new org.apache.calcite.sql.parser.SqlParserPos(2, 3, 2, 9),
                SqlStdOperatorTable.PERCENTILE_CONT, true, true, true, List.of(direct), List.of(0),
                -1, ImmutableBitSet.of(0), ordering, input.getRowType().getFieldList().get(0).getType(), "percentile");
        var aggregate = LogicalAggregate.create(input, List.of(), ImmutableBitSet.of(),
                List.of(ImmutableBitSet.of()), List.of(call));
        var captured = S1ExpressionSharingProbe.collect(List.of(aggregate));
        var types = S1InputUsesTable.encode(captured);
        var properties = S1RelationProperties.encode(captured, types, new S1BuiltinOperatorRegistry());
        var restored = properties.at("/relations/0/fields/calls/0");
        var extra = cluster.getTypeFactory().createSqlType(org.apache.calcite.sql.type.SqlTypeName.BOOLEAN);
        var extended = S1InputUsesTable.encode(captured, List.of(extra));
        Assert.assertEquals(properties, S1RelationProperties.encode(captured, extended, new S1BuiltinOperatorRegistry()));
        Assert.assertEquals(S1InputProducers.encode(captured, types), S1InputProducers.encode(captured, extended));
        Assert.assertEquals(1, restored.path("direct_arguments").intValue());
        Assert.assertTrue(restored.path("distinct").booleanValue());
        Assert.assertTrue(restored.path("approximate").booleanValue());
        Assert.assertTrue(restored.path("ignore_nulls").booleanValue());
        Assert.assertEquals("DESCENDING", restored.at("/collation/0/direction").asText());
        Assert.assertEquals("FIRST", restored.at("/collation/0/null_direction").asText());
        Assert.assertEquals("absent", restored.at("/filter/kind").asText());
        Assert.assertEquals(0, restored.at("/distinct_keys/fields/0").intValue());
        Assert.assertEquals(2, restored.at("/position/line").intValue());
    }
}
