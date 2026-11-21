package org.dbsp.sqlCompiler.compiler.sql.tools;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.FelderaS1RelFactory;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.S1RelTypeFactory;
import org.dbsp.util.Utilities;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;

public class S1RelNodesTests {
    private static ObjectNode capture(String sql) throws IOException {
        var mapper = Utilities.deterministicObjectMapper();
        var input = mapper.createObjectNode().put("id", "relation-constructors");
        input.set("options", mapper.valueToTree(new CompilerOptions()));
        input.putArray("submissions").addObject().put("batch", 0).put("many", true).put("sql", sql);
        var compiler = S1ExpressionCorpusProbe.compile(input);
        var plans = compiler.getPlans(new org.dbsp.util.IndentStream(new StringBuilder()));
        var roots = plans.entrySet().stream().sorted(Comparator.comparingInt(java.util.Map.Entry::getValue))
                .map(java.util.Map.Entry::getKey).toList();
        return capture(roots, S1ExpressionCorpusProbe.source(compiler), compiler.getSqlToRelCompiler().getRexBuilder());
    }

    private static ObjectNode capture(List<RelNode> roots, S1ExpressionSources.Source source, RexBuilder builder) throws IOException {
        var registry = new S1BuiltinOperatorRegistry();
        var captured = S1ExpressionSharingProbe.collect(roots, new S1ExpressionNormalizer(builder));
        var graph = S1ExpressionObjectsEncoder.encode(captured, registry, source);
        var record = Utilities.deterministicObjectMapper().createObjectNode();
        record.set("table", graph.inputs().table()); record.set("expressions", graph.expressions());
        record.set("producers", S1InputProducers.encode(captured, graph.inputs()));
        record.set("properties", S1RelationProperties.encode(captured, graph.inputs(), registry));
        return record;
    }
    private static S1RelNodes.Decoded decode(ObjectNode record) throws IOException {
        return S1RelNodes.decode(record, new S1BuiltinOperatorRegistry(), (id, name, row) ->
                org.apache.calcite.prepare.RelOptTableImpl.create(null, row, name, (org.apache.calcite.linq4j.tree.Expression) null));
    }

    @Test public void compilerRecordKindsAndRelationalFieldsRoundTrip() throws IOException {
        for (String query: List.of(
                "SELECT t.x, u.y FROM t LEFT JOIN u ON t.x = u.y WHERE t.x > 3",
                "SELECT x, SUM(x) FROM t GROUP BY x",
                "SELECT SUM(x) OVER (ORDER BY x ROWS 1 PRECEDING) FROM t",
                "SELECT x FROM t UNION SELECT y FROM u",
                "SELECT x FROM t INTERSECT ALL SELECT y FROM u",
                "SELECT x FROM t EXCEPT ALL SELECT y FROM u",
                "SELECT x FROM t ORDER BY x LIMIT 2 OFFSET 1")) {
            var record = capture("CREATE TABLE t(x INT); CREATE TABLE u(y INT); CREATE VIEW v AS " + query + ";");
            var decoded = decode(record);
            Assert.assertEquals(record.at("/producers/relations").size(), decoded.nodes().size());
        }
    }

    @Test public void explicitOutputNamesSurviveButIncompatibleTypesFail() {
        var builder = new RexBuilder(S1RelTypeFactory.create());
        var cluster = RelOptCluster.create(new VolcanoPlanner(), builder);
        var node = LogicalValues.createOneRow(cluster);
        var original = node.getRowType();
        var renamed = cluster.getTypeFactory().builder().add("stored_name", original.getFieldList().get(0).getType()).build();
        FelderaS1RelFactory.retainRowType(node, renamed);
        Assert.assertSame(renamed, node.getRowType());
        var invalid = cluster.getTypeFactory().builder().add("stored_name", SqlTypeName.BOOLEAN).build();
        Assert.assertThrows(IllegalArgumentException.class, () -> FelderaS1RelFactory.retainRowType(node, invalid));
        Assert.assertSame(renamed, node.getRowType());
    }

    @Test public void correlateConstructorHasCoverageOutsideTheObservedCorpus() throws IOException {
        var builder = new RexBuilder(S1RelTypeFactory.create());
        var cluster = RelOptCluster.create(new VolcanoPlanner(), builder);
        var left = LogicalValues.createOneRow(cluster); var right = LogicalValues.createOneRow(cluster);
        var correlate = LogicalCorrelate.create(left, right, List.of(), new CorrelationId(7), ImmutableBitSet.of(), JoinRelType.LEFT);
        var decoded = decode(capture(List.of(correlate), null, builder));
        var restored = (LogicalCorrelate) decoded.roots().get(0);
        Assert.assertEquals(7, restored.getCorrelationId().getId());
        Assert.assertEquals(JoinRelType.LEFT, restored.getJoinType());
        Assert.assertNotSame(restored.getLeft(), restored.getRight());
    }

    @Test public void unresolvedCatalogIsAnExplicitFailure() throws IOException {
        var record = capture("CREATE TABLE t(x INT); CREATE VIEW v AS SELECT x FROM t;");
        Assert.assertThrows(IOException.class, () -> S1RelNodes.decode(record, new S1BuiltinOperatorRegistry(),
                (id, name, row) -> { throw new IOException("Missing catalog declaration: " + name); }));
    }

    @Test public void uncollectExpansionAndOuterNullabilityAreIndependent() throws IOException {
        var builder = new RexBuilder(S1RelTypeFactory.create());
        var cluster = RelOptCluster.create(new VolcanoPlanner(), builder);
        var factory = cluster.getTypeFactory();
        var element = factory.builder().add("number", SqlTypeName.INTEGER).add("flag", SqlTypeName.BOOLEAN).build();
        var row = factory.builder().add("items", factory.createArrayType(element, -1)).build();
        for (boolean expand: List.of(false, true)) {
            for (boolean outer: List.of(false, true)) {
                var input = LogicalValues.createEmpty(cluster, row);
                var uncollect = new org.apache.calcite.rel.core.Uncollect(cluster, input.getTraitSet(), input, true,
                        List.of(), ImmutableBitSet.of(), ImmutableBitSet.of(0), expand, outer);
                var decoded = decode(capture(List.of(uncollect), null, builder));
                var restored = (org.apache.calcite.rel.core.Uncollect) decoded.roots().get(0);
                Assert.assertEquals(expand, restored.expandStructFields);
                Assert.assertEquals(outer, restored.isOuter);
                Assert.assertEquals(expand ? 3 : 2, restored.getRowType().getFieldCount());
                Assert.assertEquals(outer, restored.getRowType().getFieldList().get(0).getType().isNullable());
            }
        }
    }
}
