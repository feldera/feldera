package org.dbsp.sqlCompiler.compiler.sql.tools;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.dbsp.util.Utilities;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class CorrelationRecipeTests {
    @Test public void substitutionFailuresCannotFallBackToJavaRules() throws Exception {
        var compiler = S1ExpressionCorpusProbe.compile(CorrelationProbeTests.input("SELECT x FROM t"));
        var wire = S1CatalogProbe.capture(CorrelationProbeTests.input("SELECT x FROM t"),
                new S1BuiltinOperatorRegistry());
        var root = S1RelNodes.decodeCatalog(wire, new S1BuiltinOperatorRegistry()).roots().get(0);
        var builder = org.apache.calcite.tools.RelBuilder.proto().create(root.getCluster(), null);
        var optimizer = new org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.optimizer.CalciteOptimizer(
                2, builder, compiler);
        var failure = new IllegalStateException("independent rewrite failed");
        optimizer.inputTransform = (step, input) -> { throw failure; };
        Assert.assertSame(failure, Assert.assertThrows(IllegalStateException.class,
                () -> optimizer.apply(root, compiler.options)));
        optimizer.inputTransform = (step, input) -> null;
        Assert.assertThrows(NullPointerException.class, () -> optimizer.apply(root, compiler.options));
    }

    private static ObjectNode recipe(JsonNode wire) {
        var recipe = Utilities.deterministicObjectMapper().createObjectNode()
                .put("schema_version", "correlation-recipe-v1");
        recipe.set("id", wire.path("id")); recipe.putArray("nodes"); recipe.putArray("roots").add(0);
        int count = 0;
        for (var node: wire.at("/producers/relations"))
            if (node.path("form").asText().equals("LogicalCorrelate")) count++;
        recipe.put("removed_correlates", count);
        return recipe;
    }

    private static ObjectNode original(JsonNode wire) {
        var recipe = recipe(wire);
        recipe.withArray("roots").removeAll();
        for (var root: wire.at("/producers/roots")) {
            recipe.withArray("roots").add(recipe.withArray("nodes").size());
            recipe.withArray("nodes").addObject().put("kind", "original").set("relation", root);
        }
        return recipe;
    }

    @Test public void preservesIndependentRootAndRejectsChangedIdentityAndCount() throws Exception {
        var registry = new S1BuiltinOperatorRegistry();
        var wire = S1CatalogProbe.capture(CorrelationProbeTests.input("SELECT x FROM t"), registry);
        var recipe = original(wire);
        Assert.assertEquals(wire.at("/producers/roots").size(), CorrelationRecipe.decode(wire, recipe, registry).size());
        recipe.put("id", "wrong");
        Assert.assertThrows(IOException.class, () -> CorrelationRecipe.decode(wire, recipe, registry));
        recipe.set("id", wire.path("id")); recipe.put("removed_correlates", 1);
        Assert.assertThrows(IOException.class, () -> CorrelationRecipe.decode(wire, recipe, registry));
    }

    @Test public void detachedCorrelatedFilterCannotBypassVariableValidation() throws Exception {
        var registry = new S1BuiltinOperatorRegistry();
        var wire = CorrelationPlanProbe.capture(CorrelationProbeTests.input(
                "SELECT x, q.y FROM t CROSS JOIN LATERAL (SELECT y FROM u WHERE y = x) q"), registry)
                .plans().get(0);
        var recipe = recipe(wire);
        var relations = wire.at("/producers/relations");
        int filter = -1;
        for (int index = 0; index < relations.size(); index++)
            if (relations.get(index).path("form").asText().equals("LogicalFilter")) filter = index;
        Assert.assertTrue(filter >= 0);
        recipe.withArray("nodes").addObject().put("kind", "original").put("relation", filter);
        var error = Assert.assertThrows(IOException.class, () -> CorrelationRecipe.decode(wire, recipe, registry));
        Assert.assertTrue(error.getMessage(), error.getMessage().contains("free correlation"));
    }

    @Test public void sharedJoinExpansionHitsFieldBudgetBeforeUnboundedAllocation() throws Exception {
        var registry = new S1BuiltinOperatorRegistry();
        var wire = S1CatalogProbe.capture(CorrelationProbeTests.input("SELECT x FROM t"), registry);
        var recipe = original(wire);
        for (int index = 1; index <= 25; index++) {
            int previous = recipe.withArray("nodes").size() - 1;
            recipe.withArray("nodes").addObject().put("kind", "join").put("left", previous)
                    .put("right", previous).put("join_kind", "INNER").putArray("keys");
        }
        var error = Assert.assertThrows(IOException.class, () -> CorrelationRecipe.decode(wire, recipe, registry));
        Assert.assertTrue(error.getMessage(), error.getMessage().contains("budget exceeded"));
    }

    @Test public void zeroWidthDistinctCannotSynthesizeARowForAnEmptyDomain() throws Exception {
        var registry = new S1BuiltinOperatorRegistry();
        var compiler = S1ExpressionCorpusProbe.compile(CorrelationProbeTests.input("SELECT 1 AS one"));
        var cluster = compiler.getSqlToRelCompiler().cluster;
        var emptyRow = cluster.getTypeFactory().createStructType(java.util.List.of(), java.util.List.of());
        var values = org.apache.calcite.rel.logical.LogicalValues.create(cluster, emptyRow,
                com.google.common.collect.ImmutableList.of());
        var wire = CorrelationPlanProbe.encode("empty-domain", values, compiler, registry);
        var recipe = recipe(wire);
        var decoded = S1RelNodes.decodeCatalog(wire, registry);
        int empty = -1;
        for (int index = 0; index < decoded.nodes().size(); index++)
            if (decoded.nodes().get(index).getRowType().getFieldCount() == 0) empty = index;
        Assert.assertTrue("Fixture needs a zero-width input", empty >= 0);
        recipe.withArray("nodes").addObject().put("kind", "original").put("relation", empty);
        recipe.withArray("nodes").addObject().put("kind", "distinct").put("input", 0);
        var error = Assert.assertThrows(IOException.class, () -> CorrelationRecipe.decode(wire, recipe, registry));
        Assert.assertTrue(error.getMessage(), error.getMessage().contains("Empty domain"));
    }
}
