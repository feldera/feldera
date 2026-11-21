package org.dbsp.sqlCompiler.compiler.sql.tools;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.util.Utilities;
import org.junit.Assert;
import org.junit.Test;

public class CorrelationProbeTests {
    static ObjectNode input(String query) {
        var mapper = Utilities.deterministicObjectMapper();
        var input = mapper.createObjectNode().put("id", "correlation-probe");
        input.set("options", mapper.valueToTree(new CompilerOptions()));
        input.putArray("submissions").addObject().put("batch", 0).put("many", true)
                .put("sql", "CREATE TABLE t(x INT); CREATE TABLE u(y INT); CREATE TABLE w(z INT); "
                        + "CREATE VIEW v AS " + query + ";");
        return input;
    }

    @Test public void followsSubqueryRelationsBeforeConversionAndRetainsBothStageSides() throws Exception {
        var observations = CorrelationProbe.capture(input("SELECT x FROM t WHERE EXISTS "
                + "(SELECT y FROM u WHERE y = x AND EXISTS (SELECT z FROM w WHERE z = y))"));
        Assert.assertFalse(observations.isEmpty());
        Assert.assertTrue(observations.stream().anyMatch(record ->
                record.path("step").asText().equals("Remove sub-queries")
                        && record.at("/before/subqueries").intValue() >= 2));
        Assert.assertTrue(observations.stream().anyMatch(record ->
                record.at("/after/correlates").intValue() > 0));
        for (int index = 0; index < observations.size(); index++) {
            var observation = observations.get(index);
            Assert.assertEquals(index, observation.path("sequence").intValue());
            Assert.assertTrue(observation.path("completed").booleanValue());
            for (String side: new String[]{"before", "after"}) {
                var graph = observation.path(side);
                Assert.assertEquals(0, graph.path("root").intValue());
                var nodes = graph.path("relations");
                for (int id = 0; id < nodes.size(); id++) {
                    Assert.assertEquals(id, nodes.get(id).path("id").intValue());
                    for (var edge: nodes.get(id).path("inputs"))
                        Assert.assertTrue(edge.intValue() >= 0 && edge.intValue() < nodes.size());
                    for (var subquery: nodes.get(id).path("subqueries"))
                        Assert.assertTrue(subquery.path("relation").intValue() < nodes.size());
                }
            }
        }
    }

    @Test public void observesUncorrelatedControlsWithoutInventingCorrelates() throws Exception {
        var observations = CorrelationProbe.capture(input("SELECT x FROM t WHERE x > 0"));
        Assert.assertFalse(observations.isEmpty());
        for (var observation: observations) {
            Assert.assertTrue(observation.path("completed").booleanValue());
            for (String side: new String[]{"before", "after"}) {
                Assert.assertEquals(0, observation.path(side).path("correlates").intValue());
                Assert.assertEquals(0, observation.path(side).path("subqueries").intValue());
            }
        }
    }

    @Test public void verifiesFinalOptimizedSchemaAndDiagnostics() throws Exception {
        var input = input("SELECT 1 AS constant_value FROM t");
        var baseline = S1ExpressionCorpusProbe.compile(input,
                compiler -> compiler.options.ioOptions.inputCircuit = false);
        input.set("schema", baseline.metadata.asJson());
        input.set("diagnostics", baseline.messages.toJson(baseline.sources));
        Assert.assertFalse(CorrelationProbe.capture(input).isEmpty());
        input.set("schema", Utilities.deterministicObjectMapper().createObjectNode());
        Assert.assertThrows(java.io.IOException.class, () -> CorrelationProbe.capture(input));
    }

    @Test public void recordsUnnestCorrelatesBeforeTheirRewrite() throws Exception {
        var input = input("SELECT x FROM t");
        ((ObjectNode) input.path("submissions").get(0)).put("sql",
                "CREATE TABLE t(a INT ARRAY); CREATE VIEW v AS SELECT x FROM t, UNNEST(a) AS u(x);");
        var observations = CorrelationProbe.capture(input);
        Assert.assertTrue(observations.stream().anyMatch(record ->
                record.path("step").asText().equals("Remove sub-queries")
                        && record.at("/before/correlates").intValue() > 0));
    }
}
