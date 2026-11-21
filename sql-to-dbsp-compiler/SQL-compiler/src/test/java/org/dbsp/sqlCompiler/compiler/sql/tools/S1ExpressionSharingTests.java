package org.dbsp.sqlCompiler.compiler.sql.tools;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rex.RexBuilder;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class S1ExpressionSharingTests {
    @Test public void sameReferenceCanBelongToDifferentInputEnvironments() throws java.io.IOException {
        var cluster = RelOptCluster.create(new VolcanoPlanner(), new RexBuilder(new S1TypeTable.Factory()));
        var first = LogicalValues.createOneRow(cluster);
        var second = LogicalValues.createOneRow(cluster);
        var reference = cluster.getRexBuilder().makeInputRef(first.getRowType().getFieldList().get(0).getType(), 0);
        var left = LogicalProject.create(first, List.of(), List.of(reference), List.of("x"));
        var right = LogicalProject.create(second, List.of(), List.of(reference), List.of("x"));
        var result = S1ExpressionSharingProbe.audit(List.of(left, right));
        Assert.assertEquals(1, result.path("input_reference_objects").intValue());
        Assert.assertEquals(1, result.path("shared_across_distinct_inputs").intValue());
        var captured = S1ExpressionSharingProbe.collect(List.of(left, right));
        var encoded = S1InputUsesTable.encode(captured);
        var wire = encoded.table();
        var producers = S1InputProducers.encode(captured, encoded);
        ((com.fasterxml.jackson.databind.node.ArrayNode) producers.at("/relations/0/inputs")).removeAll();
        Assert.assertThrows(java.io.IOException.class, () -> S1InputProducers.validate(producers, wire));
        var decoded = S1InputUsesTable.decode(wire);
        Assert.assertSame(decoded.uses().get(0), decoded.uses().get(1));
        ((com.fasterxml.jackson.databind.node.ObjectNode) wire.at("/environments/1")).put("row", 999);
        Assert.assertThrows(java.io.IOException.class, () -> S1InputUsesTable.decode(wire));
    }

    @Test public void twoConsumersOfTheSameInputHaveOneEnvironment() {
        var cluster = RelOptCluster.create(new VolcanoPlanner(), new RexBuilder(new S1TypeTable.Factory()));
        var input = LogicalValues.createOneRow(cluster);
        var reference = cluster.getRexBuilder().makeInputRef(input.getRowType().getFieldList().get(0).getType(), 0);
        var left = LogicalProject.create(input, List.of(), List.of(reference), List.of("x"));
        var right = LogicalProject.create(input, List.of(), List.of(reference), List.of("y"));
        var result = S1ExpressionSharingProbe.audit(List.of(left, right));
        Assert.assertEquals(1, result.path("input_reference_objects").intValue());
        Assert.assertEquals(0, result.path("shared_across_distinct_inputs").intValue());
    }
}
