package org.dbsp.sqlCompiler.compiler.sql.tools;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.junit.Assert;
import org.junit.Test;
import java.io.IOException;
import java.util.List;

public class S1InputUsesTests {
    private static ObjectNode fixture() throws IOException {
        var cluster = RelOptCluster.create(new VolcanoPlanner(), new RexBuilder(new S1TypeTable.Factory()));
        var first = LogicalValues.createOneRow(cluster);
        var second = LogicalValues.createOneRow(cluster);
        var type = first.getRowType().getFieldList().get(0).getType();
        var shared = new RexInputRef(0, type);
        var equal = new RexInputRef(0, type);
        var left = LogicalProject.create(first, List.of(), List.of(shared, equal), List.of("x", "y"));
        var right = LogicalProject.create(second, List.of(), List.of(shared), List.of("x"));
        return S1InputUsesTable.encode(S1ExpressionSharingProbe.collect(List.of(left, right))).table();
    }

    @Test public void sharingAndDistinctEqualObjectsSurviveReconstruction() throws IOException {
        var decoded = S1InputUsesTable.decode(fixture());
        Assert.assertEquals(2, decoded.objects().size());
        Assert.assertEquals(3, decoded.uses().size());
        Assert.assertSame(decoded.uses().get(0), decoded.uses().get(1));
        Assert.assertNotSame(decoded.objects().get(0), decoded.objects().get(1));
        Assert.assertEquals(decoded.objects().get(0), decoded.objects().get(1));
    }

    @Test public void secondUseIsCheckedAfterFirstUseSucceeds() throws IOException {
        ObjectNode wire = fixture();
        int environment = wire.at("/uses/1/environment").intValue();
        int row = wire.at("/environments/" + environment + "/row").intValue();
        var types = (com.fasterxml.jackson.databind.node.ArrayNode) wire.at("/type_table/types");
        ObjectNode wrong = ((ObjectNode) types.get(row)).deepCopy();
        wrong.withObject("/kind").putArray("fields");
        int replacement = types.size(); types.add(wrong);
        ((ObjectNode) wire.at("/environments/" + environment)).put("row", replacement);
        Assert.assertThrows(IOException.class, () -> S1InputUsesTable.decode(wire));
    }

    @Test public void duplicateMissingAndOutOfRangeUsesFail() throws IOException {
        ObjectNode wire = fixture();
        ((ObjectNode) wire.at("/uses/1")).put("environment", wire.at("/uses/0/environment").intValue());
        ObjectNode duplicate = wire;
        Assert.assertThrows(IOException.class, () -> S1InputUsesTable.decode(duplicate));
        wire = fixture();
        ((com.fasterxml.jackson.databind.node.ArrayNode) wire.path("uses")).remove(2);
        ObjectNode missing = wire;
        Assert.assertThrows(IOException.class, () -> S1InputUsesTable.decode(missing));
        for (String path: List.of("/uses/0/object", "/uses/0/environment", "/objects/0/field")) {
            ObjectNode invalid = fixture();
            int slash = path.lastIndexOf('/');
            ((ObjectNode) invalid.at(path.substring(0, slash))).put(path.substring(slash + 1), 999999);
            Assert.assertThrows(IOException.class, () -> S1InputUsesTable.decode(invalid));
        }
    }
}
