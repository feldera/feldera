package org.dbsp.sqlCompiler.compiler.sql.tools;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.util.Utilities;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class S1ExpressionCorpusTests {
    private static ObjectNode capture(String sql, boolean allFields) throws IOException {
        var mapper = Utilities.deterministicObjectMapper();
        ObjectNode input = mapper.createObjectNode().put("id", "local-fields");
        input.set("options", mapper.valueToTree(new CompilerOptions()));
        input.putArray("submissions").addObject().put("batch", 0).put("many", true).put("sql", sql);
        return S1ExpressionCorpusProbe.capture(input, new S1BuiltinOperatorRegistry(), allFields);
    }

    private static int roots(ObjectNode capture) {
        int result = 0;
        for (var fragment: capture.path("fragments")) result += fragment.at("/table/roots").size();
        return result;
    }

    @Test public void valuesAreCapturedEvenWhenThereIsNoProject() throws IOException {
        var captured = capture("CREATE VIEW v AS SELECT 17 AS x;", true);
        Assert.assertEquals("local_fields", captured.path("scope").textValue());
        Assert.assertTrue(roots(captured) > 0);
        Assert.assertEquals(0, roots(capture("CREATE VIEW v AS SELECT 17 AS x;", false)));
    }

    @Test public void joinAndFilterExpressionsUseTheirOrderedInputSlots() throws IOException {
        String sql = "CREATE TABLE t(x INT NOT NULL);CREATE TABLE u(y BIGINT NOT NULL);"
                + "CREATE VIEW v AS SELECT x, y FROM t JOIN u ON x = y WHERE x > 3;";
        var captured = capture(sql, true);
        Assert.assertTrue(roots(captured) > roots(capture(sql, false)));
        for (var fragment: captured.path("fragments"))
            S1ExpressionTable.decode(fragment.path("table"), new S1BuiltinOperatorRegistry());
    }

    @Test public void windowCallsPreserveTheirSpecializedMetadata() throws IOException {
        String sql = "CREATE TABLE t(x INT);"
                + "CREATE VIEW v AS SELECT SUM(x) OVER (ORDER BY x ROWS 1 PRECEDING) FROM t;";
        var result = capture(sql, true);
        boolean found = false;
        for (var fragment: result.path("fragments")) {
            var decoded = S1ExpressionTable.decode(fragment.path("table"), new S1BuiltinOperatorRegistry());
            for (var root: decoded.roots())
                if (root instanceof org.apache.calcite.rel.core.Window.RexWinAggCall) found = true;
        }
        Assert.assertTrue(found);
    }
    @Test public void arrayAggregateWindowUsesTheCompilerReplacement() throws IOException {
        var result = capture("CREATE TABLE t(x INT);"
                + "CREATE VIEW v AS SELECT ARRAY_AGG(x) OVER (ORDER BY x ROWS 1 PRECEDING) FROM t;", true);
        boolean found = false;
        for (var fragment: result.path("fragments")) {
            var decoded = S1ExpressionTable.decode(fragment.path("table"), new S1BuiltinOperatorRegistry());
            for (var root: decoded.roots())
                if (root instanceof org.apache.calcite.rel.core.Window.RexWinAggCall call)
                    found |= call.getOperator() == org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.functions.SqlArrayAggFunction.INSTANCE;
        }
        Assert.assertTrue(found);
    }

    @Test public void malformedWindowMetadataAndScalarOperatorsAreRejected() throws IOException {
        var registry = new S1BuiltinOperatorRegistry();
        var sample = S1ExpressionTransportProbe.samples(registry).stream()
                .filter(item -> item.name().equals("window-aggregate")).findFirst().orElseThrow();
        ObjectNode wire = S1ExpressionEncoder.encode(sample.roots(), sample.input(), registry, sample.source());
        for (String field: new String[] {"ordinal", "distinct", "ignore_nulls"}) {
            ObjectNode invalid = wire.deepCopy();
            ((ObjectNode) invalid.at("/expressions/0/kind")).put(field, "invalid");
            Assert.assertThrows(IOException.class, () -> S1ExpressionTable.decode(invalid, registry));
        }
        ObjectNode invalid = wire.deepCopy();
        var plus = registry.identify(org.apache.calcite.sql.fun.SqlStdOperatorTable.PLUS);
        ((ObjectNode) invalid.at("/operator_objects/0")).put("registry", plus.registry())
                .put("registry_version", plus.version()).put("key", plus.key());
        Assert.assertThrows(IOException.class, () -> S1ExpressionTable.decode(invalid, registry));
    }

}
