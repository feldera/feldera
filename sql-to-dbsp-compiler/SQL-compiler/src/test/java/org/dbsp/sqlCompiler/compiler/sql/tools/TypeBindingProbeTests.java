package org.dbsp.sqlCompiler.compiler.sql.tools;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TypeBindingProbeTests {
    @Test public void s2ProjectionIncludesCastIntroducedAfterValidation() throws IOException {
        var result = TypeBindingProbe.compile(
                "CREATE TABLE t(a INTEGER NOT NULL); CREATE VIEW v AS SELECT SQRT(a) AS result FROM t;",
                false, false, true);
        var projection = result.path("s2_projection");
        Assert.assertEquals("captured", projection.path("status").asText());
        Assert.assertEquals("sqrt_d(((d)(*t_1).0))", projection.path("expression_text").asText());
        Assert.assertEquals("F64", projection.path("type_text").asText());
        Assert.assertEquals("DBSPApplyExpression", projection.path("expression").path("class").asText());
    }

    @Test public void s2SetCaptureRetainsNullFilterBeforeNonNullCast() throws IOException {
        var result = TypeBindingProbe.compile(
                "CREATE TABLE t(a INTEGER NOT NULL, b INTEGER); "
                + "CREATE VIEW v AS SELECT a AS result FROM t EXCEPT ALL SELECT b AS result FROM t;",
                false, false, true);
        var projection = result.path("s2_projection");
        Assert.assertEquals("set_maps", projection.path("status").asText());
        var maps = projection.path("maps");
        Assert.assertEquals(4, maps.size());
        Assert.assertEquals("filter", maps.get(2).path("operator").asText());
        Assert.assertEquals("(!(*t_3).0.as_ref().cloned().is_none())",
                maps.get(2).path("expression_text").asText());
        Assert.assertEquals("((i32)(*t_4).0.as_ref().cloned())",
                maps.get(3).path("expression_text").asText());
    }

    private static ObjectNode capture(String columns, String expression) throws IOException {
        var result = TypeBindingProbe.compile("CREATE TABLE t(" + columns
                + "); CREATE VIEW v AS SELECT " + expression + " AS result FROM t;", true, true);
        Assert.assertTrue(result.path("diagnostics").toString(), result.path("accepted").asBoolean());
        Assert.assertTrue(result.path("capture_errors").toString(), result.path("capture_complete").asBoolean());
        Assert.assertTrue(result.path("bindings").size() >= 2);
        return result;
    }

    private static List<JsonNode> calls(JsonNode result, String stage, String kind) {
        var calls = new ArrayList<JsonNode>();
        for (var observation: result.path("bindings")) {
            if (!observation.path("stage").asText().equals(stage)) continue;
            String field = stage.equals("validated_sql") ? "nodes" : "expressions";
            for (var node: observation.path(field))
                if (node.path("operator").path("kind").asText().equals(kind)) calls.add(node);
        }
        return calls;
    }

    @Test public void observesNumericCoercionAndRegisteredOperator() throws IOException {
        var result = capture("a VARCHAR(5) NOT NULL", "ABS(a)");
        var casts = calls(result, "validated_sql", "CAST");
        Assert.assertFalse("Validator's string-to-decimal cast must be observed", casts.isEmpty());
        Assert.assertTrue(casts.stream().anyMatch(node -> node.path("type").path("sql_type").asText().equals("DECIMAL")
                && node.path("type").path("precision").asInt() == 38
                && node.path("type").path("scale").asInt() == 19));
        var operators = calls(result, "converted_rel", "OTHER_FUNCTION");
        Assert.assertTrue(operators.toString(), operators.stream().anyMatch(node ->
                node.path("operator").path("key").asText().equals(
                        "org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.functions.CustomFunctions$AbsFunction#abs")));
    }

    @Test public void preservesFixedWidthNullabilityAndCharacterMetadata() throws IOException {
        var result = capture("a CHAR(5)", "UPPER(a)");
        var calls = calls(result, "converted_rel", "OTHER_FUNCTION");
        Assert.assertTrue(calls.stream().anyMatch(node -> node.path("type").path("sql_type").asText().equals("CHAR")
                && node.path("type").path("precision").asInt() == 5
                && node.path("type").path("nullable").asBoolean()
                && node.path("type").has("charset") && node.path("type").has("collation")));
    }

    @Test public void coalesceRecordsActualOperandsBeforeLaterLowering() throws IOException {
        var result = capture("a INTEGER, b BIGINT", "COALESCE(a,b)");
        var coalesce = calls(result, "converted_rel", "COALESCE");
        Assert.assertEquals(1, coalesce.size());
        Assert.assertEquals("BIGINT", coalesce.get(0).path("type").path("sql_type").asText());
        Assert.assertEquals("SqlStdOperatorTable.COALESCE", coalesce.get(0).path("operator").path("key").asText());
        // This configured validator retains COALESCE and does not insert a CASE
        // or uniform operand casts. The collector must not invent either one.
        Assert.assertTrue(calls(result, "converted_rel", "CASE").isEmpty());
        Assert.assertTrue(calls(result, "converted_rel", "CAST").isEmpty());
    }

    @Test public void rejectedBindingIsNotConvertedOrCountedAsCaptureFailure() throws IOException {
        var result = TypeBindingProbe.compile(
                "CREATE TABLE t(a DATE, b TIMESTAMP); CREATE VIEW v AS SELECT NULLIF(a,b) AS result FROM t;",
                true, false);
        Assert.assertFalse(result.path("accepted").asBoolean());
        Assert.assertFalse(result.path("compiler_bug").asBoolean());
        Assert.assertTrue(result.path("capture_complete").asBoolean());
        // A compiler may also compile its system error view; user rejection still remains explicit.
        Assert.assertTrue(result.path("diagnostics").size() > 0);
    }

    @Test public void observerPreservesCompilerOutputsAndDiagnostics() throws IOException {
        for (String expression: List.of("SQRT(a)", "ABS(CAST(a AS VARCHAR(5)))", "COALESCE(a,b)", "NULLIF(a,b)")) {
            String program = "CREATE TABLE t(a INTEGER, b BIGINT); CREATE VIEW v AS SELECT "
                    + expression + " AS result FROM t;";
            var baseline = TypeBindingProbe.compile(program, false, true);
            var observed = TypeBindingProbe.compile(program, true, true);
            Assert.assertTrue(observed.path("capture_errors").toString(), observed.path("capture_complete").asBoolean());
            for (String field: List.of("accepted", "compiler_bug", "schema", "diagnostics", "options"))
                Assert.assertEquals(expression + ": " + field, baseline.get(field), observed.get(field));

        }
    }
    @Test public void observerPreservesS2UnderStrictCanonicalizer() throws IOException {
        String executable = System.getProperty("feldera.typeBindings.canonicalizer");
        org.junit.Assume.assumeNotNull(executable);
        try (var canonicalizer = new CorpusDeterminism.Canonicalizer(executable)) {
            for (String expression: List.of("SQRT(a)", "ABS(CAST(a AS VARCHAR(5)))", "COALESCE(a,b)", "NULLIF(a,b)")) {
                String program = "CREATE TABLE t(a INTEGER, b BIGINT); CREATE VIEW v AS SELECT "
                        + expression + " AS result FROM t;";
                var baseline = TypeBindingProbe.compile(program, false, true);
                var observed = TypeBindingProbe.compile(program, true, true);
                var mapper = org.dbsp.util.Utilities.deterministicObjectMapper();
                var envelope = mapper.createObjectNode().put("schema_version", 1)
                        .put("producer_commit", "type-binding-observer-control")
                        .put("runtime_abi", "type-binding-observer-control")
                        .put("engine_mode", "classic").put("replayable", false);
                envelope.set("options", baseline.get("options"));
                var observation = envelope.putObject("observation").put("stage", "S2").put("sequence", 0);
                observation.putArray("path");
                envelope.set("circuit", baseline.get("s2"));
                String expected = canonicalizer.apply(envelope.toString());
                envelope.set("circuit", observed.get("s2"));
                Assert.assertEquals(expression, expected, canonicalizer.apply(envelope.toString()));
            }
        }
    }

}
