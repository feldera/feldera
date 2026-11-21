package org.dbsp.sqlCompiler.compiler.sql.tools;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlSampleSpec;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.dbsp.util.Utilities;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;

public class ParserSemanticProbeTests {
    private static ObjectNode pair(String original, String emitted) {
        var input = Utilities.deterministicObjectMapper().createObjectNode().put("id", "semantic-test");
        input.putObject("original").putArray("submissions").addObject()
                .put("sql", original).put("many", true).put("batch", 0);
        input.putObject("emitted").putArray("submissions").addObject()
                .put("sql", emitted).put("many", true).put("batch", 0);
        return input;
    }

    @Test public void astOnlyControlsNeedNotBindAndStillDetectChangedExpressions() throws Exception {
        var result = ParserSemanticProbe.compareAst(pair("SELECT x FROM missing", "SELECT x FROM missing"));
        Assert.assertTrue(result.path("equal").asBoolean());
        Assert.assertFalse(result.path("binding_executed").asBoolean());
        Assert.assertFalse(result.has("original_accepted"));
        Assert.assertFalse(ParserSemanticProbe.compareAst(pair("SELECT x FROM missing", "SELECT y FROM missing"))
                .path("equal").asBoolean());
    }

    @Test public void sampleComparisonPreservesEverySampleSpecField() throws Exception {
        String sql = "SELECT * FROM t TABLESAMPLE BERNOULLI(10) REPEATABLE(1)";
        Assert.assertTrue(ParserSemanticProbe.compareAst(pair(sql, sql)).path("equal").asBoolean());
        for (String changed: new String[]{sql.replace("BERNOULLI", "SYSTEM"), sql.replace("(10)", "(20)"),
                sql.replace(" REPEATABLE(1)", ""), sql.replace("REPEATABLE(1)", "REPEATABLE(2)")})
            Assert.assertFalse(ParserSemanticProbe.compareAst(pair(sql, changed)).path("equal").asBoolean());
    }

    private static SqlLiteral sampleLiteral(String rate, boolean repeatable, int seed) throws Exception {
        // The public factory fixes the inactive seed to zero. Construct the
        // pinned value shape directly to verify that even this field is kept.
        var constructor = SqlSampleSpec.SqlTableSampleSpec.class.getDeclaredConstructor(
                boolean.class, BigDecimal.class, boolean.class, int.class);
        constructor.setAccessible(true);
        return SqlLiteral.createSample(constructor.newInstance(true, new BigDecimal(rate), repeatable, seed),
                SqlParserPos.ZERO);
    }

    @Test public void sampleComparisonKeepsInactiveSeedAndDecimalScale() throws Exception {
        Assert.assertFalse(ParserAstEquality.equal(sampleLiteral("0.1", false, 0), sampleLiteral("0.1", false, 1)));
        Assert.assertFalse(ParserAstEquality.equal(sampleLiteral("0.1", false, 0), sampleLiteral("0.10", false, 0)));
        Assert.assertTrue(ParserAstEquality.equal(sampleLiteral("0.1", false, 0), sampleLiteral("0.1", false, 0)));
    }

    @Test public void sampleComparisonDoesNotMutateOriginalAstNodes() throws Exception {
        String sql = "SELECT * FROM t TABLESAMPLE BERNOULLI(10) REPEATABLE(1)";
        var left = (SqlSelect) SqlParser.create(sql).parseQuery();
        var right = (SqlSelect) SqlParser.create(sql).parseQuery();
        var leftFrom = (SqlCall) left.getFrom();
        var rightFrom = (SqlCall) right.getFrom();
        var leftLiteral = leftFrom.operand(1);
        var rightLiteral = rightFrom.operand(1);
        String before = left.toString();
        Assert.assertTrue(ParserAstEquality.equal(left, right));
        Assert.assertSame(leftFrom, left.getFrom());
        Assert.assertSame(rightFrom, right.getFrom());
        Assert.assertSame(leftLiteral, leftFrom.operand(1));
        Assert.assertSame(rightLiteral, rightFrom.operand(1));
        Assert.assertEquals(before, left.toString());
    }

    @Test public void sourceCoordinatesAreRetainedSeparatelyFromAstAndBinding() throws Exception {
        var result = ParserSemanticProbe.compare(pair(
                "CREATE VIEW v AS SELECT MAP[1,'a'] AS m;",
                "\nCREATE VIEW v\nAS SELECT MAP[1, 'a'] AS m;"));
        Assert.assertTrue(result.path("equal").asBoolean());
        Assert.assertFalse(result.path("statement_ranges_equal").asBoolean());
        Assert.assertFalse(result.path("original_ranges").isEmpty());
        Assert.assertFalse(result.path("emitted_ranges").isEmpty());
    }

    @Test public void changedLiteralCannotPassBecauseItsBoundTypeMatches() throws Exception {
        var result = ParserSemanticProbe.compare(pair(
                "CREATE VIEW v AS SELECT MAP[1,'a'] AS m;",
                "CREATE VIEW v AS SELECT MAP[1,'b'] AS m;"));
        Assert.assertTrue(result.path("schema_equal").asBoolean());
        Assert.assertFalse(result.path("ast_equal").asBoolean());
        Assert.assertFalse(result.path("equal").asBoolean());
    }

    @Test public void bindingChecksColumnNullability() throws Exception {
        var result = ParserSemanticProbe.compare(pair(
                "CREATE TABLE t(x INT); CREATE VIEW v AS SELECT x FROM t;",
                "CREATE TABLE t(x INT NOT NULL); CREATE VIEW v AS SELECT x FROM t;"));
        Assert.assertFalse(result.path("schema_equal").asBoolean());
        Assert.assertFalse(result.path("equal").asBoolean());
    }

    @Test public void explicitBindingRefusalsAreNotCountedAsSchemaAgreement() throws Exception {
        String sql = "CREATE OR REPLACE LOCAL VIEW v AS SELECT 1 AS n; CREATE VIEW result AS SELECT n FROM v;";
        var input = pair(sql, sql);
        ((ObjectNode) input.path("original")).put("binding_expectation", "rejected")
                .put("expected_error_type", "Not supported");
        var result = ParserSemanticProbe.compare(input);
        Assert.assertTrue(result.path("equal").asBoolean());
        Assert.assertFalse(result.path("original_accepted").asBoolean());
        Assert.assertTrue(result.path("schema_equal").isNull());
    }

    @Test public void compilerBugsCannotBecomeMatchingBindingRefusals() {
        var compiler = new org.dbsp.sqlCompiler.compiler.DBSPCompiler(
                new org.dbsp.sqlCompiler.compiler.CompilerOptions());
        compiler.messages.reportError(new org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError("probe"));
        Assert.assertThrows(java.io.IOException.class, () -> ParserSemanticProbe.requireUserErrors(compiler));
    }

    @Test public void losingLocalViewKindCannotPassAstComparison() throws Exception {
        var result = ParserSemanticProbe.compare(pair(
                "CREATE LOCAL VIEW v AS SELECT 1 AS n; CREATE VIEW result AS SELECT n FROM v;",
                "CREATE VIEW v AS SELECT 1 AS n; CREATE VIEW result AS SELECT n FROM v;"));
        Assert.assertFalse(result.path("ast_equal").asBoolean());
        Assert.assertFalse(result.path("equal").asBoolean());
    }

    @Test public void emptyManyStatementSubmissionRetainsItsBoundary() throws Exception {
        var input = pair("CREATE VIEW v AS SELECT 1 AS n;", "CREATE VIEW v AS SELECT 1 AS n;");
        for (String side: java.util.List.of("original", "emitted"))
            ((com.fasterxml.jackson.databind.node.ArrayNode) input.path(side).path("submissions"))
                    .insertObject(0).put("sql", "").put("many", true).put("batch", 0);
        Assert.assertTrue(ParserSemanticProbe.compare(input).path("equal").asBoolean());
    }

    @Test public void hintRenderingFailureDoesNotReplaceStructuralComparison() throws Exception {
        String sql = "SELECT * FROM t /*+ h('a' 'b') */ AS x";
        var same = ParserSemanticProbe.compareAst(pair(sql, sql));
        Assert.assertTrue(same.path("equal").asBoolean());
        Assert.assertEquals("java.lang.ClassCastException", same.path("statements").get(0)
                .path("original_sql_render_failure").path("class").asText());
        var changed = ParserSemanticProbe.compareAst(pair(sql, sql.replace("'b'", "'c'")));
        Assert.assertFalse(changed.path("equal").asBoolean());
    }

    @Test public void capturedCompilerFailuresRemainMismatches() {
        var record = ParserSemanticProbe.comparisonFailure("failed", new java.io.IOException("compiler failed"));
        Assert.assertFalse(record.path("equal").asBoolean());
        Assert.assertFalse(record.has("original_accepted"));
        Assert.assertTrue(record.path("comparison_failure").path("stack").asText().contains("compiler failed"));
    }

    @Test public void propertyCoordinatesRemainVisibleAndValuesStillCompare() throws Exception {
        String sql = "CREATE TABLE t(x INT) WITH ('append_only' = 'true'); CREATE VIEW v AS SELECT x FROM t;";
        var moved = ParserSemanticProbe.compare(pair(sql, "\n" + sql));
        Assert.assertTrue(moved.path("equal").asBoolean());
        Assert.assertTrue(moved.path("schema_content_equal").asBoolean());
        Assert.assertFalse(moved.path("schema_equal").asBoolean());
        Assert.assertFalse(moved.path("property_ranges_equal").asBoolean());
        Assert.assertFalse(moved.path("original_property_ranges").isEmpty());
        var changed = ParserSemanticProbe.compare(pair(sql, sql.replace("'true'", "'false'")));
        Assert.assertFalse(changed.path("equal").asBoolean());
        Assert.assertFalse(changed.path("schema_content_equal").asBoolean());
    }
}
