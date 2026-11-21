package org.dbsp.sqlCompiler.compiler.sql.tools;

import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.ProgramIdentifier;
import org.dbsp.util.Utilities;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

/** Focused construction checks, not a substitute for full S1/S2 observations. */
public class S1DetachedLoweringTests {
    @Test public void frontendOnlyWarningRestoresTheCompilerWarningFlag() throws IOException {
        var mapper = Utilities.deterministicObjectMapper();
        var input = mapper.createObjectNode().put("id", "frontend-warning");
        input.set("options", mapper.valueToTree(new CompilerOptions()));
        input.putArray("submissions").addObject().put("batch", 0).put("many", true)
                .put("sql", "DECLARE RECURSIVE VIEW unused(x INT); CREATE VIEW v AS SELECT 1 AS x;");
        var registry = new S1BuiltinOperatorRegistry();
        var decoded = S1Program.decode(S1Program.capture(input, registry), registry);
        var original = S1ExpressionCorpusProbe.compile(input);
        Assert.assertTrue(original.hasWarnings());
        var imported = S1Program.loweringCompiler(decoded);
        Assert.assertNotNull(S1Program.lower(decoded, imported));
        Assert.assertEquals(original.hasWarnings(), imported.hasWarnings());
        Assert.assertEquals(original.messages.replaySnapshot(), imported.messages.replaySnapshot());
    }

    @Test public void singleStatementApiRetainsItsUnsetInputMarker() throws IOException {
        var mapper = Utilities.deterministicObjectMapper();
        var input = mapper.createObjectNode().put("id", "single-statement");
        input.set("options", mapper.valueToTree(new CompilerOptions()));
        input.putArray("submissions").addObject().put("batch", 0).put("many", false)
                .put("sql", "CREATE VIEW v AS SELECT 1 AS x");
        var registry = new S1BuiltinOperatorRegistry();
        var decoded = S1Program.decode(S1Program.capture(input, registry), registry);
        Assert.assertEquals(DBSPCompiler.InputSource.None, decoded.context().inputSource());
        var compiler = S1Program.loweringCompiler(decoded);
        Assert.assertNotNull(S1Program.lower(decoded, compiler));
        Assert.assertEquals(DBSPCompiler.InputSource.None, compiler.inputSources);
    }

    @Test public void sourceBoundariesAndLateWarningsSurviveDetachedOptimization() throws IOException {
        var mapper = Utilities.deterministicObjectMapper();
        var options = new CompilerOptions(); options.ioOptions.inputCircuit = true;
        var input = mapper.createObjectNode().put("id", "context-replay");
        input.set("options", mapper.valueToTree(options));
        var submissions = input.putArray("submissions");
        // Separate API submissions have separate diagnostic lines even without a newline.
        submissions.addObject().put("batch", 0).put("many", true).put("sql", "CREATE TABLE t(s VARCHAR); CREATE INDEX ix ON t(s);");
        submissions.addObject().put("batch", 0).put("many", true).put("sql", "DECLARE RECURSIVE VIEW unused(x INT); LATENESS missing.x 2; CREATE VIEW v AS SELECT s RLIKE 'a(?=b)' AS found FROM t;");
        var registry = new S1BuiltinOperatorRegistry();
        var wire = S1Program.capture(input, registry);
        Assert.assertFalse(wire.path("diagnostics").toString().contains("look-ahead"));
        var decoded = S1Program.decode(wire, registry);
        Assert.assertEquals(1, decoded.context().lateness().size());
        var imported = S1Program.loweringCompiler(decoded);
        var lowered = S1Program.lower(decoded, imported);
        var baseline = S1ExpressionCorpusProbe.compileResult(input, compiler -> { });
        Assert.assertEquals(baseline.compiler().sources.replaySnapshot(), imported.sources.replaySnapshot());
        Assert.assertEquals(baseline.compiler().messages.replaySnapshot(), imported.messages.replaySnapshot());
        imported.options.ioOptions.inputCircuit = false;
        baseline.compiler().options.ioOptions.inputCircuit = false;
        Assert.assertNotNull(imported.optimizeFromS2(lowered));
        Assert.assertNotNull(baseline.compiler().optimizeFromS2(baseline.circuit()));
        Assert.assertEquals(baseline.compiler().messages.replaySnapshot(), imported.messages.replaySnapshot());
        Assert.assertTrue(imported.messages.messages.stream().anyMatch(message -> message.errorType.equals("No such view")));
        Assert.assertThrows(IllegalStateException.class, imported::getSqlToRelCompiler);
    }

    @Test public void importedStatementsLowerWithoutAParserOrBinder() throws IOException {
        for (var sql: List.of(
                "CREATE TABLE t(x INT); CREATE VIEW v AS SELECT x + 1 AS y FROM t WHERE x > 0;",
                "CREATE TYPE pair AS (x INT, y INT); CREATE TABLE t(p pair); CREATE VIEW v AS SELECT * FROM t;",
                "CREATE FUNCTION plus_one(x INT) RETURNS INT AS x + 1; CREATE TABLE t(x INT); CREATE VIEW v AS SELECT plus_one(x) AS y FROM t;",
                "CREATE LINEAR AGGREGATE total(x INT) RETURNS INT; CREATE TABLE t(x INT); CREATE VIEW v AS SELECT total(x) AS y FROM t;",
                "CREATE TABLE t(a INT NOT NULL, b INT); INSERT INTO t(b,a) VALUES (20,10); REMOVE FROM t(b,a) VALUES (20,10); CREATE VIEW v AS SELECT * FROM t;")) {
            var mapper = Utilities.deterministicObjectMapper();
            var options = new CompilerOptions(); options.ioOptions.inputCircuit = true;
            var input = mapper.createObjectNode().put("id", "detached-lowering");
            input.set("options", mapper.valueToTree(options));
            input.putArray("submissions").addObject().put("batch", 0).put("many", true).put("sql", sql);
            var registry = new S1BuiltinOperatorRegistry();
            var decoded = S1Program.decode(S1Program.capture(input, registry), registry);
            var compiler = S1Program.loweringCompiler(decoded);
            Assert.assertThrows(IllegalStateException.class, compiler::getSqlToRelCompiler);
            var circuit = S1Program.lower(decoded, compiler);
            Assert.assertNotNull(sql, circuit); Assert.assertFalse(compiler.messages.toString(), compiler.hasErrors());
            var original = new DBSPCompiler(options);
            original.submitStatementsForCompilation(sql);
            var baseline = original.getFinalCircuit(false);
            Assert.assertNotNull(sql, baseline);
            Assert.assertEquals(sql, original.getIOMetadataAsJson(), compiler.getIOMetadataAsJson());
            Assert.assertEquals(sql, baseline.allOperators.stream().map(Object::getClass).toList(),
                    circuit.allOperators.stream().map(Object::getClass).toList());
            for (int id = 0; id < baseline.allOperators.size(); id++) {
                var expected = baseline.allOperators.get(id); var actual = circuit.allOperators.get(id);
                Assert.assertEquals(sql, expected.outputCount(), actual.outputCount());
                for (int output = 0; output < expected.outputCount(); output++)
                    Assert.assertTrue(sql, expected.outputType(output).sameType(actual.outputType(output)));
            }
            if (sql.contains("REMOVE"))
                Assert.assertTrue(compiler.getTableContents().getTableContents(new ProgramIdentifier("t")).data.isEmpty());
        }
    }
}
