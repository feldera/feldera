package org.dbsp.sqlCompiler.compiler.sql;

import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.sql.tools.DiagnosticAssertions;
import org.dbsp.sqlCompiler.compiler.sql.tools.DiagnosticAssertions.Target;
import org.junit.Assert;
import org.junit.Test;

/** Check the diagnostics for invalid ROW selectors. */
public class RowSelectorTests {
    private static DBSPCompiler compile(String expression, int level) {
        var options = new CompilerOptions();
        options.ioOptions.quiet = true;
        options.languageOptions.throwOnError = false;
        options.languageOptions.optimizationLevel = level;
        var compiler = new DBSPCompiler(options);
        compiler.submitStatementsForCompilation("CREATE TABLE t(x INT NOT NULL, ts TIMESTAMP NOT NULL);\n"
                + "CREATE VIEW v AS SELECT x, ts, " + expression + " AS y FROM t;");
        compiler.getFinalCircuit(false);
        return compiler;
    }

    @Test public void invalidStringSelectorsHaveStableLocatedDiagnostics() {
        for (int level: new int[] {0, 2}) {
            for (String selector: new String[] {"CONCAT('a', '')", "'a' || ''",
                    "CASE WHEN true THEN 'a' ELSE 'b' END", "CAST(NULL AS VARCHAR)", "CAST(x AS VARCHAR)"}) {
                com.fasterxml.jackson.databind.JsonNode previous = null;
                for (int repeat = 0; repeat < 2; repeat++) {
                    var compiler = compile("CAST(ROW(x, ts) AS ROW(a INT, b TIMESTAMP))[" + selector + "]", level);
                    Assert.assertEquals(compiler.messages.toString(), 1, compiler.messages.errorCount());
                    var diagnostics = compiler.messages.toJson(compiler.sources);
                    var error = diagnostics.get(0);
                    DiagnosticAssertions.assertEquals(Target.ERROR_CLASS, "Compilation error", error.path("error_type").asText());
                    DiagnosticAssertions.assertEquals(Target.MESSAGE_BODY, "Index in ROW type does not have a constant integer or string value",
                            error.path("message").asText());
                    Assert.assertTrue(error.path("start_line_number").asInt() > 0);
                    if (previous != null)
                        DiagnosticAssertions.assertSnapshotEquals(previous, diagnostics);
                    previous = diagnostics;
                }
            }
        }
    }

    @Test public void arrayAndMapSelectorsMayRemainDynamic() {
        for (int level: new int[] {0, 2}) {
            for (String expression: new String[] {"ARRAY[10,20][x]", "MAP['1',10][CAST(x AS VARCHAR)]"}) {
                var compiler = compile(expression, level);
                Assert.assertEquals(compiler.messages.toString(), 0, compiler.messages.errorCount());
            }
        }
    }

    @Test public void missingNamedFieldsRemainValidationErrors() {
        for (int level: new int[] {0, 2}) {
            var compiler = compile("CAST(ROW(x, ts) AS ROW(\"MiXeD\" INT, b TIMESTAMP))['missing']", level);
            Assert.assertEquals(compiler.messages.toString(), 1, compiler.messages.errorCount());
            var error = compiler.messages.toJson(compiler.sources).get(0);
            DiagnosticAssertions.assertEquals(Target.ERROR_CLASS, "Compilation error", error.path("error_type").asText());
            DiagnosticAssertions.assertContains(Target.MESSAGE_BODY,
                    "does not have a field named 'missing'", error.path("message").asText());
            Assert.assertTrue(error.path("start_line_number").asInt() > 0);
        }
    }
}
