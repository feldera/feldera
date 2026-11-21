package org.dbsp.sqlCompiler.compiler.sql;

import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.sql.tools.DiagnosticAssertions;
import org.dbsp.sqlCompiler.compiler.sql.tools.DiagnosticAssertions.Target;
import org.junit.Assert;
import org.junit.Test;

/** Invalid constant operands must produce located SQL errors, not compiler bugs. */
public class ConstantRequiredTests {
    private static void rejectsNull(String query, String message) {
        for (int level: new int[] {0, 2}) {
            CompilerOptions options = new CompilerOptions();
            options.ioOptions.quiet = true;
            options.languageOptions.throwOnError = false;
            options.languageOptions.optimizationLevel = level;
            DBSPCompiler compiler = new DBSPCompiler(options);
            compiler.submitStatementsForCompilation("CREATE TABLE t(x INT NOT NULL, ts TIMESTAMP);\n"
                    + "CREATE VIEW v AS " + query + ";");
            compiler.getFinalCircuit(false);
            Assert.assertEquals(compiler.messages.toString(), 1, compiler.messages.errorCount());
            var error = compiler.messages.messages.get(0);
            DiagnosticAssertions.assertEquals(Target.ERROR_CLASS, "Not supported", error.errorType);
            DiagnosticAssertions.assertContains(Target.MESSAGE_BODY, message, error.message);
            Assert.assertTrue(compiler.messages.toJson(compiler.sources).get(0)
                    .path("start_line_number").asInt() > 0);
        }
    }

    @Test public void nullLeadOffset() {
        rejectsNull("SELECT LEAD(x, CAST(NULL AS INTEGER)) OVER (ORDER BY ts) FROM t", "LAG/LEAD offset must not be NULL");
    }

    @Test public void nullLagOffset() {
        rejectsNull("SELECT LAG(x, CAST(NULL AS INTEGER)) OVER (ORDER BY ts) FROM t", "LAG/LEAD offset must not be NULL");
    }

    @Test public void nullTumbleInterval() {
        rejectsNull("SELECT * FROM TABLE(TUMBLE(TABLE t, DESCRIPTOR(ts), CAST(NULL AS INTERVAL SECOND)))", "Tumbling window interval must not be NULL");
    }

    @Test public void nullTimestampLateness() {
        for (int level: new int[] {0, 2}) {
            CompilerOptions options = new CompilerOptions();
            options.ioOptions.quiet = true;
            options.languageOptions.throwOnError = false;
            options.languageOptions.optimizationLevel = level;
            DBSPCompiler compiler = new DBSPCompiler(options);
            compiler.submitStatementsForCompilation("CREATE TABLE t(ts TIMESTAMP LATENESS "
                    + "CAST(NULL AS INTERVAL SECOND)); CREATE VIEW v AS SELECT MAX(ts) FROM t;");
            compiler.getFinalCircuit(false);
            Assert.assertEquals(compiler.messages.toString(), 1, compiler.messages.errorCount());
            var error = compiler.messages.messages.get(0);
            DiagnosticAssertions.assertEquals(Target.ERROR_CLASS, "Compilation error", error.errorType);
            DiagnosticAssertions.assertContains(Target.MESSAGE_BODY, "LATENESS must not be NULL", error.message);
            Assert.assertTrue(compiler.messages.toJson(compiler.sources).get(0)
                    .path("start_line_number").asInt() > 0);
        }
    }
}
