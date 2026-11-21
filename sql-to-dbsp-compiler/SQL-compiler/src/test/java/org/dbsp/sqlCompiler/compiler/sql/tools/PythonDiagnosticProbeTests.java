package org.dbsp.sqlCompiler.compiler.sql.tools;

import org.junit.Assert;
import org.junit.Test;

public class PythonDiagnosticProbeTests {
    @Test public void compileRejectionIsDifferentFromUnexecutedRuntimeExpectation() {
        var rejected = PythonDiagnosticProbe.compile("CREATE VIEW v AS SELECT * FROM missing;", "not found");
        Assert.assertTrue(rejected.path("compiler_rejected").asBoolean());
        Assert.assertTrue(rejected.path("expected_text_found").asBoolean());
        Assert.assertFalse(rejected.path("python_test_passed").asBoolean());
        var accepted = PythonDiagnosticProbe.compile(
                "CREATE TABLE t(x INT); CREATE VIEW v AS SELECT x + 1 AS y FROM t;", "causes overflow");
        Assert.assertFalse(accepted.path("compiler_rejected").asBoolean());
        Assert.assertTrue(accepted.path("circuit_produced").asBoolean());
        Assert.assertFalse(accepted.path("expected_text_found").asBoolean());
        Assert.assertFalse(accepted.path("runtime_executed").asBoolean());
    }
}
