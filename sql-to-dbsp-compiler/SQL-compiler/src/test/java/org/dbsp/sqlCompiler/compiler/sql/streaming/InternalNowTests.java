package org.dbsp.sqlCompiler.compiler.sql.streaming;

import org.dbsp.sqlCompiler.CompilerMain;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIntegrateTraceRetainKeysOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIntegrateTraceRetainValuesOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPPartitionedRollingAggregateWithWaterlineOperator;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.StderrErrorReporter;
import org.dbsp.sqlCompiler.compiler.errors.CompilerMessages;
import org.dbsp.sqlCompiler.compiler.sql.StreamingTestBase;
import org.dbsp.sqlCompiler.compiler.sql.tools.BaseSQLTests;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.outer.OptimizeMaps;
import org.dbsp.sqlCompiler.compiler.visitors.outer.Passes;
import org.dbsp.util.Linq;
import org.dbsp.util.Logger;
import org.dbsp.util.Utilities;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

/** Tests that exercise the internal implementation of NOW */
public class InternalNowTests extends StreamingTestBase {
    @Test
    public void testNow() {
        Logger.INSTANCE.setLoggingLevel(DBSPCompiler.class, 2);
        String sql = """
                CREATE VIEW V AS SELECT 1, NOW() > TIMESTAMP '2022-12-12 00:00:00';""";
        DBSPCompiler compiler = this.testCompiler();
        compiler.compileStatements(sql);
        CompilerCircuitStream ccs = new CompilerCircuitStream(compiler);
        ccs.step("",
                """
                         c | compare | weight
                        ----------------------
                         1 | true    | 1""");
        this.addRustTestCase("testNow", ccs);
    }
}
