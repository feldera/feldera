package org.dbsp.sqlCompiler.compiler.sql.suites;

import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.compiler.sql.BaseSQLTests;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.TestUtil;
import org.dbsp.sqlCompiler.compiler.backend.rust.RustFileWriter;
import org.dbsp.util.Utilities;
import org.junit.Test;

import java.io.IOException;

public class TpchTest extends BaseSQLTests {
    @Test
    public void compileTpch() throws IOException, InterruptedException {
        String tpch = TestUtil.readStringFromResourceFile("tpch.sql");
        DBSPCompiler compiler = this.testCompiler();
        compiler.compileStatements(tpch);
        System.err.println(compiler.messages);
        compiler.throwIfErrorsOccurred();
        DBSPCircuit circuit = getCircuit(compiler);
        RustFileWriter writer = new RustFileWriter(compiler, testFilePath);
        writer.add(circuit);
        writer.writeAndClose();
        Utilities.compileAndTestRust(rustDirectory, true);
    }
}
