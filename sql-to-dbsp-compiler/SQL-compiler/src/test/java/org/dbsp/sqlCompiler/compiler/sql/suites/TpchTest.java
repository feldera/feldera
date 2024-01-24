package org.dbsp.sqlCompiler.compiler.sql.suites;

import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.TestUtil;
import org.dbsp.sqlCompiler.compiler.sql.BaseSQLTests;
import org.junit.Test;

import java.io.IOException;

public class TpchTest extends BaseSQLTests {
    @Test
    public void compileTpch() throws IOException {
        String tpch = TestUtil.readStringFromResourceFile("tpch.sql");
        CompilerOptions options = this.testOptions(true, true);
        DBSPCompiler compiler = new DBSPCompiler(options);
        options.languageOptions.ignoreOrderBy = true;
        options.languageOptions.outputsAreSets = true;
        compiler.compileStatements(tpch);
        System.err.println(compiler.messages);
        compiler.throwIfErrorsOccurred();
        this.addRustTestCase("tpch", compiler, getCircuit(compiler));
    }
}
