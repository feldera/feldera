package org.dbsp.sqlCompiler.compiler.sql.suites;

import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.TestUtil;
import org.dbsp.sqlCompiler.compiler.sql.tools.BaseSQLTests;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;

public class TpcDsTest extends BaseSQLTests {
    @Test @Ignore("https://issues.apache.org/jira/browse/CALCITE-6518")
    public void compileTpcds() throws IOException {
        String tpch = TestUtil.readStringFromResourceFile("tpcds.sql");
        CompilerOptions options = this.testOptions(true, true);
        DBSPCompiler compiler = new DBSPCompiler(options);
        options.languageOptions.ignoreOrderBy = true;
        compiler.compileStatements(tpch);
        CompilerCircuitStream ccs = new CompilerCircuitStream(compiler);
        ccs.showErrors();
        this.addRustTestCase("tpcds", ccs);
    }
}
