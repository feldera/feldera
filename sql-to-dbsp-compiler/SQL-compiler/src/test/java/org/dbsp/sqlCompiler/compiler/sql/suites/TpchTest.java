package org.dbsp.sqlCompiler.compiler.sql.suites;

import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.TestUtil;
import org.dbsp.sqlCompiler.compiler.sql.tools.BaseSQLTests;
import org.junit.Test;

import java.io.IOException;

public class TpchTest extends BaseSQLTests {
    @Test
    public void compileTpch() throws IOException {
        // Logger.INSTANCE.setLoggingLevel(DBSPCompiler.class, 2);
        // Logger.INSTANCE.setLoggingLevel(CalciteCompiler.class, 2);
        String tpch = TestUtil.readStringFromResourceFile("tpch.sql");
        // The following convert every view except 21 into a local view
        //tpch = tpch.replace("create view q", "create local view q");
        //tpch = tpch.replace("create local view q21", "create view q21");
        CompilerOptions options = this.testOptions(true, true);
        DBSPCompiler compiler = new DBSPCompiler(options);
        options.languageOptions.ignoreOrderBy = true;
        compiler.compileStatements(tpch);
        CompilerCircuitStream ccs = new CompilerCircuitStream(compiler);
        ccs.showErrors();
        this.addRustTestCase(ccs);
    }
}
