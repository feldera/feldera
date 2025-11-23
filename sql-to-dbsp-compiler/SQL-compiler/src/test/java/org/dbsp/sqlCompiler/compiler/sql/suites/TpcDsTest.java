package org.dbsp.sqlCompiler.compiler.sql.suites;

import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.TestUtil;
import org.dbsp.sqlCompiler.compiler.sql.tools.BaseSQLTests;
import org.dbsp.sqlCompiler.compiler.sql.tools.CompilerCircuit;
import org.junit.Test;

import java.io.IOException;

public class TpcDsTest extends BaseSQLTests {
    @Override
    public CompilerOptions testOptions() {
        CompilerOptions options = super.testOptions();
        options.languageOptions.incrementalize = true;
        options.languageOptions.optimizationLevel = 2;
        options.languageOptions.ignoreOrderBy = true;
        options.languageOptions.lenient = true;
        options.ioOptions.quiet = true;  // lots of warnings
        return options;
    }

    // TODO: Disabled in the SQL code the following views
    // q36: OVER and RANK
    // q47: OVER and RANK
    // q49: OVER and RANK
    // q51: OVER with ROWS aggregate
    // q57: OVER and RANK
    // q70: OVER and RANK (this should probably work)
    // q86: OVER and RANK
    @Test
    public void compileIndividually() throws IOException {
        String tpcds = TestUtil.readStringFromResourceFile("tpcds.sql");
        // split by keeping the comment at the beginning of each view
        String[] parts = tpcds.split("(?=/\\*)");
        String tables = parts[0];
        for (int i = 1; i < parts.length; i++) {
            String part = parts[i];
            String program = tables + part;
            DBSPCompiler compiler = new DBSPCompiler(this.testOptions());
            compiler.submitStatementsForCompilation(program);
            CompilerCircuit cc = new CompilerCircuit(compiler);
            cc.showErrors();
        }
    }
}
