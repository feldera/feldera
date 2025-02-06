package org.dbsp.sqlCompiler.compiler.sql.suites;

import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.TestUtil;
import org.dbsp.sqlCompiler.compiler.sql.tools.BaseSQLTests;
import org.dbsp.sqlCompiler.compiler.sql.tools.CompilerCircuit;
import org.junit.Test;

import java.io.IOException;

public class TpcDsTest extends BaseSQLTests {
    // TODO: Disabled in the SQL code the following views
    // q36: OVER and RANK
    // q47: OVER and RANK
    // q49: OVER and RANK
    // q51: OVER with ROWS aggregate
    // q57: OVER and RANK
    // q70: OVER and RANK (this should probably work)
    // q86: OVER and RANK
    @Test
    public void compileTpcds() throws IOException {
        String tpcds = TestUtil.readStringFromResourceFile("tpcds.sql");
        CompilerOptions options = this.testOptions(true, true);
        DBSPCompiler compiler = new DBSPCompiler(options);
        options.languageOptions.ignoreOrderBy = true;
        options.languageOptions.lenient = true;
        options.ioOptions.quiet = true;  // lots of warnings
        compiler.submitStatementsForCompilation(tpcds);
        CompilerCircuit ccs = new CompilerCircuit(compiler);
        ccs.showErrors();
        // This crashes the Rust compiler!
        // this.addRustTestCase("tpcds", ccs);
    }
}
