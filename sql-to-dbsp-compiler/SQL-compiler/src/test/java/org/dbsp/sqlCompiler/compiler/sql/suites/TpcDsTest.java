package org.dbsp.sqlCompiler.compiler.sql.suites;

import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.TestUtil;
import org.dbsp.sqlCompiler.compiler.sql.tools.BaseSQLTests;
import org.dbsp.sqlCompiler.compiler.visitors.outer.Passes;
import org.dbsp.util.Logger;
import org.junit.Test;

import java.io.IOException;

public class TpcDsTest extends BaseSQLTests {
    // TODO: Disabled in the SQL code the following views
    // q12: OVER without ORDER BY https://github.com/feldera/feldera/issues/457
    // q20: OVER without ORDER BY
    // q36: OVER and RANK
    // q47: OVER without ORDER BY
    // q49: OVER and RANK
    // q51: OVER with ROWS aggregate
    // q53: OVER without ORDER BY
    // q57: OVER without ORDER BY
    // q63: OVER without ORDER BY
    // q70: OVER and RANK (this should probably work)
    // q86: OVER and RANK
    // q89: OVER without ORDER BY
    @Test
    public void compileTpcds() throws IOException {
        Logger.INSTANCE.setLoggingLevel(Passes.class, 2);
        String tpcds = TestUtil.readStringFromResourceFile("tpcds.sql");
        CompilerOptions options = this.testOptions(true, true);
        DBSPCompiler compiler = new DBSPCompiler(options);
        options.languageOptions.ignoreOrderBy = true;
        options.languageOptions.lenient = true;
        options.ioOptions.quiet = true;  // lots of warnings
        compiler.compileStatements(tpcds);
        CompilerCircuitStream ccs = new CompilerCircuitStream(compiler);
        ccs.showErrors();
        // This crashes the Rust compiler!
        // this.addRustTestCase("tpcds", ccs);
    }
}
