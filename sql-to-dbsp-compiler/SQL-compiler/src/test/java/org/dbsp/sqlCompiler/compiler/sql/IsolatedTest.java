package org.dbsp.sqlCompiler.compiler.sql;

import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.CalciteOptimizer;
import org.dbsp.sqlCompiler.compiler.sql.tools.SqlIoTest;
import org.dbsp.util.Logger;
import org.junit.Test;

/** Used for interactive debugging: create here temporary tests. */
@SuppressWarnings("unused")
public class IsolatedTest extends SqlIoTest {
    @Override
    public DBSPCompiler testCompiler() {
        CompilerOptions options = this.testOptions(false, true);
        options.ioOptions.raw = true;
        options.languageOptions.throwOnError = true;
        options.ioOptions.quiet = false;
        options.languageOptions.incrementalize = true;
        return new DBSPCompiler(options);
    }
}
