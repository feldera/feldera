package org.dbsp.sqlCompiler.compiler.sql;

import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.sql.tools.SqlIoTest;
import org.junit.Ignore;
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
        return new DBSPCompiler(options);
    }

    @Test
    public void nullableRow() {
        this.getCCS("""
            CREATE TABLE T (j ROW(k ROW(l VARCHAR)));
            CREATE VIEW V AS SELECT t.j.k.l FROM T;""");
    }
}
