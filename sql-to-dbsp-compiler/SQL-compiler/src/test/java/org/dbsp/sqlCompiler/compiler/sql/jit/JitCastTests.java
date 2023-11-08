package org.dbsp.sqlCompiler.compiler.sql.jit;

import org.dbsp.sqlCompiler.compiler.sql.simple.CastTests;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;

public class JitCastTests extends CastTests {
    @Override
    public DBSPCompiler testCompiler() {
        CompilerOptions options = this.testOptions(false, true, true);
        return new DBSPCompiler(options);
    }
}
