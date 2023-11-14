package org.dbsp.sqlCompiler.compiler.sql.jit;

import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.sql.postgres.PostgresTimeTests;

public class JitPostgresTimeTests extends PostgresTimeTests {
    @Override
    public CompilerOptions getOptions(boolean optimize) {
        CompilerOptions options = super.getOptions(optimize);
        options.ioOptions.jit = true;
        return options;
    }
}
