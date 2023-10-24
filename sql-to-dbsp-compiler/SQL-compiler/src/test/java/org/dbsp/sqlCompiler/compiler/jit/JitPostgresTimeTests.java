package org.dbsp.sqlCompiler.compiler.jit;

import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.postgres.PostgresTimeTests;
import org.junit.Ignore;
import org.junit.Test;

public class JitPostgresTimeTests extends PostgresTimeTests {
    @Override
    public CompilerOptions getOptions(boolean optimize) {
        CompilerOptions options = super.getOptions(optimize);
        options.ioOptions.jit = true;
        return options;
    }
}
