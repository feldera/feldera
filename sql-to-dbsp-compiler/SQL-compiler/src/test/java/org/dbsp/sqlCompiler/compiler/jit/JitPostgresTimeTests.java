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

    @Test @Ignore("Time functions not yet implemented in JIT https://github.com/feldera/feldera/issues/913")
    public void testUnits() {
        super.testUnits();
    }
}
