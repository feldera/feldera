package org.dbsp.sqlCompiler.compiler.sql.simple;

import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;

// Runs the VariantTests with optimizations
public class OptimizedVariantTests extends VariantTests {
    @Override
    public CompilerOptions testOptions() {
        CompilerOptions options = super.testOptions();
        options.languageOptions.optimizationLevel = 2;
        options.languageOptions.incrementalize = false;
        return options;
    }
}
