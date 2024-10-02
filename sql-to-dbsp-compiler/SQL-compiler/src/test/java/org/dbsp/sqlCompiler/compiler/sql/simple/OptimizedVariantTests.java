package org.dbsp.sqlCompiler.compiler.sql.simple;

import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;

// Runs the VariantTests with optimizations
public class OptimizedVariantTests extends VariantTests {
    @Override
    public DBSPCompiler testCompiler() {
        CompilerOptions options = this.testOptions(false, true);
        DBSPCompiler compiler = new DBSPCompiler(options);
        this.prepareInputs(compiler);
        return compiler;
    }
}
