package org.dbsp.sqlCompiler.compiler.sql.simple;

import org.dbsp.sqlCompiler.compiler.DBSPCompiler;

/** Runs the inherited tests with the FlatVariant representation of VARIANT. */
public class FlatVariantTests extends VariantTests {
    @Override
    public void prepareInputs(DBSPCompiler compiler) {
        compiler.submitStatementForCompilation("SET feldera_flat_variant = 'on'");
        super.prepareInputs(compiler);
    }
}
