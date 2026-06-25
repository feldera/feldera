package org.dbsp.sqlCompiler.compiler.frontend.connectors.config;

import org.dbsp.sqlCompiler.compiler.frontend.connectors.ConfigReporter;
import org.dbsp.sqlCompiler.compiler.frontend.connectors.IValidateConfig;

/** Configuration for the Parquet input format.
 * Validation still catches unknown fields. */
@SuppressWarnings("unused")
public class ParquetParserConfig implements IValidateConfig {
    @Override
    public boolean validate(ConfigReporter reporter) {
        return true;
    }
}
