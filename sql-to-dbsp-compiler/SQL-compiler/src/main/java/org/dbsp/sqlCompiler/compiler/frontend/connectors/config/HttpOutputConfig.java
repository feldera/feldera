package org.dbsp.sqlCompiler.compiler.frontend.connectors.config;

import org.dbsp.sqlCompiler.compiler.frontend.connectors.ConfigReporter;
import org.dbsp.sqlCompiler.compiler.frontend.connectors.IValidateConfig;

import com.fasterxml.jackson.annotation.JsonProperty;

/** Configuration for data output via HTTP. */
@SuppressWarnings("unused")
public class HttpOutputConfig implements IValidateConfig {
    /** When {@code true}, block the pipeline if the HTTP client cannot keep up.
     * When {@code false} (the default), drop chunks instead of blocking. */
    @JsonProperty("backpressure")
    public boolean backpressure = false;

    @Override
    public boolean validate(ConfigReporter reporter) {
        return true;
    }
}
