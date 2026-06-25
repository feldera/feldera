package org.dbsp.sqlCompiler.compiler.frontend.connectors.config;

import org.dbsp.sqlCompiler.compiler.frontend.connectors.ConfigReporter;
import org.dbsp.sqlCompiler.compiler.frontend.connectors.IValidateConfig;

import com.fasterxml.jackson.annotation.JsonProperty;

/** Configuration for the Redis output connector. */
@SuppressWarnings("unused")
public class RedisOutputConfig implements IValidateConfig {
    @JsonProperty("connection_string")
    public String connectionString = "";

    @JsonProperty("key_separator")
    public String keySeparator = ":";

    @Override
    public boolean validate(ConfigReporter reporter) {
        return true;
    }
}
