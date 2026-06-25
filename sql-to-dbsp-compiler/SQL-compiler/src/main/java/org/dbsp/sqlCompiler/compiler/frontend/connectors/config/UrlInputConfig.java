package org.dbsp.sqlCompiler.compiler.frontend.connectors.config;

import org.dbsp.sqlCompiler.compiler.frontend.connectors.ConfigReporter;
import org.dbsp.sqlCompiler.compiler.frontend.connectors.IValidateConfig;

import com.fasterxml.jackson.annotation.JsonProperty;

/** Configuration for reading data from an HTTP or HTTPS URL. */
@SuppressWarnings("unused")
public class UrlInputConfig implements IValidateConfig {
    @JsonProperty("path")
    public String path = "";

    @JsonProperty("pause_timeout")
    public int pauseTimeout = 60;

    @Override
    public boolean validate(ConfigReporter reporter) {
        return true;
    }
}
