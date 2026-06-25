package org.dbsp.sqlCompiler.compiler.frontend.connectors.config;

import org.dbsp.sqlCompiler.compiler.frontend.connectors.ConfigReporter;
import org.dbsp.sqlCompiler.compiler.frontend.connectors.IValidateConfig;

import com.fasterxml.jackson.annotation.JsonProperty;

/** Configuration for writing data to a file. */
@SuppressWarnings("unused")
public class FileOutputConfig implements IValidateConfig {
    @JsonProperty("path")
    public String path = "";

    @Override
    public boolean validate(ConfigReporter reporter) {
        return true;
    }
}
