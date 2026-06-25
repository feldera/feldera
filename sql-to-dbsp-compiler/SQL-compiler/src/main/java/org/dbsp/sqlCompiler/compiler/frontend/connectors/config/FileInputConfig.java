package org.dbsp.sqlCompiler.compiler.frontend.connectors.config;

import org.dbsp.sqlCompiler.compiler.frontend.connectors.ConfigReporter;
import org.dbsp.sqlCompiler.compiler.frontend.connectors.IValidateConfig;

import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

/** Configuration for reading data from a file. */
@SuppressWarnings("unused")
public class FileInputConfig implements IValidateConfig {
    @JsonProperty("path")
    public String path = "";

    @Nullable
    @JsonProperty("buffer_size_bytes")
    public Long bufferSizeBytes = null;

    @JsonProperty("follow")
    public boolean follow = false;

    @Override
    public boolean validate(ConfigReporter reporter) {
        return true;
    }
}
