package org.dbsp.sqlCompiler.compiler.frontend.connectors.config;

import org.dbsp.sqlCompiler.compiler.frontend.connectors.ConfigReporter;
import org.dbsp.sqlCompiler.compiler.frontend.connectors.IValidateConfig;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;

import javax.annotation.Nullable;

/** Configuration for reading data from a file. */
@SuppressWarnings("unused")
public class FileInputConfig implements IValidateConfig {
    @JsonProperty("path")
    public String path = "";

    /** Read buffer size, for example {@code "64Ki"}. */
    @Nullable
    @JsonProperty("buffer_size")
    public JsonNode bufferSize = null;

    /** Deprecated; use {@link #bufferSize}. */
    @Nullable
    @JsonProperty("buffer_size_bytes")
    public JsonNode bufferSizeBytes = null;

    @JsonProperty("follow")
    public boolean follow = false;

    @Override
    public boolean validate(ConfigReporter reporter) {
        return ConfigSize.check(reporter,
                "buffer_size", this.bufferSize,
                "buffer_size_bytes", this.bufferSizeBytes);
    }
}
