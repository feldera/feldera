package org.dbsp.sqlCompiler.compiler.frontend.connectors.config;

import org.dbsp.sqlCompiler.compiler.frontend.connectors.ConfigReporter;
import org.dbsp.sqlCompiler.compiler.frontend.connectors.IValidateConfig;

import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;
import java.util.List;

/** Configuration for the JSON output format. */
@SuppressWarnings("unused")
public class JsonEncoderConfig implements IValidateConfig {
    @JsonProperty("update_format")
    public JsonUpdateFormat updateFormat = JsonUpdateFormat.InsertDelete;

    @Nullable
    @JsonProperty("json_flavor")
    public JsonFlavor jsonFlavor = null;

    @JsonProperty("buffer_size_records")
    public long bufferSizeRecords = 10_000;

    @JsonProperty("array")
    public boolean array = false;

    /** When set, only these columns appear in the Debezium message key.
     * Valid only with {@code update_format = "debezium"}. */
    @Nullable
    @JsonProperty("key_fields")
    public List<String> keyFields = null;

    @Override
    public boolean validate(ConfigReporter reporter) {
        if (keyFields != null && updateFormat != JsonUpdateFormat.Debezium) {
            reporter.warnPath("key_fields", "Invalid configuration",
                    "\"key_fields\" is only valid with \"update_format\": \"debezium\"");
            return false;
        }
        return true;
    }
}
