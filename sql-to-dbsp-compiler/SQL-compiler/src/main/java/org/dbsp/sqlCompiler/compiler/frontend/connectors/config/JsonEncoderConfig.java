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

    /** When true, a Debezium delete is followed by a tombstone.
     * Valid only with {@code update_format = "debezium"} and an index. */
    @Nullable
    @JsonProperty("tombstones_on_delete")
    public Boolean tombstonesOnDelete = null;

    @Override
    public boolean validate(ConfigReporter reporter) {
        if (keyFields != null && updateFormat != JsonUpdateFormat.Debezium) {
            reporter.warnPath("key_fields", "Invalid configuration",
                    "\"key_fields\" is only valid with \"update_format\": \"debezium\"");
            return false;
        }
        if (tombstonesOnDelete != null && updateFormat != JsonUpdateFormat.Debezium) {
            reporter.warnPath("tombstones_on_delete", "Invalid configuration",
                    "\"tombstones_on_delete\" is only valid with \"update_format\": \"debezium\"");
            return false;
        }
        return true;
    }
}
