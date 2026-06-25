package org.dbsp.sqlCompiler.compiler.frontend.connectors.config;

import org.dbsp.sqlCompiler.compiler.frontend.connectors.ConfigReporter;
import org.dbsp.sqlCompiler.compiler.frontend.connectors.IValidateConfig;

import com.fasterxml.jackson.annotation.JsonProperty;

/** Configuration for the JSON input format. */
@SuppressWarnings("unused")
public class JsonParserConfig implements IValidateConfig {
    @JsonProperty("update_format")
    public JsonUpdateFormat updateFormat = JsonUpdateFormat.InsertDelete;

    @JsonProperty("json_flavor")
    public JsonFlavor jsonFlavor = JsonFlavor.Default;

    /** When {@code true}, each input chunk is a JSON array of update objects. */
    @JsonProperty("array")
    public boolean array = false;

    /** Whether JSON values may span multiple lines. */
    @JsonProperty("lines")
    public JsonLines lines = JsonLines.Multiple;

    @Override
    public boolean validate(ConfigReporter reporter) {
        return true;
    }
}
