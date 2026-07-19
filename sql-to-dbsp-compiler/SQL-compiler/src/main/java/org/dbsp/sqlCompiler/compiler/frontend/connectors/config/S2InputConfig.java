package org.dbsp.sqlCompiler.compiler.frontend.connectors.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.compiler.frontend.connectors.ConfigReporter;
import org.dbsp.sqlCompiler.compiler.frontend.connectors.IValidateConfig;

import javax.annotation.Nullable;

/** Configuration for the S2 input connector. */
@SuppressWarnings("unused")
public class S2InputConfig implements IValidateConfig {
    @JsonProperty("basin")
    public String basin = "";

    @JsonProperty("stream")
    public String stream = "";

    @JsonProperty("auth_token")
    public String authToken = "";

    @Nullable
    @JsonProperty("endpoint")
    public String endpoint = null;

    @Nullable
    @JsonProperty("start_from")
    public JsonNode startFrom = null;

    @Override
    public boolean validate(ConfigReporter reporter) {
        boolean ok = true;
        if (basin.isBlank()) {
            reporter.warnPath("basin", "Invalid configuration",
                    "required field \"basin\" is missing or empty");
            ok = false;
        }
        if (stream.isBlank()) {
            reporter.warnPath("stream", "Invalid configuration",
                    "required field \"stream\" is missing or empty");
            ok = false;
        }
        if (authToken.isBlank()) {
            reporter.warnPath("auth_token", "Invalid configuration",
                    "required field \"auth_token\" is missing or empty");
            ok = false;
        }
        if (startFrom != null && !isValidStartFrom(startFrom)) {
            reporter.warnPath("start_from", "Invalid configuration",
                    "\"start_from\" must be \"Beginning\", \"Tail\", or an object containing exactly one of \"SeqNum\", \"Timestamp\", or \"TailOffset\" with a non-negative integer value");
            ok = false;
        }
        return ok;
    }

    private static boolean isValidStartFrom(JsonNode value) {
        if (value.isTextual()) {
            String text = value.asText();
            return text.equals("Beginning") || text.equals("Tail");
        }
        if (!value.isObject() || value.size() != 1)
            return false;
        String field = value.fieldNames().next();
        if (!field.equals("SeqNum") && !field.equals("Timestamp") && !field.equals("TailOffset"))
            return false;
        JsonNode position = value.get(field);
        return position.isIntegralNumber() && position.canConvertToLong() && position.asLong() >= 0;
    }
}
