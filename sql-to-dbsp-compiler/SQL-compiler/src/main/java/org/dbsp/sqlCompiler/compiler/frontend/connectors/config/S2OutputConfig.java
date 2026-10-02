package org.dbsp.sqlCompiler.compiler.frontend.connectors.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.dbsp.sqlCompiler.compiler.frontend.connectors.ConfigReporter;
import org.dbsp.sqlCompiler.compiler.frontend.connectors.IValidateConfig;

import javax.annotation.Nullable;

/** Configuration for the S2 output connector. */
@SuppressWarnings("unused")
public class S2OutputConfig implements IValidateConfig {
    @JsonProperty("basin")
    public String basin = "";

    @JsonProperty("stream")
    public String stream = "";

    @JsonProperty("auth_token")
    public String authToken = "";

    @Nullable
    @JsonProperty("endpoint")
    public String endpoint = null;

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
        return ok;
    }
}
