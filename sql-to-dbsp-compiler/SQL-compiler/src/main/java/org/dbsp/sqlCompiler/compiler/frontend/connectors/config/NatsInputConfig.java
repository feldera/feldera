package org.dbsp.sqlCompiler.compiler.frontend.connectors.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.dbsp.sqlCompiler.compiler.frontend.connectors.ConfigReporter;
import org.dbsp.sqlCompiler.compiler.frontend.connectors.IValidateConfig;

import javax.annotation.Nullable;

/** Configuration for the NATS JetStream input connector. */
@SuppressWarnings("unused")
public class NatsInputConfig implements IValidateConfig {
    @Nullable
    @JsonProperty("connection_config")
    public NatsConnectOptionsConfig connectionConfig = null;

    @JsonProperty("stream_name")
    public String streamName = "";

    @JsonProperty("inactivity_timeout_secs")
    public long inactivityTimeoutSecs = 60;

    @JsonProperty("retry_interval_secs")
    public long retryIntervalSecs = 5;

    @Nullable
    @JsonProperty("consumer_config")
    public NatsConsumerConfig consumerConfig = null;

    @Override
    public boolean validate(ConfigReporter reporter) {
        boolean ok = true;
        if (streamName.isBlank()) {
            reporter.warnPath("stream_name", "Invalid configuration",
                    "required field \"stream_name\" is missing or empty");
            ok = false;
        }
        if (inactivityTimeoutSecs < 1) {
            reporter.warnPath("inactivity_timeout_secs", "Invalid configuration",
                    "\"inactivity_timeout_secs\" must be at least 1");
            ok = false;
        }
        if (retryIntervalSecs < 1) {
            reporter.warnPath("retry_interval_secs", "Invalid configuration",
                    "\"retry_interval_secs\" must be at least 1");
            ok = false;
        }
        if (connectionConfig == null) {
            reporter.warn("Invalid configuration",
                    "required field \"connection_config\" is missing");
            ok = false;
        } else if (connectionConfig.serverUrl.isBlank()) {
            reporter.warnPath("connection_config/server_url", "Invalid configuration",
                    "required field \"connection_config.server_url\" is missing or empty");
            ok = false;
        }
        if (consumerConfig == null) {
            reporter.warn("Invalid configuration",
                    "required field \"consumer_config\" is missing");
            ok = false;
        } else if (consumerConfig.deliverPolicy == null || consumerConfig.deliverPolicy.isNull()) {
            reporter.warnPath("consumer_config/deliver_policy", "Invalid configuration",
                    "required field \"consumer_config.deliver_policy\" is missing");
            ok = false;
        }
        return ok;
    }
}
