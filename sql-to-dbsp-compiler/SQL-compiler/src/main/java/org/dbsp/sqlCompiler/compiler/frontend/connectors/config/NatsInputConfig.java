package org.dbsp.sqlCompiler.compiler.frontend.connectors.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
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

    /**
     * Maximum time to wait for the next message before running a stream/server health
     * check, for example {@code "60s"}.  Must be at least one second.
     */
    @Nullable
    @JsonProperty("inactivity_timeout")
    public JsonNode inactivityTimeout = null;

    /** Deprecated; use {@link #inactivityTimeout}. */
    @JsonProperty("inactivity_timeout_secs")
    @Nullable
    public JsonNode inactivityTimeoutSecs = null;

    /**
     * Delay between automatic reconnect attempts while in retry mode, for example
     * {@code "5s"}.  Must be at least one second.
     */
    @Nullable
    @JsonProperty("retry_interval")
    public JsonNode retryInterval = null;

    /** Deprecated; use {@link #retryInterval}. */
    @JsonProperty("retry_interval_secs")
    @Nullable
    public JsonNode retryIntervalSecs = null;

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
        ConfigDuration.Setting inactivityTimeout = ConfigDuration.resolve(reporter,
                "inactivity_timeout", this.inactivityTimeout,
                "inactivity_timeout_secs", this.inactivityTimeoutSecs,
                ConfigDuration.NANOS_PER_SECOND);
        ok &= ConfigDuration.checkMinimum(reporter, inactivityTimeout,
                ConfigDuration.NANOS_PER_SECOND, "at least 1 second, for example \"1s\"");
        ConfigDuration.Setting retryInterval = ConfigDuration.resolve(reporter,
                "retry_interval", this.retryInterval,
                "retry_interval_secs", this.retryIntervalSecs,
                ConfigDuration.NANOS_PER_SECOND);
        ok &= ConfigDuration.checkMinimum(reporter, retryInterval,
                ConfigDuration.NANOS_PER_SECOND, "at least 1 second, for example \"1s\"");
        if (connectionConfig == null) {
            reporter.warn("Invalid configuration",
                    "required field \"connection_config\" is missing");
            ok = false;
        } else {
            if (connectionConfig.serverUrl.isBlank()) {
                reporter.warnPath("connection_config/server_url", "Invalid configuration",
                        "required field \"connection_config.server_url\" is missing or empty");
                ok = false;
            }
            ok &= connectionConfig.validateDurations(reporter, "connection_config");
        }
        if (consumerConfig == null) {
            reporter.warn("Invalid configuration",
                    "required field \"consumer_config\" is missing");
            ok = false;
        } else {
            if (consumerConfig.deliverPolicy == null || consumerConfig.deliverPolicy.isNull()) {
                reporter.warnPath("consumer_config/deliver_policy", "Invalid configuration",
                        "required field \"consumer_config.deliver_policy\" is missing");
                ok = false;
            }
            ok &= consumerConfig.validateDurations(reporter, "consumer_config");
        }
        return ok;
    }
}
