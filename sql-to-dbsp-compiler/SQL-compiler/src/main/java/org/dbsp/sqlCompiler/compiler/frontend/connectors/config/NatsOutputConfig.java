package org.dbsp.sqlCompiler.compiler.frontend.connectors.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.compiler.frontend.connectors.ConfigReporter;
import org.dbsp.sqlCompiler.compiler.frontend.connectors.IValidateConfig;

import javax.annotation.Nullable;
import java.util.List;

/** Configuration for the NATS JetStream output connector. */
@SuppressWarnings("unused")
public class NatsOutputConfig implements IValidateConfig {
    @Nullable
    @JsonProperty("connection_config")
    public NatsConnectOptionsConfig connectionConfig = null;

    @JsonProperty("subject")
    public String subject = "";

    @Nullable
    @JsonProperty("headers")
    public List<NatsHeaderConfig> headers = null;

    @JsonProperty("message_id")
    public NatsMessageId messageId = NatsMessageId.content_hash;

    /**
     * How long to wait for the server's publish acknowledgment before
     * retrying the publish, for example {@code "5s"}.  Must be at least one second.
     */
    @Nullable
    @JsonProperty("publish_timeout")
    public JsonNode publishTimeout = null;

    /** Deprecated; use {@link #publishTimeout}. */
    @JsonProperty("publish_timeout_secs")
    @Nullable
    public JsonNode publishTimeoutSecs = null;

    /** Override for the maximum message size in bytes; must be at least 1 when set. */
    @Nullable
    @JsonProperty("max_message_size")
    public Long maxMessageSize = null;

    @Override
    public boolean validate(ConfigReporter reporter) {
        boolean ok = true;
        if (subject.isBlank()) {
            reporter.warnPath("subject", "Invalid configuration",
                    "required field \"subject\" is missing or empty");
            ok = false;
        }
        ConfigDuration.Setting publishTimeout = ConfigDuration.resolve(reporter,
                "publish_timeout", this.publishTimeout,
                "publish_timeout_secs", this.publishTimeoutSecs,
                ConfigDuration.NANOS_PER_SECOND);
        ok &= ConfigDuration.checkMinimum(reporter, publishTimeout,
                ConfigDuration.NANOS_PER_SECOND, "at least 1 second, for example \"1s\"");
        if (maxMessageSize != null && maxMessageSize < 1) {
            reporter.warnPath("max_message_size", "Invalid configuration",
                    "\"max_message_size\" must be at least 1");
            ok = false;
        }
        if (headers != null) {
            for (int i = 0; i < headers.size(); i++) {
                if (headers.get(i).key.isBlank()) {
                    reporter.warnPath("headers/" + i + "/key", "Invalid configuration",
                            "header key must not be empty");
                    ok = false;
                }
            }
        }
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
        return ok;
    }
}
