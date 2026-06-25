package org.dbsp.sqlCompiler.compiler.frontend.connectors.config;

import org.dbsp.sqlCompiler.compiler.frontend.connectors.ConfigReporter;
import org.dbsp.sqlCompiler.compiler.frontend.connectors.IValidateConfig;

import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;

import javax.annotation.Nullable;
import java.util.List;

/** Configuration for writing data to a Kafka topic. */
@SuppressWarnings("unused")
public class KafkaOutputConfig implements IValidateConfig {
    @JsonProperty("topic")
    public String topic = "";

    @Nullable
    @JsonProperty("headers")
    public List<JsonNode> headers = null;

    @Nullable
    @JsonProperty("log_level")
    public KafkaLogLevel logLevel = null;

    @JsonProperty("initialization_timeout_secs")
    public int initializationTimeoutSecs = 60;

    @Nullable
    @JsonProperty("fault_tolerance")
    public JsonNode faultTolerance = null;

    @Nullable
    @JsonProperty("kafka_service")
    public String kafkaService = null;

    @Nullable
    @JsonProperty("region")
    public String region = null;

    /** Absorbs flattened {@code kafka_options} keys so they are not rejected as unknown. */
    @JsonAnySetter
    public void setKafkaOption(String key, Object value) {}

    @Override
    public boolean validate(ConfigReporter reporter) {
        boolean ok = true;
        if (topic.isBlank()) {
            reporter.warn("Invalid configuration",
                    "required field \"topic\" is missing or empty");
            ok = false;
        }
        return ok;
    }
}
