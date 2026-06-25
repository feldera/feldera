package org.dbsp.sqlCompiler.compiler.frontend.connectors.config;

import org.dbsp.sqlCompiler.compiler.frontend.connectors.ConfigReporter;
import org.dbsp.sqlCompiler.compiler.frontend.connectors.IValidateConfig;

import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;

import javax.annotation.Nullable;
import java.util.List;

/** Configuration for reading data from a Kafka topic. */
@SuppressWarnings("unused")
public class KafkaInputConfig implements IValidateConfig {
    @JsonProperty("topic")
    public String topic = "";

    @Nullable
    @JsonProperty("log_level")
    public KafkaLogLevel logLevel = null;

    @JsonProperty("group_join_timeout_secs")
    public int groupJoinTimeoutSecs = 10;

    @Nullable
    @JsonProperty("poller_threads")
    public Integer pollerThreads = null;

    /** Complex enum with data variants; validated structurally by the runtime. */
    @Nullable
    @JsonProperty("start_from")
    public JsonNode startFrom = null;

    @Nullable
    @JsonProperty("region")
    public String region = null;

    @Nullable
    @JsonProperty("partitions")
    public List<Integer> partitions = null;

    @JsonProperty("resume_earliest_if_data_expires")
    public boolean resumeEarliestIfDataExpires = false;

    @Nullable
    @JsonProperty("include_headers")
    public Boolean includeHeaders = null;

    @Nullable
    @JsonProperty("include_timestamp")
    public Boolean includeTimestamp = null;

    @Nullable
    @JsonProperty("include_partition")
    public Boolean includePartition = null;

    @Nullable
    @JsonProperty("include_offset")
    public Boolean includeOffset = null;

    @Nullable
    @JsonProperty("include_topic")
    public Boolean includeTopic = null;

    @JsonProperty("synchronize_partitions")
    public boolean synchronizePartitions = false;

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
