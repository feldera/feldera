package org.dbsp.sqlCompiler.compiler.frontend.connectors.config;

import org.dbsp.sqlCompiler.compiler.frontend.connectors.ConfigReporter;
import org.dbsp.sqlCompiler.compiler.frontend.connectors.IValidateConfig;

import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;
import java.util.Set;

/** Configuration for the Delta Table output transport. */
@SuppressWarnings("unused")
public class DeltaTableWriterConfig implements IValidateConfig {
    private static final Set<String> VALID_INTERVAL_UNITS = Set.of(
            "nanosecond", "nanoseconds", "microsecond", "microseconds",
            "millisecond", "milliseconds", "second", "seconds",
            "minute", "minutes", "hour", "hours", "day", "days", "week", "weeks");

    @JsonProperty("uri")
    public String uri = "";

    @JsonProperty("mode")
    public DeltaTableWriteMode mode = DeltaTableWriteMode.Append;

    @Nullable
    @JsonProperty("checkpoint_interval")
    public Long checkpointInterval = null;

    /** Must follow Delta Lake interval syntax: {@code "interval <N> <unit>"}. */
    @Nullable
    @JsonProperty("log_retention_duration")
    public String logRetentionDuration = null;

    @Nullable
    @JsonProperty("enable_expired_log_cleanup")
    public Boolean enableExpiredLogCleanup = null;

    @Nullable
    @JsonProperty("max_retries")
    public Long maxRetries = null;

    /** Must be {@code > 0} when set. */
    @Nullable
    @JsonProperty("threads")
    public Long threads = null;

    /** Absorbs flattened {@code object_store_config} keys so they are not rejected as unknown. */
    @JsonAnySetter
    public void setObjectStoreOption(String key, Object value) {}

    @Override
    public boolean validate(ConfigReporter reporter) {
        boolean ok = true;
        if (threads != null && threads == 0) {
            reporter.warnPath("threads", "Invalid configuration",
                    "\"threads\" must be greater than 0");
            ok = false;
        }
        if (logRetentionDuration != null) {
            String error = validateDeltaInterval(logRetentionDuration);
            if (error != null) {
                reporter.warnPath("log_retention_duration", "Invalid configuration",
                        "invalid \"log_retention_duration\" value \""
                        + logRetentionDuration + "\": " + error);
                ok = false;
            }
        }
        return ok;
    }

    /** Validates a Delta Lake interval string (e.g. {@code "interval 30 days"}).
     * @return an error message, or {@code null} if the value is valid. */
    @Nullable
    static String validateDeltaInterval(String value) {
        String[] tokens = value.strip().split("\\s+");
        if (tokens.length != 3)
            return "expected format \"interval <N> <unit>\" (e.g. \"interval 30 days\")";
        if (!tokens[0].equals("interval"))
            return "expected the value to start with lowercase \"interval\"";
        long number;
        try {
            number = Long.parseLong(tokens[1]);
        } catch (NumberFormatException e) {
            return "cannot parse '" + tokens[1] + "' as integer: " + e.getMessage();
        }
        if (number < 0)
            return "interval cannot be negative";
        if (!VALID_INTERVAL_UNITS.contains(tokens[2]))
            return "unknown unit '" + tokens[2]
                    + "'; expected one of nanosecond[s], microsecond[s], millisecond[s],"
                    + " second[s], minute[s], hour[s], day[s], week[s]";
        return null;
    }
}
