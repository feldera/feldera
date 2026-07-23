package org.dbsp.sqlCompiler.compiler.frontend.connectors.config;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.dbsp.sqlCompiler.compiler.frontend.connectors.ConfigReporter;
import org.dbsp.sqlCompiler.compiler.frontend.connectors.IValidateConfig;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;

/** Configuration for reading snapshots from Snowflake. */
@SuppressWarnings("unused")
public class SnowflakeReaderConfig implements IValidateConfig {
    @JsonProperty("account")
    public String account = "";

    @JsonProperty("user")
    public String user = "";

    @JsonProperty("authenticator")
    public SnowflakeAuthenticator authenticator = SnowflakeAuthenticator.SnowflakeJwt;

    @Nullable
    @JsonProperty("role")
    public String role = null;

    @Nullable
    @JsonProperty("warehouse")
    public String warehouse = null;

    @Nullable
    @JsonProperty("database")
    public String database = null;

    @Nullable
    @JsonProperty("schema")
    public String schema = null;

    @JsonProperty("private_key_file")
    public String privateKeyFile = "";

    @Nullable
    @JsonAlias({"private_key_passphrase", "private_key_file_password"})
    @JsonProperty("private_key_file_pwd")
    public String privateKeyFilePwd = null;

    @JsonProperty("table")
    public String table = "";

    @JsonProperty("column_mapping")
    public Map<String, String> columnMapping = new HashMap<>();

    @JsonProperty("number_mode")
    public SnowflakeNumberMode numberMode = SnowflakeNumberMode.Decimal;

    @JsonProperty("mode")
    public SnowflakeIngestMode mode = SnowflakeIngestMode.Snapshot;

    @JsonProperty("transaction_mode")
    public SnowflakeTransactionMode transactionMode = SnowflakeTransactionMode.None;

    @Nullable
    @JsonProperty("snapshot_filter")
    public String snapshotFilter = null;

    @JsonProperty("num_parsers")
    public int numParsers = 4;

    @Nullable
    @JsonProperty("max_concurrent_readers")
    public Long maxConcurrentReaders = null;

    @Override
    public boolean validate(ConfigReporter reporter) {
        boolean ok = true;
        ok &= this.checkNonEmpty(reporter, this.account, "account");
        ok &= this.checkNonEmpty(reporter, this.user, "user");
        ok &= this.checkNonEmpty(reporter, this.privateKeyFile, "private_key_file");
        ok &= this.checkNonEmpty(reporter, this.table, "table");

        if (this.columnMapping == null) {
            reporter.warnPath("column_mapping", "Invalid configuration",
                    "\"column_mapping\" must be an object");
            ok = false;
        } else {
            for (Map.Entry<String, String> entry : this.columnMapping.entrySet()) {
                if (entry.getKey().isBlank()) {
                    reporter.warnPath("column_mapping", "Invalid configuration",
                            "\"column_mapping\" contains an empty Feldera column name");
                    ok = false;
                }
                if (entry.getValue() == null || entry.getValue().isBlank()) {
                    reporter.warnPath("column_mapping", "Invalid configuration",
                            "\"column_mapping\" contains an empty Snowflake column name");
                    ok = false;
                }
            }
        }

        ok &= checkOptionalNonEmpty(reporter, this.role, "role");
        ok &= checkOptionalNonEmpty(reporter, this.warehouse, "warehouse");
        ok &= checkOptionalNonEmpty(reporter, this.database, "database");
        ok &= checkOptionalNonEmpty(reporter, this.schema, "schema");
        ok &= checkOptionalNonEmpty(reporter, this.privateKeyFilePwd, "private_key_file_pwd");
        ok &= checkOptionalNonEmpty(reporter, this.snapshotFilter, "snapshot_filter");

        if (this.maxConcurrentReaders != null && this.maxConcurrentReaders <= 0) {
            reporter.warnPath("max_concurrent_readers", "Invalid configuration",
                    "\"max_concurrent_readers\" must be greater than 0");
            ok = false;
        }

        if (this.numParsers <= 0) {
            reporter.warnPath("num_parsers", "Invalid configuration",
                    "\"num_parsers\" must be greater than 0");
            ok = false;
        }

        return ok;
    }

    private boolean checkOptionalNonEmpty(ConfigReporter reporter, @Nullable String value, String name) {
        if (value != null && value.isBlank()) {
            reporter.warnPath(name, "Invalid configuration",
                    "field \"" + name + "\" must not be empty when specified");
            return false;
        }
        return true;
    }
}
