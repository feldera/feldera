package org.dbsp.sqlCompiler.compiler.frontend.connectors.config;

import org.dbsp.sqlCompiler.compiler.frontend.connectors.ConfigReporter;
import org.dbsp.sqlCompiler.compiler.frontend.connectors.IValidateConfig;

import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

/** Configuration for the raw byte input format. */
@SuppressWarnings("unused")
public class RawParserConfig implements IValidateConfig {
    /** Whether to ingest each transport chunk as a single row ({@code blob})
     * or split on newlines ({@code lines}). */
    @JsonProperty("mode")
    public RawParserMode mode = RawParserMode.Blob;

    /** Table column that will store the raw value.
     * Required when the table has more than one column; that constraint
     * cannot be checked here without schema context. */
    @Nullable
    @JsonProperty("column_name")
    public String columnName = null;

    @Override
    public boolean validate(ConfigReporter reporter) {
        return true;
    }
}
