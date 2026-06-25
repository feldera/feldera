package org.dbsp.sqlCompiler.compiler.frontend.connectors.config;

import org.dbsp.sqlCompiler.compiler.frontend.connectors.ConfigReporter;
import org.dbsp.sqlCompiler.compiler.frontend.connectors.IValidateConfig;

import com.fasterxml.jackson.annotation.JsonProperty;

/** Configuration for the Parquet output format. */
@SuppressWarnings("unused")
public class ParquetEncoderConfig implements IValidateConfig {
    /** Number of records to buffer before writing a new Parquet file. */
    @JsonProperty("buffer_size_records")
    public long bufferSizeRecords = 100_000;

    @Override
    public boolean validate(ConfigReporter reporter) {
        return true;
    }
}
