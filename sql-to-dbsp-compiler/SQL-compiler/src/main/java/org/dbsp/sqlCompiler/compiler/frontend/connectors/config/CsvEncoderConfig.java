package org.dbsp.sqlCompiler.compiler.frontend.connectors.config;

import org.dbsp.sqlCompiler.compiler.frontend.connectors.ConfigReporter;
import org.dbsp.sqlCompiler.compiler.frontend.connectors.IValidateConfig;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.dbsp.util.Utilities;

/** Configuration for the CSV output format of a connector */
@SuppressWarnings("unused")
public class CsvEncoderConfig implements IValidateConfig {
    /** Field delimiter.  Must be an ASCII character. */
    @JsonProperty("delimiter")
    public char delimiter = ',';

    /** Number of records to buffer before flushing output. */
    @JsonProperty("buffer_size_records")
    public long bufferSizeRecords = 10_000;

    @Override
    public boolean validate(ConfigReporter reporter) {
        if (delimiter > 127) {
            reporter.warnPath("delimiter", "Invalid configuration",
                    "field \"delimiter\" must be an ASCII character; got " +
                    Utilities.singleQuote(String.valueOf(delimiter)));
            return false;
        }
        return true;
    }
}
