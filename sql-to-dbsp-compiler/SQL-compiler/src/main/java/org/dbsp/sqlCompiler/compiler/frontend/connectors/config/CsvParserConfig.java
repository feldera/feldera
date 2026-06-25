package org.dbsp.sqlCompiler.compiler.frontend.connectors.config;

import org.dbsp.sqlCompiler.compiler.frontend.connectors.ConfigReporter;
import org.dbsp.sqlCompiler.compiler.frontend.connectors.IValidateConfig;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;

/** Configuration for the CSV input connector format. */
@SuppressWarnings("unused")
public class CsvParserConfig implements IValidateConfig {
    /** Field delimiter. Must be an ASCII character. */
    @JsonProperty("delimiter")
    public char delimiter = ',';

    /** Whether the input begins with a header line, which is skipped. */
    @JsonProperty("headers")
    public boolean headers = false;

    /** Quote character. Must be an ASCII character. */
    @JsonProperty("quote")
    public char quote = '"';

    /** Escape character for quoted fields.
     * When {@code null} (the default), doubled quotes are used instead. */
    @Nullable
    @JsonProperty("escape")
    public Character escape = null;

    /** Enable double-quote escaping inside quoted fields. */
    @JsonProperty("double_quote")
    public boolean doubleQuote = true;

    /** Enable quoting. When {@code false}, every newline terminates a record. */
    @JsonProperty("quoting")
    public boolean quoting = true;

    /** Lines starting with this character are treated as comments and skipped.
     * {@code null} disables comment handling. */
    @Nullable
    @JsonProperty("comment")
    public Character comment = null;

    /** Allow records with a variable number of fields. */
    @JsonProperty("flexible")
    public boolean flexible = true;

    /** Whitespace trimming policy. */
    @JsonProperty("trim")
    public CsvTrim trim = CsvTrim.None;

    /** Additional constraints: all single-character fields must be ASCII
     * (the underlying CSV library uses single-byte characters), and {@code delimiter} and
     * {@code quote} must be distinct. */
    @Override
    public boolean validate(ConfigReporter reporter) {
        boolean ok = true;
        for (var entry : new Object[][]{
                {"delimiter", delimiter},
                {"quote",     quote},
                {"escape",    escape},
                {"comment",   comment}}) {
            String name = (String) entry[0];
            Character c = (Character) entry[1];
            if (c != null && c > 127) {
                reporter.warnPath(name, "Invalid configuration",
                        "field " + Utilities.doubleQuote(name, false) +
                        " must be an ASCII character; got " + Utilities.singleQuote(c.toString()));
                ok = false;
            }
        }
        if (ok && delimiter == quote) {
            reporter.warnPath("delimiter", "Invalid configuration",
                    "fields \"delimiter\" and \"quote\" must be different characters");
            ok = false;
        }
        return ok;
    }
}
