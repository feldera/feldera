package org.dbsp.sqlCompiler.compiler.frontend.connectors;

import org.dbsp.sqlCompiler.compiler.IErrorReporter;
import org.dbsp.sqlCompiler.compiler.errors.SourcePosition;
import org.dbsp.sqlCompiler.compiler.errors.SourcePositionRange;
import org.dbsp.util.Utilities;

/**
 * A reporter that can resolve field names to source positions within a JSON config object.
 */
public final class ConfigReporter {
    private final IErrorReporter reporter;
    private final String outerJson;
    private final String configJPath;
    /** Start source position of the JSON document that is being validated. */
    private final SourcePosition outerStart;

    /**
     * Creates a reporter that resolves field names to source positions within a JSON
     * config object.
     *
     * @param reporter      Underlying error reporter used to emit warnings.
     * @param outerJson     The raw connectors-array JSON string (the value of the SQL
     *                      {@code 'connectors'} property).  {@code configPointer} is a
     *                      JSON Pointer path into this array that identifies the config block.
     * @param configPointer JSON Pointer path to the config block within {@code outerJson}
     *                      (e.g. {@code "/0/transport/config"}).  Field-level warnings
     *                      append a {@code /fieldName} suffix to this pointer to locate
     *                      the exact key in the source.
     * @param outerStart    Absolute source position in the SQL program
     *                      of {@code outerJson}'s first character,
     *                      used to convert within-JSON byte offsets to source positions.
     */
    public ConfigReporter(IErrorReporter reporter, String outerJson,
                          String configPointer, SourcePosition outerStart) {
        this.reporter = reporter;
        this.outerJson = outerJson;
        this.configJPath = configPointer;
        this.outerStart = outerStart;
    }

    /** Turns a JSON Pointer suffix such as {@code "a/b"} into the field name {@code "a.b"}. */
    public static String fieldName(String relativePath) {
        return relativePath.replace('/', '.');
    }

    /**
     * Names the faulty part of a value for an error message: just the part when it is
     * the whole value, otherwise the part and the value it came from, as in
     * {@code 'Gi512Mi' in size '1Gi512Mi'}.
     *
     * @param part The faulty part.
     * @param text The whole value as written.
     * @param kind What the value is, such as {@code "size"}.
     */
    public static String partOf(String part, String text, String kind) {
        String quoted = Utilities.singleQuote(part);
        return part.equals(text) ? quoted : quoted + " in " + kind + " " + Utilities.singleQuote(text);
    }

    /**
     * Report a warning at the key token identified by
     * {@code relativePath} within the config object.  {@code relativePath} is a
     * slash-separated relative JSON Pointer suffix (e.g. {@code "delimiter"} or
     * {@code "quoting/delimiter"}); it is appended to the config pointer.
     */
    public void warnPath(String relativePath, String category, String message) {
        String pointer = this.configJPath + "/" + relativePath;
        SourcePosition pos = Utilities.jsonPointerLocation(this.outerJson, pointer, false);
        SourcePositionRange range = pos != null
                ? pos.relativeTo(this.outerStart).asRange()
                : SourcePositionRange.INVALID;
        this.reporter.reportWarning(range, category, message);
    }

    /** Report a warning that does not correspond to a single field (points at the config block). */
    public void warn(String category, String message) {
        SourcePosition pos = Utilities.jsonPointerLocation(this.outerJson, this.configJPath, true);
        SourcePositionRange range = pos != null
                ? pos.relativeTo(this.outerStart).asRange()
                : SourcePositionRange.INVALID;
        this.reporter.reportWarning(range, category, message);
    }
}
