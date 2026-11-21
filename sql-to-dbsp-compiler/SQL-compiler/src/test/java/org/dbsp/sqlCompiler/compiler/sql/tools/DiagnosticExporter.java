package org.dbsp.sqlCompiler.compiler.sql.tools;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.TestUtil;
import org.dbsp.sqlCompiler.compiler.errors.CompilerMessages;
import org.dbsp.util.Utilities;
import org.junit.runner.Result;
import org.junit.runner.Description;

import javax.annotation.Nullable;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Objects;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.TreeMap;

/** Captures evaluated error and warning assertions and their actual compiler diagnostics. */
public final class DiagnosticExporter {
    @Nullable private static DiagnosticExporter active;

    private final Path directory;
    private final BufferedWriter records;
    private final BufferedWriter directAssertions;
    private final ObjectNode manifest;
    private final ThreadLocal<Description> currentTest = new ThreadLocal<>();
    private long count;
    private long directCount;

    private DiagnosticExporter(Path directory) throws IOException {
        this.directory = directory;
        var mapper = Utilities.deterministicObjectMapper();
        this.manifest = mapper.createObjectNode();
        this.manifest.put("schema_version", 1);
        this.manifest.put("assertion_contract", "rendered-fields-v3");
        this.manifest.put("direct_assertion_contract", "typed-targets-v1");
        this.manifest.putArray("capture_helpers")
                .add("BaseSQLTests.statementsFailingInCompilation")
                .add("BaseSQLTests.shouldWarn")
                .add("TestUtil.assertMessagesContain");
        for (String identity: new String[] {"producer", "calcite", "runtime"}) {
            String value = System.getProperty("feldera.diagnostics." + identity, "");
            if (value.isBlank())
                throw new IllegalArgumentException("Missing diagnostic build identity: " + identity);
            this.manifest.put(identity + "_commit", value);
        }
        Files.createDirectory(directory);
        Properties resources = new Properties();
        try (var stream = Objects.requireNonNull(DBSPCompiler.class.getResourceAsStream(
                "/org/apache/calcite/runtime/CalciteResource.properties"));
             var reader = new InputStreamReader(stream, StandardCharsets.UTF_8)) {
            resources.load(reader);
        }
        var templates = new TreeMap<String, String>();
        resources.forEach((key, value) -> templates.put(key.toString(), value.toString()));
        Files.writeString(directory.resolve("calcite-messages.json"), mapper.writeValueAsString(templates),
                StandardOpenOption.CREATE_NEW);
        this.records = Files.newBufferedWriter(directory.resolve("java-diagnostics.jsonl"),
                StandardOpenOption.CREATE_NEW);
        this.directAssertions = Files.newBufferedWriter(directory.resolve("direct-assertions.jsonl"),
                StandardOpenOption.CREATE_NEW);
    }

    public static void start() throws IOException {
        String directory = System.getProperty("feldera.diagnostics.dir", "");
        if (!directory.isBlank())
            active = new DiagnosticExporter(Path.of(directory));
    }

    public static void testStarted(Description description) {
        if (active != null)
            active.currentTest.set(description);
    }

    public static void testFinished() {
        if (active != null)
            active.currentTest.remove();
    }

    enum AssertionKind { COMPILATION_ERROR, WARNINGS, MESSAGES }

    /** Record the exact values already checked by a direct assertion. These
     * records have no inferred compiler context or reconstructed diagnostic. */
    static void recordDirect(DiagnosticAssertions.Target target, String comparison,
                             Object expected, Object actual) {
        recordDirect(target, comparison, expected, actual, null);
    }

    static void recordDirect(DiagnosticAssertions.Target target, String comparison,
                             Object expected, Object actual, @Nullable String context) {
        if (active == null)
            return;
        Description test = active.currentTest.get();
        if (test == null || test.getClassName() == null || test.getMethodName() == null)
            throw new IllegalStateException("Direct diagnostic assertion has no active JUnit test");
        var mapper = Utilities.deterministicObjectMapper();
        var row = mapper.createObjectNode();
        row.put("schema_version", 1);
        row.put("source_test", test.getClassName() + "#" + test.getMethodName());
        var site = StackWalker.getInstance().walk(frames -> frames
                .filter(frame -> !frame.getClassName().equals(DiagnosticExporter.class.getName())
                        && !frame.getClassName().equals(DiagnosticAssertions.class.getName())
                        && !((target == DiagnosticAssertions.Target.STRUCTURED_WARNINGS
                              || target == DiagnosticAssertions.Target.STRUCTURED_ERRORS)
                             && frame.getClassName().equals(BaseSQLTests.class.getName())))
                .findFirst().orElseThrow());
        row.put("call_site", site.getClassName() + ":" + site.getLineNumber());
        row.put("target", target.name().toLowerCase(java.util.Locale.ROOT));
        row.put("comparison", comparison);
        if (context != null)
            row.put("context", context);
        row.set("expected", mapper.valueToTree(expected));
        row.set("actual", mapper.valueToTree(actual));
        try {
            active.directAssertions.write(mapper.writeValueAsString(row));
            active.directAssertions.newLine();
            active.directCount++;
        } catch (IOException error) {
            throw new UncheckedIOException(error);
        }
    }

    /** Record TestUtil's literal assertion without guessing its test identity
     * from helper stack frames. The JUnit listener owns that identity. */
    public static void recordMessages(CompilerMessages messages, String expected, String rendered) {
        if (active == null)
            return;
        Description test = active.currentTest.get();
        if (test == null || test.getClassName() == null || test.getMethodName() == null)
            throw new IllegalStateException("Diagnostic assertion has no active JUnit test");
        record(messages.compiler, expected, false, AssertionKind.MESSAGES, rendered,
                test.getClassName() + "#" + test.getMethodName());
    }

    static List<Integer> visibleDiagnostics(DBSPCompiler compiler, AssertionKind kind) {
        var visible = new ArrayList<Integer>();
        for (int index = 0; index < compiler.messages.messages.size(); index++) {
            boolean warning = compiler.messages.messages.get(index).warning;
            boolean shown = kind == AssertionKind.WARNINGS ? warning
                    : compiler.options.ioOptions.emitJsonErrors || !compiler.options.ioOptions.quiet || !warning;
            if (shown)
                visible.add(index);
        }
        return visible;
    }

    /** Candidate fields use the same Java matching semantics as the assertion.
     * A match across rendered fields remains unresolved, not a body dependency. */
    static ArrayNode fieldMatches(JsonNode diagnostics, String expected, boolean regex, List<Integer> visible) {
        if (!diagnostics.isArray())
            throw new IllegalArgumentException("Expected a diagnostic array");
        var result = Utilities.deterministicObjectMapper().createArrayNode();
        for (int index: visible) {
            var diagnostic = diagnostics.get(index);
            var fields = Utilities.deterministicObjectMapper().createArrayNode();
            for (String field: new String[] {"message", "error_type", "snippet"}) {
                if (BaseSQLTests.matches(diagnostic.path(field).textValue(), expected, regex))
                    fields.add(field);
            }
            if (!fields.isEmpty())
                result.addObject().put("diagnostic_index", index).set("fields", fields);
        }
        return result;
    }

    static void record(BaseSQLTests test, DBSPCompiler compiler, String expected, boolean regex,
                       AssertionKind kind, String rendered) {
        record(compiler, expected, regex, kind, rendered,
                test.getClass().getName() + "#" + test.currentTestName.getMethodName());
    }

    private static void record(DBSPCompiler compiler, String expected, boolean regex,
                               AssertionKind kind, String rendered, String sourceTest) {
        if (active == null)
            return;
        var mapper = Utilities.deterministicObjectMapper();
        ObjectNode row = mapper.createObjectNode();
        row.put("schema_version", 1);
        row.put("source_test", sourceTest);
        var callSite = StackWalker.getInstance().walk(frames -> frames
                .filter(frame -> !frame.getClassName().equals(DiagnosticExporter.class.getName())
                        && !frame.getClassName().equals(BaseSQLTests.class.getName())
                        && !frame.getClassName().equals(TestUtil.class.getName()))
                .findFirst().orElseThrow());
        row.put("call_site", callSite.getClassName() + ":" + callSite.getLineNumber());
        row.put("expected", expected);
        row.put("is_regex", regex);
        row.put("assertion_kind", kind.name().toLowerCase(java.util.Locale.ROOT));
        // Preserve precisely what shouldMatch checked: warnings use a filtered
        // list, whereas errors use the complete formatted diagnostic stream.
        row.put("rendered", rendered);
        // Negative compilations do not reach the successful-circuit exporter.
        // Keep their actual API submissions so parser positions can be measured
        // on the negative corpus instead of reconstructed from message snippets.
        row.set("submissions", mapper.valueToTree(compiler.getSourceSubmissions()));
        var diagnostics = compiler.messages.toJson(compiler.sources);
        var visible = visibleDiagnostics(compiler, kind);
        row.set("diagnostics", diagnostics);
        row.set("visible_diagnostics", mapper.valueToTree(visible));
        row.set("field_matches", fieldMatches(diagnostics, expected, regex, visible));
        try {
            active.records.write(mapper.writeValueAsString(row));
            active.records.newLine();
            active.count++;
        } catch (IOException error) {
            throw new UncheckedIOException(error);
        }
    }

    public static void finish(Result result) throws IOException {
        if (active == null)
            return;
        var exporter = active;
        active = null;
        exporter.records.close();
        exporter.directAssertions.close();
        exporter.manifest.put("records", exporter.count);
        exporter.manifest.put("direct_assertions", exporter.directCount);
        exporter.manifest.put("junit_tests", result.getRunCount());
        exporter.manifest.put("junit_failures", result.getFailureCount());
        exporter.manifest.put("junit_ignored", result.getIgnoreCount());
        exporter.manifest.put("junit_successful", result.wasSuccessful());
        Files.writeString(exporter.directory.resolve("complete.json"), exporter.manifest.toString(),
                StandardOpenOption.CREATE_NEW);
    }
}
