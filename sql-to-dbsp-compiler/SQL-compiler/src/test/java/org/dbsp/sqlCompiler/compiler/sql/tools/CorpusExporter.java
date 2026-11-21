package org.dbsp.sqlCompiler.compiler.sql.tools;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.util.RawValue;
import org.dbsp.sqlCompiler.compiler.backend.ToJsonInnerVisitor;
import org.dbsp.sqlCompiler.compiler.backend.ToJsonOuterVisitor;
import org.dbsp.sqlCompiler.compiler.frontend.TableData;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.ProgramIdentifier;
import org.dbsp.util.IndentStream;
import org.dbsp.util.JsonStream;
import org.dbsp.util.Utilities;
import org.junit.AssumptionViolatedException;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;

/** Exports JUnit runtime-oracle cases after their bodies have populated the streams.
 * The source/options snapshot precedes INSERT steps, which mutate the compiler. */
final class CorpusExporter {
    private static final ObjectMapper MAPPER = Utilities.deterministicObjectMapper();
    private static final String PREFIX = "feldera.corpus.";

    private record Pending(TestCase test, String program, JsonNode submissions, JsonNode options,
                           JsonNode compilationOptions,
                           JsonNode schema, JsonNode diagnostics) {}

    private final Path root;
    private final String producerCommit;
    private final String calciteCommit;
    private final String runtimeCommit;
    private final List<Pending> pending = new ArrayList<>();

    CorpusExporter(Path root, String producerCommit, String calciteCommit, String runtimeCommit) {
        this.root = root;
        this.producerCommit = producerCommit;
        this.calciteCommit = calciteCommit;
        this.runtimeCommit = runtimeCommit;
        if (producerCommit.isBlank() || calciteCommit.isBlank() || runtimeCommit.isBlank())
            throw new IllegalArgumentException("Corpus export requires compiler, Calcite, and runtime revisions");
    }

    static boolean exportOnly() {
        boolean only = Boolean.getBoolean(PREFIX + "only");
        if (only && System.getProperty(PREFIX + "dir", "").isBlank())
            throw new IllegalArgumentException("feldera.corpus.only requires feldera.corpus.dir");
        return only;
    }

    @Nullable static CorpusExporter fromSystemProperties() {
        String directory = System.getProperty(PREFIX + "dir", "");
        exportOnly();
        if (directory.isBlank())
            return null;
        return new CorpusExporter(Path.of(directory),
                System.getProperty(PREFIX + "producer", ""),
                System.getProperty(PREFIX + "calcite", ""),
                System.getProperty(PREFIX + "runtime", ""));
    }

    void register(TestCase test) {
        var compiler = test.ccs.compiler;
        ObjectNode compilationOptions = MAPPER.valueToTree(compiler.options);
        // Gen2Runner temporarily compiles incrementally, then restores the test's
        // option. Keep that option for interpreting its expected steps, and record
        // the circuit's actual mode separately for compilation replay.
        ((ObjectNode) compilationOptions.get("languageOptions"))
                .put("incrementalize", test.ccs.circuit.incremental);
        this.pending.add(new Pending(test, compiler.sources.getWholeProgram(),
                MAPPER.valueToTree(compiler.getSourceSubmissions()),
                MAPPER.valueToTree(compiler.options), compilationOptions, compiler.metadata.asJson(),
                compiler.messages.toJson(compiler.sources)));
    }

    /** The directory is keyed by the full JUnit identity, not by allocation ids or SQL text. */
    static String directoryName(String sourceTest) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
                    .digest(sourceTest.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is required by the Java runtime", e);
        }
    }

    void finish(String sourceTest, @Nullable Throwable failure) {
        Path directory = this.root.resolve(directoryName(sourceTest));
        try {
            Files.createDirectories(this.root);
            Files.createDirectory(directory);
            for (int index = 0; index < this.pending.size(); index++) {
                ObjectNode record = this.caseRecord(sourceTest, index, this.pending.get(index), failure);
                write(directory.resolve(String.format(java.util.Locale.ROOT, "case-%04d.json", index)), record);
            }
            ObjectNode manifest = MAPPER.createObjectNode();
            manifest.put("schema_version", 1);
            manifest.put("source_test", sourceTest);
            manifest.put("case_count", this.pending.size());
            manifest.put("exported", failure == null ? this.pending.size() : 0);
            manifest.put("skipped", failure == null ? 0 : this.pending.size());
            manifest.put("test_body_status", failure == null ? "passed"
                    : failure instanceof AssumptionViolatedException ? "skipped" : "failed");
            if (failure != null)
                manifest.put("reason", failure.toString());
            write(directory.resolve("complete.json"), manifest);
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot export corpus case " + sourceTest + " to " + directory, e);
        }
    }

    private static void write(Path path, ObjectNode value) throws IOException {
        // RawValue keeps high-precision decimals in the typed IR intact.
        Files.writeString(path, MAPPER.writeValueAsString(value), StandardOpenOption.CREATE_NEW);
    }

    private ObjectNode caseRecord(String sourceTest, int index, Pending pending, @Nullable Throwable failure) {
        TestCase test = pending.test;
        ObjectNode record = MAPPER.createObjectNode();
        record.put("schema_version", 1);
        record.put("source_test", sourceTest);
        record.put("case_index", index);
        record.put("name", test.name);
        record.put("producer_commit", this.producerCommit);
        record.put("calcite_commit", this.calciteCommit);
        record.put("runtime_commit", this.runtimeCommit);
        record.put("program", pending.program);
        record.set("submissions", pending.submissions);
        record.set("options", pending.options);
        record.set("compilation_options", pending.compilationOptions);
        record.set("schema", pending.schema);
        record.set("diagnostics", pending.diagnostics);
        record.put("engine_mode", pending.options.at("/ioOptions/gen2").asBoolean() ? "gen2" : "classic");
        record.put("runtime_verified", false);
        if (failure != null) {
            record.put("status", "skipped");
            record.put("reason", "Test body did not complete: " + failure);
            return record;
        }
        record.put("status", "exported");
        record.put("expected_runtime_error", test.message);
        record.put("step_semantics", pending.options.at("/languageOptions/incrementalize").asBoolean()
                ? "delta" : "whole_relation");
        record.set("input_order", MAPPER.valueToTree(test.ccs.stream.inputTables));
        record.set("output_order", MAPPER.valueToTree(test.ccs.stream.outputTables));
        ArrayNode inputs = record.putArray("circuit_inputs");
        for (var name: test.ccs.circuit.getInputTables())
            inputs.add(identifier(name));
        ArrayNode outputs = record.putArray("circuit_outputs");
        for (var sink: test.ccs.circuit.sinkOperators.values()) {
            ObjectNode output = outputs.addObject();
            output.set("name", identifier(sink.viewName));
            output.put("system", sink.metadata.system);
        }
        var visitor = ToJsonOuterVisitor.create(test.ccs.compiler, 1);
        visitor.apply(test.ccs.circuit);
        record.putRawValue("circuit", new RawValue(visitor.getJsonString()));
        ArrayNode commands = record.putArray("commands");
        for (IStreamCommand command: test.ccs.stream.commands) {
            ObjectNode step = commands.addObject();
            if (command instanceof BlockForCompaction) {
                step.put("kind", "compact");
            } else if (command instanceof InputOutputChange change) {
                step.put("kind", "step");
                step.set("inputs", sets(test, change.inputs));
                step.set("outputs", sets(test, change.outputs));
            } else {
                throw new IllegalStateException("Unsupported corpus command " + command.getClass().getName());
            }
        }
        return record;
    }

    private static ObjectNode identifier(ProgramIdentifier name) {
        ObjectNode result = MAPPER.createObjectNode();
        result.put("text", name.name());
        result.put("quoted", name.isQuoted());
        return result;
    }

    private static ArrayNode sets(TestCase test, Change change) {
        ArrayNode result = MAPPER.createArrayNode();
        for (TableData table: change.sets) {
            ObjectNode entry = result.addObject();
            entry.set("name", identifier(table.name()));
            entry.set("primary_keys", MAPPER.valueToTree(table.primaryKeys()));
            var stream = new IndentStream(new StringBuilder()).setIndentAmount(0);
            var visitor = new ToJsonInnerVisitor(test.ccs.compiler, new JsonStream(stream), 1);
            visitor.apply(table.data());
            entry.putRawValue("zset_ir", new RawValue(visitor.getJsonString()));
        }
        return result;
    }
}
