package org.dbsp.sqlCompiler.compiler.sql.tools;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.core.JsonGenerator;
import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSinkOperator;
import org.dbsp.sqlCompiler.circuit.operator.IInputOperator;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.InputColumnMetadata;
import org.dbsp.sqlCompiler.compiler.backend.ToJsonOuterVisitor;
import org.dbsp.sqlCompiler.compiler.frontend.TableData;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.ProgramIdentifier;
import org.dbsp.util.ExplicitShuffle;
import org.dbsp.util.IdShuffle;
import org.dbsp.util.Linq;
import org.dbsp.util.Shuffle;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

/**
 * Writes every runtime test case as a directory the forge {@code pipeline} binary can run,
 * instead of compiling the cases to Rust.  Enabled by {@code FELDERA_CRUCIBLE_CASES_DIR}.
 *
 * <p>A case directory holds {@code program_ir.json} (the pipeline config's {@code program_ir}),
 * {@code case.json} (the manifest), and {@code steps/<k>/in/<table>.json} plus
 * {@code steps/<k>/out/<view>.json} in the JSON {@code insert_delete} format.
 * A case that cannot be exported still gets a manifest, carrying {@code export_error}.
 */
public final class CrucibleCaseExport {
    static final String ENV_VARIABLE = "FELDERA_CRUCIBLE_CASES_DIR";
    static final int PROTOCOL = 1;

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .enable(JsonGenerator.Feature.WRITE_BIGDECIMAL_AS_PLAIN)
            .disable(SerializationFeature.INDENT_OUTPUT);
    /** Next case index per Java test, so repeated cases of one test get distinct directories. */
    private static final Map<String, Integer> NEXT_INDEX = new HashMap<>();
    /** Whether each test compiler was incremental before {@link #adjustOptions} forced it. */
    private static final Map<CompilerOptions, Boolean> SOURCE_INCREMENTAL =
            Collections.synchronizedMap(new IdentityHashMap<>());

    private CrucibleCaseExport() {}

    public static boolean isEnabled() {
        String dir = System.getenv(ENV_VARIABLE);
        return dir != null && !dir.isEmpty();
    }

    /** Compile for the Gen-2 engine as the platform does: the IR of an incremental circuit.
     * Multi-crate output cannot carry the IR. */
    static void adjustOptions(CompilerOptions options) {
        if (!isEnabled())
            return;
        SOURCE_INCREMENTAL.put(options, options.languageOptions.incrementalize);
        options.ioOptions.gen2 = true;
        options.ioOptions.crates = "";
        options.languageOptions.incrementalize = true;
    }

    static void exportAll(List<TestCase> testCases) {
        for (TestCase testCase : testCases) {
            Path caseDirectory = nextCaseDirectory(testCase.javaTestName);
            try {
                exportCase(testCase, caseDirectory);
            } catch (Throwable error) {
                writeErrorManifest(caseDirectory, testCase.javaTestName, testCase.message, error);
            }
        }
        SOURCE_INCREMENTAL.clear();
    }

    /** Records a Java test that threw before it could register its cases. */
    static void recordFailedTest(String javaTestName, Throwable error) {
        writeErrorManifest(nextCaseDirectory(javaTestName), javaTestName, null, error);
    }

    private static synchronized Path nextCaseDirectory(String javaTestName) {
        int index = NEXT_INDEX.merge(javaTestName, 1, Integer::sum) - 1;
        String directoryName = javaTestName.replace('#', '.') + "." + index;
        return Paths.get(System.getenv(ENV_VARIABLE), directoryName);
    }

    private static void exportCase(TestCase testCase, Path caseDirectory) throws IOException {
        DBSPCircuit circuit = testCase.ccs.circuit;
        if (!testCase.ccs.compiler.options.ioOptions.gen2)
            throw new IllegalStateException("The test builds its own compiler without the Gen-2 options");
        Files.createDirectories(caseDirectory);
        writeProgramIr(testCase, caseDirectory.resolve("program_ir.json"));

        List<IInputOperator> sources = new ArrayList<>(circuit.sourceOperators.values());
        List<DBSPSinkOperator> sinks = new ArrayList<>(circuit.sinkOperators.values());
        InputOutputChangeStream stream = testCase.ccs.stream;
        Shuffle inputShuffle = streamToCircuitOrder(stream.inputTables, Linq.map(sources, IInputOperator::getTableName));
        Shuffle outputShuffle = streamToCircuitOrder(stream.outputTables, Linq.map(sinks, sink -> sink.metadata.viewName));

        boolean sourceIncremental = SOURCE_INCREMENTAL.getOrDefault(testCase.ccs.compiler.options, true);
        ArrayNode steps = MAPPER.createArrayNode();
        @Nullable Change previousInputs = null;
        @Nullable Change previousOutputs = null;
        int stepIndex = 0;
        for (IStreamCommand command : stream.commands) {
            if (command instanceof BlockForCompaction) {
                steps.add(MAPPER.createObjectNode().put("kind", "compact"));
            } else if (command instanceof InputOutputChange change) {
                Path stepDirectory = caseDirectory.resolve("steps").resolve(Integer.toString(stepIndex));
                Change inputs = change.getInputs().shuffle(inputShuffle).simplify(testCase.ccs.compiler);
                Change outputs = change.getOutputs().shuffle(outputShuffle).simplify(testCase.ccs.compiler);
                Change inputDelta = inputs;
                Change outputDelta = outputs;
                if (!sourceIncremental) {
                    // A non-incremental step maps its input to its output; the incremental
                    // circuit sees the change from the previous step and emits the output's change.
                    inputDelta = delta(inputs, previousInputs);
                    outputDelta = delta(outputs, previousOutputs);
                    previousInputs = inputs;
                    previousOutputs = outputs;
                }
                writeInputs(inputDelta, sources, stepDirectory.resolve("in"));
                writeOutputs(outputDelta, sinks, stepDirectory.resolve("out"));
                steps.add(MAPPER.createObjectNode().put("kind", "change"));
            } else {
                throw new IllegalStateException("Unexpected test command " + command.getClass().getSimpleName());
            }
            stepIndex++;
        }

        ObjectNode manifest = baseManifest(testCase.javaTestName, testCase.message);
        manifest.put("incremental", testCase.ccs.compiler.options.languageOptions.incrementalize);
        manifest.put("source_incremental", sourceIncremental);
        manifest.put("optimization_level", testCase.ccs.compiler.options.languageOptions.optimizationLevel);
        manifest.set("tables", namesArray(Linq.map(sources, IInputOperator::getTableName)));
        manifest.set("views", namesArray(Linq.map(sinks, sink -> sink.metadata.viewName)));
        manifest.set("steps", steps);
        writeJson(caseDirectory.resolve("case.json"), manifest);
    }

    /** {@code current - previous}, set by set; the first step's delta is the step itself.
     * A step that lists no sets (unchecked outputs) has no delta either. */
    private static Change delta(Change current, @Nullable Change previous) {
        if (previous == null || previous.getSetCount() == 0 || current.getSetCount() == 0)
            return current;
        if (previous.getSetCount() != current.getSetCount())
            throw new IllegalStateException("Steps list " + previous.getSetCount() + " and "
                    + current.getSetCount() + " sets");
        TableData[] sets = new TableData[current.getSetCount()];
        for (int index = 0; index < sets.length; index++)
            sets[index] = current.getSet(index).minus(previous.getSet(index).data());
        return new Change(sets);
    }

    /** A test may list its tables or views in its own order; an empty list means circuit order. */
    private static Shuffle streamToCircuitOrder(List<String> streamOrder, List<ProgramIdentifier> circuitOrder) {
        if (streamOrder.isEmpty())
            return new IdShuffle(circuitOrder.size());
        return ExplicitShuffle.computePermutation(streamOrder, Linq.map(circuitOrder, ProgramIdentifier::name));
    }

    /** Both halves are already JSON text, so they are spliced rather than re-parsed. */
    private static void writeProgramIr(TestCase testCase, Path path) throws IOException {
        ToJsonOuterVisitor visitor = ToJsonOuterVisitor.create(testCase.ccs.compiler, 1);
        visitor.apply(testCase.ccs.circuit);
        String schema = MAPPER.writeValueAsString(testCase.ccs.compiler.metadata.asJson());
        String programIr = "{\"mir\":{},\"program_schema\":" + schema
                + ",\"circuit_ir\":" + visitor.getJsonString() + "}";
        Files.writeString(path, programIr, StandardCharsets.UTF_8);
    }

    private static void writeInputs(Change inputs, List<IInputOperator> sources, Path directory) throws IOException {
        if (inputs.getSetCount() == 0)
            return;
        if (inputs.getSetCount() != sources.size())
            throw new IllegalStateException("Step has " + inputs.getSetCount() + " input sets for "
                    + sources.size() + " tables");
        Files.createDirectories(directory);
        for (int index = 0; index < sources.size(); index++) {
            IInputOperator source = sources.get(index);
            List<InputColumnMetadata> columns = new ArrayList<>(source.getMetadata().getColumns());
            ArrayNode records = FelderaJsonEncoder.encodeZSet(inputs.getSet(index).data(), columns);
            writeJson(directory.resolve(sqlName(source.getTableName()) + ".json"), records);
        }
    }

    /** A step that lists fewer outputs than the circuit has skips the system views, as the Rust tests do. */
    private static void writeOutputs(Change outputs, List<DBSPSinkOperator> sinks, Path directory) throws IOException {
        if (outputs.getSetCount() == 0)
            return;
        List<DBSPSinkOperator> checked = sinks;
        if (outputs.getSetCount() < sinks.size())
            checked = Linq.where(sinks, sink -> !sink.metadata.system);
        if (outputs.getSetCount() != checked.size())
            throw new IllegalStateException("Step has " + outputs.getSetCount() + " output sets for "
                    + checked.size() + " views");
        Files.createDirectories(directory);
        for (int index = 0; index < checked.size(); index++) {
            DBSPSinkOperator sink = checked.get(index);
            ArrayNode records = FelderaJsonEncoder.encodeZSet(outputs.getSet(index).data(), sink.metadata.columns);
            writeJson(directory.resolve(sqlName(sink.metadata.viewName) + ".json"), records);
        }
    }

    private static void writeErrorManifest(
            Path caseDirectory, String javaTestName, @Nullable String expectPanic, Throwable error) {
        try {
            Files.createDirectories(caseDirectory);
            ObjectNode manifest = baseManifest(javaTestName, expectPanic);
            StringWriter trace = new StringWriter();
            error.printStackTrace(new PrintWriter(trace));
            manifest.put("export_error", error.getClass().getSimpleName() + ": " + error.getMessage());
            manifest.put("export_error_trace", trace.toString());
            writeJson(caseDirectory.resolve("case.json"), manifest);
        } catch (IOException io) {
            throw new RuntimeException("Cannot write the case manifest in " + caseDirectory, io);
        }
    }

    private static ObjectNode baseManifest(String javaTestName, @Nullable String expectPanic) {
        ObjectNode manifest = MAPPER.createObjectNode();
        manifest.put("protocol", PROTOCOL);
        manifest.put("java_test", javaTestName);
        if (expectPanic != null)
            manifest.put("expect_panic", expectPanic);
        return manifest;
    }

    /** The name as the pipeline's HTTP API spells it: a quoted identifier keeps its quotes. */
    static String sqlName(ProgramIdentifier identifier) {
        return identifier.isQuoted() ? "\"" + identifier.name() + "\"" : identifier.name();
    }

    private static ArrayNode namesArray(List<ProgramIdentifier> identifiers) {
        ArrayNode names = MAPPER.createArrayNode();
        for (ProgramIdentifier identifier : identifiers)
            names.add(sqlName(identifier));
        return names;
    }

    private static void writeJson(Path path, Object value) throws IOException {
        Files.writeString(path, MAPPER.writeValueAsString(value), StandardCharsets.UTF_8);
    }
}
