package org.dbsp.sqlCompiler.compiler.sql.tools;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.util.RawValue;
import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSinkOperator;
import org.dbsp.sqlCompiler.circuit.operator.IInputOperator;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.backend.ToJsonInnerVisitor;
import org.dbsp.sqlCompiler.compiler.backend.ToJsonOuterVisitor;
import org.dbsp.sqlCompiler.compiler.frontend.TableData;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.ProgramIdentifier;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.RelColumnMetadata;
import org.dbsp.sqlCompiler.ir.expression.DBSPZSetExpression;
import org.dbsp.util.ExplicitShuffle;
import org.dbsp.util.IdShuffle;
import org.dbsp.util.IndentStream;
import org.dbsp.util.JsonStream;
import org.dbsp.util.Linq;
import org.dbsp.util.Shuffle;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/** Runs test cases on the Gen-2 engine instead of compiling to Rust. */
public final class Gen2Runner {
    static final String ENV_VARIABLE = "GEN2_PATH";
    static final String PIPELINE_BINARY = "pipeline";
    static final String EXPANDER_BINARY = "ir-expander";
    static final int MAX_REPORTED_ROWS = 20;
    static final int MAX_PIPELINE_NAME_CHARS = 200;
    static final ObjectMapper MAPPER = new ObjectMapper();

    @Nullable
    private static final Path BINARIES = binaries();
    /** Whether each test compiler was incremental before {@link #beforeCompile} forced it. */
    private static final Map<CompilerOptions, Boolean> SOURCE_INCREMENTAL =
            Collections.synchronizedMap(new IdentityHashMap<>());
    @Nullable
    private static Engine engine;
    @Nullable
    private static RuntimeException engineStartFailure;

    private Gen2Runner() {}

    /** The {@code pipeline} and {@code ir-expander} processes that all tests in this JVM share,
     * and their work directory. */
    private record Engine(Gen2Pipeline pipeline, Gen2Expander expander, Path workDirectory) {
        void stop() {
            this.pipeline.stop();
            this.expander.stop();
        }
    }

    /** The table a step feeds: its name in the HTTP API, its column names, and the change. */
    private record Input(String table, List<String> columns, String literal) {}

    /** One step: each table's change, and each checked view's expected change by connector index. */
    private record Step(List<Input> inputs, Map<Integer, String> outputs) {}

    @Nullable
    private static Path binaries() {
        String path = System.getenv(ENV_VARIABLE);
        return path == null || path.isEmpty() ? null : Paths.get(path).toAbsolutePath().normalize();
    }

    public static boolean isEnabled() {
        return BINARIES != null;
    }

    /** Compiles the program's IR for the Gen-2 engine, as the platform does. */
    static void adjustOptions(CompilerOptions options) {
        if (isEnabled())
            options.ioOptions.gen2 = true;
    }

    /** Sets compiler options for test execution. */
    static void beforeCompile(DBSPCompiler compiler) {
        CompilerOptions options = compiler.options;
        if (!isEnabled() || !options.ioOptions.gen2)
            return;
        SOURCE_INCREMENTAL.putIfAbsent(options, options.languageOptions.incrementalize);
        options.languageOptions.incrementalize = true;
    }

    static void testFinished() {
        SOURCE_INCREMENTAL.clear();
    }

    /** Runs {@code testCase} on the Gen-2 engine.
     * @throws AssertionError when a view's output differs from the expected change, or when
     *         the pipeline fails other than with the panic the case expects. */
    public static synchronized void run(TestCase testCase) {
        DBSPCompiler compiler = testCase.ccs.compiler;
        if (!compiler.options.ioOptions.gen2)
            throw new IllegalStateException("The test builds its own compiler without the Gen-2 options");
        DBSPCircuit circuit = testCase.ccs.circuit;
        List<IInputOperator> sources = new ArrayList<>(circuit.sourceOperators.values());
        List<DBSPSinkOperator> sinks = new ArrayList<>(circuit.sinkOperators.values());
        List<DBSPSinkOperator> checked = new ArrayList<>();
        List<Step> steps = steps(testCase, sources, sinks, checked);

        Engine engine = engine();
        Path outputDirectory = engine.workDirectory.resolve("outputs");
        try {
            // Each test case starts with empty view output files.
            Gen2Pipeline.deleteRecursively(outputDirectory);
            Files.createDirectories(outputDirectory);
            String config = pipelineConfig(testCase, checked, outputDirectory);
            List<String> views = Linq.map(checked, sink -> sqlName(sink.metadata.viewName));
            runSteps(testCase, steps, views, outputDirectory, config, engine);
        } catch (IOException error) {
            throw new RuntimeException("Gen-2 run of " + testCase.javaTestName + ": " + error.getMessage(), error);
        }
    }

    private static void runSteps(TestCase testCase, List<Step> steps, List<String> views, Path outputDirectory,
                                 String config, Engine engine) throws IOException {
        Gen2Pipeline pipeline = engine.pipeline;
        try {
            pipeline.load(config);
            pipeline.waitUntilQuiet();
        } catch (Gen2Pipeline.EngineException error) {
            if (failedAsExpected(testCase, pipeline))
                return;
            throw new RuntimeException("Gen-2 pipeline did not load the program: " + error.getMessage()
                    + "\n" + error.logTail);
        }

        long[] offsets = new long[views.size()];
        List<String> differences = new ArrayList<>();
        for (int stepIndex = 0; stepIndex < steps.size(); stepIndex++) {
            Step step = steps.get(stepIndex);
            Map<String, byte[]> inputs = new LinkedHashMap<>();
            for (Input input : step.inputs) {
                Gen2Expander.Expanded expanded = engine.expander.expand(input.literal, input.columns);
                if (expanded.rows() > 0)
                    inputs.put(input.table, expanded.stream());
            }
            try {
                if (!inputs.isEmpty())
                    pipeline.runStep(inputs);
            } catch (Gen2Pipeline.EngineException error) {
                // A failed pipeline takes the next load; a hung or dead one needs a new process.
                boolean expected = failedAsExpected(testCase, pipeline);
                if (!pipeline.isAlive() || error.isTimeout)
                    pipeline.stop();
                if (expected)
                    return;
                throw new AssertionError("Gen-2 step " + stepIndex + " failed: " + error.getMessage()
                        + "\n" + error.logTail);
            }
            for (int view = 0; view < views.size(); view++) {
                byte[] produced = readFrom(outputFile(outputDirectory, view), offsets[view]);
                offsets[view] += produced.length;
                String expected = step.outputs.get(view);
                if (expected == null)
                    continue;
                List<String> rows = engine.expander.compare(expected, produced);
                if (!rows.isEmpty())
                    differences.add(describe(stepIndex, views.get(view), rows));
            }
        }
        if (testCase.message != null)
            throw new AssertionError("Expected a panic containing '" + testCase.message
                    + "'; the Gen-2 pipeline did not fail");
        if (!differences.isEmpty())
            throw new AssertionError("Gen-2 output differs (+ only expected, - only produced):\n"
                    + String.join("\n", differences));
    }

    /** Whether the case expects the pipeline to fail, and it failed with the expected message. */
    private static boolean failedAsExpected(TestCase testCase, Gen2Pipeline pipeline) {
        String failure = pipeline.failureText();
        return testCase.message != null && failure.contains(testCase.message);
    }

    private static String describe(int stepIndex, String view, List<String> rows) {
        List<String> shown = rows.subList(0, Math.min(rows.size(), MAX_REPORTED_ROWS));
        StringBuilder text = new StringBuilder("step " + stepIndex + ", view " + view + ":");
        for (String row : shown)
            text.append("\n  ").append(row);
        if (rows.size() > shown.size())
            text.append("\n  ... ").append(rows.size() - shown.size()).append(" more");
        return text.toString();
    }

    /** The steps of the case.  Adds each view some step checks to {@code checked}, which
     * numbers the views' output connectors. */
    private static List<Step> steps(TestCase testCase, List<IInputOperator> sources,
                                    List<DBSPSinkOperator> sinks, List<DBSPSinkOperator> checked) {
        DBSPCompiler compiler = testCase.ccs.compiler;
        InputOutputChangeStream stream = testCase.ccs.stream;
        Shuffle inputShuffle = streamToCircuitOrder(stream.inputTables, Linq.map(sources, IInputOperator::getTableName));
        Shuffle outputShuffle = streamToCircuitOrder(stream.outputTables, Linq.map(sinks, sink -> sink.metadata.viewName));
        boolean sourceIncremental = SOURCE_INCREMENTAL.getOrDefault(compiler.options, true);
        List<List<String>> columns = Linq.map(sources, source -> columnNames(compiler, source.getTableName()));

        List<Step> steps = new ArrayList<>();
        @Nullable Change previousInputs = null;
        @Nullable Change previousOutputs = null;
        for (IStreamCommand command : stream.commands) {
            // The engine compacts on its own; a test waiting for compaction adds no step.
            if (command instanceof BlockForCompaction)
                continue;
            InputOutputChange change = (InputOutputChange) command;
            Change inputs = change.getInputs().shuffle(inputShuffle).simplify(compiler);
            Change outputs = change.getOutputs().shuffle(outputShuffle).simplify(compiler);
            if (!sourceIncremental) {
                // A non-incremental test lists each step's whole tables and whole views. The engine
                // runs the incremental circuit, which takes the tables' change since the previous
                // step and emits the views' change, so the step becomes those differences.
                Change wholeInputs = inputs;
                Change wholeOutputs = outputs;
                inputs = delta(inputs, previousInputs);
                outputs = delta(outputs, previousOutputs);
                previousInputs = wholeInputs;
                previousOutputs = wholeOutputs;
            }
            steps.add(new Step(inputs(compiler, inputs, sources, columns), outputs(compiler, outputs, sinks, checked)));
        }
        return steps;
    }

    private static List<Input> inputs(DBSPCompiler compiler, Change inputs, List<IInputOperator> sources,
                                      List<List<String>> columns) {
        if (inputs.getSetCount() == 0)
            return List.of();
        if (inputs.getSetCount() != sources.size())
            throw new IllegalStateException("Step has " + inputs.getSetCount() + " input sets for "
                    + sources.size() + " tables");
        List<Input> result = new ArrayList<>();
        for (int index = 0; index < sources.size(); index++) {
            DBSPZSetExpression data = inputs.getSet(index).data();
            if (!data.isEmpty())
                result.add(new Input(sqlName(sources.get(index).getTableName()), columns.get(index),
                        literal(compiler, data)));
        }
        return result;
    }

    private static Map<Integer, String> outputs(DBSPCompiler compiler, Change outputs, List<DBSPSinkOperator> sinks,
                                                List<DBSPSinkOperator> checked) {
        if (outputs.getSetCount() == 0)
            return Map.of();
        List<DBSPSinkOperator> listed = sinks;
        // A step with fewer outputs than views leaves out the system views, as in the Rust tests.
        if (outputs.getSetCount() < sinks.size())
            listed = Linq.where(sinks, sink -> !sink.metadata.system);
        if (outputs.getSetCount() != listed.size())
            throw new IllegalStateException("Step has " + outputs.getSetCount() + " output sets for "
                    + listed.size() + " views");
        Map<Integer, String> result = new LinkedHashMap<>();
        for (int index = 0; index < listed.size(); index++) {
            DBSPSinkOperator sink = listed.get(index);
            if (!checked.contains(sink))
                checked.add(sink);
            result.put(checked.indexOf(sink), literal(compiler, outputs.getSet(index).data()));
        }
        return result;
    }

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

    /** The table's column names as the engine matches them: a case-insensitive name in lower case. */
    private static List<String> columnNames(DBSPCompiler compiler, ProgramIdentifier table) {
        List<RelColumnMetadata> columns = compiler.metadata.getTableDescription(table).getColumns();
        return Linq.map(columns, column -> column.getName().isQuoted()
                ? column.getName().name() : column.getName().name().toLowerCase(Locale.ROOT));
    }

    /** The name as the pipeline's HTTP API spells it: a quoted identifier keeps its quotes. */
    static String sqlName(ProgramIdentifier identifier) {
        return identifier.isQuoted() ? "\"" + identifier.name() + "\"" : identifier.name();
    }

    private static String literal(DBSPCompiler compiler, DBSPZSetExpression zset) {
        IndentStream stream = new IndentStream(new StringBuilder());
        stream.setIndentAmount(0);
        ToJsonInnerVisitor visitor = new ToJsonInnerVisitor(compiler, new JsonStream(stream), 1);
        visitor.apply(zset);
        return visitor.getJsonString();
    }

    /** The configuration a platform would give the pipeline. */
    private static String pipelineConfig(TestCase testCase, List<DBSPSinkOperator> checked, Path outputDirectory)
            throws IOException {
        ObjectNode outputs = MAPPER.createObjectNode();
        for (int index = 0; index < checked.size(); index++) {
            ObjectNode connector = outputs.putObject("view" + index);
            connector.put("stream", sqlName(checked.get(index).metadata.viewName));
            ObjectNode transport = connector.putObject("transport");
            transport.put("name", "file_output");
            transport.putObject("config").put("path", outputFile(outputDirectory, index).toString());
            ObjectNode format = connector.putObject("format");
            format.put("name", "arrow_ipc");
            format.putObject("config");
        }
        String name = testCase.javaTestName;
        ObjectNode config = MAPPER.createObjectNode();
        config.put("name", name.substring(0, Math.min(name.length(), MAX_PIPELINE_NAME_CHARS)));
        config.put("workers", 1);
        config.putRawValue("program_ir", new RawValue(programIr(testCase.ccs)));
        config.putObject("inputs");
        config.set("outputs", outputs);
        return MAPPER.writeValueAsString(config);
    }

    /** The configuration's {@code program_ir}, as the platform composes it from the compiler's
     * outputs.  The circuit IR is spliced in as written: parsing it would round decimals that
     * do not fit a double. */
    private static String programIr(CompilerCircuitStream ccs) {
        ToJsonOuterVisitor visitor = ToJsonOuterVisitor.create(ccs.compiler, 1);
        visitor.apply(ccs.circuit);
        return "{\"mir\":{},\"program_schema\":" + ccs.compiler.metadata.asJson()
                + ",\"circuit_ir\":" + visitor.getJsonString() + "}";
    }

    private static Path outputFile(Path outputDirectory, int view) {
        return outputDirectory.resolve("view" + view);
    }

    /** What the file holds past {@code offset}: the view's output since the previous step. */
    private static byte[] readFrom(Path file, long offset) throws IOException {
        if (!Files.exists(file))
            return new byte[0];
        byte[] produced = Files.readAllBytes(file);
        return Arrays.copyOfRange(produced, (int) offset, produced.length);
    }

    private static synchronized Engine engine() {
        if (engineStartFailure != null)
            throw engineStartFailure;
        if (engine == null) {
            try {
                Path binaries = Objects.requireNonNull(BINARIES, ENV_VARIABLE + " is not set");
                // Under the build directory, so a clean removes the logs.
                Path workDirectory = Files.createTempDirectory(
                        Files.createDirectories(Paths.get("target", "gen2")), "run-").toAbsolutePath();
                build(binaries, workDirectory);
                engine = new Engine(new Gen2Pipeline(binaries.resolve(PIPELINE_BINARY), workDirectory),
                        new Gen2Expander(binaries.resolve(EXPANDER_BINARY), workDirectory), workDirectory);
                Engine started = engine;
                Runtime.getRuntime().addShutdownHook(new Thread(started::stop));
                System.err.println("Gen-2 runner: binaries in " + binaries + ", logs in " + workDirectory);
            } catch (IOException | InterruptedException | RuntimeException error) {
                engineStartFailure = new IllegalStateException("Cannot start the Gen-2 engine: " + error.getMessage(), error);
                throw engineStartFailure;
            }
        }
        return engine;
    }

    /** Builds the binaries when {@code binaries} is a Cargo target directory, so a run never
     * uses stale ones.  Anywhere else, such as an unpacked CI artifact, they must exist. */
    private static void build(Path binaries, Path workDirectory) throws IOException, InterruptedException {
        Path target = binaries.getParent();
        Path workspace = target == null ? null : target.getParent();
        boolean isCargoTarget = target != null && workspace != null
                && target.getFileName().toString().equals("target")
                && Files.isRegularFile(workspace.resolve("Cargo.toml"));
        if (isCargoTarget) {
            String profile = binaries.getFileName().toString();
            List<String> command = List.of("cargo", "build", "--profile", profile.equals("debug") ? "dev" : profile,
                    "-p", "crucible", "--bin", PIPELINE_BINARY, "-p", "ir-expander", "--bin", EXPANDER_BINARY);
            Path log = workDirectory.resolve("cargo-build.log");
            System.err.println("Gen-2 runner: " + String.join(" ", command) + " in " + workspace + " (log: " + log + ")");
            ProcessBuilder builder = new ProcessBuilder(command)
                    .directory(workspace.toFile())
                    .redirectErrorStream(true)
                    .redirectOutput(log.toFile());
            // A CARGO_TARGET_DIR from the environment would build the binaries somewhere else.
            builder.environment().put("CARGO_TARGET_DIR", target.toString());
            Process cargo = builder.start();
            if (cargo.waitFor() != 0)
                throw new IllegalStateException("building the binaries failed:\n"
                        + Files.readString(log, StandardCharsets.UTF_8));
        }
        for (String binary : List.of(PIPELINE_BINARY, EXPANDER_BINARY))
            if (!Files.isExecutable(binaries.resolve(binary)))
                throw new IllegalStateException(ENV_VARIABLE + " has no executable '" + binary + "': " + binaries);
    }
}
