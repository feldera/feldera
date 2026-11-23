package org.dbsp.sqlCompiler.compiler.sql.tools;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.codec.digest.DigestUtils;
import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.backend.ToJsonOuterVisitor;
import org.dbsp.sqlCompiler.compiler.backend.CircuitReplay;
import org.dbsp.sqlCompiler.compiler.backend.CircuitDumps;
import org.dbsp.sqlCompiler.compiler.backend.RustCircuitPass;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitTransform;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.compiler.backend.rust.RustFileWriter;
import org.dbsp.sqlCompiler.compiler.backend.rust.StubsWriter;
import org.dbsp.sqlCompiler.compiler.backend.rust.multi.MultiCratesWriter;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitPostfix;
import org.dbsp.util.IndentStream;
import org.dbsp.util.Utilities;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.Map;
import java.util.TreeMap;
import java.util.zip.GZIPOutputStream;

/** Recompile an ordered corpus in one JVM. Run this entry point in separate JVMs
 * to compare complete workloads, including JVM-global name allocation. */
public final class CorpusDeterminism {
    private static final boolean VERIFY_IR = Boolean.getBoolean("feldera.determinism.verifyIr");
    private static final boolean SEAM_ARTIFACTS = Boolean.getBoolean("feldera.determinism.seamArtifacts");
    private static final ObjectMapper MAPPER = Utilities.deterministicObjectMapper();

    /** Length framing distinguishes ["ab", "c"] from ["a", "bc"]. */
    static final class Fingerprint {
        private final MessageDigest digest = DigestUtils.getSha256Digest();
        private long count;
        private long bytes;

        void add(String value) {
            this.add(value.getBytes(StandardCharsets.UTF_8));
        }

        void add(byte[] value) {
            this.digest.update(ByteBuffer.allocate(Long.BYTES).putLong(value.length).array());
            this.digest.update(value);
            this.count++;
            this.bytes += value.length;
        }

        ObjectNode finish() {
            ObjectNode result = MAPPER.createObjectNode();
            result.put("count", this.count);
            result.put("bytes", this.bytes);
            result.put("sha256", HexFormat.of().formatHex(this.digest.digest()));
            return result;
        }
    }

    /** A synchronous streaming connection keeps at most one observation in memory. */
    static final class Canonicalizer implements AutoCloseable {
        final String executable;
        private final Process process;
        private final BufferedWriter input;
        private final BufferedReader output;

        Canonicalizer(String executable) throws IOException {
            this.executable = executable;
            this.process = new ProcessBuilder(executable, VERIFY_IR
                    ? "--verify-ir-stream" : "--canonicalize-stream")
                    .redirectError(ProcessBuilder.Redirect.INHERIT).start();
            this.input = new BufferedWriter(new OutputStreamWriter(
                    this.process.getOutputStream(), StandardCharsets.UTF_8));
            this.output = new BufferedReader(new InputStreamReader(
                    this.process.getInputStream(), StandardCharsets.UTF_8));
        }

        String apply(String observation) throws IOException {
            this.input.write(observation);
            this.input.newLine();
            this.input.flush();
            String result = this.output.readLine();
            if (result == null)
                throw new IOException("Canonicalizer terminated before returning an observation");
            return result;
        }

        Path roundTripCheckpoint(Path input) throws IOException {
            Path output = input.resolveSibling(input.getFileName() + ".rust");
            Process writer = new ProcessBuilder(this.executable, "--roundtrip-circuit", input.toString())
                    .redirectOutput(output.toFile()).redirectError(ProcessBuilder.Redirect.INHERIT).start();
            try {
                if (writer.waitFor() != 0)
                    throw new IOException("Rust IR writer failed for " + input);
                return output;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted while writing " + output, e);
            } finally {
                writer.destroy();
            }
        }

        @Override public void close() throws IOException {
            try {
                this.input.close();
                if (this.output.readLine() != null)
                    throw new IOException("Unexpected extra canonicalizer response");
                if (this.process.waitFor() != 0)
                    throw new IOException("Canonicalizer failed with exit code " + this.process.exitValue());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted while closing canonicalizer", e);
            } finally {
                this.output.close();
                this.process.destroy();
            }
        }
    }

    private static final class Observations {
        private final Canonicalizer canonicalizer;
        private final S4Coverage coverage = Boolean.getBoolean("feldera.determinism.passCoverage")
                ? new S4Coverage() : null;
        private final boolean finalOnly;
        private final String firstStage;
        private final Map<String, Fingerprint> stages = new TreeMap<>();
        private int sequence;
        private String lastStage = "";
        private ObjectNode finalHeader;

        Observations(Canonicalizer canonicalizer, boolean finalOnly, boolean replayS3) {
            this.canonicalizer = canonicalizer;
            this.finalOnly = finalOnly;
            this.firstStage = replayS3 ? "S3" : "S2";
        }

        void accept(String json) {
            try {
                ObjectNode envelope = (ObjectNode) MAPPER.reader()
                        .with(com.fasterxml.jackson.databind.DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
                        .readTree(json);
                JsonNode identity = envelope.get("observation");
                String stage = identity.get("stage").asText();
                if (this.coverage != null)
                    this.coverage.accept(identity);
                if (identity.get("sequence").asInt() != this.sequence
                        || (this.sequence == 0 && !stage.equals(this.firstStage))
                        || this.lastStage.equals("S3"))
                    throw new IOException("Missing or reordered observation");
                if (!this.finalOnly || stage.equals("S3")) {
                    // S3 import has no optimizer prefix. Normalize only its local
                    // observation sequence, as the --final-s3 comparator does.
                    if (this.finalOnly)
                        ((ObjectNode) identity).put("sequence", 0);
                    this.stages.computeIfAbsent(stage, ignored -> new Fingerprint())
                            .add(this.canonicalizer.apply(this.finalOnly ? envelope.toString() : json));
                }
                this.sequence++;
                this.lastStage = stage;
                if (stage.equals("S3")) {
                    envelope.remove("circuit");
                    this.finalHeader = envelope;
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        String canonicalArtifact(String circuit) throws IOException {
            if (this.finalHeader == null)
                throw new IOException("Canonical artifact comparison requires a final S3 observation");
            ObjectNode envelope = this.finalHeader.deepCopy();
            // The canonicalizer uses one physical line per request. Parse with exact
            // decimals and duplicate detection before emitting compact JSON.
            JsonNode parsed = MAPPER.reader()
                    .with(com.fasterxml.jackson.databind.DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
                    .with(com.fasterxml.jackson.core.JsonParser.Feature.STRICT_DUPLICATE_DETECTION)
                    .readTree(circuit);
            envelope.set("circuit", parsed);
            return this.canonicalizer.apply(envelope.toString());
        }

        ObjectNode finish() throws IOException {
            if (!this.lastStage.equals("S3"))
                throw new IOException("Successful compilation has no final S3 observation");
            return finishFingerprints(this.stages);
        }
    }

    static ObjectNode finishFingerprints(Map<String, Fingerprint> values) {
        ObjectNode result = MAPPER.createObjectNode();
        values.forEach((key, value) -> result.set(key, value.finish()));
        return result;
    }

    private static void removeOutput(Path directory) throws IOException {
        try (var files = Files.walk(directory)) {
            for (Path file: files.sorted(Comparator.reverseOrder()).toList())
                Files.delete(file);
        }
    }

    /** Use the same back-end writers as BaseSQLTests, without its input generators. */
    static ObjectNode rust(DBSPCompiler compiler, DBSPCircuit circuit, Path directory) throws IOException {
        Files.createDirectory(directory);
        try {
            circuit.setName("circuit");
            if (compiler.options.ioOptions.multiCrates()) {
                var writer = new MultiCratesWriter(directory.toString(), "x", true);
                writer.add(circuit);
                writer.write(compiler);
                var stubs = new StubsWriter(writer.stubsDirectory().resolve(DBSPCompiler.STUBS_FILE_NAME));
                stubs.add(circuit);
                stubs.write(compiler);
            } else {
                try (var stream = new PrintStream(Files.newOutputStream(directory.resolve("circuit.rs")),
                        false, StandardCharsets.UTF_8)) {
                    var writer = new RustFileWriter(new CircuitPostfix(compiler));
                    writer.setOutputBuilder(new IndentStream(stream));
                    writer.add(circuit);
                    writer.write(compiler);
                }
                new StubsWriter(directory.resolve(DBSPCompiler.STUBS_FILE_NAME)).write(compiler);
            }
            try (var stream = new PrintStream(Files.newOutputStream(directory.resolve("dataflow.json")),
                    false, StandardCharsets.UTF_8)) {
                compiler.getDataflow(stream, circuit);
            }
            ObjectNode result = MAPPER.createObjectNode();
            try (var files = Files.walk(directory)) {
                for (Path file: files.filter(Files::isRegularFile).sorted().toList()) {
                    String name = directory.relativize(file).toString();
                    try (var input = Files.newInputStream(file)) {
                        // Dataflow has the same parsed-JSON contract in both engines.
                        // Restored Calcite plans may use a different pretty printer.
                        result.put(name, name.equals("dataflow.json")
                                ? jsonFingerprint(input) : DigestUtils.sha256Hex(input));
                    }
                }
            }
            return result;
        } finally {
            removeOutput(directory);
        }
    }

    private record Gen2Artifacts(ObjectNode bytes, ObjectNode semantic) { }

    private static String jsonFingerprint(java.io.InputStream input) throws IOException {
        JsonNode parsed = MAPPER.reader()
                .with(com.fasterxml.jackson.databind.DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
                .readTree(input);
        return DigestUtils.sha256Hex(parsed.toString());
    }

    private static Gen2Artifacts gen2(DBSPCompiler compiler, DBSPCircuit circuit, Path directory,
                                      Observations observations) throws IOException {
        Files.createDirectory(directory);
        try {
            circuit.setName("circuit");
            var visitor = ToJsonOuterVisitor.create(compiler, 1);
            visitor.apply(circuit);
            ObjectNode result = MAPPER.createObjectNode();
            String circuitJson = visitor.getJsonString();
            result.put("circuit.json", DigestUtils.sha256Hex(circuitJson));
            ObjectNode semantic = MAPPER.createObjectNode();
            if (SEAM_ARTIFACTS)
                semantic.put("circuit.json", DigestUtils.sha256Hex(observations.canonicalArtifact(circuitJson)));
            Path dataflow = directory.resolve("dataflow.json");
            try (var stream = new PrintStream(Files.newOutputStream(dataflow),
                    false, StandardCharsets.UTF_8)) {
                compiler.getDataflow(stream, circuit);
            }
            try (var input = Files.newInputStream(dataflow)) {
                result.put("dataflow.json", DigestUtils.sha256Hex(input));
            }
            if (SEAM_ARTIFACTS) {
                try (var input = Files.newInputStream(dataflow)) {
                    semantic.put("dataflow.json", jsonFingerprint(input));
                }
            }
            return new Gen2Artifacts(result, semantic);
        } finally {
            removeOutput(directory);
        }
    }

    private static ObjectNode compile(JsonNode source, Canonicalizer canonicalizer,
                                      boolean capture, boolean verifyWorker, boolean replayS2, boolean finalOnly,
                                      boolean replayS3, boolean liveFinal, Path rustDirectory, BufferedWriter coverageOutput,
                                      JsonNode importedProgram, S1BuiltinOperatorRegistry registry) throws IOException {
        // Each SQL request gets a fresh compiler process in the hybrid driver.
        // Some UDFs emit these names verbatim, so sharding must not change the initial counter.
        if (liveFinal)
            DBSPVariablePath.reset();
        var decoded = importedProgram == null ? null : S1Program.decode(importedProgram, registry);
        CompilerOptions options = decoded == null ? MAPPER.treeToValue(source.get("options"), CompilerOptions.class) : decoded.context().options();
        if (decoded != null) options.ioOptions.inputCircuit = false;
        options.ioOptions.dumpProducerCommit = System.getProperty("feldera.determinism.producer", "");
        options.ioOptions.dumpRuntimeAbi = System.getProperty("feldera.determinism.runtime", "");
        if (liveFinal)
            options.ioOptions.rustOptimizerPasses = Boolean.getBoolean("feldera.optimizer.rustPasses");
        if (verifyWorker || liveFinal)
            options.ioOptions.writeS3 = rustDirectory.resolve("input.json").toString();
        if (replayS2) {
            if (options.ioOptions.inputCircuit)
                throw new IOException("Corpus S2 replay requires optimized output");
            options.ioOptions.inputCircuit = true;
        }
        DBSPCompiler compiler = decoded == null ? new DBSPCompiler(options) : S1Program.loweringCompiler(decoded);
        Observations observations = new Observations(canonicalizer, finalOnly, replayS3 || (liveFinal && finalOnly));
        Map<String, Map<String, Integer>> childPlacements = new TreeMap<>();
        Map<String, Integer> rustPasses = new TreeMap<>();
        Map<String, Integer> castIterations = new TreeMap<>();
        Map<String, Integer> graphCleanups = new TreeMap<>();
        var unusedFieldIterations = new java.util.concurrent.atomic.AtomicInteger();
        var clockScans = new java.util.concurrent.atomic.AtomicInteger();
        var nativeLimiterPlacements = new java.util.concurrent.atomic.AtomicInteger();
        var linearRetentionGraphs = new java.util.concurrent.atomic.AtomicInteger();
        var asofMoves = new java.util.concurrent.atomic.AtomicInteger();
        var temporalSplits = new java.util.concurrent.atomic.AtomicInteger();
        var temporalMergeFallbacks = new java.util.concurrent.atomic.AtomicInteger();
        if (liveFinal) {
            compiler.stateObserver = new DBSPCompiler.StateObserver() {
                public void before(String container, int index, int iteration, CircuitTransform pass, DBSPCircuit input) {
                    if (pass.getName().equals("InsertLimiters"))
                        nativeLimiterPlacements.incrementAndGet();
                }

                public void after(String container, int index, int iteration, CircuitTransform pass,
                                  DBSPCircuit output, boolean completed) {
                    if (completed)
                        childPlacements.computeIfAbsent(container, unused -> new TreeMap<>())
                                .merge(pass.getName(), 1, Integer::sum);
                    if (completed && container.equals("Repeat")
                            && (pass.getName().equals("ExpandSafeCasts") || pass.getName().equals("ExpandUnsafeCasts")))
                        castIterations.merge(pass.getName(), 1, Integer::sum);
                    if (completed && container.equals("Repeat") && pass.getName().equals("UnusedFieldsOnePass"))
                        unusedFieldIterations.incrementAndGet();
                    if (completed && pass.getName().equals("DeadCode"))
                        graphCleanups.merge(container, 1, Integer::sum);
                    if (completed && pass.getName().equals("ContainsNow")
                            && (container.equals("ImplementNow") || container.equals("CheckNow")))
                        clockScans.incrementAndGet();
                    if (completed && container.equals("Repeat") && pass.getName().equals("BreakFilters"))
                        temporalSplits.incrementAndGet();
                    if (completed && container.equals("LinearPostprocessRetainKeys") && pass.getName().equals("Graph"))
                        linearRetentionGraphs.incrementAndGet();
                    if (completed && container.equals("LowerAsof") && pass.getName().equals("MoveGC"))
                        asofMoves.incrementAndGet();
                    if (completed && pass instanceof RustCircuitPass rust) {
                        if (rust.usedGeneralTemporalAnalysis())
                            temporalMergeFallbacks.incrementAndGet();
                        else
                            rustPasses.merge(pass.getName(), 1, Integer::sum);
                    }
                }
            };
        }
        Map<String, Fingerprint> hashes = new TreeMap<>();
        if (capture && !replayS2 && !replayS3 && !(liveFinal && finalOnly))
            compiler.circuitObservationHook = observations::accept;
        // The baseline disables circuit serialization but retains the passive hash callback.
        compiler.hashInputHook = (kind, value) -> hashes.computeIfAbsent(kind, ignored -> new Fingerprint()).add(value);
        DBSPCircuit circuit;
        if (decoded == null) {
            JsonNode submissions = source.get("submissions");
            if (submissions == null || !submissions.isArray() || submissions.isEmpty())
                throw new IOException("Replay requires exported SQL submission boundaries");
            for (JsonNode submission: submissions) {
                if (submission.get("batch").asInt() != 0)
                    throw new IOException("Multiple compilation batches require a state replay: " + source.get("id"));
                if (submission.get("many").asBoolean())
                    compiler.submitStatementsForCompilation(submission.get("sql").asText());
                else
                    compiler.submitStatementForCompilation(submission.get("sql").asText());
            }
            circuit = compiler.getFinalCircuit(false);
        } else {
            var lowered = S1Program.lower(decoded, (org.dbsp.sqlCompiler.compiler.S1LoweringCompiler) compiler);
            circuit = lowered == null ? null : compiler.optimizeFromS2(lowered);
        }
        compiler.throwIfErrorsOccurred();
        if (circuit == null)
            throw new IOException("Corpus case produced no circuit: " + source.get("id"));
        if (replayS2 || replayS3) {
            CircuitReplay.Stage stage = replayS3 ? CircuitReplay.Stage.S3 : CircuitReplay.Stage.S2;
            Path seam = rustDirectory.resolveSibling(rustDirectory.getFileName() + "." + stage + ".json");
            CircuitReplay.write(compiler, circuit, seam, stage);
            // Compare hashes recomputed on import, not those used to export S3.
            hashes.clear();
            options.ioOptions.inputCircuit = false;
            var liveObserver = compiler.stateObserver;
            compiler = DBSPCompiler.forCircuitReplay(options);
            compiler.stateObserver = liveObserver;
            if (!liveFinal || !finalOnly)
                compiler.circuitObservationHook = observations::accept;
            compiler.hashInputHook = (kind, value) ->
                    hashes.computeIfAbsent(kind, ignored -> new Fingerprint()).add(value);
            Path importedSeam = VERIFY_IR ? canonicalizer.roundTripCheckpoint(seam) : seam;
            circuit = CircuitReplay.read(compiler, importedSeam, stage);
            compiler.throwIfErrorsOccurred();
            // On failure retain the seam beside the partial result for diagnosis.
            if (VERIFY_IR)
                Files.delete(importedSeam);
            Files.delete(seam);
        }
        ObjectNode result = MAPPER.createObjectNode();
        if (liveFinal) {
            if (finalOnly) {
                compiler.circuitObservationHook = observations::accept;
                new CircuitDumps(compiler, 0).write("S3", circuit);
                compiler.circuitObservationHook = null;
            }
            result.set("child_placements", MAPPER.valueToTree(childPlacements));
            result.set("rust_passes", MAPPER.valueToTree(rustPasses));
            result.set("cast_iterations", MAPPER.valueToTree(castIterations));
            result.put("unused_field_iterations", unusedFieldIterations.get());
            result.put("clock_scans", clockScans.get());
            result.put("window_sharing_enabled", !compiler.metadata.windowSharingDisabled());
            result.put("noop_iterations", graphCleanups.getOrDefault("RemoveNoops", 0));
            result.put("native_limiter_placements", nativeLimiterPlacements.get());
            result.put("linear_retention_graphs", linearRetentionGraphs.get());
            result.put("asof_retention_moves", asofMoves.get());
            result.put("temporal_filter_iterations", temporalSplits.get());
            result.put("temporal_merge_fallbacks", temporalMergeFallbacks.get());
            result.set("dead_code_by_container", MAPPER.valueToTree(graphCleanups));
            result.put("star_joins_enabled", !compiler.metadata.noStarJoins()
                    && !compiler.metadata.disabledOptimizations().contains("CreateStarJoins"));
            result.put("validate_input_weights", compiler.metadata.enforcePositiveInputs());
        }
        result.set("id", source.get("id"));
        result.set("input_sha256", source.get("input_sha256"));
        result.set("schema", compiler.metadata.asJson());
        result.set("diagnostics", compiler.messages.toJson(compiler.sources));
        result.put("exported_schema_equal", result.get("schema").equals(source.get("schema")));
        result.put("exported_diagnostics_equal", result.get("diagnostics").equals(source.get("diagnostics")));
        if (verifyWorker) {
            P2Artifacts.verify(compiler, circuit, rustDirectory, canonicalizer, finishFingerprints(hashes));
            result.put("worker_artifacts_equal", true);
            return result;
        }
        if (capture)
            result.set("observations", observations.finish());
        if (options.ioOptions.gen2) {
            Gen2Artifacts artifacts = gen2(compiler, circuit, rustDirectory, observations);
            result.set("gen2", SEAM_ARTIFACTS ? artifacts.semantic() : artifacts.bytes());
            if (SEAM_ARTIFACTS)
                result.set("gen2_bytes", artifacts.bytes());
        } else
            result.set("rust", rust(compiler, circuit, rustDirectory));
        compiler.throwIfErrorsOccurred();
        if (coverageOutput != null) {
            coverageOutput.write(java.util.Objects.requireNonNull(observations.coverage).finish(source.get("id").asText()).toString());
            coverageOutput.newLine();
            coverageOutput.flush();
        }
        result.set("hash_inputs", finishFingerprints(hashes));
        return result;
    }

    public static void main(String[] args) throws Exception {
        boolean replayS1 = args.length == 6 && args[4].equals("replay-s1");
        if ((!replayS1 && args.length != 5) || !java.util.Set.of(
                "live-s2", "live-s3", "p2-worker", "capture", "baseline", "replay-s1", "replay-s2", "capture-s3", "replay-s3").contains(args[4]) || (args[4].equals("replay-s1") && !replayS1))
            throw new IllegalArgumentException("Usage: CorpusDeterminism INPUT.jsonl OUTPUT_DIR CANONICALIZER RUST_DIR "
                    + "live-s2|live-s3|p2-worker|capture|baseline|replay-s2|capture-s3|replay-s3|replay-s1 [PROGRAM.jsonl]");
        boolean replayS2 = args[4].equals("replay-s2") || args[4].equals("live-s2");
        boolean replayS3 = args[4].equals("replay-s3");
        boolean liveFinal = args[4].equals("live-s3") || args[4].equals("live-s2");
        boolean finalOnly = (liveFinal && !Boolean.getBoolean("feldera.determinism.fullOptimizerTrace"))
                || replayS3 || args[4].equals("capture-s3");
        boolean verifyWorker = args[4].equals("p2-worker");
        boolean capture = !verifyWorker && !args[4].equals("baseline");
        if (SEAM_ARTIFACTS && !capture)
            throw new IllegalArgumentException("Seam artifacts require captured observations");
        boolean passCoverage = Boolean.getBoolean("feldera.determinism.passCoverage");
        if (passCoverage && (!capture || finalOnly))
            throw new IllegalArgumentException("Pass coverage requires complete-prefix capture or S2 replay");
        boolean compressed = Boolean.getBoolean("feldera.determinism.compress");
        boolean keepGoing = Boolean.getBoolean("feldera.determinism.keepGoing");
        String recordsFile = compressed ? "cases.jsonl.gz" : "cases.jsonl";
        Path directory = Path.of(args[1]);
        Files.createDirectory(directory);
        long count = 0;
        long failed = 0;
        // Never replace an existing output tree; each case cleans up only its own files.
        Path rustDirectory = Path.of(args[3]);
        if (Files.exists(rustDirectory))
            throw new IOException("Rust output directory already exists: " + rustDirectory);
        var registry = replayS1 ? new S1BuiltinOperatorRegistry() : null;
        try (var programs = replayS1 ? Files.newBufferedReader(Path.of(args[5])) : null;
             var coverageOutput = passCoverage ? Files.newBufferedWriter(
                    directory.resolve("s4-paths.jsonl"), StandardOpenOption.CREATE_NEW) : null;
             Canonicalizer canonicalizer = new Canonicalizer(args[2]);
             var input = Files.newBufferedReader(Path.of(args[0]));
             var rawOutput = Files.newOutputStream(directory.resolve(recordsFile), StandardOpenOption.CREATE_NEW);
             var output = new BufferedWriter(new OutputStreamWriter(
                     compressed ? new GZIPOutputStream(rawOutput) : rawOutput, StandardCharsets.UTF_8))) {
            String line;
            while ((line = input.readLine()) != null) {
                JsonNode source = MAPPER.readTree(line);
                System.err.println("Determinism case " + count + ": " + source.get("id").asText());
                JsonNode imported = null;
                if (programs != null) {
                    String programLine = programs.readLine();
                    if (programLine == null) throw new IOException("Missing imported program");
                    imported = MAPPER.readTree(programLine);
                    if (!source.path("id").equals(imported.path("id"))) throw new IOException("Imported program identity differs");
                }
                ObjectNode result;
                // Failed cases retain their artifacts. A distinct directory lets
                // an audit continue without deleting evidence or reusing outputs.
                Path caseOutput = keepGoing ? rustDirectory.resolveSibling(
                        rustDirectory.getFileName() + "-case-" + count) : rustDirectory;
                try {
                    result = compile(source, canonicalizer, capture, verifyWorker, replayS2,
                            finalOnly, replayS3, liveFinal, caseOutput, coverageOutput, imported, registry);
                } catch (RuntimeException | AssertionError | IOException error) {
                    if (!keepGoing)
                        throw error;
                    failed++;
                    error.printStackTrace(System.err);
                    result = MAPPER.createObjectNode();
                    result.set("id", source.get("id"));
                    result.set("input_sha256", source.get("input_sha256"));
                    ObjectNode failure = result.putObject("failure");
                    failure.put("class", error.getClass().getName());
                    failure.put("message", error.toString());
                }
                output.write(result.toString());
                output.newLine();
                output.flush();
                count++;
            }
            if (programs != null && programs.readLine() != null) throw new IOException("Extra imported programs");
        }
        if (count == 0)
            throw new IOException("Empty corpus is not determinism evidence");
        ObjectNode complete = MAPPER.createObjectNode();
        complete.put("schema_version", 1);
        complete.put("cases", count);
        complete.put("failed_cases", failed);
        complete.put("capture", capture);
        complete.put("verify_worker", verifyWorker);
        complete.put("verify_ir", VERIFY_IR);
        complete.put("entry_point", replayS1 ? "S1" : replayS3 ? "S3" : replayS2 ? "S2" : "SQL");
        complete.put("observation_contract", finalOnly ? "final-s3-v1" : "full-prefix-v1");
        if (liveFinal)
            complete.put("initial_generated_name", 0);
        complete.put("artifact_contract", SEAM_ARTIFACTS ? "seam-v1" : "exact-v1");
        complete.put("dataflow_contract", "all-engines-v1");
        complete.put("records_file", recordsFile);
        complete.put("producer", System.getProperty("feldera.determinism.producer", ""));
        complete.put("runtime", System.getProperty("feldera.determinism.runtime", ""));
        try (var input = Files.newInputStream(Path.of(args[0]))) {
            complete.put("input_sha256", DigestUtils.sha256Hex(input));
        }
        Files.writeString(directory.resolve("complete.json"), complete.toString(), StandardOpenOption.CREATE_NEW);
    }
}
