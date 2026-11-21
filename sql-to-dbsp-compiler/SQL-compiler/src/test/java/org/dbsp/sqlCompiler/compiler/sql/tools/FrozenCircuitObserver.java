package org.dbsp.sqlCompiler.compiler.sql.tools;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.codec.digest.DigestUtils;
import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.backend.ToJsonOuterVisitor;
import org.dbsp.sqlCompiler.compiler.backend.CircuitReplay;
import org.dbsp.sqlCompiler.compiler.errors.SourcePositionRange;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.RelReference;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.util.Utilities;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.Objects;
import javax.annotation.Nullable;

/** Observe a pass on the uninterrupted Java compilation of each frozen SQL input. */
abstract class FrozenCircuitObserver implements DBSPCompiler.StateObserver {
    final ObjectNode observation;
    private final DBSPCompiler compiler;
    private final Map<RelReference, String> renderedSql = new HashMap<>();

    FrozenCircuitObserver(DBSPCompiler compiler, ObjectNode observation) {
        this.compiler = compiler;
        this.observation = observation;
    }

    final JsonNode snapshot(DBSPCircuit circuit) {
        return snapshot(snapshotWriter(circuit));
    }

    final ToJsonOuterVisitor snapshotWriter(DBSPCircuit circuit) {
        var writer = ToJsonOuterVisitor.createForReplay(this.compiler, this.renderedSql);
        writer.apply(circuit);
        return writer;
    }

    final JsonNode snapshot(ToJsonOuterVisitor writer) {
        try {
            return Utilities.deterministicObjectMapper().readTree(writer.getJsonString());
        } catch (JsonProcessingException error) {
            throw new IllegalStateException(error);
        }
    }

    abstract void finish();

    /** Observe individual emissions before the compiler filters and deduplicates them. */
    void diagnostic(SourcePositionRange range, boolean warning, boolean continuation, String type, String message) {}

    static void run(Path population, BiFunction<DBSPCompiler, ObjectNode, FrozenCircuitObserver> factory)
            throws IOException {
        run(population, factory, null);
    }

    private static CompilerOptions options(JsonNode source) throws JsonProcessingException {
        var options = Utilities.deterministicObjectMapper().treeToValue(source.required("options"), CompilerOptions.class);
        options.ioOptions.writeS3 = "observed-circuit";
        return options;
    }

    private static void identifyCheckpoint(CompilerOptions options) {
        options.ioOptions.dumpProducerCommit = System.getProperty("feldera.determinism.producer", "pass-corpus-oracle");
        options.ioOptions.dumpRuntimeAbi = System.getProperty("feldera.determinism.runtime", "pass-corpus-oracle");
    }

    private static DBSPCircuit compile(DBSPCompiler compiler, JsonNode source) {
        for (var submission : source.required("submissions")) {
            if (submission.required("batch").asInt() != 0)
                throw new IllegalArgumentException("Expected one compilation batch at " + source.get("id"));
            if (submission.required("many").asBoolean())
                compiler.submitStatementsForCompilation(submission.required("sql").asText());
            else
                compiler.submitStatementForCompilation(submission.required("sql").asText());
        }
        var circuit = compiler.getFinalCircuit(false);
        compiler.throwIfErrorsOccurred();
        return Objects.requireNonNull(circuit);
    }

    /** Capture one S2 per population row so both implementations observe the same SQL plan IDs. */
    static void captureS2(Path population, Path directory) throws IOException {
        Files.createDirectory(directory);
        var mapper = Utilities.deterministicObjectMapper();
        try (var reader = Files.newBufferedReader(population)) {
            String line;
            while ((line = reader.readLine()) != null) {
                var source = mapper.readTree(line);
                var options = options(source);
                identifyCheckpoint(options);
                options.ioOptions.inputCircuit = true;
                DBSPVariablePath.reset();
                var compiler = new DBSPCompiler(options);
                CircuitReplay.write(compiler, compile(compiler, source), directory.resolve(DigestUtils.sha256Hex(line) + ".json"));
            }
        }
    }

    static void run(Path population, BiFunction<DBSPCompiler, ObjectNode, FrozenCircuitObserver> factory,
                    @Nullable Path checkpoints) throws IOException {
        var mapper = Utilities.deterministicObjectMapper();
        try (var reader = Files.newBufferedReader(population)) {
            String line;
            while ((line = reader.readLine()) != null) {
                JsonNode source = mapper.readTree(line);
                var record = mapper.createObjectNode();
                record.set("id", source.required("id"));
                record.set("input_sha256", source.required("input_sha256"));
                var options = options(source);
                if (checkpoints != null)
                    identifyCheckpoint(options);
                DBSPVariablePath.reset();
                var compiler = new DBSPCompiler(options, checkpoints != null) {
                    @Override
                    public void reportProblem(SourcePositionRange range, boolean warning, boolean continuation,
                                              String type, String message) {
                        if (this.stateObserver instanceof FrozenCircuitObserver observer)
                            observer.diagnostic(range, warning, continuation, type, message);
                        super.reportProblem(range, warning, continuation, type, message);
                    }
                };
                var observer = factory.apply(compiler, record);
                compiler.stateObserver = observer;
                record.put("gen2", options.ioOptions.gen2);
                if (checkpoints == null) {
                    compile(compiler, source);
                } else {
                    Path checkpoint = checkpoints.resolve(DigestUtils.sha256Hex(line) + ".json");
                    try (var bytes = Files.newInputStream(checkpoint)) {
                        record.put("s2_sha256", DigestUtils.sha256Hex(bytes));
                    }
                    CircuitReplay.read(compiler, checkpoint, CircuitReplay.Stage.S2);
                    compiler.throwIfErrorsOccurred();
                }
                observer.finish();
                System.out.println(mapper.writeValueAsString(record));
                if (System.out.checkError())
                    throw new IOException("Rust pass consumer closed its input");
            }
        }
    }
}
