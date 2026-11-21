package org.dbsp.sqlCompiler.compiler.backend;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.RelReference;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitTransform;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.compiler.visitors.outer.monotonicity.InsertLimiters;
import org.dbsp.sqlCompiler.compiler.visitors.outer.monotonicity.LimiterInputs;
import org.dbsp.util.Utilities;
import org.dbsp.util.IndentStream;
import org.dbsp.util.JsonStream;
import com.fasterxml.jackson.databind.util.RawValue;
import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Map;
import java.util.HashMap;
import java.io.UncheckedIOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Locale;

/** Internal differential-test observations, separate from the public Gen-2 payload.
 * No dump is a replay checkpoint: compiler metadata and pass side state are incomplete. */
public final class CircuitDumps {
    private final DBSPCompiler compiler;
    private final Map<RelReference, String> renderedSql = new HashMap<>();
    @Nullable private final Path directory;
    private final ObjectMapper mapper = Utilities.deterministicObjectMapper();
    private final ArrayNode passPath = this.mapper.createArrayNode();
    private int sequence = 0;

    public CircuitDumps(DBSPCompiler compiler, int compilation) {
        this.compiler = compiler;
        CompilerOptions.IO options = compiler.options.ioOptions;
        if (options.dumpProducerCommit.isBlank() || options.dumpRuntimeAbi.isBlank())
            throw new IllegalArgumentException("Circuit dumps require --dump-producer-commit and --dump-runtime-abi");
        if (compiler.circuitObservationHook != null) {
            if (!options.dumpCircuits.isEmpty())
                throw new IllegalArgumentException("Choose streaming observations or a dump directory, not both");
            this.directory = null;
            return;
        }
        Path root = Path.of(options.dumpCircuits);
        try {
            Files.createDirectories(root);
            for (int index = compilation; ; index++) {
                Path path = root.resolve(String.format(Locale.ROOT, "compilation-%04d", index));
                try {
                    Files.createDirectory(path);
                } catch (FileAlreadyExistsException ignored) {
                    continue;
                }
                this.directory = path;
                break;
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot create a circuit dump in " + root
                    + ". Check that the directory is writable.", e);
        }
    }

    /** Capture effective options, excluding locations and presentation-only settings.
     * Serialize the public fields so a newly added compiler option is captured by default. */
    static ObjectNode effectiveOptions(CompilerOptions compilerOptions) {
        ObjectNode options = Utilities.deterministicObjectMapper().valueToTree(compilerOptions);
        options.remove("help");
        ObjectNode io = (ObjectNode) options.get("ioOptions");
        io.remove(List.of("dumpCircuits", "dumpProducerCommit", "dumpRuntimeAbi",
                "inputFile", "outputFile", "errorFile", "emitJsonSchema", "emitPlan", "emitDataflow",
                "loggingLevel", "verbosity", "quiet", "showVersion", "crates", "readS2", "writeS2", "readS3", "writeS3",
                "rustOptimizerPasses"));
        io.put("multiCrates", compilerOptions.ioOptions.multiCrates());
        return options;
    }

    public void write(String stage, DBSPCircuit circuit) {
        var text = new IndentStream(new StringBuilder()).setIndentAmount(this.directory == null ? 0 : 1);
        var inner = new ToJsonInnerVisitor(this.compiler, new JsonStream(text, true), 1);
        var visitor = new ToJsonOuterVisitor(this.compiler, 1, inner);
        visitor.apply(circuit);
        ObjectNode header = this.mapper.createObjectNode();
        header.put("schema_version", 1);
        header.put("producer_commit", this.compiler.options.ioOptions.dumpProducerCommit);
        header.put("runtime_abi", this.compiler.options.ioOptions.dumpRuntimeAbi);
        header.put("engine_mode", this.compiler.options.ioOptions.gen2 ? "gen2" : "classic");
        header.set("options", effectiveOptions(this.compiler.options));
        header.put("replayable", false);
        ObjectNode observation = header.putObject("observation");
        observation.put("stage", stage);
        observation.put("sequence", this.sequence);
        observation.set("path", this.passPath);
        if (this.directory == null) {
            header.putRawValue("circuit", new RawValue(visitor.getJsonString()));
            java.util.Objects.requireNonNull(this.compiler.circuitObservationHook).accept(header.toString());
            this.sequence++;
            return;
        }
        Path file = this.directory.resolve(String.format(Locale.ROOT, "%06d-%s.json", this.sequence, stage));
        try (var writer = Files.newBufferedWriter(file, StandardOpenOption.CREATE_NEW);
             JsonGenerator json = this.mapper.getFactory().createGenerator(writer)) {
            json.useDefaultPrettyPrinter();
            json.writeStartObject();
            var fields = header.fields();
            while (fields.hasNext()) {
                var field = fields.next();
                json.writeFieldName(field.getKey());
                json.writeTree(field.getValue());
            }
            json.writeFieldName("circuit");
            // Preserve the serializer's numeric precision and duplicate-key evidence.
            json.writeRawValue(visitor.getJsonString());
            json.writeEndObject();
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot write circuit dump " + file, e);
        }
        this.sequence++;
    }

    private void push(String container, String pass, int index, int iteration) {
        ObjectNode frame = this.passPath.addObject();
        frame.put("container", container);
        frame.put("pass", pass);
        frame.put("index", index);
        frame.put("iteration", iteration);
    }

    /** Observe a circuit after an in-place operation, such as topological sorting. */
    public void observe(String container, int index, String operation, DBSPCircuit circuit) {
        this.push(container, operation, index, 0);
        try {
            this.write("S4", circuit);
        } finally {
            this.passPath.remove(this.passPath.size() - 1);
        }
    }

    /** Observe a pass within its enclosing pass/iteration path, including nested calls. */
    public DBSPCircuit apply(String container, int index, int iteration,
                             CircuitTransform pass, DBSPCircuit circuit) {
        this.push(container, pass.getName(), index, iteration);
        try {
            boolean captureState = this.directory != null
                    && ((pass.getName().equals("FuseIntoArrayAgg") && Boolean.getBoolean("feldera.dump.passState"))
                    || (pass instanceof InsertLimiters && Boolean.getBoolean("feldera.dump.limiterState")));
            ObjectNode state = captureState ? this.mapper.createObjectNode() : null;
            if (state != null && pass instanceof InsertLimiters limiters) {
                state.set("operators", LimiterInputs.capture(limiters, circuit));
                state.put("gen2", this.compiler.options.ioOptions.gen2);
                this.writeLimiterCheckpoint("input", circuit);
                state.put("weight_type", org.dbsp.sqlCompiler.ir.type.user.DBSPTypeWeight.INSTANCE.getId());
            }
            int entrySequence = this.sequence;
            long nextName = captureState ? DBSPVariablePath.nextGeneratedName() : 0;
            if (state != null && pass instanceof InsertLimiters) {
                state.put("next_generated_name", nextName);
                this.writeLimiterFile("entry", state.toString());
            }
            DBSPCircuit result = pass.apply(circuit);
            if (state != null) {
                state.put("entry_sequence", entrySequence);
                state.put("exit_sequence", this.sequence);
                state.set("path", this.passPath);
                state.put("next_generated_name", nextName);
                state.put("next_generated_name_after", DBSPVariablePath.nextGeneratedName());
                if (pass instanceof InsertLimiters)
                    this.writeLimiterCheckpoint("result", result);
                try {
                    Files.writeString(this.directory.resolve("pass-state.jsonl"), state + "\n",
                            StandardOpenOption.CREATE, StandardOpenOption.APPEND);
                } catch (IOException e) {
                    throw new UncheckedIOException("Cannot write pass state", e);
                }
            }
            if (this.compiler.messages.exitCode == 0)
                this.write("S4", result);
            return result;
        } finally {
            this.passPath.remove(this.passPath.size() - 1);
        }
    }

    private void writeLimiterCheckpoint(String role, DBSPCircuit circuit) {
        ToJsonOuterVisitor writer = ToJsonOuterVisitor.createForReplay(this.compiler, this.renderedSql);
        circuit.accept(writer);
        this.writeLimiterFile(role, writer.getJsonString());
    }

    private void writeLimiterFile(String role, String value) {
        String file = String.format(Locale.ROOT, "%06d-limiters-%s.json", this.sequence, role);
        try {
            Files.writeString(java.util.Objects.requireNonNull(this.directory).resolve(file),
                    value, StandardOpenOption.CREATE_NEW);
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot write limiter checkpoint", e);
        }
    }

    /** Written only after successful compilation; incomplete directories have no marker. */
    public void complete(String finalStage) {
        // The streaming consumer owns its completion record after checking all outputs.
        if (this.directory == null)
            return;
        ObjectNode manifest = this.mapper.createObjectNode();
        manifest.put("schema_version", 1);
        manifest.put("dump_count", this.sequence);
        manifest.put("final_stage", finalStage);
        try {
            Files.writeString(this.directory.resolve("complete.json"),
                    this.mapper.writeValueAsString(manifest), StandardOpenOption.CREATE_NEW);
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot complete circuit dumps in " + this.directory, e);
        }
    }
}
