package org.dbsp.sqlCompiler.compiler.sql.tools;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectReader;
import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.backend.CircuitReplay;
import org.dbsp.sqlCompiler.compiler.backend.ToJsonOuterVisitor;
import org.dbsp.util.Utilities;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.TreeMap;

/** Compare published worker artifacts with the live Java compiler before it releases the circuit. */
final class P2Artifacts {
    private static final ObjectReader JSON = Utilities.deterministicObjectMapper().reader()
            .with(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);

    private static void run(String... args) throws IOException {
        Process process = new ProcessBuilder(args)
                .redirectError(ProcessBuilder.Redirect.INHERIT)
                .redirectOutput(ProcessBuilder.Redirect.DISCARD).start();
        try {
            if (process.waitFor() != 0)
                throw new IOException(args[0] + " failed for " + args[args.length - 1]);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException(e);
        }
    }

    private static JsonNode read(Path file) throws IOException {
        try (var input = Files.newInputStream(file)) {
            return JSON.readTree(input);
        }
    }

    private static void compare(Path file, JsonNode expected) throws IOException {
        if (!read(file).equals(expected)) {
            Files.writeString(file.resolveSibling("java-" + file.getFileName()), expected.toString());
            throw new IOException("Rust and Java differ for " + file);
        }
    }

    record RuntimeCircuit(DBSPCompiler compiler, DBSPCircuit circuit) {}

    /** The classic printer needs the catalog and sources restored alongside the worker's circuit. */
    static RuntimeCircuit classicRuntime(JsonNode record, Path directory, String worker) throws IOException {
        var effective = record.has("compilation_options") ? record.get("compilation_options") : record.required("options");
        CompilerOptions options = Utilities.deterministicObjectMapper().treeToValue(effective, CompilerOptions.class);
        if (options.ioOptions.gen2)
            throw new IOException("Classic runtime replay requires classic compiler options");
        var seam = directory.resolve("front.json");
        options.ioOptions.writeS3 = seam.toString();
        options.ioOptions.dumpProducerCommit = record.required("producer_commit").asText();
        options.ioOptions.dumpRuntimeAbi = record.required("runtime_commit").asText();
        options.ioOptions.rustOptimizerPasses = Boolean.getBoolean("feldera.runtime.rustOptimizerPasses");
        var compiler = new DBSPCompiler(options);
        for (var submission : record.required("submissions")) {
            if (submission.required("batch").asInt() != 0)
                throw new IOException("Multiple compilation batches require state replay");
            var sql = submission.required("sql").asText();
            if (submission.required("many").asBoolean())
                compiler.submitStatementsForCompilation(sql);
            else
                compiler.submitStatementForCompilation(sql);
        }
        var direct = compiler.getFinalCircuit(false);
        compiler.throwIfErrorsOccurred();
        if (direct == null)
            throw new IOException("SQL submission produced no circuit");
        CircuitReplay.write(compiler, direct, seam, CircuitReplay.Stage.S3);
        var attempt = directory.resolve("worker");
        run(worker, "--s3-worker", options.ioOptions.dumpProducerCommit, options.ioOptions.dumpRuntimeAbi,
                seam.toString(), attempt.toString());
        var replay = DBSPCompiler.forCircuitReplay(options);
        var circuit = CircuitReplay.read(replay, attempt.resolve("result/s3.json"), CircuitReplay.Stage.S3);
        replay.throwIfErrorsOccurred();
        if (circuit == null)
            throw new IOException("Worker checkpoint produced no circuit");
        return new RuntimeCircuit(replay, circuit);
    }

    static void verify(DBSPCompiler compiler, DBSPCircuit circuit, Path directory,
                       CorpusDeterminism.Canonicalizer comparator, JsonNode expectedHashes)
            throws IOException {
        String worker = System.getProperty("feldera.p2.worker");
        if (worker == null)
            throw new IOException("Set feldera.p2.worker to the sql2dbsp executable");
        Files.createDirectory(directory);
        Path seam = directory.resolve("input.json");
        CircuitReplay.write(compiler, circuit, seam, CircuitReplay.Stage.S3);
        Path attempt = directory.resolve("attempt");
        run(worker, "--s3-worker", compiler.options.ioOptions.dumpProducerCommit,
                compiler.options.ioOptions.dumpRuntimeAbi, seam.toString(), attempt.toString());
        Path result = attempt.resolve("result");
        if (!read(result.resolve("result.json")).path("status").asText().equals("ok"))
            throw new IOException("Worker did not publish a successful attempt in " + result);
        try (var stream = new PrintStream(directory.resolve("dataflow.json").toFile(), StandardCharsets.UTF_8)) {
            compiler.getDataflow(stream, circuit);
        }
        compare(result.resolve("dataflow.json"), read(directory.resolve("dataflow.json")));
        compare(result.resolve("schema.json"), compiler.metadata.asJson());
        compare(result.resolve("diagnostics.json"), compiler.messages.toJson(compiler.sources));
        boolean gen2 = compiler.options.ioOptions.gen2;
        Path importedSeam = gen2 ? comparator.roundTripCheckpoint(seam) : result.resolve("s3.json");
        var restored = DBSPCompiler.forCircuitReplay(compiler.options);
        var hashes = new TreeMap<String, CorpusDeterminism.Fingerprint>();
        restored.hashInputHook = (kind, value) ->
                hashes.computeIfAbsent(kind, ignored -> new CorpusDeterminism.Fingerprint()).add(value);
        var resumed = CircuitReplay.read(restored, importedSeam, CircuitReplay.Stage.S3);
        restored.throwIfErrorsOccurred();
        if (resumed == null || !CorpusDeterminism.finishFingerprints(hashes).equals(expectedHashes))
            throw new IOException("Imported S3 did not reproduce the Java hash inputs for " + directory);
        if (gen2) {
            circuit.setName("circuit");
            var writer = ToJsonOuterVisitor.create(compiler, 1);
            writer.apply(circuit);
            Path expected = directory.resolve("circuit.json");
            Files.writeString(expected, writer.getJsonString());
            run(comparator.executable, expected.toString(), result.resolve("circuit.json").toString());
            resumed.setName("circuit");
            var restoredWriter = ToJsonOuterVisitor.create(restored, 1);
            restoredWriter.apply(resumed);
            var restoredFile = directory.resolve("resumed.json");
            Files.writeString(restoredFile, restoredWriter.getJsonString());
            run(comparator.executable, expected.toString(), restoredFile.toString());
        } else {
            compare(result.resolve("s3.json"), read(seam));
            var direct = CorpusDeterminism.rust(compiler, circuit, directory.resolve("direct"));
            var imported = CorpusDeterminism.rust(restored, resumed, directory.resolve("resumed"));
            if (!direct.equals(imported))
                throw new IOException("Generated Rust workspace or dataflow differs after S3 import: " + directory);
        }
        try (var files = Files.walk(directory)) {
            for (var file : files.sorted(Comparator.reverseOrder()).toList())
                Files.delete(file);
        }
    }
}
