package org.dbsp.sqlCompiler.compiler.sql.tools;

import org.apache.calcite.rex.RexBuilder;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.S1LoweringCompiler;
import org.dbsp.sqlCompiler.compiler.backend.CircuitDumps;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.S1RelTypeFactory;
import org.dbsp.util.Utilities;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;

/** Compare S2 construction using imported statements. The imported compiler obtains its
 * options, sources, and pending lateness checks from the typed program.
 * Only frontend diagnostics are replayed; lowering regenerates its own diagnostics. */
public final class S1LoweringProbe {
    private S1LoweringProbe() { }

    public static void main(String[] args) throws IOException {
        if (args.length != 5 || (!args[0].equals("baseline") && !args[0].equals("import")))
            throw new IOException("usage: S1LoweringProbe baseline|import INPUT.jsonl PROGRAM.jsonl OUTPUT_DIR CANONICALIZER");
        var mapper = Utilities.deterministicObjectMapper().copy()
                .enable(com.fasterxml.jackson.core.JsonParser.Feature.STRICT_DUPLICATE_DETECTION)
                .enable(com.fasterxml.jackson.databind.DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
        var registry = new S1BuiltinOperatorRegistry();
        var directory = Path.of(args[3]); Files.createDirectory(directory);
        long cases = 0;
        try (var inputs = Files.newBufferedReader(Path.of(args[1]));
             var programs = Files.newBufferedReader(Path.of(args[2]));
             var output = Files.newBufferedWriter(directory.resolve("cases.jsonl"));
             var canonicalizer = new CorpusDeterminism.Canonicalizer(args[4])) {
            String line;
            while ((line = inputs.readLine()) != null) {
                var input = mapper.readTree(line);
                String programLine = programs.readLine();
                if (programLine == null) throw new IOException("Missing imported program");
                var program = mapper.readTree(programLine);
                if (!input.path("id").equals(program.path("id"))) throw new IOException("Program identity differs");
                var options = mapper.treeToValue(input.path("options"), CompilerOptions.class);
                options.ioOptions.inputCircuit = true;
                options.ioOptions.dumpProducerCommit = "p0-s1-lowering";
                options.ioOptions.dumpRuntimeAbi = "c6ede756d";
                DBSPCompiler compiler;
                org.dbsp.sqlCompiler.circuit.DBSPCircuit circuit;
                try {
                    if (args[0].equals("baseline")) {
                        var compiled = S1ExpressionCorpusProbe.compileResult(input, instance -> {
                            instance.options.ioOptions.dumpProducerCommit = options.ioOptions.dumpProducerCommit;
                            instance.options.ioOptions.dumpRuntimeAbi = options.ioOptions.dumpRuntimeAbi;
                        });
                        compiler = compiled.compiler(); circuit = compiled.circuit();
                    } else {
                        var decoded = S1Program.decode(program, registry);
                        var restoredOptions = decoded.context().options();
                        restoredOptions.ioOptions.dumpProducerCommit = options.ioOptions.dumpProducerCommit;
                        restoredOptions.ioOptions.dumpRuntimeAbi = options.ioOptions.dumpRuntimeAbi;
                        var imported = new S1LoweringCompiler(restoredOptions, new RexBuilder(S1RelTypeFactory.create()),
                                decoded.relations().expressions().operators());
                        decoded.context().restore(imported);
                        compiler = imported; circuit = imported.lower(decoded.statements(),
                                index -> decoded.diagnostics().before().get(index).apply(imported),
                                () -> decoded.diagnostics().after().apply(imported));
                    }
                    if (circuit == null || compiler.hasErrors()) throw new IOException("Lowering failed: " + compiler.messages);
                    var observations = new ArrayList<String>(); compiler.circuitObservationHook = observations::add;
                    new CircuitDumps(compiler, 0).write("S2", circuit);
                    if (observations.size() != 1) throw new IOException("Expected exactly one S2 observation");
                    var record = mapper.createObjectNode(); record.set("id", input.path("id"));
                    record.set("circuit", mapper.readTree(canonicalizer.apply(observations.get(0))));
                    record.set("schema", compiler.metadata.asJson());
                    record.set("diagnostics", compiler.messages.toJson(compiler.sources));
                    output.write(record.toString()); output.newLine(); output.flush(); cases++;
                } catch (RuntimeException | IOException error) {
                    throw new IOException("S1 lowering failed for " + input.path("id"), error);
                }
            }
            if (programs.readLine() != null || cases == 0) throw new IOException("Program population differs");
        }
        var complete = mapper.createObjectNode().put("complete", true).put("cases", cases).put("S1_parity_proven", false)
                .put("scope", "S2 construction; typed options/source context; frontend diagnostics replayed between lowering calls");
        Files.writeString(directory.resolve("complete.json"), complete.toPrettyString());
    }
}
