package org.dbsp.sqlCompiler.compiler.sql.tools;

import com.fasterxml.jackson.databind.node.ArrayNode;
import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.circuit.operator.DBSPNestedOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.backend.ToJsonOuterVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitTransform;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashSet;
import java.util.zip.GZIPOutputStream;

/** Record the live stream kinds after each original top-level optimizer pass. */
public final class StreamKindsProbe {
    private StreamKindsProbe() {}

    private static void appendKinds(Iterable<DBSPOperator> operators, ArrayNode result) {
        for (var operator : operators) {
            if (operator instanceof DBSPNestedOperator region)
                appendKinds(region.getAllOperators(), result);
            var ports = result.addArray();
            for (int port = 0; port < operator.outputCount(); port++) {
                if (operator.hasOutput(port))
                    ports.add(operator.outputKind(port).name());
                else
                    ports.addNull();
            }
        }
    }

    public static void main(String[] args) throws IOException {
        if (args.length != 4 || !(args[2].equals("classic") || args[2].equals("gen2"))
                || !(args[3].equals("sets") || args[3].equals("bags")))
            throw new IllegalArgumentException("usage: StreamKindsProbe INPUT.jsonl OUTPUT.jsonl.gz classic|gen2 sets|bags");
        var mapper = Utilities.deterministicObjectMapper();
        var identities = new HashSet<String>();
        try (var input = Files.newBufferedReader(Path.of(args[0]));
             var output = new BufferedWriter(new OutputStreamWriter(new GZIPOutputStream(
                     Files.newOutputStream(Path.of(args[1]), StandardOpenOption.CREATE_NEW)), StandardCharsets.UTF_8))) {
            String line;
            while ((line = input.readLine()) != null) {
                var test = mapper.readTree(line);
                var identity = test.required("id").textValue();
                if (identity == null || !identities.add(identity))
                    throw new IllegalArgumentException("Missing or duplicate stream-kind case identity");
                var options = MultisetProbe.options("S3", args[2]);
                options.languageOptions.outputsAreSets = args[3].equals("sets");
                var compiler = new DBSPCompiler(options);
                compiler.stateObserver = new DBSPCompiler.StateObserver() {
                    @Override
                    public void before(String container, int index, int iteration,
                                       CircuitTransform pass, DBSPCircuit circuit) {}

                    @Override
                    public void after(String container, int index, int iteration,
                                      CircuitTransform pass, @Nullable DBSPCircuit circuit, boolean completed) {
                        if (!container.equals("Optimizer") || !completed || circuit == null || compiler.hasErrors())
                            return;
                        try {
                            var record = mapper.createObjectNode();
                            record.put("id", identity + "/" + index + "/" + pass.getName());
                            appendKinds(circuit.allOperators, record.putArray("expected"));
                            var writer = ToJsonOuterVisitor.createForReplay(compiler);
                            writer.apply(circuit);
                            record.set("circuit", mapper.readTree(writer.getJsonString()));
                            output.write(mapper.writeValueAsString(record));
                            output.newLine();
                        } catch (IOException error) {
                            throw new UncheckedIOException(error);
                        }
                    }
                };
                compiler.submitStatementsForCompilation(test.required("sql").textValue());
                if (compiler.getFinalCircuit(false) == null)
                    throw new IllegalStateException("Missing final circuit for " + identity);
                compiler.throwIfErrorsOccurred();
            }
        }
        if (identities.isEmpty())
            throw new IllegalArgumentException("Stream-kind capture needs a nonempty input");
    }
}
