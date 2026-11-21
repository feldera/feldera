package org.dbsp.sqlCompiler.compiler.sql.tools;

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.backend.JsonDecoder;
import org.dbsp.util.Utilities;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashSet;
import java.util.Map;
import java.util.TreeMap;

/** Counts actual boundary forms before selecting replay coverage. No outcome is filtered. */
public final class SeamInventory {
    private SeamInventory() { }

    public static void main(String[] args) throws Exception {
        if (args.length != 3 || !(args[2].equals("S2") || args[2].equals("S3")))
            throw new IllegalArgumentException("Usage: SeamInventory INPUT.jsonl OUTPUT.jsonl S2|S3");
        var mapper = Utilities.deterministicObjectMapper();
        var ids = new HashSet<String>();
        try (var input = Files.newBufferedReader(Path.of(args[0]));
             var output = Files.newBufferedWriter(Path.of(args[1]), StandardOpenOption.CREATE_NEW)) {
            String line;
            while ((line = input.readLine()) != null) {
                JsonNode request = mapper.readTree(line);
                String id = request.path("id").asText();
                if (id.isBlank() || !ids.add(id))
                    throw new IllegalArgumentException("Duplicate or empty corpus identity");
                CompilerOptions options = mapper.treeToValue(request.get("options"), CompilerOptions.class);
                options.ioOptions.inputCircuit = args[2].equals("S2");
                options.ioOptions.quiet = true;
                options.languageOptions.throwOnError = false;
                DBSPCompiler compiler = new DBSPCompiler(options);
                var result = mapper.createObjectNode();
                result.put("id", id);
                result.put("stage", args[2]);
                Map<String, Integer> operators = new TreeMap<>();
                Map<String, Integer> declarations = new TreeMap<>();
                Map<String, String> decoderOwners = new TreeMap<>();
                try {
                    for (JsonNode submission: request.required("submissions")) {
                        if (submission.path("batch").asInt(-1) != 0)
                            throw new IllegalArgumentException("Multiple batches need state replay");
                        if (submission.path("many").asBoolean())
                            compiler.submitStatementsForCompilation(submission.required("sql").asText());
                        else
                            compiler.submitStatementForCompilation(submission.required("sql").asText());
                    }
                    var circuit = compiler.getFinalCircuit(false);
                    if (circuit != null) {
                        for (var operator: circuit.allOperators) {
                            Class<?> type = operator.getClass();
                            operators.merge(type.getSimpleName(), 1, Integer::sum);
                            decoderOwners.put(type.getSimpleName(), decoderOwner(type));
                        }
                        for (var declaration: circuit.declarations) {
                            Class<?> type = declaration.item.getClass();
                            declarations.merge(type.getSimpleName(), 1, Integer::sum);
                            decoderOwners.put(type.getSimpleName(), decoderOwner(type));
                        }
                    }
                    result.put("accepted", circuit != null && !compiler.hasErrors());
                    result.put("compiler_bug", compiler.messages.messages.stream().anyMatch(message ->
                            message.errorType.equals("Compiler error") || message.errorType.startsWith("This is a bug")));
                } catch (RuntimeException | AssertionError error) {
                    result.put("accepted", false);
                    result.put("compiler_bug", true);
                    result.put("exception", error.toString());
                }
                result.set("operators", mapper.valueToTree(operators));
                result.set("declarations", mapper.valueToTree(declarations));
                result.set("decoder_owners", mapper.valueToTree(decoderOwners));
                result.set("diagnostics", compiler.messages.toJson(compiler.sources));
                output.write(mapper.writeValueAsString(result));
                output.newLine();
                output.flush();
            }
        }
    }

    private static String decoderOwner(Class<?> type) {
        try {
            return type.getMethod("fromJson", JsonNode.class, JsonDecoder.class).getDeclaringClass().getSimpleName();
        } catch (NoSuchMethodException error) {
            return "missing";
        }
    }
}
