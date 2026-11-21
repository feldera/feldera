package org.dbsp.sqlCompiler.compiler.sql.tools;

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.optimizer.RustFoldExecutor;
import org.dbsp.util.Utilities;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashSet;
import java.util.Set;

/** Captures folding at level 2 with the exported SQL submission boundaries. */
public final class FoldProbe {
    private FoldProbe() { }

    public static void main(String[] args) throws Exception {
        boolean rust = args.length == 5 && args[3].equals("rust");
        String stage = System.getProperty("feldera.fold.stage", "S2");
        if (!stage.equals("S2") && !stage.equals("S3"))
            throw new IllegalArgumentException("Fold observation stage must be S2 or S3");
        if (args.length != 3 && !(args.length == 4 && args[3].equals("baseline")) && !rust)
            throw new IllegalArgumentException("Usage: FoldProbe INPUT.jsonl CASES.jsonl FOLDS.jsonl [baseline | rust EXECUTABLE]");
        Path folds = Path.of(args[2]);
        Files.createFile(folds);
        if (args.length == 3 || rust)
            System.setProperty("feldera.fold.trace", folds.toString());
        RustFoldExecutor worker = rust ? new RustFoldExecutor(args[4], Path.of(args[2] + ".worker.jsonl")) : null;
        Set<String> ids = new HashSet<>();
        var mapper = Utilities.deterministicObjectMapper();
        try (var input = Files.newBufferedReader(Path.of(args[0]));
             var output = Files.newBufferedWriter(Path.of(args[1]), StandardOpenOption.CREATE_NEW)) {
            String line;
            while ((line = input.readLine()) != null) {
                JsonNode request = mapper.readTree(line);
                String id = request.path("id").asText();
                if (id.isEmpty() || !ids.add(id))
                    throw new IllegalArgumentException("Duplicate/empty case identity");
                System.setProperty("feldera.fold.case", id);
                CompilerOptions options = request.has("options")
                        ? mapper.treeToValue(request.get("options"), CompilerOptions.class) : new CompilerOptions();
                options.ioOptions.quiet = true;
                options.ioOptions.inputCircuit = stage.equals("S2");
                options.languageOptions.throwOnError = false;
                options.languageOptions.optimizationLevel = 2;
                DBSPCompiler compiler = new DBSPCompiler(options);
                if (worker != null)
                    compiler.getSqlToRelCompiler().cluster.getPlanner().setExecutor(worker);
                var result = mapper.createObjectNode();
                result.put("id", id);
                result.put("stage", stage);
                long start = Files.size(folds);
                try {
                    if (request.has("submissions")) {
                        for (JsonNode submission: request.get("submissions")) {
                            if (submission.path("batch").asInt(-1) != 0)
                                throw new IllegalArgumentException("Multiple batches need explicit state replay");
                            if (submission.path("many").asBoolean())
                                compiler.submitStatementsForCompilation(submission.path("sql").asText());
                            else
                                compiler.submitStatementForCompilation(submission.path("sql").asText());
                        }
                    } else {
                        compiler.submitStatementsForCompilation(request.path("program").asText());
                    }
                    compiler.getFinalCircuit(false);
                    result.put("accepted", !compiler.hasErrors());
                    result.put("compiler_bug", compiler.messages.messages.stream().anyMatch(message ->
                            message.errorType.equals("Compiler error") || message.errorType.startsWith("This is a bug")));
                } catch (RuntimeException | AssertionError error) {
                    result.put("accepted", false);
                    result.put("compiler_bug", true);
                    result.put("exception", error.getClass().getName());
                    result.put("detail", error.getMessage());
                }
                result.put("fold_bytes", Files.size(folds) - start);
                result.set("diagnostics", compiler.messages.toJson(compiler.sources));
                result.set("schema", compiler.metadata.asJson());
                output.write(mapper.writeValueAsString(result));
                output.newLine();
                output.flush();
            }
        } finally {
            if (worker != null)
                worker.close();
            System.clearProperty("feldera.fold.trace");
            System.clearProperty("feldera.fold.case");
        }
    }
}
