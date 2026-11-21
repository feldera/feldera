package org.dbsp.sqlCompiler.compiler.sql.tools;

import org.apache.calcite.plan.RelOptUtil;
import org.dbsp.util.Utilities;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashSet;

/** Reconstruct Rust recipes without invoking a Java rewrite rule. */
public final class CorrelationRecipeProbe {
    private CorrelationRecipeProbe() { }

    public static void main(String[] args) throws IOException {
        if (args.length != 3)
            throw new IOException("usage: CorrelationRecipeProbe FRAGMENTS.jsonl RECIPES.jsonl OUTPUT_DIR");
        var mapper = Utilities.deterministicObjectMapper().copy()
                .enable(com.fasterxml.jackson.core.JsonParser.Feature.STRICT_DUPLICATE_DETECTION);
        var output = Path.of(args[2]); Files.createDirectory(output);
        var registry = new S1BuiltinOperatorRegistry();
        var identities = new HashSet<String>();
        long rewritten = 0, refused = 0;
        try (var fragments = Files.newBufferedReader(Path.of(args[0]));
             var recipes = Files.newBufferedReader(Path.of(args[1]));
             var results = Files.newBufferedWriter(output.resolve("cases.jsonl"), StandardOpenOption.CREATE_NEW)) {
            String line;
            while ((line = fragments.readLine()) != null) {
                var recipeLine = recipes.readLine();
                if (recipeLine == null || line.length() > 16 * 1024 * 1024
                        || recipeLine.length() > 64 * 1024 * 1024 || identities.size() >= 20_000)
                    throw new IOException("Missing recipe or input limit exceeded");
                var fragment = mapper.readTree(line); var recipe = mapper.readTree(recipeLine);
                var id = fragment.path("id");
                if (!id.isTextual() || id.asText().isEmpty() || id.asText().length() > 4096
                        || !identities.add(id.asText()) || !id.equals(recipe.path("id")))
                    throw new IOException("Missing, duplicate or mismatched fragment identity");
                var row = mapper.createObjectNode(); row.set("id", id);
                String status = recipe.path("status").asText(); row.put("status", status);
                if (status.equals("rewritten")) {
                    try {
                        var roots = CorrelationRecipe.decode(fragment, recipe.path("recipe"), registry);
                        var plans = row.putArray("plans");
                        for (var root: roots) plans.add(RelOptUtil.toString(root));
                        rewritten++;
                    } catch (RuntimeException | IOException error) {
                        throw new IOException("Recipe reconstruction failed for " + id, error);
                    }
                } else if (status.equals("refused") && recipe.path("reason").isTextual()) {
                    row.set("reason", recipe.path("reason")); refused++;
                } else throw new IOException("Unknown recipe outcome");
                results.write(row.toString()); results.newLine(); results.flush();
            }
            if (recipes.readLine() != null || identities.isEmpty())
                throw new IOException("Extra recipe or empty input");
        }
        Files.writeString(output.resolve("complete.json"), mapper.createObjectNode()
                .put("complete", true).put("fragments", identities.size())
                .put("rewritten", rewritten).put("refused", refused)
                .put("runtime_executed", false).toPrettyString() + "\n", StandardOpenOption.CREATE_NEW);
    }
}
