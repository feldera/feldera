package org.dbsp.sqlCompiler.compiler.sql.tools;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.codec.digest.DigestUtils;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.util.Utilities;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/** Additional option-activation programs, kept separate from exported corpus counts. */
public final class S4ActivationInputs {
    private S4ActivationInputs() { }

    public static void main(String[] args) throws IOException {
        if (args.length != 1)
            throw new IllegalArgumentException("Usage: S4ActivationInputs OUTPUT.jsonl");
        var mapper = Utilities.deterministicObjectMapper();
        String baseSql = "CREATE TABLE t(k INT NOT NULL, v INT);\n"
                + "CREATE TABLE unused_t(x INT);\n"
                + "CREATE VIEW q AS SELECT SUM(v) AS total FROM t GROUP BY k;";
        try (var writer = Files.newBufferedWriter(Path.of(args[0]), StandardOpenOption.CREATE_NEW)) {
            for (int configuration = 0; configuration < 32; configuration++) {
                int mask = configuration % 16;
                boolean activateMetadata = configuration >= 16;
                String sql = (activateMetadata ? "SET FELDERA_AVOID_STAR_JOINS = TRUE;\n"
                        + "SET ENFORCE_POSITIVE_INPUTS = TRUE;\n" : "") + baseSql;
                var options = new CompilerOptions();
                options.languageOptions.incrementalize = (mask & 1) != 0;
                options.languageOptions.outputsAreSets = (mask & 2) != 0;
                options.languageOptions.generateInputForEveryTable = (mask & 4) != 0;
                options.ioOptions.gen2 = (mask & 8) != 0;
                options.ioOptions.quiet = true;
                options.ioOptions.correlatedColumns = activateMetadata;
                options.ioOptions.trimInputs = activateMetadata;
                ObjectNode source = mapper.createObjectNode();
                source.put("id", "s4-option-activation/" + configuration);
                source.put("program", sql);
                source.set("options", mapper.valueToTree(options));
                var submission = source.putArray("submissions").addObject();
                submission.put("sql", sql);
                submission.put("many", true);
                submission.put("batch", 0);
                var compiler = new DBSPCompiler(options);
                compiler.submitStatementsForCompilation(sql);
                var circuit = compiler.getFinalCircuit(false);
                if (circuit == null || compiler.hasErrors())
                    throw new IOException("Option-activation program failed: " + mask);
                source.set("schema", compiler.metadata.asJson());
                source.set("diagnostics", compiler.messages.toJson(compiler.sources));
                source.put("input_sha256", DigestUtils.sha256Hex(source.toString()));
                writer.write(source.toString());
                writer.newLine();
                writer.flush();
            }
        }
    }
}
