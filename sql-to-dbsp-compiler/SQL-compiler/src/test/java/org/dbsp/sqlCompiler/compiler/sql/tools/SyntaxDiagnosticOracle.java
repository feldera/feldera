package org.dbsp.sqlCompiler.compiler.sql.tools;

import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.util.Utilities;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/** Collect product diagnostic payloads for independent submissions rejected by the raw parser. */
public final class SyntaxDiagnosticOracle {
    private SyntaxDiagnosticOracle() {}

    public static void main(String[] args) throws IOException {
        if (args.length != 2)
            throw new IllegalArgumentException("Usage: SyntaxDiagnosticOracle INPUT.jsonl OUTPUT.jsonl");
        var mapper = Utilities.deterministicObjectMapper();
        try (var reader = Files.newBufferedReader(Path.of(args[0]));
             var writer = Files.newBufferedWriter(Path.of(args[1]), StandardOpenOption.CREATE_NEW)) {
            for (String line; (line = reader.readLine()) != null;) {
                var input = mapper.readTree(line);
                var compiler = new DBSPCompiler(new CompilerOptions());
                compiler.submitStatementsForCompilation(input.required("sql").textValue());
                compiler.getFinalCircuit(false);
                var result = mapper.createObjectNode();
                result.set("id", input.required("id"));
                result.set("messages", compiler.messages.toJson(compiler.sources));
                writer.write(mapper.writeValueAsString(result));
                writer.newLine();
            }
        }
    }
}
