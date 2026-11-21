package org.dbsp.sqlCompiler.compiler.sql.tools;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.util.Utilities;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Locale;

/** Compile extracted Python negative-test SQL; do not deploy or execute pipelines. */
public final class PythonDiagnosticProbe {
    private PythonDiagnosticProbe() { }

    static ObjectNode compile(String sql, String expected) {
        var options = new CompilerOptions();
        options.languageOptions.incrementalize = true;
        options.ioOptions.quiet = true;
        var compiler = new DBSPCompiler(options);
        compiler.submitStatementsForCompilation(sql);
        var circuit = compiler.getFinalCircuit(false);
        var result = Utilities.deterministicObjectMapper().createObjectNode();
        result.set("options", Utilities.deterministicObjectMapper().valueToTree(options));
        result.put("compiler_rejected", compiler.hasErrors());
        result.put("circuit_produced", circuit != null);
        result.set("diagnostics", compiler.messages.toJson(compiler.sources));
        String rendered = compiler.messages.toString();
        result.put("rendered", rendered);
        result.put("expected_text_found", rendered.toLowerCase(Locale.ROOT).contains(expected.strip().toLowerCase(Locale.ROOT)));
        result.put("runtime_executed", false);
        result.put("python_test_passed", false);
        return result;
    }

    public static void main(String[] args) throws IOException {
        if (args.length != 2) throw new IOException("Usage: PythonDiagnosticProbe INPUT.json OUTPUT.jsonl");
        var mapper = Utilities.deterministicObjectMapper();
        var input = mapper.readTree(Files.readString(Path.of(args[0])));
        if (input.path("schema_version").asInt() != 1 || !input.path("complete").asBoolean())
            throw new IOException("A completed Python SQL extraction is required");
        try (var writer = Files.newBufferedWriter(Path.of(args[1]), StandardOpenOption.CREATE_NEW)) {
            for (JsonNode row: input.path("records")) {
                ObjectNode result;
                if (row.path("active").asBoolean())
                    result = compile(row.path("sql").textValue(), row.path("expected").textValue());
                else
                    result = mapper.createObjectNode().put("disposition", "not_registered").put("runtime_executed", false);
                result.set("id", row.path("id"));
                writer.write(result.toString()); writer.newLine(); writer.flush();
            }
        }
    }
}
