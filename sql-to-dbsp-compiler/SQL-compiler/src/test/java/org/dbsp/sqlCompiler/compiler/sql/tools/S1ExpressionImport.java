package org.dbsp.sqlCompiler.compiler.sql.tools;

import org.dbsp.util.Utilities;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/** Independent command-line entry point for the bounded expression reader. */
public final class S1ExpressionImport {
    private S1ExpressionImport() { }

    public static void main(String[] args) throws IOException {
        if (args.length != 2) throw new IOException("usage: S1ExpressionImport INPUT.json OUTPUT_DIR");
        var decoded = S1ExpressionTable.read(Path.of(args[0]), new S1BuiltinOperatorRegistry());
        Path output = Path.of(args[1]); Files.createDirectory(output);
        var complete = Utilities.deterministicObjectMapper().createObjectNode();
        complete.put("schema_version", 1).put("complete", true).put("expressions", decoded.expressions().size())
                .put("roots", decoded.roots().size()).put("runtime_executed", false).put("S1_parity_proven", false);
        Utilities.deterministicObjectMapper().writerWithDefaultPrettyPrinter()
                .writeValue(output.resolve("complete.json").toFile(), complete);
    }
}
