package org.dbsp.sqlCompiler.compiler.sql.tools;

import com.fasterxml.jackson.core.JsonParser;
import org.dbsp.sqlCompiler.compiler.backend.CircuitReplay;
import org.dbsp.util.Utilities;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/** Local differential driver for the portion between the Rust binder and the Gen-2 writer. */
public final class BoundProgramReplay {
    private BoundProgramReplay() {}

    public static void main(String[] args) throws IOException {
        if (args.length != 4 || !(args[3].equals("S2") || args[3].equals("S3")))
            throw new IllegalArgumentException("Usage: BoundProgramReplay PROGRAM.json OUTPUT_DIR COMPILER_REVISION S2|S3");
        var mapper = Utilities.deterministicObjectMapper().copy()
                .enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION);
        var decoded = S1Program.decode(mapper.readTree(Path.of(args[0]).toFile()), new S1BuiltinOperatorRegistry());
        var options = decoded.context().options().ioOptions;
        if (!options.gen2)
            throw new IllegalArgumentException("Expected a Gen-2 bound program");
        var stage = CircuitReplay.Stage.valueOf(args[3]);
        options.inputCircuit = stage == CircuitReplay.Stage.S2;
        options.dumpProducerCommit = args[2];
        options.dumpRuntimeAbi = args[2];
        var compiler = S1Program.loweringCompiler(decoded);
        var lowered = S1Program.lower(decoded, compiler);
        compiler.throwIfErrorsOccurred();
        if (lowered == null)
            throw new IOException("No circuit after lowering the bound program");
        var circuit = stage == CircuitReplay.Stage.S2 ? lowered : compiler.optimizeFromS2(lowered);
        compiler.throwIfErrorsOccurred();
        var directory = Files.createDirectory(Path.of(args[1]));
        CircuitReplay.write(compiler, circuit, directory.resolve(stage == CircuitReplay.Stage.S2 ? "s2.json" : "s3.json"), stage);
    }
}
