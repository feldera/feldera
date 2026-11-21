package org.dbsp.sqlCompiler.compiler.sql.tools;

import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.backend.JsonDecoder;
import org.dbsp.util.Utilities;
import org.dbsp.sqlCompiler.compiler.backend.ToJsonOuterVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.outer.TagRegions;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/** Capture a single Java region pass before and after an active annotation conflict. */
public final class RegionPassOracle {
    private static void write(DBSPCompiler compiler, DBSPCircuit circuit, Path target) throws Exception {
        var writer = ToJsonOuterVisitor.createForReplay(compiler);
        writer.apply(circuit);
        Files.writeString(target, writer.getJsonString(), StandardOpenOption.CREATE_NEW);
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 3)
            throw new IllegalArgumentException("Expected circuit input, before, after");
        var options = new CompilerOptions();
        options.ioOptions.gen2 = true;
        var compiler = DBSPCompiler.forCircuitReplay(options);
        var raw = Utilities.deterministicObjectMapper().readTree(Path.of(args[0]).toFile());
        var circuit = new JsonDecoder(compiler.metadata).decodeOuter(raw, DBSPCircuit.class);
        write(compiler, circuit, Path.of(args[1]));
        var after = new TagRegions(compiler).apply(circuit);
        write(compiler, after, Path.of(args[2]));
    }
}
