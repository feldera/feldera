package org.dbsp.sqlCompiler.compiler.backend.rust;

import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.errors.CompilationError;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.IDBSPNode;
import org.dbsp.util.Utilities;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.util.List;

/** This class helps generate Rust code.
 * It is given a set of circuit and functions and generates a code in multiple crates. */
public class RustCratesWriter extends RustWriter {
    public final String outputDirectory;

    public RustCratesWriter(String outputDirectory) {
        this.outputDirectory = outputDirectory;
    }

    File rootDirectory() {
        File file = new File(this.outputDirectory);
        if (!file.exists() || !file.isDirectory())
            throw new CompilationError("Output directory does not exist " +
                    Utilities.singleQuote(this.outputDirectory));
        Utilities.deleteContents(file);
        return file;
    }

    public void add(DBSPCircuit circuit) {
        this.toWrite.add(circuit);
    }

    public void write(DBSPCompiler compiler) throws IOException {
        StructuresUsed used = this.analyze(compiler);
        File rootDirectory = this.rootDirectory();
        File cargo = new File(rootDirectory, CrateGenerator.CARGO);
        PrintStream cargoStream = new PrintStream(Files.newOutputStream(cargo.toPath()));
        cargoStream.println("[workspace]");
        cargoStream.println("members = [");

        CrateGenerator main = new CrateGenerator(rootDirectory, "main");
        CrateGenerator types = new CrateGenerator(rootDirectory, "types");
        types.setUsed(used);
        cargoStream.println("   " + Utilities.doubleQuote(types.crateName) + ",");
        for (IDBSPNode node: this.toWrite) {
            if (node.is(IDBSPInnerNode.class))
                types.add(node);
            else
                main.add(node);
        }
        types.write(compiler);
        main.write(compiler);

        cargoStream.println("   " + Utilities.doubleQuote(main.crateName));
        cargoStream.println("]");
        cargoStream.close();
    }
}
