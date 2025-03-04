package org.dbsp.sqlCompiler.compiler.backend.rust;

import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.errors.CompilationError;
import org.dbsp.util.Utilities;

import java.io.File;

/** This class helps generate Rust code.
 * It is given a set of circuit and functions and generates a code in multiple crates. */
public class RustCratesWriter extends RustWriter {
    public final String outputDirectory;

    public RustCratesWriter(String outputDirectory) {
        this.outputDirectory = outputDirectory;
    }

    public void add(DBSPCircuit circuit) {
        this.toWrite.add(circuit);
    }

    public void write(DBSPCompiler compiler) {
        File file = new File(this.outputDirectory);
        if (!file.exists() || !file.isDirectory())
            throw new CompilationError("Output directory does not exist " +
                    Utilities.singleQuote(this.outputDirectory));
        // TODO
    }
}
