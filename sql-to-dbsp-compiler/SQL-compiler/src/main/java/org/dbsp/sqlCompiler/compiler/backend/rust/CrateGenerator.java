package org.dbsp.sqlCompiler.compiler.backend.rust;

import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.errors.CompilationError;
import org.dbsp.sqlCompiler.ir.IDBSPNode;
import org.dbsp.util.Utilities;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

/** Generates a Rust crate in a specified directory. */
public class CrateGenerator {
    public final File baseDirectory;
    public final String crateName;
    public RustWriter.StructuresUsed used;

    public static final String CARGO = "Cargo.toml";
    public static final String LIB = "lib.rs";
    final List<IDBSPNode> toWrite;

    public CrateGenerator(File baseDirectory, String crateName) {
        this.crateName = crateName;
        this.baseDirectory = baseDirectory;
        this.toWrite = new ArrayList<>();
        this.used = new RustWriter.StructuresUsed();
    }

    void generateCargo(PrintStream stream) {
        stream.println("[package]");
        stream.print("name = \"");
        stream.print(this.crateName);
        stream.println("\"");
        final String relativePath = "../../..";
        String cargo = """
                version = "0.1.0"
                edition = "2021"
                publish = false
                
                [dependencies]
                paste = { version = "1.0.12" }
                derive_more = { version = "0.99.17", features = ["add", "not", "from"] }
                dbsp = { path = "$ROOT/crates/dbsp", features = ["backend-mode"] }
                dbsp_adapters = { path = "$ROOT/crates/adapters", default-features = false }
                feldera-types = { path = "$ROOT/crates/feldera-types" }
                feldera-sqllib = { path = "$ROOT/crates/sqllib" }
                serde = { version = "1.0", features = ["derive"] }
                compare = { version = "0.1.0" }
                size-of = { version = "0.1.5", package = "feldera-size-of" }
                rust_decimal = { package = "feldera_rust_decimal", version = "1.33.1-feldera.1" }
                rust_decimal_macros = { version = "1.36" }
                serde_json = { version = "1.0.127", features = ["arbitrary_precision"] }
                rkyv = { version = "0.7.45", default-features = false, features = ["std", "size_64"] }
                
                [target.'cfg(not(target_env = "msvc"))'.dependencies]
                tikv-jemallocator = { version = "0.5.4", features = ["profiling", "unprefixed_malloc_on_supported_platforms"] }
                
                [dev-dependencies]
                uuid = { version = "1.6.1" }
                
                [patch.crates-io]
                datafusion = { git = "https://github.com/ryzhyk/datafusion.git", rev = "f561db7" }
                datafusion-common = { git = "https://github.com/ryzhyk/datafusion.git", rev = "f561db7" }
                datafusion-expr = { git = "https://github.com/ryzhyk/datafusion.git", rev = "f561db7" }
                datafusion-functions = { git = "https://github.com/ryzhyk/datafusion.git", rev = "f561db7" }
                datafusion-functions-aggregate = { git = "https://github.com/ryzhyk/datafusion.git", rev = "f561db7" }
                datafusion-physical-expr = { git = "https://github.com/ryzhyk/datafusion.git", rev = "f561db7" }
                datafusion-physical-plan = { git = "https://github.com/ryzhyk/datafusion.git", rev = "f561db7" }
                datafusion-proto = { git = "https://github.com/ryzhyk/datafusion.git", rev = "f561db7" }
                datafusion-sql = { git = "https://github.com/ryzhyk/datafusion.git", rev = "f561db7" }
                
                [lib]
                path = "src/lib.rs"
                doctest = false""";
        cargo = cargo.replace("$ROOT", relativePath);
        stream.println(cargo);
    }

    public void add(IDBSPNode node) {
        this.toWrite.add(node);
    }

    public void write(DBSPCompiler compiler) throws IOException {
        if (!this.baseDirectory.exists())
            throw new RuntimeException(
                    "Directory " + Utilities.singleQuote(this.baseDirectory.getPath()) + " does not exist");
        if (!this.baseDirectory.isDirectory())
            throw new RuntimeException(
                    Utilities.singleQuote(this.baseDirectory.getPath()) + " is not a directory");
        File crateRoot = new File(this.baseDirectory, this.crateName);
        if (crateRoot.exists())
            throw new CompilationError("Directory " + Utilities.singleQuote(crateRoot.getPath()) + " already exists");
        boolean success = crateRoot.mkdir();
        if (!success)
            throw new RuntimeException("Could not create directory " + Utilities.singleQuote(crateRoot.getPath()));
        File cargo = new File(crateRoot, CARGO);
        PrintStream cargoStream = new PrintStream(Files.newOutputStream(cargo.toPath()));
        this.generateCargo(cargoStream);
        cargoStream.close();

        File src = new File(crateRoot, "src");
        success = src.mkdir();
        if (!success)
            throw new RuntimeException("Could not create directory " + Utilities.singleQuote(src.getPath()));
        File lib = new File(src, LIB);
        PrintStream rustStream = new PrintStream(Files.newOutputStream(lib.toPath()));
        RustFileWriter writer = new RustFileWriter(rustStream);
        for (IDBSPNode node: this.toWrite)
            writer.add(node);
        writer.writeAndClose(compiler);
    }

    public void setUsed(RustWriter.StructuresUsed used) {
        this.used = used;
    }
}
