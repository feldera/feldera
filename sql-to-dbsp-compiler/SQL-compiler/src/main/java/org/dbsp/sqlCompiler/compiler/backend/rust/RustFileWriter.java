package org.dbsp.sqlCompiler.compiler.backend.rust;

import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.IDBSPNode;
import org.dbsp.util.IndentStream;
import org.dbsp.util.IndentStreamBuilder;
import org.dbsp.util.ProgramAndTester;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;

/** This class helps generate Rust code.
 * It is given a set of circuit and functions and generates a compilable Rust file. */
public class RustFileWriter extends RustWriter {
    boolean slt = false;
    boolean generateUdfInclude = true;
    boolean findUsed = true;
    StructuresUsed used = new StructuresUsed();

    public RustFileWriter() {}

    /** Preamble used when generating Rust code. */
    String rustPreamble() {
        String preamble = super.rustPreamble();
        if (this.slt) {
            preamble += """
                #[cfg(test)]
                use sltsqlvalue::*;
                """;
        }
        return preamble;
    }

    /** Special support for running the SLT tests */
    public RustFileWriter forSlt() {
        this.slt = true;
        return this;
    }
    
    public RustFileWriter withUdf(boolean udf) {
        this.generateUdfInclude = udf;
        return this;
    }

    public RustFileWriter(String outputFile)
            throws IOException {
        this.outputStream = new PrintStream(outputFile, StandardCharsets.UTF_8);
    }

    void generateStructures(IndentStream stream) {
        super.generateStructures(this.used, stream);
        if (this.slt) {
            stream.append("#[cfg(test)]").newline()
                    .append("sltsqlvalue::to_sql_row_impl! {").increase();
            for (int i : this.used.tupleSizesUsed) {
                if (i <= 10)
                    // These are already pre-declared
                    continue;
                stream.append(this.tup(i));
                stream.append(",\n");
            }
            stream.decrease().append("}\n\n");
        }
    }

    String generatePreamble() {
        IndentStream stream = new IndentStreamBuilder();
        stream.append(COMMON_PREAMBLE);
        long max = this.used.getMaxTupleSize();
        if (max > 120) {
            // this is just a guess
            stream.append("#![recursion_limit = \"")
                    .append(max * 2)
                    .append("\"]")
                    .newline();
        }

        stream.append("""
                      #[cfg(test)]
                      use hashing::*;""")
                .newline();
        stream.append(this.rustPreamble())
                .newline();
        this.generateStructures(stream);
        return stream.toString();
    }

    public void add(ProgramAndTester pt) {
        if (pt.program() != null)
            this.add(pt.program());
        this.add(pt.tester());
    }

    public void setUsed(StructuresUsed used) {
        this.used = used;
        this.findUsed = false;
    }

    public void write(DBSPCompiler compiler) {
        assert this.outputStream != null;
        if (this.findUsed)
            this.used = this.analyze(compiler);
        this.outputStream.println(generatePreamble());
        if (this.generateUdfInclude)
            this.outputStream.println(generateUdfInclude());
        for (String dep: this.dependencies)
            this.outputStream.println("use " + dep + "::*;");
        for (IDBSPNode node: this.toWrite) {
            String str;
            IDBSPInnerNode inner = node.as(IDBSPInnerNode.class);
            if (inner != null) {
                str = ToRustInnerVisitor.toRustString(compiler, inner, false);
            } else {
                DBSPCircuit outer = node.to(DBSPCircuit.class);
                str = ToRustVisitor.toRustString(compiler, outer);
            }
            this.outputStream.println(str);
            this.outputStream.println();
        }
    }

    public void writeAndClose(DBSPCompiler compiler) {
        assert this.outputStream != null;
        this.write(compiler);
        this.outputStream.close();
    }
}
