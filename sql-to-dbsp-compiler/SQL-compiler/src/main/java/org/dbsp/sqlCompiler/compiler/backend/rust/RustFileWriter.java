package org.dbsp.sqlCompiler.compiler.backend.rust;

import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.ir.DBSPFunction;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.IDBSPNode;
import org.dbsp.util.IndentStream;
import org.dbsp.util.IndentStreamBuilder;
import org.dbsp.util.ProgramAndTester;
import org.dbsp.util.Utilities;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

/** This class helps generate Rust code.
 * It is given a set of circuit and functions and generates a compilable Rust file. */
public class RustFileWriter extends RustWriter {
    final PrintStream outputStream;
    boolean slt = false;

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

    public RustFileWriter(PrintStream outputStream) {
        this.outputStream = outputStream;
    }

    /** Special support for running the SLT tests */
    public RustFileWriter forSlt() {
        this.slt = true;
        return this;
    }

    public RustFileWriter(String outputFile)
            throws IOException {
        this(new PrintStream(outputFile, StandardCharsets.UTF_8));
    }

    void generateStructures(StructuresUsed used, IndentStream stream) {
        super.generateStructures(used, stream);
        if (this.slt) {
            stream.append("#[cfg(test)]").newline()
                    .append("sltsqlvalue::to_sql_row_impl! {").increase();
            for (int i : used.tupleSizesUsed) {
                if (i <= 10)
                    // These are already pre-declared
                    continue;
                stream.append(this.tup(i));
                stream.append(",\n");
            }
            stream.decrease().append("}\n\n");
        }
    }

    String generatePreamble(StructuresUsed used) {
        IndentStream stream = new IndentStreamBuilder();
        stream.append(commonPreamble);
        long max = used.getMaxTupleSize();
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
        this.generateStructures(used, stream);

        String stubs = Utilities.getBaseName(DBSPCompiler.STUBS_FILE_NAME);
        stream.append("mod ")
                .append(stubs)
                .append(";")
                .newline()
                .append("mod ")
                .append(Utilities.getBaseName(DBSPCompiler.UDF_FILE_NAME))
                .append(";")
                .newline()
                .append("use crate::")
                .append(stubs)
                .append("::*;")
                .newline();
        return stream.toString();
    }

    public void add(ProgramAndTester pt) {
        if (pt.program() != null)
            this.add(pt.program());
        this.add(pt.tester());
    }

    public void add(IDBSPNode node) {
        this.toWrite.add(node);
    }

    public void write(DBSPCompiler compiler) {
        StructuresUsed used = this.analyze(compiler);
        this.outputStream.println(generatePreamble(used));
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
        this.write(compiler);
        this.outputStream.close();
    }
}
