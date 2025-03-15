package org.dbsp.sqlCompiler.compiler.backend.rust;

import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.IDBSPNode;
import org.dbsp.util.ProgramAndTester;

import java.util.HashSet;
import java.util.Set;

/** This class helps generate Rust code.
 * It is given a set of circuit and functions and generates a compilable Rust file. */
public class RustFileWriter extends RustWriter {
    boolean slt = false;
    boolean generateUdfInclude = true;
    boolean generateMalloc = true;
    boolean findUsed = true;
    boolean test = false;
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
        this.test = true;
        return this;
    }

    public RustFileWriter withTest(boolean test) {
        this.test = test;
        return this;
    }
    
    public RustFileWriter withUdf(boolean udf) {
        this.generateUdfInclude = udf;
        return this;
    }

    public RustFileWriter withMalloc(boolean malloc) {
        this.generateMalloc = malloc;
        return this;
    }

    void generateStructures() {
        super.generateStructures(this.used);
        if (this.slt) {
            this.getOutputStream().append("#[cfg(test)]").newline()
                    .append("sltsqlvalue::to_sql_row_impl! {").increase();
            for (int i : this.used.tupleSizesUsed) {
                if (i <= StructuresUsed.PREDEFINED)
                    // These are already pre-declared
                    continue;
                this.getOutputStream().append(this.tup(i)).append(",").newline();
            }
            this.getOutputStream().decrease().append("}").newline().newline();
        }
    }

    void generatePreamble() {
        this.getOutputStream().append(COMMON_PREAMBLE);
        long max = this.used.getMaxTupleSize();
        if (max > 120) {
            // this is just a guess
            this.getOutputStream().append("#![recursion_limit = \"")
                    .append(max * 2)
                    .append("\"]")
                    .newline();
        }

        if (this.slt) {
            this.getOutputStream().append("""
                            #[cfg(test)]
                            use hashing::*;""")
                    .newline();
        }
        this.getOutputStream().append(this.rustPreamble())
                .newline();
        this.generateStructures();
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
        this.generatePreamble();
        if (this.generateMalloc)
            this.outputStream.append(BaseRustCodeGenerator.ALLOC_PREAMBLE);
        if (this.generateUdfInclude)
            this.generateUdfInclude();
        if (this.test)
            this.outputStream.append("""
            #[cfg(test)]
            use readers::*;""");
        for (String dep: this.dependencies)
            this.getOutputStream().append("use ").append(dep).append("::*;");
        Set<String> declarationsDone = new HashSet<>();
        for (IDBSPNode node: this.toWrite) {
            String str;
            IDBSPInnerNode inner = node.as(IDBSPInnerNode.class);
            if (inner != null) {
                str = ToRustInnerVisitor.toRustString(compiler, inner, false);
            } else {
                DBSPCircuit outer = node.to(DBSPCircuit.class);
                str = ToRustVisitor.toRustString(compiler, outer, declarationsDone);
            }
            this.getOutputStream().append(str).newline();
        }
    }
}
