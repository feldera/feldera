package org.dbsp.sqlCompiler.compiler.backend.rust;

import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.backend.rust.multi.ProjectDeclarations;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.IDBSPNode;
import org.dbsp.sqlCompiler.ir.statement.DBSPStructItem;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeStruct;
import org.dbsp.util.ProgramAndTester;
import org.dbsp.util.Utilities;

import java.util.ArrayList;
import java.util.List;

/** This class helps generate Rust code.
 * It is given a set of circuit and functions and generates a compilable Rust file. */
public class RustFileWriter extends RustWriter {
    StructuresUsed used = new StructuresUsed();
    boolean findUsed = true;
    boolean slt = false;
    boolean test = false;

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

    void generateStructures() {
        super.generateStructures(this.used);
        if (this.slt) {
            this.builder().append("#[cfg(test)]").newline()
                    .append("sltsqlvalue::to_sql_row_impl! {").increase();
            for (int i : this.used.tupleSizesUsed) {
                if (used.isPredefined(i))
                    // These are already pre-declared
                    continue;
                this.builder().append(this.tup(i)).append(",").newline();
            }
            this.builder().decrease().append("}").newline().newline();
        }
    }

    void generatePreamble() {
        this.builder().append(COMMON_PREAMBLE);
        long max = this.used.getMaxTupleSize();
        if (max > 120) {
            // this is just a guess
            this.builder().append("#![recursion_limit = \"")
                    .append(max * 2)
                    .append("\"]")
                    .newline();
        }

        if (this.slt) {
            this.builder().append("""
                            #[cfg(test)]
                            use hashing::*;""")
                    .newline();
        }
        this.builder().append(this.rustPreamble())
                .newline();
        this.generateStructures();
    }

    public void add(ProgramAndTester pt) {
        if (pt.program() != null)
            this.add(pt.program());
        this.addTest(pt.tester());
    }

    @Override
    public void addTest(IDBSPNode node) {
        // Do NOT add to the testNodes list
        this.toWrite.add(node);
    }

    public RustFileWriter setUsed(StructuresUsed used) {
        this.used = used;
        this.findUsed = false;
        return this;
    }

    public void write(DBSPCompiler compiler) {
        Utilities.enforce(this.outputBuilder != null);
        if (this.findUsed) {
            this.used = this.analyze(compiler);
        }
        this.generatePreamble();
        if (!this.used.tupleSizesUsed.isEmpty()) {
            this.builder().append("use dbsp::declare_tuples;").newline();
        }
        if (this.generateMalloc)
            this.outputBuilder.append(BaseRustCodeGenerator.ALLOC_PREAMBLE);
        if (this.generateUdfInclude)
            this.generateUdfInclude();
        if (this.test)
            this.builder().append("""
                    #[cfg(test)]
                    use readers::*;""").newline();

        for (String dep : this.dependencies)
            this.builder().append("use ").append(dep).append("::*;");

        if (this.declareSourceMap) {
            SourcePositionResource.generateDeclaration(this.outputBuilder);
        }

        ProjectDeclarations declarationsDone = new ProjectDeclarations();
        for (IDBSPNode node : this.toWrite) {
            IDBSPInnerNode inner = node.as(IDBSPInnerNode.class);
            if (inner != null) {
                if (inner.is(DBSPStructItem.class)) {
                    var list = this.generateStructHelpers(compiler, inner.to(DBSPStructItem.class).type, declarationsDone);
                    for (var e: list)
                        ToRustInnerVisitor.toRustString(compiler, this.builder(), e, null, false);
                } else {
                    ToRustInnerVisitor.toRustString(compiler, this.builder(), inner, null, false);
                }
            } else {
                DBSPCircuit outer = node.to(DBSPCircuit.class);
                ToRustVisitor.toRustString(compiler, this.builder(), outer, declarationsDone);
            }
            this.builder().newline();
        }
    }

    List<DBSPStructItem> generateStructHelpers(DBSPCompiler compiler, DBSPType struct, ProjectDeclarations done) {
        List<DBSPTypeStruct> nested = new ArrayList<>();
        List<DBSPStructItem> result = new ArrayList<>();
        ToRustVisitor.FindNestedStructs fn = new ToRustVisitor.FindNestedStructs(compiler, nested);
        fn.apply(struct);
        for (DBSPTypeStruct s: nested) {
            if (done.contains(s.name.name()))
                continue;
            DBSPStructItem item = new DBSPStructItem(s, null);
            result.add(item);
            done.declare(item.getName());
        }
        return result;
    }
}
