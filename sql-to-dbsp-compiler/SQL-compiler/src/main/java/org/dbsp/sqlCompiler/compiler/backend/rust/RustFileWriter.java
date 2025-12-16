package org.dbsp.sqlCompiler.compiler.backend.rust;

import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.InputColumnMetadata;
import org.dbsp.sqlCompiler.compiler.backend.rust.multi.ProjectDeclarations;
import org.dbsp.sqlCompiler.compiler.visitors.outer.LateMaterializations;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.IDBSPNode;
import org.dbsp.sqlCompiler.ir.statement.DBSPStructItem;
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
    final LateMaterializations materializations;

    public RustFileWriter(LateMaterializations materializations) {
        this.materializations = materializations;
    }

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

        ToRustInnerVisitor innerVisitor = new ToRustInnerVisitor(compiler, this.builder(), null, false);
        ProjectDeclarations declarationsDone = new ProjectDeclarations();
        for (IDBSPNode node : this.toWrite) {
            IDBSPInnerNode inner = node.as(IDBSPInnerNode.class);
            if (inner != null) {
                var list = this.findStructs(inner, compiler, declarationsDone);
                for (var e: list)
                    e.accept(innerVisitor);
                if (!inner.is(DBSPStructItem.class))
                    // If it's a struct item, it is part of the list above
                    inner.accept(innerVisitor);
            } else {
                DBSPCircuit outer = node.to(DBSPCircuit.class);
                ToRustVisitor visitor = new ToRustVisitor(
                        compiler, this.builder(), outer.metadata, declarationsDone, this.materializations);
                visitor.apply(outer);
            }
            this.builder().newline();
        }
    }

    List<DBSPStructItem> findStructs(IDBSPInnerNode node, DBSPCompiler compiler, ProjectDeclarations done) {
        List<DBSPStructItem> structs = new ArrayList<>();
        ToRustVisitor.FindNestedStructs fn = new ToRustVisitor.FindNestedStructs(compiler, structs);
        fn.apply(node);
        DBSPStructItem it = node.as(DBSPStructItem.class);
        if (it != null && it.metadata != null) {
            // Discover structs used in the metadata
            for (int i = 0; i < it.metadata.getColumnCount(); i++) {
                InputColumnMetadata meta = it.metadata.getColumnMetadata(i);
                if (meta.defaultValue != null)
                    fn.apply(meta.defaultValue);
                if (meta.lateness != null)
                    fn.apply(meta.lateness);
                if (meta.watermark != null)
                    fn.apply(meta.watermark);
            }
        }

        List<DBSPStructItem> result = new ArrayList<>();
        for (DBSPStructItem item: fn.structs) {
            if (done.contains(item.getName()))
                continue;
            result.add(item);
            done.declare(item.getName());
        }
        return result;
    }
}
