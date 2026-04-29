package org.dbsp.sqlCompiler.compiler.sql.tools;

import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.backend.rust.ToRustVisitor;
import org.dbsp.sqlCompiler.compiler.backend.rust.multi.ProjectDeclarations;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitTransform;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.outer.LateMaterializations;
import org.dbsp.util.IndentStreamBuilder;

/**
 * Helper class for testing.  Holds together
 * - the compiler that is used to compile a program,
 * - the circuit */
public class CompilerCircuit {
    public final DBSPCompiler compiler;
    final DBSPCircuit circuit;

    public CompilerCircuit(DBSPCompiler compiler) {
        this.compiler = compiler;
        this.circuit = BaseSQLTests.getCircuit(compiler);
    }

    public void showErrors() {
        this.compiler.messages.show(System.err);
        this.compiler.messages.clear();
    }

    public void visit(CircuitVisitor visitor) {
        visitor.apply(this.circuit);
    }

    public void visit(CircuitTransform visitor) {
        visitor.apply(this.circuit);
    }

    public DBSPCircuit getCircuit() {
        return this.circuit;
    }

    public String getRustSources() {
        IndentStreamBuilder builder = new IndentStreamBuilder();
        ToRustVisitor visitor = new ToRustVisitor(
                this.compiler, builder, this.getCircuit().getMetadata(),
                new ProjectDeclarations(), new LateMaterializations(this.compiler));
        visitor.apply(this.getCircuit());
        return builder.toString();
    }
}
