package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.util.IWritesLogs;

import java.util.function.Supplier;

/** Applies a CircuitTransform if some condition is true */
public class Conditional implements IWritesLogs, CircuitTransform {
    final Supplier<Boolean> test;
    final DBSPCompiler compiler;
    public final CircuitTransform transform;
    final long id;

    public Conditional(DBSPCompiler compiler, CircuitTransform visitor, Supplier<Boolean> test) {
        this.compiler = compiler;
        this.transform = visitor;
        this.test = test;
        this.id = CircuitVisitor.crtId++;
    }

    @Override
    public DBSPCircuit apply(DBSPCircuit circuit) {
        if (this.test.get())
            return this.transform.apply(circuit);
        return circuit;
    }

    @Override
    public String toString() {
        return this.id + " Conditional " + this.transform;
    }

    @Override
    public String getName() {
        return this.toString();
    }
}
