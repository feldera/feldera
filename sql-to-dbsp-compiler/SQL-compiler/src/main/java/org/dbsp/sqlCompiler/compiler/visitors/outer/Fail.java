package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.compiler.IErrorReporter;
import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.util.IWritesLogs;

/** This visitor fails with an error message when it is invoked */
public class Fail implements IWritesLogs, CircuitTransform {
    final IErrorReporter errorReporter;
    final String message;

    public Fail(IErrorReporter reporter, String message) {
        this.errorReporter = reporter;
        this.message = message;
    }

    @Override
    public String toString() {
        return "Fail";
    }

    @Override
    public DBSPCircuit apply(DBSPCircuit circuit) {
        throw new InternalCompilerError(this.message, circuit.getNode());
    }

    @Override
    public String getName() {
        return this.toString();
    }
}
