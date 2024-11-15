package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.compiler.IErrorReporter;
import org.dbsp.sqlCompiler.compiler.errors.SourcePositionRange;
import org.dbsp.util.IWritesLogs;
import org.dbsp.util.Logger;

/** Applies a CircuitTransform until the circuit stops changing. */
public class Repeat implements IWritesLogs, CircuitTransform {
    final IErrorReporter errorReporter;
    public final CircuitTransform transform;

    public Repeat(IErrorReporter reporter, CircuitTransform visitor) {
        this.errorReporter = reporter;
        this.transform = visitor;
    }

    @Override
    public DBSPCircuit apply(DBSPCircuit circuit) {
        // In some cases more repeats are needed.
        // Some optimizations may require a number of iterations given by the size of the circuit
        // but some may require a number of iterations that depends on the complexity of the
        // inner expressions.  ConvertCasts is such an example */
        int maxRepeats = Math.max(circuit.size(), 10);
        int repeats = 0;
        while (true) {
            DBSPCircuit result = this.transform.apply(circuit);
            Logger.INSTANCE.belowLevel(this, 4)
                    .append("After ")
                    .append(this.transform.toString())
                    .newline()
                    .appendSupplier(result::toString)
                    .newline();
            if (result.sameCircuit(circuit))
                return circuit;
            circuit = result;
            repeats++;
            if (repeats == maxRepeats) {
                this.errorReporter.reportError(SourcePositionRange.INVALID,
                        "InfiniteLoop",
                        "Repeated optimization " + this.transform + " " +
                        repeats + " times without convergence");
                return result;
            }
        }
    }

    @Override
    public String toString() {
        return "Repeat " + this.transform;
    }

    @Override
    public String getName() {
        return "Repeat_" + this.transform.getName();
    }
}
