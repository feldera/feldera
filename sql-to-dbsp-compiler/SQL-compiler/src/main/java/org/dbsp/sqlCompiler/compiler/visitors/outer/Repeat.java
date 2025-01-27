package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.ICompilerComponent;
import org.dbsp.sqlCompiler.compiler.errors.SourcePositionRange;
import org.dbsp.util.IWritesLogs;
import org.dbsp.util.Logger;

/** Applies a CircuitTransform until the circuit stops changing. */
public class Repeat implements IWritesLogs, CircuitTransform, ICompilerComponent {
    final DBSPCompiler compiler;
    public final CircuitTransform transform;
    public final long id;
    public final int repeats;

    public Repeat(DBSPCompiler compiler, CircuitTransform visitor, int repeats) {
        this.compiler = compiler;
        this.transform = visitor;
        this.id = CircuitVisitor.crtId++;
        this.repeats = repeats;
    }

    public Repeat(DBSPCompiler compiler, CircuitTransform visitor) {
        this(compiler, visitor, Integer.MAX_VALUE);
    }

    @Override
    public DBSPCompiler compiler() {
        return this.compiler;
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
            Logger.INSTANCE.belowLevel(this, 1)
                    .append("Iteration ")
                    .append(repeats)
                    .newline();
            DBSPCircuit result = this.transform.apply(circuit);
            Logger.INSTANCE.belowLevel(this, 4)
                    .append("After ")
                    .appendSupplier(this.transform::toString)
                    .newline()
                    .appendSupplier(result::toString)
                    .newline();
            if (result.sameCircuit(circuit))
                return circuit;
            circuit = result;
            repeats++;
            if (repeats == this.repeats)
                return result;
            if (repeats == maxRepeats) {
                this.compiler.reportError(SourcePositionRange.INVALID,
                        "InfiniteLoop",
                        "Repeated optimization " + this.transform + " " +
                        repeats + " times without convergence");
                return result;
            }
        }
    }

    @Override
    public String toString() {
        return this.id + " Repeat " + this.transform;
    }

    @Override
    public String getName() {
        return "Repeat_" + this.transform.getName();
    }
}
