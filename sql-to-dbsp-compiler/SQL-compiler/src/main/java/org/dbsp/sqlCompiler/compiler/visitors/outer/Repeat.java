package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.ICompilerComponent;
import org.dbsp.sqlCompiler.compiler.backend.RustCircuitPass;
import org.dbsp.sqlCompiler.compiler.errors.SourcePositionRange;
import org.dbsp.util.IWritesLogs;
import org.dbsp.util.Logger;

import java.util.function.IntPredicate;

/** Applies a CircuitTransform until the circuit stops changing. */
public class Repeat implements IWritesLogs, CircuitTransform, ICompilerComponent {
    public final DBSPCompiler compiler;
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

    private final class Iteration implements IntPredicate {
        DBSPCircuit circuit;

        Iteration(DBSPCircuit circuit) {
            this.circuit = circuit;
        }

        @Override
        public boolean test(int index) {
            Logger.INSTANCE.belowLevel(Repeat.this, 1)
                    .append("Iteration ")
                    .append(index)
                    .newline();
            DBSPCircuit result = compiler.applyObservedPass("Repeat", 0, index, transform, this.circuit);
            Logger.INSTANCE.belowLevel(Repeat.this, 4)
                    .append("After ")
                    .appendSupplier(transform::toString)
                    .newline()
                    .appendSupplier(result::toString)
                    .newline();
            if (result.sameCircuit(this.circuit))
                return false;
            this.circuit = result;
            return true;
        }
    }

    @Override
    public DBSPCircuit apply(DBSPCircuit circuit) {
        var step = new Iteration(circuit);
        int largest = circuit.largestComponentSize();
        int exhausted;
        if (this.compiler.options.ioOptions.rustOptimizerPasses) {
            exhausted = RustCircuitPass.repeat(largest, this.repeats, step);
        } else {
            // Expression rewrites such as ConvertCasts may need several passes on a small circuit.
            exhausted = Math.max(largest, 10);
            for (int index = 0; index < exhausted; index++) {
                if (!step.test(index) || index + 1 == this.repeats)
                    return step.circuit;
            }
        }
        if (exhausted != 0)
            this.compiler.reportError(SourcePositionRange.INVALID, "InfiniteLoop",
                    "Repeated optimization " + this.transform + " " + exhausted + " times without convergence");
        return step.circuit;
    }

    @Override
    public String toString() {
        return this.id + " Repeat " + this.transform;
    }

    @Override
    public String getName() {
        return this.transform.getName();
    }
}
