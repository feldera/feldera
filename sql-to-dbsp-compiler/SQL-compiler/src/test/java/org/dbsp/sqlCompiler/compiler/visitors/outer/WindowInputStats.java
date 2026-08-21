package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.operator.DBSPWindowOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.sql.tools.CompilerCircuit;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTupleBase;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

/** What the windows of a circuit read, for tests that count shared window inputs. */
public class WindowInputStats extends CircuitVisitor {
    /** Ids of operators that are left inputs to windows */
    public final List<Long> leftInputIds = new ArrayList<>();
    /** For each input the width of its value tuple */
    public final List<Integer> leftInputValueWidth = new ArrayList<>();

    public WindowInputStats(DBSPCompiler compiler) {
        super(compiler);
    }

    @Override
    public void postorder(DBSPWindowOperator operator) {
        this.leftInputIds.add(operator.left().node().id);
        this.leftInputValueWidth.add(
                operator.getOutputIndexedZSetType().elementType.to(DBSPTypeTupleBase.class).size());
    }

    /** Number of distinct left inputs for Window operators in the circuit */
    public int distinctInputCount() {
        return new HashSet<>(this.leftInputIds).size();
    }

    /** The statistics of the windows in `circuit` */
    public static WindowInputStats windows(CompilerCircuit circuit) {
        WindowInputStats stats = new WindowInputStats(circuit.compiler);
        circuit.visit(stats);
        return stats;
    }
}
