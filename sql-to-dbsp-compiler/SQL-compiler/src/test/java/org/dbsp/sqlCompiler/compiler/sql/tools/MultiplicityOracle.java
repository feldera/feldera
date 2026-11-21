package org.dbsp.sqlCompiler.compiler.sql.tools;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.circuit.operator.DBSPNestedOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.backend.RustCircuitPass;
import org.dbsp.sqlCompiler.compiler.backend.ToJsonOuterVisitor.OptimizerSnapshots;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitTransform;
import org.dbsp.sqlCompiler.compiler.visitors.outer.keys.MultisetAnalysis;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;

import java.io.IOException;
import java.nio.file.Path;

/** Compare every published port at each uninterrupted multiset analysis,
 * including the analysis whose result feeds the persistent-ID calculation. */
public final class MultiplicityOracle extends FrozenCircuitObserver {
    private final MultisetAnalysis candidate;
    private final RustCircuitPass rust;
    private int invocations;
    private int sets;
    private int multisets;
    private int secondary;

    private MultiplicityOracle(DBSPCompiler compiler, ObjectNode record) {
        super(compiler, record);
        this.candidate = new MultisetAnalysis(compiler);
        this.rust = new RustCircuitPass(compiler, this.candidate, new OptimizerSnapshots(compiler));
    }

    @Override
    public void before(String container, int index, int iteration, CircuitTransform pass, DBSPCircuit circuit) { }

    private void compare(Iterable<DBSPOperator> operators, MultisetAnalysis java) {
        for (var op : operators) {
            for (int i = 0; i < op.outputCount(); i++) {
                if (op instanceof DBSPNestedOperator nested && nested.internalOutputs.get(i) == null)
                    continue;
                var port = op.getOutput(i);
                boolean expected = java.isMultiset(port);
                if (expected != this.candidate.isMultiset(port))
                    throw new AssertionError(this.observation.get("id") + " " + op + " output " + i
                            + " expected multiset=" + expected);
                if (expected)
                    this.multisets++;
                else
                    this.sets++;
                if (i > 0)
                    this.secondary++;
            }
            if (op instanceof DBSPNestedOperator nested)
                this.compare(nested.getAllOperators(), java);
        }
    }

    @Override
    public void after(String container, int index, int iteration, CircuitTransform pass,
                      DBSPCircuit circuit, boolean completed) {
        if (!(pass instanceof MultisetAnalysis java) || !completed)
            return;
        long before = DBSPVariablePath.nextGeneratedName();
        if (this.rust.apply(circuit) != circuit || DBSPVariablePath.nextGeneratedName() != before)
            throw new AssertionError("Multiplicity analysis changed the circuit or generated-name counter");
        this.compare(circuit.getAllOperators(), java);
        this.invocations++;
    }

    @Override
    void finish() {
        this.observation.put("invocations", this.invocations).put("sets", this.sets)
                .put("multisets", this.multisets).put("secondary", this.secondary);
    }

    public static void main(String[] args) throws IOException {
        run(Path.of(args[0]), MultiplicityOracle::new);
    }
}
