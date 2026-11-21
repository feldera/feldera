package org.dbsp.sqlCompiler.compiler.sql.tools;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIncrementalConstantOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPLeftJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitCloneVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitTransform;
import org.dbsp.sqlCompiler.compiler.visitors.outer.PropagateEmptySources;
import org.dbsp.sqlCompiler.ir.expression.DBSPIndexedZSetExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

/** Replay the empty-right branch of a left join using its original typed closure.
 * The ordinary SQL optimizer can eliminate an empty relation before this pass. */
public final class EmptyRightJoinOracle extends FrozenCircuitObserver {
    private final DBSPCompiler compiler;
    private final ArrayNode invocations;

    private EmptyRightJoinOracle(DBSPCompiler compiler, ObjectNode record) {
        super(compiler, record);
        this.compiler = compiler;
        record.put("pass", "PropagateEmptySources");
        this.invocations = record.putArray("invocations");
    }

    @Override
    public void before(String container, int index, int iteration, CircuitTransform pass, DBSPCircuit circuit) {
        if (!this.invocations.isEmpty() || !(pass instanceof PropagateEmptySources))
            return;
        for (var operator : circuit.getAllOperators()) {
            if (operator instanceof DBSPLeftJoinOperator) {
                this.observeEmptyRight(circuit);
                return;
            }
        }
    }

    private void observeEmptyRight(DBSPCircuit circuit) {
        long saved = DBSPVariablePath.nextGeneratedName();
        try {
            var input = new CircuitCloneVisitor(this.compiler, false) {
                @Override
                public void postorder(DBSPLeftJoinOperator operator) {
                    var type = operator.right().outputType();
                    var value = new DBSPIndexedZSetExpression(type.getNode(), type);
                    var empty = new DBSPIncrementalConstantOperator(operator.getRelNode(), value);
                    this.addOperator(empty);
                    var inputs = List.of(this.mapped(operator.left()), empty.outputPort());
                    this.map(operator, operator.withInputs(inputs, false).to(DBSPSimpleOperator.class));
                }
            }.apply(circuit);
            var record = this.invocations.addObject();
            record.put("names_before", DBSPVariablePath.nextGeneratedName());
            record.set("before", this.snapshot(input));
            var output = new PropagateEmptySources(this.compiler).apply(input);
            record.put("names_after", DBSPVariablePath.nextGeneratedName());
            record.set("after", this.snapshot(output));
        } finally {
            DBSPVariablePath.restoreGeneratedNames(saved);
        }
    }

    @Override
    public void after(String container, int index, int iteration, CircuitTransform pass,
                      DBSPCircuit circuit, boolean completed) {}

    @Override
    void finish() {
        if (this.invocations.isEmpty())
            throw new IllegalArgumentException("The selected program does not reach this pass with a left join");
    }

    public static void main(String[] args) throws IOException {
        if (args.length != 1)
            throw new IllegalArgumentException("Expected a frozen left-join population file");
        run(Path.of(args[0]), EmptyRightJoinOracle::new);
    }
}
