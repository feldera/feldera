package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.operator.DBSPInputMapWithWaterlineOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSinkOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSourceMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSourceMultisetOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.ir.type.user.StreamKind;

/** Checks the stream kinds of a circuit.
 * Computing the kind of an operator's outputs validates its inputs, so this visitor
 * computes the kind of every output.  It also checks that the tables and the outputs carry
 * the kind the circuit's mode prescribes: deltas in an incremental circuit, collections
 * otherwise. */
public class ValidateStreamKinds extends CircuitVisitor {
    public ValidateStreamKinds(DBSPCompiler compiler) {
        super(compiler);
    }

    StreamKind circuitKind() {
        return this.getCircuit().incremental ? StreamKind.DELTA : StreamKind.COLLECTION;
    }

    void checkTable(DBSPOperator table, StreamKind actual) {
        StreamKind expected = this.circuitKind();
        if (actual != expected)
            throw new InternalCompilerError("Table " + table + " produces a stream of " + actual +
                    " in a circuit whose tables must produce " + expected, table);
    }

    @Override
    public void postorder(DBSPOperator operator) {
        for (int i = 0; i < operator.outputCount(); i++)
            if (operator.hasOutput(i))
                operator.outputKind(i);
    }

    @Override
    public void postorder(DBSPSourceMultisetOperator table) {
        this.postorder(table.to(DBSPOperator.class));
        this.checkTable(table, table.outputKind(0));
    }

    @Override
    public void postorder(DBSPSourceMapOperator table) {
        this.postorder(table.to(DBSPOperator.class));
        this.checkTable(table, table.outputKind(0));
    }

    @Override
    public void postorder(DBSPInputMapWithWaterlineOperator table) {
        this.postorder(table.to(DBSPOperator.class));
        this.checkTable(table, table.outputKind(0));
    }

    @Override
    public void postorder(DBSPSinkOperator sink) {
        this.postorder(sink.to(DBSPOperator.class));
        // The error view collects the errors of each step, whatever the circuit's mode
        if (sink.metadata.system)
            return;
        StreamKind expected = this.circuitKind();
        StreamKind actual = sink.input().kind();
        if (actual != expected)
            throw new InternalCompilerError("Output " + sink.viewName.singleQuote() +
                    " receives a stream of " + actual + " but the circuit must produce " + expected, sink);
    }
}
