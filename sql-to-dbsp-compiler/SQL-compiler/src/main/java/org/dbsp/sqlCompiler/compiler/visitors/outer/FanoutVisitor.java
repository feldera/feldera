package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.compiler.IErrorReporter;
import org.dbsp.util.Utilities;

import java.util.HashMap;
import java.util.Map;

/**
 * A visitor which computes the fanout of each operator.
 * This assumes that there is no dead code.
 */
public class FanoutVisitor extends CircuitVisitor {
    final Map<DBSPOperator, Integer> fanout = new HashMap<>();

    public FanoutVisitor(IErrorReporter errorReporter) {
        super(errorReporter);
    }

    void reference(DBSPOperator node) {
        this.fanout.compute(node, (k, v) -> v == null ? 1 : v + 1);
    }

    @Override
    public void postorder(DBSPOperator node) {
        for (DBSPOperator input: node.inputs) {
            this.reference(input);
        }
    }

    public int getFanout(DBSPOperator operator) {
        return Utilities.getExists(this.fanout, operator);
    }
}
