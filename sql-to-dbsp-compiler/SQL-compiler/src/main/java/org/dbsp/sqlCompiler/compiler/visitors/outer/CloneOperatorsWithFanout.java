package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.circuit.operator.DBSPFilterOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPUnaryOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.util.Linq;

/** We do something very simple: if an operator has a fanout greater than 1,
 * and if any of its successors can be merged with it, we just make a clone of the operator. */
public class CloneOperatorsWithFanout extends CircuitCloneWithGraphsVisitor {
    public CloneOperatorsWithFanout(DBSPCompiler compiler, CircuitGraphs graphs) {
        super(compiler, graphs, false);
    }

    boolean shouldCloneInput(DBSPUnaryOperator operator) {
        DBSPOperator input = operator.input().node();
        int inputFanout = this.getGraph().getFanout(input);
        if (inputFanout == 1)
            return false;
        return input.is(DBSPMapOperator.class) ||
                input.is(DBSPMapIndexOperator.class) ||
                input.is(DBSPFilterOperator.class);
    }

    void cloneInput(DBSPUnaryOperator operator) {
        if (this.shouldCloneInput(operator)) {
            DBSPUnaryOperator input = operator.input().node().to(DBSPUnaryOperator.class);
            OutputPort inputInput = this.mapped(input.input());
            DBSPOperator inputClone = input.withInputs(Linq.list(inputInput), true);
            this.addOperator(inputClone);
            DBSPOperator replace = operator.withInputs(Linq.list(inputClone.getOutput(0)), true);
            this.map(operator.getOutput(0), replace.getOutput(0));
        } else {
            super.postorder(operator);
        }
    }

    @Override
    public void postorder(DBSPMapOperator operator) {
        this.cloneInput(operator);
    }

    @Override
    public void postorder(DBSPMapIndexOperator operator) {
        this.cloneInput(operator);
    }

    @Override
    public void postorder(DBSPFilterOperator operator) {
        this.cloneInput(operator);
    }
}
