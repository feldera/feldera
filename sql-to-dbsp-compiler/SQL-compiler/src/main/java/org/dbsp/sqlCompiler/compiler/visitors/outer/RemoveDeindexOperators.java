package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.operator.*;
import org.dbsp.sqlCompiler.compiler.IErrorReporter;

/** Convert Deindex operators into simple Map operators. */
public class RemoveDeindexOperators extends CircuitCloneVisitor {
    public RemoveDeindexOperators(IErrorReporter reporter) {
        super(reporter, false);
    }

    @Override
    public void postorder(DBSPDeindexOperator operator) {
        OutputPort input = this.mapped(operator.input());
        DBSPMapOperator result = new DBSPMapOperator(operator.getNode(), operator.getFunction(),
                operator.getOutputZSetType(), input);
        this.map(operator, result);
    }
}
