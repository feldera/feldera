package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.circuit.operator.*;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;

/** Convert Deindex operators into simple Map operators. */
public class RemoveDeindexOperators extends CircuitCloneVisitor {
    public RemoveDeindexOperators(DBSPCompiler compiler) {
        super(compiler, false);
    }

    @Override
    public void postorder(DBSPDeindexOperator operator) {
        OutputPort input = this.mapped(operator.input());
        DBSPMapOperator result = new DBSPMapOperator(operator.getRelNode(), operator.getFunction(),
                operator.getOutputZSetType(), input);
        this.map(operator, result);
    }
}
