package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPViewOperator;
import org.dbsp.sqlCompiler.compiler.IErrorReporter;

/** Remove DBSPViewOperator operators */
public class RemoveViewOperators extends CircuitCloneVisitor {
    public RemoveViewOperators(IErrorReporter reporter) {
        super(reporter, false);
    }

    @Override
    public void postorder(DBSPViewOperator operator) {
        DBSPOperator input = this.mapped(operator.input());
        this.map(operator, input, false);
    }
}
