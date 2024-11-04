package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPViewOperator;
import org.dbsp.sqlCompiler.circuit.operator.OperatorPort;
import org.dbsp.sqlCompiler.compiler.IErrorReporter;

/** Remove DBSPViewOperator operators that are not recursive. */
public class RemoveViewOperators extends CircuitCloneVisitor {
    public RemoveViewOperators(IErrorReporter reporter) {
        super(reporter, false);
    }

    @Override
    public void postorder(DBSPViewOperator operator) {
        if (operator.metadata.recursive) {
            super.postorder(operator);
            return;
        }
        OperatorPort input = this.mapped(operator.input());
        this.map(operator.getOutput(), input, false);
    }
}
