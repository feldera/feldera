package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.operator.DBSPDifferentiateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIntegrateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPUnaryOperator;
import org.dbsp.sqlCompiler.circuit.operator.OutputPort;
import org.dbsp.sqlCompiler.compiler.IErrorReporter;

/** Remove I immediately after D.
 * Run after OptimizeIncrementalVisitor in case this pattern is left over. */
public class RemoveIAfterD extends CircuitCloneVisitor {
    public RemoveIAfterD(IErrorReporter reporter) {
        super(reporter, false);
    }

    @Override
    public void postorder(DBSPIntegrateOperator operator) {
        OutputPort source = this.mapped(operator.input());
        if (source.node().is(DBSPDifferentiateOperator.class)) {
            DBSPUnaryOperator integral = source.node().to(DBSPUnaryOperator.class);
            this.map(operator.outputPort(), integral.input(), false);  // It should already be there
            return;
        }
        super.postorder(operator);
    }
}
