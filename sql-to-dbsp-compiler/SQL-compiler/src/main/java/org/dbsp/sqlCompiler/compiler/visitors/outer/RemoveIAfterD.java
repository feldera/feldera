package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.operator.DBSPConstantOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDifferentiateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIntegrateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPUnaryOperator;
import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIncrementalConstantOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;

/** Remove I immediately after D, and replace I after an incremental constant with the constant.
 * Run after OptimizeIncrementalVisitor in case these patterns are left over. */
public class RemoveIAfterD extends CircuitCloneVisitor {
    public RemoveIAfterD(DBSPCompiler compiler) {
        super(compiler, false);
    }

    @Override
    public void postorder(DBSPIntegrateOperator operator) {
        OutputPort source = this.mapped(operator.input());
        if (source.node().is(DBSPDifferentiateOperator.class)) {
            DBSPUnaryOperator integral = source.node().to(DBSPUnaryOperator.class);
            this.map(operator.outputPort(), integral.input(), false);  // It should already be there
            return;
        }
        if (source.node().is(DBSPIncrementalConstantOperator.class)) {
            DBSPSimpleOperator changes = source.simpleNode();
            DBSPSimpleOperator constant = new DBSPConstantOperator(operator.getRelNode(), changes.getFunction())
                    .copyAnnotations(changes);
            this.map(operator, constant);
            return;
        }
        super.postorder(operator);
    }
}
