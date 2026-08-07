package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.circuit.operator.DBSPConstantOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDifferentiateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSinkOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;

import java.util.HashSet;
import java.util.Set;

/** Give a warning when a view is fed from a constant operator
 * (via potentially a differentiator) */
public class ConstantViews extends CircuitVisitor {
    final Set<OutputPort> constant = new HashSet<>();

    public ConstantViews(DBSPCompiler compiler) {
        super(compiler);
    }

    @Override
    public void postorder(DBSPConstantOperator operator) {
        this.constant.add(operator.outputPort());
    }

    @Override
    public void postorder(DBSPDifferentiateOperator operator) {
        if (this.constant.contains(operator.input())) {
            this.constant.add(operator.outputPort());
        }
    }

    @Override
    public void postorder(DBSPSinkOperator operator) {
        if (operator.viewName.equals(DBSPCompiler.ERROR_VIEW_NAME))
            return;
        if (this.constant.contains(operator.input())) {
            this.compiler.reportWarning(
                    operator.getSourcePosition(),
                    "View is constant",
                    "View " + operator.viewName.singleQuote() + " does not depend on any input data");
        }
    }
}
