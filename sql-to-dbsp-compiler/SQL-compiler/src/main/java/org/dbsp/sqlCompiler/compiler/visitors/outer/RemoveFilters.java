package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.circuit.operator.DBSPConstantOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPFilterOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPBoolLiteral;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;

/** Removes filters constant predicates. */
public class RemoveFilters extends CircuitCloneVisitor {
    public RemoveFilters(DBSPCompiler compiler) {
        super(compiler, false);
    }

    @Nullable
    DBSPBoolLiteral isConstant(DBSPClosureExpression closure) {
        return closure.body.as(DBSPBoolLiteral.class);
    }

    @Override
    public void postorder(DBSPFilterOperator operator) {
        DBSPClosureExpression closure = operator.getClosureFunction();
        DBSPBoolLiteral filter = this.isConstant(closure);
        if (filter != null) {
            Utilities.enforce(filter.value != null);
            if (filter.value) {
                OutputPort input = this.mapped(operator.input());
                this.map(operator.outputPort(), input, false);
            } else {
                DBSPType outputType = operator.getType();
                DBSPExpression value = PropagateEmptySources.emptySet(outputType);
                DBSPConstantOperator result = new DBSPConstantOperator(
                        operator.getRelNode(), value, operator.isMultiset);
                this.map(operator, result);
            }
        }
        else {
            super.postorder(operator);
        }
    }
}
