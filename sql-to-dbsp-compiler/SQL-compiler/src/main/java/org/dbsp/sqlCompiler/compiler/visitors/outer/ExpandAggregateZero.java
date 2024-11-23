package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.operator.DBSPAggregateZeroOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPConstantOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPNegateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSumOperator;
import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.TypeCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPZSetLiteral;
import org.dbsp.util.Linq;

/** Replaces the {@link DBSPAggregateZeroOperator} with a graph;
 * see the diagram in the definition of this operator. */
public class ExpandAggregateZero extends CircuitCloneVisitor {
    public ExpandAggregateZero(DBSPCompiler compiler) {
        super(compiler, false);
    }

    @Override
    public void postorder(DBSPAggregateZeroOperator operator) {
        CalciteObject node = operator.getNode();
        DBSPExpression emptySetResult = operator.getFunction();
        OutputPort input = this.mapped(operator.input());
        DBSPVariablePath _t = emptySetResult.getType().ref().var();
        DBSPExpression toZero = emptySetResult.closure(_t);
        DBSPSimpleOperator map1 = new DBSPMapOperator(
                node, toZero, TypeCompiler.makeZSet(emptySetResult.getType()), input);
        this.addOperator(map1);
        DBSPSimpleOperator neg = new DBSPNegateOperator(node, map1.outputPort());
        this.addOperator(neg);
        DBSPSimpleOperator constant = new DBSPConstantOperator(
                node, new DBSPZSetLiteral(emptySetResult), false, false);
        this.addOperator(constant);
        DBSPSimpleOperator sum = new DBSPSumOperator(node, Linq.list(constant.outputPort(), neg.outputPort(), input));
        this.map(operator, sum);
    }
}
