package org.dbsp.sqlCompiler.compiler.visitors.outer.recursive;

import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.circuit.operator.DBSPAntiJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPLeftJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPNestedOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSumOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitCloneVisitor;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;

/** The {@link org.dbsp.sqlCompiler.circuit.operator.DBSPLeftJoinOperator} does not
 * work inside recursive components; replace it with a standard expansion using a normal join
 * and an antijoin. */
class RemoveLeftJoins extends CircuitCloneVisitor {
    public RemoveLeftJoins(DBSPCompiler compiler) {
        super(compiler, false);
    }

    @Override
    public void postorder(DBSPLeftJoinOperator operator) {
        if (this.getParent().is(DBSPNestedOperator.class)) {
            OutputPort left = this.mapped(operator.left());
            OutputPort right = this.mapped(operator.right());
            DBSPJoinOperator join = new DBSPJoinOperator(
                    operator.getRelNode(), operator.getOutputZSetType(), operator.getFunction(), operator.isMultiset,
                    left, right);
            this.addOperator(join);
            DBSPAntiJoinOperator anti = new DBSPAntiJoinOperator(
                    operator.getRelNode(), left, right);
            this.addOperator(anti);

            // Fill in right-hand side fields with null; this is produced by
            // a specialized version of the function of the left join operator
            // where we substitute None for the right input.
            DBSPClosureExpression closure = operator.getClosureFunction();
            DBSPVariablePath var = anti.getOutputIndexedZSetType().getKVRefType().var(closure.getNode());
            DBSPClosureExpression function = closure.call(
                    var.field(0).deref().applyCloneIfNeeded().borrow(),
                    var.field(1).deref().applyCloneIfNeeded().borrow(),
                    closure.parameters[2].getType().deref().none().borrow()).closure(var);
            DBSPMapOperator map = new DBSPMapOperator(operator.getRelNode(), function, anti.outputPort());
            this.addOperator(map);

            DBSPSumOperator sum = new DBSPSumOperator(operator.getRelNode(), join.outputPort(), map.outputPort());
            this.map(operator, sum);
            return;
        }
        super.postorder(operator);
    }
}
