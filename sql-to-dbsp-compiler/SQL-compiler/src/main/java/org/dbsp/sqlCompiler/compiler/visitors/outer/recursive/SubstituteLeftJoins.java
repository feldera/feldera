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
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeIndexedZSet;

/** The {@link DBSPLeftJoinOperator} does not work inside recursive components;
 * replace it with the "standard" expansion:
 * lj(a, b) = sum(join(a, b), map(antijoin(a, b)))
 * */
public class SubstituteLeftJoins extends CircuitCloneVisitor {
    public SubstituteLeftJoins(DBSPCompiler compiler) {
        super(compiler, false);
    }

    /**
     * Specializes the closure from a LeftJoinOperator, i.e.,
     * Given a function `closure` with 3 parameters closure(k, l, r), where r is nullable,
     * produces a closure with the following body: |x| closure(*x.0, *x.1, None) */
    public static DBSPClosureExpression createMapFunction(DBSPCompiler compiler, DBSPLeftJoinOperator join) {
        DBSPClosureExpression closure = join.getClosureFunction();
        // Result type is from the left input
        DBSPTypeIndexedZSet ix = join.left().getOutputIndexedZSetType();
        DBSPVariablePath var = ix.getKVRefType().var(closure.getNode());
        return closure.call(
                        var.field(0).deref().applyCloneIfNeeded().borrow(),
                        var.field(1).deref().applyCloneIfNeeded().borrow(),
                        closure.parameters[2].getType().deref().none().borrow())
                .reduce(compiler)
                .closure(var);
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
            DBSPAntiJoinOperator anti = new DBSPAntiJoinOperator(operator.getRelNode(), left, right);
            this.addOperator(anti);

            // Fill in right-hand side fields with null; this is produced by
            // a specialized version of the function of the left join operator
            // where we substitute None for the right input.
            DBSPClosureExpression function = SubstituteLeftJoins.createMapFunction(compiler, operator);
            DBSPMapOperator map = new DBSPMapOperator(operator.getRelNode(), function, anti.outputPort());
            this.addOperator(map);

            DBSPSumOperator sum = new DBSPSumOperator(operator.getRelNode(), join.outputPort(), map.outputPort());
            this.map(operator, sum);
            return;
        }
        super.postorder(operator);
    }
}
