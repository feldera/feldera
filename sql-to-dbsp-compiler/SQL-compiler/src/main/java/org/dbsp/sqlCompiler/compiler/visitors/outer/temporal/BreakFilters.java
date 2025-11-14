package org.dbsp.sqlCompiler.compiler.visitors.outer.temporal;

import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.circuit.operator.DBSPFilterOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSubtractOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSumOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.inner.Simplify;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitCloneVisitor;
import org.dbsp.sqlCompiler.ir.expression.DBSPBinaryExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPOpcode;
import org.dbsp.sqlCompiler.ir.expression.DBSPUnaryExpression;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeBool;

/** Visitor which finds filters that contain NOW expressions and breaks them into
 * simpler filters. */
public class BreakFilters extends CircuitCloneVisitor {
    final ContainsNow cn;

    public BreakFilters(DBSPCompiler compiler) {
        super(compiler, false);
        this.cn = new ContainsNow(compiler, true);
    }

    @Override
    public void postorder(DBSPFilterOperator filter) {
        DBSPClosureExpression function = filter.getClosureFunction();
        this.cn.apply(function);
        if (!this.cn.found) {
            super.postorder(filter);
            return;
        }

        OutputPort input = this.mapped(filter.input());
        Simplify simplify = new Simplify(this.compiler());
        function = simplify.apply(function).to(DBSPClosureExpression.class);

        DBSPExpression body = function.body;
        if (body.is(DBSPUnaryExpression.class)) {
            DBSPUnaryExpression unary = body.to(DBSPUnaryExpression.class);
            if (unary.opcode == DBSPOpcode.WRAP_BOOL) {
                body = unary.source;
            }
        }

        if (body.is(DBSPBinaryExpression.class)) {
            DBSPBinaryExpression bin = body.to(DBSPBinaryExpression.class);
            if (bin.opcode == DBSPOpcode.AND) {
                DBSPClosureExpression first = bin.left.wrapBoolIfNeeded().closure(function.parameters);
                DBSPClosureExpression second = bin.right.wrapBoolIfNeeded().closure(function.parameters);
                DBSPFilterOperator firstFilter =
                        new DBSPFilterOperator(filter.getRelNode(), first, input);
                this.addOperator(firstFilter);
                DBSPFilterOperator secondFilter =
                        new DBSPFilterOperator(filter.getRelNode(), second, firstFilter.outputPort());
                this.map(filter, secondFilter);
                return;
            } else if (bin.opcode == DBSPOpcode.OR) {
                // I -> filter(a || b) is rewritten as
                // (I -> filter(a)) union all (I -> filter(b)) except (I -> filter(a && b))
                DBSPClosureExpression first = bin.left.wrapBoolIfNeeded().closure(function.parameters);
                DBSPClosureExpression second = bin.right.wrapBoolIfNeeded().closure(function.parameters);
                DBSPFilterOperator firstFilter =
                        new DBSPFilterOperator(filter.getRelNode(), first, input);
                this.addOperator(firstFilter);

                DBSPFilterOperator secondFilter =
                        new DBSPFilterOperator(filter.getRelNode(), second, input);
                this.addOperator(secondFilter);

                DBSPSumOperator sum = new DBSPSumOperator(
                        filter.getRelNode(), firstFilter.outputPort(), secondFilter.outputPort());
                this.addOperator(sum);

                boolean nullable = bin.left.getType().mayBeNull || bin.right.getType().mayBeNull;
                DBSPClosureExpression and =
                        new DBSPBinaryExpression(first.getNode(), DBSPTypeBool.create(nullable), DBSPOpcode.AND,
                                bin.left, bin.right).wrapBoolIfNeeded().closure(function.parameters);
                DBSPFilterOperator thirdFilter =
                        new DBSPFilterOperator(filter.getRelNode(), and, input);
                this.addOperator(thirdFilter);

                DBSPSubtractOperator sub = new DBSPSubtractOperator(
                        filter.getRelNode(), sum.outputPort(), thirdFilter.outputPort());
                // Notice that no DISTINCT is necessary
                this.map(filter, sub);
                return;
            }
        }

        super.postorder(filter);
    }
}
