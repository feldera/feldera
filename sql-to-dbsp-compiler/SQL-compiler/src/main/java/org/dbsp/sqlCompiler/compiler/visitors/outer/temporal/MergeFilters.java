package org.dbsp.sqlCompiler.compiler.visitors.outer.temporal;

import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.circuit.operator.DBSPFilterOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.ExpressionCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitCloneVisitor;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPOpcode;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeIndexedZSet;
import org.dbsp.util.Utilities;

/** Visitor which finds consecutive filters that contain NOW expressions that can be implemented in a single WINDOW
 * operator and merges them into a single filter operation. */
public class MergeFilters extends CircuitCloneVisitor {
    final ContainsNow cn;

    public MergeFilters(DBSPCompiler compiler) {
        super(compiler, false);
        this.cn = new ContainsNow(compiler, true);
    }

    private boolean canBeMerged(DBSPFilterOperator source, DBSPFilterOperator filter) {
        // We want to only merge filters that can be combined into a single window.
        // They should look like "col > expression0" and "col < expression1", respectively, for the same column.
        var s = FindComparisons.decomposeIntoTemporalFilters(this.compiler, source,
                source.getClosureFunction().ensureTree(this.compiler).to(DBSPClosureExpression.class));
        var f = FindComparisons.decomposeIntoTemporalFilters(this.compiler, filter,
                filter.getClosureFunction().ensureTree(this.compiler).to(DBSPClosureExpression.class));
        if (s.size() != 1 || f.size() != 1)
            return false;
        BooleanExpression se = s.get(0);
        BooleanExpression fe = f.get(0);
        if (!se.is(TemporalFilter.class) || !fe.is(TemporalFilter.class))
            return false;
        return se.compatible(fe);
    }

    @Override
    public void postorder(DBSPFilterOperator filter) {
        OutputPort input = this.mapped(filter.input());
        if (!input.node().is(DBSPFilterOperator.class) || filter.outputType().is(DBSPTypeIndexedZSet.class)) {
            super.postorder(filter);
            return;
        }

        DBSPFilterOperator source = input.to(DBSPFilterOperator.class);
        DBSPClosureExpression sourceFunction = source.getClosureFunction();
        DBSPClosureExpression function = filter.getClosureFunction();
        this.cn.apply(function);
        boolean found = this.cn.found;
        this.cn.apply(sourceFunction);
        if (!found || !this.cn.found) {
            super.postorder(filter);
            return;
        }

        if (!this.canBeMerged(source, filter)) {
            super.postorder(filter);
            return;
        }

        Utilities.enforce(sourceFunction.parameters.length == 1);
        Utilities.enforce(function.parameters.length == 1);
        DBSPVariablePath var = sourceFunction.parameters[0].getType().var();
        DBSPExpression combined = ExpressionCompiler.makeBinaryExpression(
                sourceFunction.getNode(),
                function.getResultType(),
                DBSPOpcode.AND,
                sourceFunction.call(var),
                function.call(var)).closure(var).reduce(this.compiler);
        DBSPFilterOperator newFilter = new DBSPFilterOperator(filter.getRelNode(), combined, source.input());
        this.map(filter, newFilter);
    }
}
