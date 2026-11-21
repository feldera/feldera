package org.dbsp.sqlCompiler.compiler.ir;

import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.backend.RustCircuitPass;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.sql.tools.ExpressionBuilder;
import org.dbsp.sqlCompiler.compiler.visitors.inner.EquivalenceContext;
import org.dbsp.sqlCompiler.ir.aggregate.DBSPAggregateList;
import org.dbsp.sqlCompiler.ir.aggregate.DBSPFold;
import org.dbsp.sqlCompiler.ir.aggregate.NonLinearAggregate;
import org.dbsp.sqlCompiler.ir.expression.DBSPApplyExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPAssignmentExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPBlockExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.statement.DBSPExpressionStatement;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeWeight;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class AggregateFoldTests {
    final ExpressionBuilder b = new ExpressionBuilder();

    DBSPFold lower(DBSPAggregateList list) {
        var compiler = new DBSPCompiler(new CompilerOptions());
        long start = DBSPVariablePath.nextGeneratedName();
        var expected = list.asFold(compiler);
        if (!Boolean.getBoolean("feldera.optimizer.compareRustAggregateFold"))
            return expected;
        long end = DBSPVariablePath.nextGeneratedName();
        DBSPVariablePath.restoreGeneratedNames(start);
        var actual = RustCircuitPass.lowerAggregateFold(compiler, list);
        Assert.assertEquals(end, DBSPVariablePath.nextGeneratedName());
        Assert.assertTrue(EquivalenceContext.equiv(expected, actual));
        return actual;
    }

    @Test
    public void emptyFoldUsesTheSharedAccumulatorType() {
        var fold = lower(new DBSPAggregateList(CalciteObject.EMPTY, b.i32().var(), List.of()));
        Assert.assertSame(DBSPTypeTuple.EMPTY, fold.increment.parameters[0].getType().deref());
        Assert.assertSame(DBSPTypeTuple.EMPTY, fold.postProcess.parameters[0].getType());
        Assert.assertSame(DBSPTypeWeight.INSTANCE, fold.increment.parameters[2].getType());
    }

    @Test
    public void combineInPlaceAndValueUpdatesWithCustomWeight() {
        var row = b.i32().var();
        var acc = b.i32().var();
        var weight = b.i32().var();
        var sum = b.add(acc, weight);
        var zero = b.lit(0);
        var mutable = new NonLinearAggregate(CalciteObject.EMPTY, zero,
                new DBSPAssignmentExpression(acc, sum).closure(acc, row, weight),
                zero, DBSPTypeWeight.INSTANCE);
        var value = new NonLinearAggregate(CalciteObject.EMPTY, zero, sum.closure(acc, row, weight),
                b.neg(acc).closure(acc), zero, DBSPTypeWeight.INSTANCE);
        var fold = lower(new DBSPAggregateList(CalciteObject.EMPTY, row, List.of(mutable, value)));
        Assert.assertSame(weight.getType(), fold.increment.parameters[2].getType());
        Assert.assertEquals(2, fold.postProcess.getResultType().to(DBSPTypeTuple.class).size());
    }

    @Test
    public void preserveDistinctRowOccurrencesWhenRebindingTheSameVariable() {
        var row = b.i32().var();
        var first = new DBSPVariablePath(row.variable, row.getType());
        var second = new DBSPVariablePath(row.variable, row.getType());
        var incoming = new DBSPApplyExpression("read_pair", b.i32(), first, second);
        var zero = b.lit(0);
        var component = new NonLinearAggregate(CalciteObject.EMPTY, zero,
                incoming.closure(b.i32().var(), row, DBSPTypeWeight.INSTANCE.var()),
                zero, DBSPTypeWeight.INSTANCE);
        var fold = lower(new DBSPAggregateList(CalciteObject.EMPTY, row, List.of(component)));
        var body = fold.increment.body.to(DBSPBlockExpression.class);
        var assignment = body.contents.get(0).to(DBSPExpressionStatement.class).expression
                .to(DBSPAssignmentExpression.class);
        var call = assignment.right.to(DBSPApplyExpression.class);
        Assert.assertSame(first, call.arguments[0]);
        Assert.assertSame(second, call.arguments[1]);
    }
}
