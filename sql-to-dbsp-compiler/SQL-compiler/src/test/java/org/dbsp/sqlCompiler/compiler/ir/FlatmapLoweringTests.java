package org.dbsp.sqlCompiler.compiler.ir;

import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.sql.tools.ExpressionBuilder;
import org.dbsp.sqlCompiler.compiler.visitors.inner.EquivalenceContext;
import org.dbsp.sqlCompiler.compiler.visitors.outer.LowerCircuitVisitor;
import org.dbsp.sqlCompiler.ir.expression.DBSPBlockExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPFlatmap;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeArray;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeMap;
import org.dbsp.util.ExplicitShuffle;
import org.dbsp.util.IdShuffle;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class FlatmapLoweringTests {
    final ExpressionBuilder b = new ExpressionBuilder();

    private DBSPBlockExpression lower(DBSPFlatmap flatmap) {
        var compiler = new DBSPCompiler(new CompilerOptions());
        long start = DBSPVariablePath.nextGeneratedName();
        var expected = LowerCircuitVisitor.rewriteFlatmap(flatmap, compiler).to(DBSPClosureExpression.class);
        if (!Boolean.getBoolean("feldera.optimizer.compareRustFlatmap"))
            return expected.body.to(DBSPBlockExpression.class);
        long end = DBSPVariablePath.nextGeneratedName();
        DBSPVariablePath.restoreGeneratedNames(start);
        compiler.options.ioOptions.rustOptimizerPasses = true;
        var actual = LowerCircuitVisitor.rewriteFlatmap(flatmap, compiler).to(DBSPClosureExpression.class);
        Assert.assertEquals(end, DBSPVariablePath.nextGeneratedName());
        Assert.assertTrue(EquivalenceContext.equiv(expected, actual));
        return actual.body.to(DBSPBlockExpression.class);
    }

    @Test public void shuffleRepeatsOrdinalAndDropsPassthrough() {
        var rowType = b.tup(b.i32(), new DBSPTypeArray(b.tup(b.i32(), b.i32()), true));
        var row = rowType.ref().var();
        var argument = b.tup(b.i32(), b.i32(), b.i32()).ref().var();
        var value = argument.deref().field(1).closure(argument);
        var ordinal = argument.deref().field(2).closure(argument);
        var flatmap = new DBSPFlatmap(CalciteObject.EMPTY, rowType, row.deref().field(1).closure(row),
                List.of(0), List.of(value, ordinal), b.i32(), new ExplicitShuffle(3, List.of(2, 1, 2)));
        Assert.assertEquals(2, lower(flatmap).contents.size());
    }

    @Test public void nullableMapEmitsKeyAndValueWithPassthrough() {
        var rowType = b.tup(b.i32(), new DBSPTypeMap(b.i32(), b.i32n(), true));
        var row = rowType.ref().var();
        var flatmap = new DBSPFlatmap(CalciteObject.EMPTY, rowType, row.deref().field(1).closure(row),
                List.of(0), null, null, new IdShuffle(3));
        Assert.assertEquals(3, lower(flatmap).contents.size());
    }
}
