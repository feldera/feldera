package org.dbsp.sqlCompiler.compiler.ir;

import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.sql.tools.ExpressionBuilder;
import org.dbsp.sqlCompiler.compiler.visitors.inner.DetectShuffle;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeCode;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeInteger;
import org.junit.Assert;
import org.junit.Test;

/** Tests for the DetectShuffle visitor */
public class DetectShuffleTests {
    final ExpressionBuilder b = new ExpressionBuilder();

    @Test
    public void detectShuffle() {
        final DBSPCompiler compiler = new DBSPCompiler(new CompilerOptions());
        final var tuple = b.tup(
                DBSPTypeInteger.getType(CalciteObject.EMPTY, DBSPTypeCode.INT64, false),
                DBSPTypeInteger.getType(CalciteObject.EMPTY, DBSPTypeCode.INT32, false),
                b.tup(DBSPTypeInteger.getType(CalciteObject.EMPTY, DBSPTypeCode.INT16, true)));
        final var var = b.refVar(tuple);

        // simple shuffle
        final DBSPClosureExpression clo0 =
                b.tuple(b.field(var, 0), b.field(var, 2)).closure(var);
        var shuffle = DetectShuffle.analyze(compiler, clo0);
        Assert.assertNotNull(shuffle);
        Assert.assertEquals("[0, 2]", shuffle.toString());

        // clone()
        final DBSPClosureExpression clo1 =
                b.tuple(b.field(var, 0).applyClone(), b.field(var, 1)).closure(var);
        shuffle = DetectShuffle.analyze(compiler, clo1);
        Assert.assertNotNull(shuffle);
        Assert.assertEquals("[0, 1]", shuffle.toString());

        // repeated fields
        final DBSPClosureExpression clo2 =
                b.tuple(b.field(var, 2), b.field(var, 2)).closure(var);
        shuffle = DetectShuffle.analyze(compiler, clo2);
        Assert.assertNotNull(shuffle);
        Assert.assertEquals("[2, 2]", shuffle.toString());

        // not a shuffle: constant field
        final DBSPClosureExpression clo3 =
                b.tuple(b.field(var, 2), b.lit(10)).closure(var);
        shuffle = DetectShuffle.analyze(compiler, clo3);
        Assert.assertNull(shuffle);

        // not a shuffle: nested deref
        final DBSPClosureExpression clo4 =
                b.tuple(b.field(var, 2).field(0), b.field(var, 1)).closure(var);
        shuffle = DetectShuffle.analyze(compiler, clo4);
        Assert.assertNull(shuffle);
    }
}
