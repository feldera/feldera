package org.dbsp.sqlCompiler.compiler.ir;

import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.sql.tools.BaseSQLTests;
import org.dbsp.sqlCompiler.compiler.sql.tools.ExpressionBuilder;
import org.dbsp.sqlCompiler.compiler.visitors.inner.CanonicalForm;
import org.dbsp.sqlCompiler.compiler.visitors.inner.Simplify;
import org.dbsp.sqlCompiler.compiler.visitors.inner.SimplifyConditionals;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPLetExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPOpcode;
import org.junit.Assert;
import org.junit.Test;

/** Unit tests for {@link SimplifyConditionals} */
public class TestSimplifyConditionals extends BaseSQLTests {
    final ExpressionBuilder b = new ExpressionBuilder();

    @Test
    public void testVariable() {
        DBSPCompiler compiler = this.testCompiler();
        // clo = |x: boolean| if (x) { x } else { !x }
        var clo = b.lambda(b.bool(), x ->
                b.ifThenElse(x.deepCopy(), x.deepCopy(), x.deepCopy().not()));

        SimplifyConditionals sc = new SimplifyConditionals(compiler);
        var result = sc.apply(clo);
        CanonicalForm cf = new CanonicalForm(compiler);
        result = cf.apply(result);

        Assert.assertEquals("""
                (|p0: b|
                (if p0 {
                    true
                } else {
                    (! false)
                }))""", result.toString());
        Simplify simplify = new Simplify(compiler);
        result = simplify.apply(result);
        result = cf.apply(result);

        Assert.assertEquals("""
                (|p0: b|
                true)""", result.toString());
    }

    @Test
    public void testComplexComparison() {
        DBSPCompiler compiler = this.testCompiler();
        // |x: i32| {
        //    if ((x + 1) < 0) {
        //      if ((x + 1) < 0) { 0 } else { 1 }
        //    } else {
        //      2
        //    }
        // }
        var clo = b.lambda(b.i32(), x -> {
            DBSPExpression lZ = b.binary(DBSPOpcode.LT, b.add(x, b.lit(1)), b.lit(0));
            return b.ifThenElse(
                    lZ.deepCopy(),
                    b.ifThenElse(lZ.deepCopy(), b.lit(0), b.lit(1)),
                    b.lit(2));
        });
        CanonicalForm cf = new CanonicalForm(compiler);

        SimplifyConditionals sc = new SimplifyConditionals(compiler);
        var result = sc.apply(clo);
        result = cf.apply(result);
        Assert.assertEquals("""
                (|p0: i32|
                (if ((p0 + 1) < 0) {
                    (if true {
                        0
                    } else {
                        1
                    })
                } else {
                    2
                }))""", result.toString());

        Simplify simplify = new Simplify(compiler);
        result = simplify.apply(result);
        result = cf.apply(result);
        Assert.assertEquals("""
                (|p0: i32|
                (if ((p0 + 1) < 0) {
                    0
                } else {
                    2
                }))""", result.toString());
    }

    @Test
    public void testAliasedVariable() {
        DBSPCompiler compiler = this.testCompiler();
        // The compiler never reuses variable names, but this is supposed to work
        // too; the let deliberately rebinds the lambda's own parameter node.
        // inner = |x: i32| {
        //    if ((x + 1) < 0) {
        //      let x = x + 1;
        //      if ((x + 1) < 0) { 0 } else { 1 }
        //    } else {
        //      2
        //    }
        var clo = b.lambda(b.i32(), x -> {
            DBSPExpression xPlusOne = b.add(x, b.lit(1));
            DBSPExpression lZ = b.binary(DBSPOpcode.LT, xPlusOne, b.lit(0));
            var innerIf = b.ifThenElse(lZ.deepCopy(), b.lit(0), b.lit(1));
            var let = new DBSPLetExpression(x, xPlusOne.deepCopy(), innerIf);
            return b.ifThenElse(lZ.deepCopy(), let, b.lit(2));
        });
        CanonicalForm cf = new CanonicalForm(compiler);
        var initial = cf.apply(clo);
        Assert.assertEquals("""
                (|p0: i32|
                (if ((p0 + 1) < 0) {
                    {let p1 = (p0 + 1);
                    (if ((p1 + 1) < 0) {
                        0
                    } else {
                        1
                    })}
                } else {
                    2
                }))""", initial.toString());

        // This cannot be simplified
        SimplifyConditionals sc = new SimplifyConditionals(compiler);
        var result = sc.apply(clo);
        result = cf.apply(result);
        Assert.assertEquals(initial.toString(), result.toString());
    }
}
