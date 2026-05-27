package org.dbsp.sqlCompiler.compiler.ir;

import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.sql.tools.BaseSQLTests;
import org.dbsp.sqlCompiler.compiler.visitors.inner.CanonicalForm;
import org.dbsp.sqlCompiler.compiler.visitors.inner.Simplify;
import org.dbsp.sqlCompiler.compiler.visitors.inner.SimplifyConditionals;
import org.dbsp.sqlCompiler.ir.expression.DBSPBinaryExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPIfExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPLetExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPOpcode;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPI32Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPIntLiteral;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeCode;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeBool;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeInteger;
import org.junit.Assert;
import org.junit.Test;

/** Unit tests for {@link SimplifyConditionals} */
public class TestSimplifyConditionals extends BaseSQLTests {
    @Test
    public void testVariable() {
        DBSPCompiler compiler = this.testCompiler();
        DBSPType b = DBSPTypeBool.INSTANCE;
        DBSPVariablePath x = b.var();
        // inner = |x: boolean| if (x) { x } else { !x }
        DBSPClosureExpression clo = new DBSPIfExpression(
                CalciteObject.EMPTY,
                x.deepCopy(),
                x.deepCopy(),
                x.deepCopy().not()).closure(x);

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
        DBSPType i32 = DBSPTypeInteger.getType(CalciteObject.EMPTY, DBSPTypeCode.INT32, false);
        DBSPType b = DBSPTypeBool.INSTANCE;

        DBSPVariablePath x = i32.var();
        DBSPIntLiteral two = new DBSPI32Literal(CalciteObject.EMPTY, i32, 2);
        DBSPIntLiteral one = new DBSPI32Literal(CalciteObject.EMPTY, i32, 1);
        DBSPIntLiteral zero = new DBSPI32Literal(CalciteObject.EMPTY, i32, 0);
        DBSPExpression xPlusOne = new DBSPBinaryExpression(CalciteObject.EMPTY, i32, DBSPOpcode.ADD, x, one);
        DBSPExpression lZ = new DBSPBinaryExpression(CalciteObject.EMPTY, b, DBSPOpcode.LT, xPlusOne, zero);
        // |x: i32| {
        //    if ((x + 1) < 0) {
        //      if ((x + 1) < 0) { 0 } else { 1 }
        //    } else {
        //      2
        //    }
        // }
        var innerIf = new DBSPIfExpression(
                CalciteObject.EMPTY,
                lZ.deepCopy(),
                zero.deepCopy(),
                one.deepCopy());
        var clo = new DBSPIfExpression(
                CalciteObject.EMPTY,
                lZ.deepCopy(),
                innerIf,
                two).closure(x);
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
        DBSPType i32 = DBSPTypeInteger.getType(CalciteObject.EMPTY, DBSPTypeCode.INT32, false);
        DBSPType b = DBSPTypeBool.INSTANCE;

        // The compiler never reuses variable names, but this is supposed to work too.
        DBSPVariablePath x = i32.var();
        DBSPIntLiteral two = new DBSPI32Literal(CalciteObject.EMPTY, i32, 2);
        DBSPIntLiteral one = new DBSPI32Literal(CalciteObject.EMPTY, i32, 1);
        DBSPIntLiteral zero = new DBSPI32Literal(CalciteObject.EMPTY, i32, 0);
        DBSPExpression xPlusOne = new DBSPBinaryExpression(CalciteObject.EMPTY, i32, DBSPOpcode.ADD, x, one);
        DBSPExpression lZ = new DBSPBinaryExpression(CalciteObject.EMPTY, b, DBSPOpcode.LT, xPlusOne, zero);
        // inner = |x: i32| {
        //    if ((x + 1) < 0) {
        //      let x = x + 1;
        //      if ((x + 1) < 0) { 0 } else { 1 }
        //    } else {
        //      2
        //    }
        var innerIf = new DBSPIfExpression(
                CalciteObject.EMPTY,
                lZ.deepCopy(),
                zero.deepCopy(),
                one.deepCopy());
        var let = new DBSPLetExpression(x, xPlusOne.deepCopy(), innerIf);
        var clo = new DBSPIfExpression(
                CalciteObject.EMPTY,
                lZ.deepCopy(),
                let,
                two).closure(x);
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
