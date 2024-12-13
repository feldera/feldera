package org.dbsp.sqlCompiler.compiler.ir;

import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.inner.EquivalenceContext;
import org.dbsp.sqlCompiler.ir.expression.DBSPBinaryExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPBlockExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPOpcode;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPI32Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPLiteral;
import org.dbsp.sqlCompiler.ir.statement.DBSPLetStatement;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.util.Linq;
import org.junit.Assert;
import org.junit.Test;

/** Unit tests for expression equivalence */
public class EquivalenceTests {
    @Test
    public void testEquiv() {
        DBSPLiteral zero0 = new DBSPI32Literal(0);
        DBSPType i32 = zero0.getType();
        DBSPLiteral zero1 = new DBSPI32Literal(0);
        Assert.assertTrue(EquivalenceContext.equiv(zero0, zero1));

        DBSPLiteral one = new DBSPI32Literal(1);
        Assert.assertFalse(EquivalenceContext.equiv(zero0, one));

        DBSPExpression plus0 = new DBSPBinaryExpression(
                CalciteObject.EMPTY, zero0.getType(), DBSPOpcode.ADD, zero0, one);
        DBSPExpression plus1 = new DBSPBinaryExpression(
                CalciteObject.EMPTY, zero0.getType(), DBSPOpcode.ADD, zero1, one);
        Assert.assertTrue(EquivalenceContext.equiv(plus0, plus1));

        DBSPExpression plus2 = new DBSPBinaryExpression(
                CalciteObject.EMPTY, zero0.getType(), DBSPOpcode.ADD, one, one);
        Assert.assertFalse(EquivalenceContext.equiv(plus2, plus1));

        DBSPVariablePath var0 = new DBSPVariablePath("x", i32);
        DBSPVariablePath var1 = new DBSPVariablePath("y", i32);
        // Expressions cannot have free variables
        Assert.assertThrows(AssertionError.class, () -> EquivalenceContext.equiv(var0, var1));
    }

    @Test
    public void testLambdas() {
        DBSPLiteral zero0 = new DBSPI32Literal(0);
        DBSPType i32 = zero0.getType();
        DBSPVariablePath x = new DBSPVariablePath("x", i32);
        DBSPExpression id0 = x.closure(x);

        DBSPVariablePath x1 = new DBSPVariablePath("x", i32);
        DBSPExpression id1 = x1.closure(x1);
        Assert.assertTrue(EquivalenceContext.equiv(id0, id1));

        DBSPVariablePath y = new DBSPVariablePath("y", i32);
        DBSPExpression id2 = y.closure(y);
        Assert.assertTrue(EquivalenceContext.equiv(id0, id2));

        DBSPVariablePath x2 = new DBSPVariablePath("x", i32);
        DBSPVariablePath y2 = new DBSPVariablePath("y", i32);
        DBSPExpression plus0 = new DBSPBinaryExpression(
                CalciteObject.EMPTY, i32, DBSPOpcode.ADD, x2, y2);
        DBSPExpression lambda0 = plus0.closure(x2, y2);

        DBSPVariablePath x3 = new DBSPVariablePath("x", i32);
        DBSPVariablePath y3 = new DBSPVariablePath("y", i32);
        DBSPExpression plus1 = new DBSPBinaryExpression(
                CalciteObject.EMPTY, i32, DBSPOpcode.ADD, y3, x3);
        DBSPExpression lambda1 = plus1.closure(x3, y3);
        // Compiler doesn't know that ADD is commutative
        Assert.assertFalse(EquivalenceContext.equiv(lambda0, lambda1));

        DBSPVariablePath x4 = new DBSPVariablePath("x", i32);
        DBSPVariablePath y4 = new DBSPVariablePath("y", i32);
        DBSPExpression plus1_1 = new DBSPBinaryExpression(
                CalciteObject.EMPTY, i32, DBSPOpcode.ADD, x4, y4);
        DBSPExpression lambda2 = plus1_1.closure(x4, y4);
        Assert.assertTrue(EquivalenceContext.equiv(lambda0, lambda2));

        DBSPLetStatement stat0 = new DBSPLetStatement("z", plus0);
        DBSPBlockExpression block0 = new DBSPBlockExpression(Linq.list(stat0), stat0.getVarReference());
        DBSPLetStatement stat1 = new DBSPLetStatement("w", plus1);
        DBSPBlockExpression block1 = new DBSPBlockExpression(Linq.list(stat1), stat1.getVarReference());
        DBSPExpression blockLambda0 = block0.closure(
                x.to(DBSPVariablePath.class),
                y.deepCopy().to(DBSPVariablePath.class));
        DBSPExpression blockLambda1 = block1.closure(
                y.deepCopy().to(DBSPVariablePath.class),
                x.deepCopy().to(DBSPVariablePath.class));
        Assert.assertTrue(EquivalenceContext.equiv(blockLambda0, blockLambda1));
    }
}
