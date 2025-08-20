package org.dbsp.sqlCompiler.compiler.ir;

import org.dbsp.sqlCompiler.circuit.operator.DBSPConstantOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteEmptyRel;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.inner.EquivalenceContext;
import org.dbsp.sqlCompiler.compiler.visitors.inner.ExpressionsCSE;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.inner.ResolveReferences;
import org.dbsp.sqlCompiler.compiler.visitors.inner.ValueNumbering;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.expression.DBSPBinaryExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPBlockExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPIfExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPLetExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPOpcode;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPUnaryExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.expression.DBSPZSetExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPBoolLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPI32Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPLiteral;
import org.dbsp.sqlCompiler.ir.statement.DBSPLetStatement;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeBool;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeInteger;
import org.dbsp.util.Linq;
import org.junit.Assert;
import org.junit.Test;

/** Unit tests for expression equivalence */
public class EquivalenceTests {
    static DBSPExpression neg(DBSPExpression expression) {
        return new DBSPUnaryExpression(
                expression.getNode(), expression.getType(), DBSPOpcode.NEG, expression);
    }

    static DBSPExpression binary(DBSPOpcode opcode, DBSPExpression left, DBSPExpression right) {
        return new DBSPBinaryExpression(
                left.getNode(), left.getType(), opcode, left, right);
    }

    static DBSPExpression add(DBSPExpression left, DBSPExpression right) {
        return binary(DBSPOpcode.ADD, left, right);
    }

    @Test
    public void testCSE() {
        DBSPCompiler compiler = new DBSPCompiler(new CompilerOptions());
        DBSPType i = new DBSPTypeInteger(CalciteObject.EMPTY, 32, true, true);
        DBSPTypeTuple tuple = new DBSPTypeTuple(i, i, i);
        DBSPVariablePath var = tuple.ref().var();
        DBSPExpression c = new DBSPI32Literal(1);
        DBSPExpression v0 = var.deref().field(0);
        DBSPExpression v1 = var.deref().field(1);
        DBSPExpression v2 = var.deref().field(2);
        DBSPExpression v0m = neg(v0);
        DBSPExpression p0 = binary(DBSPOpcode.DIV, v0m, v1);
        DBSPExpression a = binary(DBSPOpcode.MUL, p0, p0);
        DBSPExpression b = add(v2, c);
        DBSPExpression d = add(b, b);
        DBSPExpression body = new DBSPTupleExpression(a, d);
        DBSPClosureExpression closure = body.closure(var.asParameter());

        DBSPOperator fake = new DBSPConstantOperator(
                CalciteEmptyRel.INSTANCE, new DBSPZSetExpression(new DBSPBoolLiteral()), false, false);
        ValueNumbering numbering = new ValueNumbering(compiler);
        numbering.setOperatorContext(fake);
        numbering.apply(closure);
        ExpressionsCSE cse = new ExpressionsCSE(compiler, numbering.canonical);
        cse.setOperatorContext(fake);
        cse.apply(closure);
        IDBSPInnerNode translated = cse.get(closure);
        InnerVisitor visitor = new InnerVisitor(compiler) {
            int vars = 0;
            public void postorder(DBSPLetExpression expression) {
                this.vars++;
            }

            @Override
            public void endVisit() {
                // Find 2 common subexpressions; one has a single use
                Assert.assertEquals(2, this.vars);
            }
        };
        visitor.apply(translated);
    }

    @Test
    public void testCSENested() {
        DBSPCompiler compiler = new DBSPCompiler(new CompilerOptions());
        DBSPType i = new DBSPTypeInteger(CalciteObject.EMPTY, 32, true, true);
        DBSPTypeTuple tuple = new DBSPTypeTuple(i, i);
        DBSPVariablePath var = tuple.ref().var();

        DBSPLetStatement stat0 = new DBSPLetStatement("t0",
                add(var.deref().field(0), var.deref().field(1)));

        DBSPVariablePath t = tuple.ref().var();
        DBSPExpression let = new DBSPLetExpression(
                t, new DBSPTupleExpression(
                        stat0.getVarReference(),
                        neg(stat0.getVarReference())).borrow(),
                new DBSPTupleExpression(neg(t.deref().field(0)), neg(t.deref().field(0))));

        DBSPLetStatement stat1 = new DBSPLetStatement("t1", let);
        DBSPExpression block = new DBSPBlockExpression(
                Linq.list(stat0, stat1),
                stat1.getVarReference());
        DBSPClosureExpression closure = block.closure(var.asParameter());

        DBSPOperator fake = new DBSPConstantOperator(
                CalciteEmptyRel.INSTANCE, new DBSPZSetExpression(new DBSPBoolLiteral()), false, false);
        ValueNumbering numbering = new ValueNumbering(compiler);
        numbering.setOperatorContext(fake);
        numbering.apply(closure);
        ExpressionsCSE cse = new ExpressionsCSE(compiler, numbering.canonical);
        cse.setOperatorContext(fake);
        cse.apply(closure);
        IDBSPInnerNode translated = cse.get(closure);
        ResolveReferences resolver = new ResolveReferences(compiler, false);
        // Crash on incorrect translation.
        resolver.apply(translated);
    }

    @Test
    public void testConditionalCSE() {
        DBSPCompiler compiler = new DBSPCompiler(new CompilerOptions());
        DBSPType i = new DBSPTypeInteger(CalciteObject.EMPTY, 32, true, true);
        DBSPType b = DBSPTypeBool.create(true);
        DBSPTypeTuple tuple = new DBSPTypeTuple(i, i, i);
        DBSPVariablePath var = tuple.ref().var();
        DBSPExpression z = new DBSPI32Literal(1);
        DBSPExpression cond = new DBSPBinaryExpression(
                CalciteObject.EMPTY, b, DBSPOpcode.GTE, var.deref().field(0), z)
                .wrapBoolIfNeeded();

        DBSPExpression un = neg(var.deref().field(1));

        DBSPExpression if0 = new DBSPIfExpression(
                CalciteObject.EMPTY, cond, un, var.deref().field(2));
        DBSPExpression if1 = new DBSPIfExpression(
                CalciteObject.EMPTY, cond, var.deref().field(2), un);

        DBSPExpression body = new DBSPTupleExpression(if0, if1);
        DBSPClosureExpression closure = body.closure(var.asParameter());

        ValueNumbering numbering = new ValueNumbering(compiler);
        numbering.apply(closure);
        ExpressionsCSE cse = new ExpressionsCSE(compiler, numbering.canonical);
        DBSPOperator fake = new DBSPConstantOperator(
                CalciteEmptyRel.INSTANCE, new DBSPZSetExpression(new DBSPBoolLiteral()), false, false);
        cse.setOperatorContext(fake);
        cse.apply(closure);
        IDBSPInnerNode translated = cse.get(closure);

        InnerVisitor visitor = new InnerVisitor(compiler) {
            int vars = 0;
            public void postorder(DBSPLetExpression expression) {
                this.vars++;
            }

            @Override
            public void endVisit() {
                // Find 2 common subexpressions
                Assert.assertEquals(2, this.vars);
            }
        };
        visitor.apply(translated);
    }

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
        Assert.assertThrows(InternalCompilerError.class, () -> EquivalenceContext.equiv(var0, var1));
    }

    @SuppressWarnings("SuspiciousNameCombination")
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

        DBSPTypeTuple ii = new DBSPTypeTuple(i32, i32);
        DBSPTypeTuple iii = new DBSPTypeTuple(i32, i32, i32);
        DBSPVariablePath x5 = new DBSPVariablePath("x", ii);
        DBSPVariablePath y5 = new DBSPVariablePath("y", iii);
        DBSPExpression x0 = x5.field(0).closure(x5);
        DBSPExpression y0 = y5.field(0).closure(y5);
        Assert.assertFalse(EquivalenceContext.equiv(x0, y0));
    }
}
