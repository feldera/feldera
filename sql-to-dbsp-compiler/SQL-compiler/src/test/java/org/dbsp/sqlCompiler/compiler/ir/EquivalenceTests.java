package org.dbsp.sqlCompiler.compiler.ir;

import org.dbsp.sqlCompiler.circuit.operator.DBSPConstantOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteEmptyRel;
import org.dbsp.sqlCompiler.compiler.sql.tools.ExpressionBuilder;
import org.dbsp.sqlCompiler.compiler.visitors.inner.EquivalenceContext;
import org.dbsp.sqlCompiler.compiler.visitors.inner.ExpressionsCSE;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.inner.ResolveReferences;
import org.dbsp.sqlCompiler.compiler.visitors.inner.ValueNumbering;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.expression.DBSPBlockExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPLetExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPOpcode;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.expression.DBSPZSetExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPBoolLiteral;
import org.dbsp.sqlCompiler.ir.statement.DBSPLetStatement;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTuple;
import org.dbsp.util.Linq;
import org.junit.Assert;
import org.junit.Test;

/** Unit tests for expression equivalence.
 * The alpha-equivalence tests build variables by hand: they need specific
 * names shared between distinct nodes, which {@link ExpressionBuilder}
 * avoids by construction. */
public class EquivalenceTests {
    final ExpressionBuilder b = new ExpressionBuilder();

    /** Count the let expressions that CSE introduced */
    static void assertLets(DBSPCompiler compiler, IDBSPInnerNode node, int expected) {
        InnerVisitor visitor = new InnerVisitor(compiler) {
            int lets = 0;

            @Override
            public void postorder(DBSPLetExpression expression) {
                this.lets++;
            }

            @Override
            public void endVisit() {
                Assert.assertEquals(expected, this.lets);
            }
        };
        visitor.apply(node);
    }

    static IDBSPInnerNode cse(DBSPCompiler compiler, DBSPClosureExpression closure) {
        DBSPOperator fake = new DBSPConstantOperator(
                CalciteEmptyRel.INSTANCE, new DBSPZSetExpression(new DBSPBoolLiteral()), false);
        ValueNumbering numbering = new ValueNumbering(compiler);
        numbering.setOperatorContext(fake);
        numbering.apply(closure);
        ExpressionsCSE cse = new ExpressionsCSE(compiler, numbering.canonical);
        cse.setOperatorContext(fake);
        cse.apply(closure);
        return cse.get(closure);
    }

    @Test
    public void testCSE() {
        DBSPCompiler compiler = new DBSPCompiler(new CompilerOptions());
        DBSPClosureExpression closure = b.closure(b.tup(b.i32n(), b.i32n(), b.i32n()), t -> {
            DBSPExpression p0 = b.binary(DBSPOpcode.DIV, b.neg(b.field(t, 0)), b.field(t, 1));
            DBSPExpression a = b.binary(DBSPOpcode.MUL, p0, p0);
            DBSPExpression sum = b.add(b.field(t, 2), b.lit(1));
            return b.tuple(a, b.add(sum, sum));
        });
        // Find 2 common subexpressions; one has a single use
        assertLets(compiler, cse(compiler, closure), 2);
    }

    @Test
    public void testLetEquivalenceContextUnchanged() {
        // Comparing let expressions may not modify the caller's context
        DBSPExpression let0 = b.let(b.lit(2, true), x -> b.add(x, x));
        DBSPExpression let1 = b.let(b.lit(2, true), x -> b.add(x, x));
        EquivalenceContext context = new EquivalenceContext();
        Assert.assertTrue(context.equivalent(let0, let1));
        context.leftDeclaration.mustBeEmpty();
        context.rightDeclaration.mustBeEmpty();
    }

    @Test
    public void testCSENested() {
        DBSPCompiler compiler = new DBSPCompiler(new CompilerOptions());
        DBSPTypeTuple tuple = b.tup(b.i32n(), b.i32n());
        DBSPVariablePath var = b.refVar(tuple);

        DBSPLetStatement stat0 = new DBSPLetStatement("t0",
                b.add(b.field(var, 0), b.field(var, 1)));

        DBSPVariablePath t = b.refVar(tuple);
        DBSPExpression let = new DBSPLetExpression(
                t, new DBSPTupleExpression(
                        stat0.getVarReference(),
                        b.neg(stat0.getVarReference())).borrow(),
                b.tuple(b.neg(b.field(t, 0)), b.neg(b.field(t, 0))));

        DBSPLetStatement stat1 = new DBSPLetStatement("t1", let);
        DBSPExpression block = new DBSPBlockExpression(
                Linq.list(stat0, stat1),
                stat1.getVarReference());
        DBSPClosureExpression closure = block.closure(var);

        // Crash on incorrect translation.
        ResolveReferences resolver = new ResolveReferences(compiler, false);
        resolver.apply(cse(compiler, closure));
    }

    @Test
    public void testConditionalCSE() {
        DBSPCompiler compiler = new DBSPCompiler(new CompilerOptions());
        DBSPClosureExpression closure = b.closure(b.tup(b.i32n(), b.i32n(), b.i32n()), t -> {
            DBSPExpression cond = b.binary(DBSPOpcode.GTE, b.field(t, 0), b.lit(1))
                    .wrapBoolIfNeeded();
            DBSPExpression un = b.neg(b.field(t, 1));
            return b.tuple(
                    b.ifThenElse(cond, un, b.field(t, 2)),
                    b.ifThenElse(cond, b.field(t, 2), un));
        });
        // Find 2 common subexpressions
        assertLets(compiler, cse(compiler, closure), 2);
    }

    @Test
    public void testEquiv() {
        DBSPExpression zero0 = b.lit(0);
        DBSPType i32 = zero0.getType();
        DBSPExpression zero1 = b.lit(0);
        Assert.assertTrue(EquivalenceContext.equiv(zero0, zero1));

        DBSPExpression one = b.lit(1);
        Assert.assertFalse(EquivalenceContext.equiv(zero0, one));

        DBSPExpression plus0 = b.add(zero0, one);
        DBSPExpression plus1 = b.add(zero1, one);
        Assert.assertTrue(EquivalenceContext.equiv(plus0, plus1));

        DBSPExpression plus2 = b.add(one, one);
        Assert.assertFalse(EquivalenceContext.equiv(plus2, plus1));

        DBSPVariablePath var0 = new DBSPVariablePath("x", i32);
        DBSPVariablePath var1 = new DBSPVariablePath("y", i32);
        // Expressions cannot have free variables
        Assert.assertThrows(InternalCompilerError.class, () -> EquivalenceContext.equiv(var0, var1));
    }

    @SuppressWarnings("SuspiciousNameCombination")
    @Test
    public void testLambdas() {
        DBSPType i32 = b.i32();
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
        DBSPExpression plus0 = b.add(x2, y2);
        DBSPExpression lambda0 = plus0.closure(x2, y2);

        DBSPVariablePath x3 = new DBSPVariablePath("x", i32);
        DBSPVariablePath y3 = new DBSPVariablePath("y", i32);
        DBSPExpression plus1 = b.add(y3, x3);
        DBSPExpression lambda1 = plus1.closure(x3, y3);
        // Compiler doesn't know that ADD is commutative
        Assert.assertFalse(EquivalenceContext.equiv(lambda0, lambda1));

        DBSPVariablePath x4 = new DBSPVariablePath("x", i32);
        DBSPVariablePath y4 = new DBSPVariablePath("y", i32);
        DBSPExpression plus1_1 = b.add(x4, y4);
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

        DBSPTypeTuple ii = b.tup(i32, i32);
        DBSPTypeTuple iii = b.tup(i32, i32, i32);
        DBSPVariablePath x5 = new DBSPVariablePath("x", ii);
        DBSPVariablePath y5 = new DBSPVariablePath("y", iii);
        DBSPExpression x0 = x5.field(0).closure(x5);
        DBSPExpression y0 = y5.field(0).closure(y5);
        Assert.assertFalse(EquivalenceContext.equiv(x0, y0));
    }
}
