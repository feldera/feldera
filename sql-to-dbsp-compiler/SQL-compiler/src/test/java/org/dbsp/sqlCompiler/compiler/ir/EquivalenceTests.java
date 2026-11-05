package org.dbsp.sqlCompiler.compiler.ir;

import org.dbsp.sqlCompiler.circuit.operator.DBSPConstantOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteEmptyRel;
import org.dbsp.sqlCompiler.compiler.sql.tools.ExpressionBuilder;
import org.dbsp.sqlCompiler.compiler.visitors.inner.EquivalenceContext;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.inner.ResolveReferences;
import org.dbsp.sqlCompiler.compiler.visitors.outer.InnerCSE;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.expression.DBSPApplyExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPBlockExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPLazyExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPLetExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPOpcode;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.expression.DBSPZSetExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPBoolLiteral;
import org.dbsp.sqlCompiler.ir.path.DBSPPath;
import org.dbsp.sqlCompiler.ir.path.DBSPSimplePathSegment;
import org.dbsp.sqlCompiler.ir.statement.DBSPLetStatement;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeCode;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTuple;
import org.dbsp.util.Linq;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;

/** Unit tests for expression equivalence.
 * The alpha-equivalence tests build variables by hand: they need specific
 * names shared between distinct nodes, which {@link ExpressionBuilder}
 * avoids by construction. */
public class EquivalenceTests {
    final ExpressionBuilder b = new ExpressionBuilder();

    /** Count the LazyCell variables that CSE introduced */
    static void assertCseVariables(DBSPCompiler compiler, IDBSPInnerNode node, int expected) {
        InnerVisitor visitor = new InnerVisitor(compiler) {
            int variables = 0;

            @Override
            public void postorder(DBSPLazyExpression expression) {
                this.variables++;
            }

            @Override
            public void endVisit() {
                Assert.assertEquals(expected, this.variables);
            }
        };
        visitor.apply(node);
    }

    static IDBSPInnerNode cse(DBSPCompiler compiler, DBSPClosureExpression closure) {
        DBSPOperator fake = new DBSPConstantOperator(
                CalciteEmptyRel.INSTANCE, new DBSPZSetExpression(new DBSPBoolLiteral()));
        InnerCSE cse = new InnerCSE(compiler);
        cse.setOperatorContext(fake);
        return cse.apply(closure);
    }

    /** Fails if 'node' references a variable that is not in scope */
    static void assertResolves(DBSPCompiler compiler, IDBSPInnerNode node) {
        new ResolveReferences(compiler, false).apply(node);
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
        assertCseVariables(compiler, cse(compiler, closure), 2);
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
        assertResolves(compiler, cse(compiler, closure));
    }

    @Test
    public void testCSEAfterLastLet() {
        // |t| { let a = t.0; let c = t.1; Tup(a + c, a + c) }
        // The variable for a + c must follow 'let c'
        DBSPCompiler compiler = new DBSPCompiler(new CompilerOptions());
        DBSPVariablePath t = b.refVar(b.tup(b.i32n(), b.i32n()));
        DBSPLetStatement a = new DBSPLetStatement("a", b.field(t, 0));
        DBSPLetStatement c = new DBSPLetStatement("c", b.field(t, 1));
        DBSPExpression sum0 = b.add(a.getVarReference(), c.getVarReference());
        DBSPExpression sum1 = b.add(a.getVarReference(), c.getVarReference());
        DBSPExpression block = new DBSPBlockExpression(Linq.list(a, c), b.tuple(sum0, sum1));
        IDBSPInnerNode result = cse(compiler, block.closure(t));
        assertCseVariables(compiler, result, 1);
        assertResolves(compiler, result);
    }

    @Test
    public void testCSENestedBlocks() {
        // |t| { let a = t.0; { let c = t.1; Tup((a + c) * (a + c), f(a + 1), g(a + 1)) } }
        // CSE creates variables for a + c and a + 1; each variable must follow the let statements of its operands.
        DBSPCompiler compiler = new DBSPCompiler(new CompilerOptions());
        DBSPVariablePath t = b.refVar(b.tup(b.i32n(), b.i32n()));
        DBSPLetStatement a = new DBSPLetStatement("a", b.field(t, 0));
        DBSPLetStatement c = new DBSPLetStatement("c", b.field(t, 1));
        DBSPExpression product = b.binary(b.i32n(), DBSPOpcode.MUL,
                b.add(a.getVarReference(), c.getVarReference()),
                b.add(a.getVarReference(), c.getVarReference()));
        DBSPExpression inner = new DBSPBlockExpression(Linq.list(c), b.tuple(product,
                b.call("f", b.add(a.getVarReference(), b.lit(1, true))),
                b.call("g", b.add(a.getVarReference(), b.lit(1, true)))));
        IDBSPInnerNode result = cse(compiler, new DBSPBlockExpression(Linq.list(a), inner).closure(t));
        assertCseVariables(compiler, result, 2);
        assertResolves(compiler, result);
    }

    @Test
    public void testCSESharedAcrossClosures() {
        // |t| Tup(f(|u| u + e), e, e), where e = t.0 + 1 is a single Java object.
        // The variable for e that the inner closure declares is not in scope in the outer closure.
        DBSPCompiler compiler = new DBSPCompiler(new CompilerOptions());
        DBSPVariablePath t = b.refVar(b.tup(b.i32n()));
        DBSPExpression e = b.add(b.field(t, 0), b.lit(1, true));
        DBSPVariablePath u = b.i32n().var();
        DBSPClosureExpression inner = b.add(u, e).closure(u);
        DBSPClosureExpression closure = b.tuple(b.call(b.i32n(), "f", inner), e, e).closure(t);
        assertResolves(compiler, cse(compiler, closure));
    }

    @Test
    public void testCSECountsUsersPerClosure() {
        // |t| Tup(f("x"), g(t.0, |u| f("x") + (u + f("x"))))
        // Only the inner closure uses f("x") twice, so only the inner closure gets a variable.
        DBSPCompiler compiler = new DBSPCompiler(new CompilerOptions());
        DBSPVariablePath t = b.refVar(b.tup(b.i32()));
        DBSPClosureExpression inner = b.lambda(b.i32(),
                u -> b.add(b.call("f", b.lit("x")), b.add(u, b.call("f", b.lit("x")))));
        DBSPClosureExpression closure = b.tuple(
                b.call("f", b.lit("x")), b.call("g", b.field(t, 0), inner)).closure(t);
        assertCseVariables(compiler, cse(compiler, closure), 1);
    }

    @Test
    public void testCSEDistinguishesTypes() {
        // Each pair of fields differs only in a type, so CSE may not merge them
        DBSPCompiler compiler = new DBSPCompiler(new CompilerOptions());
        DBSPType i128 = b.integer(DBSPTypeCode.INT128, false);
        DBSPType i64 = b.integer(DBSPTypeCode.INT64, false);
        DBSPVariablePath t = b.refVar(b.tup(i128, b.i32()));
        DBSPType decimal10 = b.decimal(10, 2);
        DBSPType decimal20 = b.decimal(20, 4);
        DBSPExpression toDecimal10 = b.unary(decimal10, DBSPOpcode.INTEGER_TO_DECIMAL, b.field(t, 0));
        DBSPExpression toDecimal20 = b.unary(decimal20, DBSPOpcode.INTEGER_TO_DECIMAL, b.field(t, 0));
        DBSPExpression onTuple = b.call("f", new DBSPTupleExpression(false, b.field(t, 1)));
        DBSPExpression onNullableTuple = b.call("f", new DBSPTupleExpression(true, b.field(t, 1)));
        DBSPExpression gI32 = new DBSPPath(new DBSPSimplePathSegment("g", b.i32())).toExpression();
        DBSPExpression gI64 = new DBSPPath(new DBSPSimplePathSegment("g", i64)).toExpression();
        DBSPExpression callI32 = new DBSPApplyExpression(gI32, b.i32(), b.field(t, 1));
        DBSPExpression callI64 = new DBSPApplyExpression(gI64, b.i32(), b.field(t, 1));
        DBSPClosureExpression closure = b.tuple(
                toDecimal10, toDecimal20, onTuple, onNullableTuple, callI32, callI64).closure(t);
        assertCseVariables(compiler, cse(compiler, closure), 0);
    }

    @Test
    public void testCSEUsersWithoutNumber() {
        // |t| Tup(e + unwrap(t.1), e + unwrap(t.1)), where e = t.0 + 1.
        // Unwrap has no value number, so neither sum has one; e has two users.
        DBSPCompiler compiler = new DBSPCompiler(new CompilerOptions());
        DBSPVariablePath t = b.refVar(b.tup(b.i32(), b.i32n()));
        DBSPExpression[] sums = new DBSPExpression[2];
        for (int i = 0; i < sums.length; i++) {
            DBSPExpression e = b.add(b.field(t, 0), b.lit(1));
            DBSPExpression unwrap = b.unwrap(b.field(t, 1));
            sums[i] = b.add(e, unwrap);
        }
        DBSPClosureExpression closure = b.tuple(sums).closure(t);
        assertCseVariables(compiler, cse(compiler, closure), 1);
    }

    @Test
    public void testCSEMutableVariables() {
        // |acc, vec: &mut Tup1<i32?>, row| Tup(f(acc.0), g(&mut acc), f(acc.0), h(vec), h(vec), f(row.0), f(row.0))
        // g can change acc between the two calls f(acc.0), and each call h(vec) can change vec.
        // Only f(row.0) is a common subexpression.
        DBSPCompiler compiler = new DBSPCompiler(new CompilerOptions());
        DBSPVariablePath acc = b.tup(b.i32n()).var();
        DBSPVariablePath vec = b.tup(b.i32n()).ref(true).var();
        DBSPVariablePath row = b.tup(b.i32n()).var();
        DBSPClosureExpression closure = b.tuple(
                b.call("f", acc.field(0)),
                b.call("g", acc.borrow(true)),
                b.call("f", acc.field(0)),
                b.call("h", vec),
                b.call("h", vec),
                b.call("f", row.field(0)),
                b.call("f", row.field(0))).closure(acc, vec, row);
        assertCseVariables(compiler, cse(compiler, closure), 1);
    }

    @Test
    public void testCSEMutableFieldBorrow() {
        // |acc, row| Tup(f(acc.0), g(&mut acc.0), f(acc.0), f(row.0), f(row.0))
        // g can change acc.0 between the two calls f(acc.0); only f(row.0) is a common subexpression.
        DBSPCompiler compiler = new DBSPCompiler(new CompilerOptions());
        DBSPVariablePath acc = b.tup(b.i32n()).var();
        DBSPVariablePath row = b.tup(b.i32n()).var();
        DBSPClosureExpression closure = b.tuple(
                b.call("f", acc.field(0)),
                b.call("g", acc.field(0).borrow(true)),
                b.call("f", acc.field(0)),
                b.call("f", row.field(0)),
                b.call("f", row.field(0))).closure(acc, row);
        assertCseVariables(compiler, cse(compiler, closure), 1);
    }

    @Test
    public void testCSEDecimalLiteral() {
        // |t| Tup(f(1.50), g(1.50))
        // A decimal literal is cheap, so CSE does not share it.
        DBSPCompiler compiler = new DBSPCompiler(new CompilerOptions());
        DBSPVariablePath t = b.refVar(b.tup(b.i32()));
        DBSPType decimal = b.decimal(10, 2);
        DBSPExpression literal0 = b.lit(new BigDecimal("1.50"), decimal);
        DBSPExpression literal1 = b.lit(new BigDecimal("1.50"), decimal);
        DBSPClosureExpression closure = b.tuple(b.call("f", literal0), b.call("g", literal1)).closure(t);
        assertCseVariables(compiler, cse(compiler, closure), 0);
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
        assertCseVariables(compiler, cse(compiler, closure), 2);
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
