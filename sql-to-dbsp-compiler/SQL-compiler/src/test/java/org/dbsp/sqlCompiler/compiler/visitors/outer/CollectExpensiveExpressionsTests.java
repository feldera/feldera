package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.compiler.sql.tools.BaseSQLTests;
import org.dbsp.sqlCompiler.compiler.sql.tools.ExpressionBuilder;
import org.dbsp.sqlCompiler.compiler.visitors.outer.FuseExpensiveMaps.CollectExpensiveExpressions;
import org.dbsp.sqlCompiler.compiler.visitors.outer.FuseExpensiveMaps.MapInfo;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTuple;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

/** Tests for {@link FuseExpensiveMaps.CollectExpensiveExpressions} and for the
 * equivalence check {@link FuseExpensiveMaps} runs on the collected expressions.
 * Each test shows the analyzed function in a comment, as toString() prints it. */
public class CollectExpensiveExpressionsTests extends BaseSQLTests {
    final ExpressionBuilder b = new ExpressionBuilder();
    final DBSPTypeTuple pair = this.b.tup(this.b.i32(), this.b.i32());

    /** Collect the minimal expensive expressions of |param| expression */
    List<DBSPExpression> collect(DBSPExpression expression, DBSPVariablePath param) {
        DBSPClosureExpression closure = expression.closure(param);
        CollectExpensiveExpressions collector =
                new CollectExpensiveExpressions(this.testCompiler(), closure);
        collector.apply(closure.body);
        return collector.minimal;
    }

    /** Only the call is minimal, not the addition containing it */
    @Test
    public void testCall() {
        // |t_0: &Tup2<i32, i32>| f(*t_0.0) + 1
        // Extracted: f(*t_0.0)
        DBSPVariablePath t = b.refVar(this.pair);
        DBSPExpression call = b.call("f", b.field(t, 0));
        Assert.assertEquals(List.of(call), this.collect(b.add(call, b.lit(1)), t));
    }

    /** Of two nested calls only the inner one is minimal */
    @Test
    public void testNestedCalls() {
        // |t_0: &Tup2<i32, i32>| f(g(*t_0.0))
        // Extracted: g(*t_0.0)
        DBSPVariablePath t = b.refVar(this.pair);
        DBSPExpression inner = b.call("g", b.field(t, 0));
        Assert.assertEquals(List.of(inner), this.collect(b.call("f", inner), t));
    }

    @Test
    public void testCheap() {
        // |t_0: &Tup2<i32, i32>| *t_0.0 + *t_0.1
        // Extracted: nothing
        DBSPVariablePath t = b.refVar(this.pair);
        DBSPExpression expression = b.add(b.field(t, 0), b.field(t, 1));
        Assert.assertEquals(List.of(), this.collect(expression, t));
    }

    /** Unrelated calls are both minimal */
    @Test
    public void testTwoCalls() {
        // |t_0: &Tup2<i32, i32>| f(*t_0.0) + g(*t_0.1)
        // Extracted: f(*t_0.0), g(*t_0.1)
        DBSPVariablePath t = b.refVar(this.pair);
        DBSPExpression f = b.call("f", b.field(t, 0));
        DBSPExpression g = b.call("g", b.field(t, 1));
        Assert.assertEquals(List.of(f, g), this.collect(b.add(f, g), t));
    }

    /** The same node used twice is collected once per occurrence: node sharing
     * in the IR does not mean the generated code evaluates it once. */
    @Test
    public void testSharedNode() {
        // |t_0: &Tup2<i32, i32>| Tup2::new(f(*t_0.0), f(*t_0.0), )
        // Extracted: f(*t_0.0), f(*t_0.0) -- the same node, twice
        DBSPVariablePath t = b.refVar(this.pair);
        DBSPExpression call = b.call("f", b.field(t, 0));
        Assert.assertEquals(List.of(call, call), this.collect(b.tuple(call, call), t));
    }

    /** A call whose argument is a lambda is minimal as a whole: a value under
     * the lambda is computed per lambda invocation, not per row. */
    @Test
    public void testLambdaOpaque() {
        // |t_0: &Tup2<i32, i32>| map_array(*t_0.0, |t_1: i32| h(t_1))
        // Extracted: map_array(*t_0.0, |t_1: i32| h(t_1)) -- the whole call
        DBSPVariablePath t = b.refVar(this.pair);
        DBSPExpression outer = b.call("map_array",
                b.field(t, 0), b.lambda(b.i32(), a -> b.call("h", a)));
        Assert.assertEquals(List.of(outer), this.collect(outer, t));
    }

    /** The collector recurses into a let expression; the initializer only
     * uses the parameter and is collected, while the consumer fragment
     * referencing the let variable is not comparable out of context. */
    @Test
    public void testLetTransparent() {
        // |t_0: &Tup2<i32, i32>| { let t_1 = f(*t_0.0); g(t_1) }
        // Extracted: f(*t_0.0) -- g(t_1) is rejected, it uses t_1
        DBSPVariablePath t = b.refVar(this.pair);
        DBSPExpression initializer = b.call("f", b.field(t, 0));
        DBSPExpression let = b.let(initializer, x -> b.call("g", x));
        Assert.assertEquals(List.of(initializer), this.collect(let, t));
    }

    /** A closed fragment next to an open one is still collected */
    @Test
    public void testLetSibling() {
        // |t_0: &Tup2<i32, i32>| {
        //    let t_1 = f(*t_0.0);
        //    g(t_1) + h(*t_0.1)
        // }
        // Extracted: f(*t_0.0), h(*t_0.1)
        DBSPVariablePath t = b.refVar(this.pair);
        DBSPExpression initializer = b.call("f", b.field(t, 0));
        DBSPExpression closed = b.call("h", b.field(t, 1));
        DBSPExpression let = b.let(initializer, x -> b.add(b.call("g", x), closed));
        Assert.assertEquals(List.of(initializer, closed), this.collect(let, t));
    }

    /** A variable reference is open in every scope below its declaration */
    @Test
    public void testNestedLet() {
        // |t_0: &Tup2<i32, i32>| {
        //    let t_1 = f(*t_0.0);
        //    {
        //       let t_2 = g(t_1);
        //       h(t_2)
        //     }}
        // Extracted: f(*t_0.0) -- everything else uses t_1 or t_2
        DBSPVariablePath t = b.refVar(this.pair);
        DBSPExpression initializer = b.call("f", b.field(t, 0));
        DBSPExpression outer = b.let(initializer,
                x -> b.let(b.call("g", x),
                        y -> b.call("h", y)));
        Assert.assertEquals(List.of(initializer), this.collect(outer, t));
    }

    /** When every expensive fragment references the let variable, the whole
     * let expression is the minimal closed unit. */
    @Test
    public void testLetWholesale() {
        // |t_0: &Tup2<i32, i32>| {
        //    let t_1 = (*t_0.0 + 1);
        //    g(t_1)
        // }
        // Extracted: the whole let expression
        DBSPVariablePath t = b.refVar(this.pair);
        DBSPExpression let = b.let(b.add(b.field(t, 0), b.lit(1)), x -> b.call("g", x));
        Assert.assertEquals(List.of(let), this.collect(let, t));
    }

    /** The collector recurses into block expressions the same way */
    @Test
    public void testBlockTransparent() {
        // |t_0: &Tup2<i32, i32>| {
        //     let t_1: i32 = f(*t_0.0);
        //     (t_1 + 1)
        // }
        // Extracted: f(*t_0.0)
        DBSPVariablePath t = b.refVar(this.pair);
        DBSPExpression initializer = b.call("f", b.field(t, 0));
        DBSPExpression block = b.block(initializer, s -> b.add(s, b.lit(1)));
        Assert.assertEquals(List.of(initializer), this.collect(block, t));
    }

    /** The equivalence check must work on every pair of collected expressions. */
    @Test
    public void testLetEquivalence() {
        // |t_0: &Tup2<i32, i32>| {
        //    let t_1 = f(*t_0.0);
        //    g(t_1)
        // }
        // Extracted: f(*t_0.0)
        DBSPVariablePath t = b.refVar(this.pair);
        DBSPExpression leftLet = b.let(b.call("f", b.field(t, 0)), x -> b.call("g", x));
        MapInfo left = new MapInfo(null,
                b.tuple(leftLet).closure(t), this.collect(leftLet, t));

        // Alpha-equivalent to leftLet:
        // |u_0: &Tup2<i32, i32>| {
        //    let u_1 = f(*u_0.0);
        //    g(u_1)
        // }
        // Extracted: f(*u_0.0), equivalent to f(*t_0.0)
        DBSPVariablePath u = b.refVar(this.pair);
        DBSPExpression sameLet = b.let(b.call("f", b.field(u, 0)), y -> b.call("g", y));
        MapInfo same = new MapInfo(null,
                b.tuple(sameLet).closure(u), this.collect(sameLet, u));

        // Differs from leftLet in the initializer:
        // |v_0: &Tup2<i32, i32>| {
        //    let v_1 = f(*v_0.1);
        //    g(v_1)
        // }
        // Extracted: f(*v_0.1), not equivalent to f(*t_0.0)
        DBSPVariablePath v = b.refVar(this.pair);
        DBSPExpression otherLet = b.let(b.call("f", b.field(v, 1)), z -> b.call("g", z));
        MapInfo other = new MapInfo(null,
                b.tuple(otherLet).closure(v), this.collect(otherLet, v));

        Assert.assertTrue(anyEquivalent(left, same));
        Assert.assertFalse(anyEquivalent(left, other));
    }

    /** The comparison the fusion decision performs, via fingerprints */
    static boolean anyEquivalent(MapInfo left, MapInfo right) {
        boolean result = false;
        for (DBSPExpression a : left.minimal())
            for (DBSPExpression c : right.minimal())
                // Must not crash for any pair
                result |= FuseExpensiveMaps.equivalent(left, a, right, c);
        return result;
    }
}
