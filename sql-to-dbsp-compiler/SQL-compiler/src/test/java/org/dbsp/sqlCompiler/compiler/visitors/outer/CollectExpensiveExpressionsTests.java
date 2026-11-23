package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.circuit.operator.DBSPConstantOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapOperator;
import org.dbsp.sqlCompiler.compiler.ProgramMetadata;
import org.dbsp.sqlCompiler.compiler.backend.RustCircuitPass;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteEmptyRel;
import org.dbsp.sqlCompiler.compiler.sql.tools.BaseSQLTests;
import org.dbsp.sqlCompiler.compiler.sql.tools.ExpressionBuilder;
import org.dbsp.sqlCompiler.compiler.visitors.outer.FuseExpensiveMaps.MapInfo;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPFieldExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPZSetExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPLetExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTuple;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/** Tests for {@link FuseExpensiveMaps.CollectExpensiveExpressions} and for the
 * equivalence check {@link FuseExpensiveMaps} runs on the collected expressions.
 * Each test shows the analyzed function in a comment, as toString() prints it. */
public class CollectExpensiveExpressionsTests extends BaseSQLTests {
    final ExpressionBuilder b = new ExpressionBuilder();
    final DBSPTypeTuple pair = this.b.tup(this.b.i32(), this.b.i32());

    @Test
    public void fusionCandidatesReferToFragmentsInTheSimplifiedClosure() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustMapFusionGroups"));
        var site = CalciteEmptyRel.INSTANCE;
        var input = new DBSPConstantOperator(site, DBSPZSetExpression.emptyWithElementType(this.pair));
        var row = b.refVar(this.pair);
        var field = b.field(row, 0);
        var redundant = new DBSPFieldExpression(site, b.tuple(field), 0);
        var call = b.call("costly", redundant);
        var selected = new DBSPMapOperator(site, b.tuple(call, call).closure(row), input.outputPort());
        var circuit = new DBSPCircuit(new ProgramMetadata(), false);
        circuit.addOperator(input);
        for (var rejected : List.of(b.tuple(field), b.tuple(b.call("costly", b.call("NoW"))), call))
            circuit.addOperator(new DBSPMapOperator(site, rejected.closure(row), input.outputPort()));
        circuit.addOperator(selected);
        for (boolean hybrid : List.of(false, true)) {
            var compiler = this.testCompiler();
            compiler.options.ioOptions.rustOptimizerPasses = hybrid;
            var scan = new FuseExpensiveMaps.FindFusableMaps(compiler);
            scan.apply(circuit);
            var candidates = scan.groups.get(input.outputPort());
            Assert.assertEquals(1, candidates.size());
            var candidate = candidates.get(0);
            Assert.assertSame(selected, candidate.operator());
            Assert.assertEquals(2, candidate.minimal().size());
            Assert.assertNotSame(call, candidate.minimal().get(0));
            Assert.assertSame(candidate.body().get(0), candidate.minimal().get(0));
            Assert.assertSame(candidate.body().get(1), candidate.minimal().get(1));
            Assert.assertEquals(List.of(candidate), scan.clusters.get(selected));
        }
    }

    @Test
    public void singletonFusionNeedsAnEquivalentExpensiveField() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustMapFusionGroups"));
        var compiler = this.testCompiler();
        var row = b.refVar(this.pair);
        var field = b.field(row, 0);
        var first = b.let(field, x -> b.call("f", x));
        var renamed = b.let(field.deepCopy(), y -> b.call("f", y));
        var other = b.let(b.field(row, 1), y -> b.call("f", y));
        var direct = new FuseExpensiveMaps.FindFusableMaps(compiler);
        for (var pair : List.of(b.tuple(first, renamed), b.tuple(first, other), b.tuple(field, field))) {
            var closure = pair.closure(row);
            boolean expected = direct.hasDuplicatedExpensiveField(new MapInfo(null, closure, List.of()));
            Assert.assertEquals(Boolean.valueOf(expected), RustCircuitPass.hasDuplicateExpensiveField(compiler, closure));
        }
    }

    @Test
    public void siblingFusionConnectsMapsThroughRenamedFragments() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustMapFusionGroups"));
        var compiler = this.testCompiler();
        var site = CalciteEmptyRel.INSTANCE;
        var input = new DBSPConstantOperator(site, DBSPZSetExpression.emptyWithElementType(this.pair));
        var circuit = new DBSPCircuit(new ProgramMetadata(), false);
        circuit.addOperator(input);
        var maps = new ArrayList<DBSPMapOperator>();
        for (var functions : List.of(List.of("f"), List.of("other"), List.of("g"), List.of("f", "g"))) {
            var row = b.refVar(this.pair);
            var fields = functions.stream().map(name -> b.call(name, b.field(row, 0)))
                    .toArray(DBSPExpression[]::new);
            var map = new DBSPMapOperator(site, b.tuple(fields).closure(row), input.outputPort());
            maps.add(map);
            circuit.addOperator(map);
        }
        for (boolean hybrid : List.of(false, true)) {
            compiler.options.ioOptions.rustOptimizerPasses = hybrid;
            var scan = new FuseExpensiveMaps.FindFusableMaps(compiler);
            scan.apply(circuit);
            var candidates = scan.groups.get(input.outputPort());
            Assert.assertArrayEquals(new int[][]{{0, 2, 3}, {1}}, RustCircuitPass.siblingMapGroups(compiler, candidates));
            Assert.assertNull(scan.clusters.get(maps.get(1)));
            for (int i : List.of(0, 2, 3))
                Assert.assertEquals(List.of(candidates.get(0), candidates.get(2), candidates.get(3)),
                        scan.clusters.get(maps.get(i)));
        }
    }

    /** Collect the minimal expensive expressions of |param| expression */
    List<DBSPExpression> collect(DBSPExpression expression, DBSPVariablePath param) {
        DBSPClosureExpression closure = expression.closure(param);
        var compiler = this.testCompiler();
        compiler.options.ioOptions.rustOptimizerPasses = Boolean.getBoolean("feldera.optimizer.compareRustMapFusionGroups");
        return FuseExpensiveMaps.minimalFragments(compiler, closure);
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

    @Test
    public void testShadowedParameter() {
        var param = new DBSPVariablePath("x", b.i32());
        var shadow = new DBSPVariablePath("x", b.i32());
        var init = b.call("before", param);
        var body = new DBSPLetExpression(shadow, init, b.call("inside", shadow));
        var later = b.call("after", param);
        Assert.assertEquals(List.of(init, later), this.collect(b.tuple(body, later), param));
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
