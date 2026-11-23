package org.dbsp.sqlCompiler.compiler.visitors.inner;

import org.dbsp.sqlCompiler.compiler.backend.RustCircuitPass;
import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.sql.tools.BaseSQLTests;
import org.dbsp.sqlCompiler.compiler.sql.tools.ExpressionBuilder;
import org.dbsp.sqlCompiler.ir.aggregate.DBSPAggregateList;
import org.dbsp.sqlCompiler.ir.expression.DBSPBlockExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPLetExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.statement.DBSPLetStatement;
import org.junit.Assert;
import org.junit.Test;

/** Tests for {@link ResolveReferences}. */
public class ResolveReferencesTests extends BaseSQLTests {
    final ExpressionBuilder b = new ExpressionBuilder();

    ReferenceMap resolve(DBSPExpression expression, boolean allowFree) {
        var compiler = this.testCompiler();
        ResolveReferences resolver = new ResolveReferences(compiler, allowFree);
        resolver.apply(expression);
        if (!allowFree && Boolean.getBoolean("feldera.optimizer.compareRustLimiterApply")) {
            var nativeMap = RustCircuitPass.inspectWaterline(compiler, expression).references();
            Assert.assertEquals(resolver.reference.declarations, nativeMap.declarations);
        }
        return resolver.reference;
    }

    /** Uses of the closure parameter resolve to the parameter */
    @Test
    public void testParameterResolves() {
        DBSPVariablePath t = b.refVar(b.tup(b.i32(), b.i32()));
        DBSPClosureExpression closure = b.add(b.field(t, 0), b.field(t, 1)).closure(t);
        ReferenceMap references = this.resolve(closure, false);
        Assert.assertSame(closure.parameters[0], references.get(t));
    }

    /** Let expression */
    @Test
    public void testLetResolves() {
        DBSPVariablePath t = b.refVar(b.tup(b.i32(), b.i32()));
        DBSPVariablePath[] bound = new DBSPVariablePath[1];
        DBSPExpression let = b.let(b.call("f", b.field(t, 0)),
                x -> {
                    bound[0] = x;
                    return b.call("g", x);
                });
        ReferenceMap references = this.resolve(let.closure(t), false);
        Assert.assertSame(let, references.get(bound[0]));
    }

    @Test
    public void testLetBinderDistinctNode() {
        DBSPVariablePath t = b.refVar(b.tup(b.i32(), b.i32()));
        DBSPVariablePath binder = b.i32().var();
        DBSPVariablePath use = new DBSPVariablePath(binder.variable, b.i32());
        DBSPExpression let = new DBSPLetExpression(
                binder, b.call("f", b.field(t, 0)), b.call("g", use));
        DBSPClosureExpression closure = b.tuple(let).closure(t);

        ReferenceMap references = this.resolve(closure, false);
        Assert.assertSame(let, references.get(binder));
        Assert.assertSame(let, references.get(use));

        // ValueNumbering visits the binder node and enforces that it resolves
        new ValueNumbering(this.testCompiler()).apply(closure);
    }

    @Test
    public void valueNumbersRetainClosureUsesAndDiscardMutableDependencies() {
        var compiler = this.testCompiler();
        var row = b.refVar(b.tup(b.i32(), b.i32()));
        var first = b.call("f", b.lit("🦀#7#"), b.field(row, 0));
        var second = first.deepCopy();
        var write = b.i32().var();
        var read = b.call("g", write);
        var nested = b.lambda(b.i32(), x -> b.let(b.add(x, b.lit(1)),
                y -> b.block(b.call("h", y), z -> b.tuple(z, first.deepCopy()))));
        var body = b.tuple(first, second, nested, read, write.borrow(true)).closure(row, write);
        ValueNumbering direct = new ValueNumbering(compiler);
        direct.apply(body);
        Assert.assertSame(direct.canonical.get(first), direct.canonical.get(second));
        Assert.assertTrue(direct.canonical.get(first).expensive);
        Assert.assertFalse(direct.canonical.containsKey(read));
        if (Boolean.getBoolean("feldera.optimizer.compareRustValueNumbers")) {
            compiler.options.ioOptions.rustOptimizerPasses = true;
            ValueNumbering nativePass = new ValueNumbering(compiler);
            nativePass.apply(body);
            Assert.assertEquals(direct.foundAssignment, nativePass.foundAssignment);
            Assert.assertEquals(direct.canonical.keySet(), nativePass.canonical.keySet());
            var rewrite = ValueNumbering.forRewrite(compiler);
            rewrite.apply(body);
            direct.canonical.forEach((node, expected) -> {
                var actual = nativePass.canonical.get(node);
                Assert.assertSame(expected.expression, actual.expression);
                Assert.assertEquals(expected.expensive, actual.expensive);
                Assert.assertEquals(expected.dependsOn, actual.dependsOn);
                Assert.assertEquals(expected.users, actual.users);
                for (var owner : java.util.List.of(body, nested)) {
                    boolean share = expected.expensive && !ExpressionsCSE.neverShared(expected.expression)
                            && expected.manyUsers(direct.canonical, owner);
                    var planned = rewrite.canonical.get(node);
                    Assert.assertEquals(share, planned != null && planned.manyUsers(rewrite.canonical, owner));
                }
            });
        }
    }

    @Test
    public void mutableBorrowDistinguishesAFieldFromAComputedValue() {
        var compiler = this.testCompiler();
        var row = b.refVar(b.tup(b.i32()));
        var field = b.field(row, 0);
        var read = b.call("f", field);
        for (var place : java.util.List.of(field, read)) {
            var closure = b.tuple(read, place.borrow(true), read.deepCopy()).closure(row);
            for (boolean nativePass : java.util.List.of(false, true)) {
                if (nativePass && !Boolean.getBoolean("feldera.optimizer.compareRustValueNumbers"))
                    continue;
                compiler.options.ioOptions.rustOptimizerPasses = nativePass;
                var numbering = new ValueNumbering(compiler);
                numbering.apply(closure);
                Assert.assertEquals(place == read, numbering.foundAssignment);
                Assert.assertEquals(place == read, numbering.canonical.containsKey(read));
            }
        }
    }

    /** A let initializer resolves in the scope outside the let:
     * in let x = 1; let x = x + 1; x
     * the second initializer sees the first x, the consumer the second. */
    @Test
    public void testShadowing() {
        DBSPVariablePath outerBinder = b.i32().var();
        String name = outerBinder.variable;
        DBSPVariablePath innerBinder = new DBSPVariablePath(name, b.i32());
        DBSPVariablePath initializerUse = new DBSPVariablePath(name, b.i32());
        DBSPVariablePath consumerUse = new DBSPVariablePath(name, b.i32());
        DBSPLetExpression inner = new DBSPLetExpression(
                innerBinder, b.add(initializerUse, b.lit(1)), consumerUse);
        DBSPLetExpression outer = new DBSPLetExpression(outerBinder, b.lit(1), inner);

        ReferenceMap references = this.resolve(outer, false);
        Assert.assertSame(outer, references.get(initializerUse));
        Assert.assertSame(inner, references.get(consumerUse));
        Assert.assertSame(outer, references.get(outerBinder));
        Assert.assertSame(inner, references.get(innerBinder));
    }

    /** A lambda parameter shadows an outer variable with the same name */
    @Test
    public void testLambdaShadowing() {
        DBSPVariablePath outerParam = b.i32().var();
        String name = outerParam.variable;
        DBSPVariablePath outerUse = new DBSPVariablePath(name, b.i32());
        DBSPVariablePath innerParam = new DBSPVariablePath(name, b.i32());
        DBSPVariablePath innerUse = new DBSPVariablePath(name, b.i32());
        DBSPClosureExpression inner = b.call("h", innerUse).closure(innerParam);
        DBSPClosureExpression outer = b.call("m", outerUse, inner).closure(outerParam);

        ReferenceMap references = this.resolve(outer, false);
        Assert.assertSame(outer.parameters[0], references.get(outerUse));
        Assert.assertSame(inner.parameters[0], references.get(innerUse));
    }

    /** A variable declared by a let statement resolves to the statement */
    @Test
    public void testBlockStatement() {
        DBSPVariablePath t = b.refVar(b.tup(b.i32(), b.i32()));
        DBSPVariablePath[] bound = new DBSPVariablePath[1];
        DBSPExpression block = b.block(b.call("f", b.field(t, 0)),
                s -> {
                    bound[0] = s;
                    return b.add(s, b.lit(1));
                });
        ReferenceMap references = this.resolve(block.closure(t), false);
        DBSPLetStatement statement = block.to(DBSPBlockExpression.class)
                .contents.get(0).to(DBSPLetStatement.class);
        Assert.assertSame(statement, references.get(bound[0]));
    }

    /** A free variable is tolerated only when allowed */
    @Test
    public void testFreeVariable() {
        DBSPVariablePath free = b.i32().var();
        DBSPExpression expression = b.add(free, b.lit(1));

        // The only test that needs the resolver itself, for the flag
        ResolveReferences resolver = new ResolveReferences(this.testCompiler(), true);
        resolver.apply(expression);
        Assert.assertTrue(resolver.freeVariablesFound);
        Assert.assertNull(resolver.reference.get(free));

        Assert.assertThrows(InternalCompilerError.class,
                () -> this.resolve(expression, false));
    }

    @Test
    public void partialResolutionRetainsBindingsBeforeAConflict() {
        var compiler = this.testCompiler();
        var free = new DBSPVariablePath("missing", b.i32());
        var parameter = new DBSPVariablePath("outer", b.i32());
        var shared = new DBSPVariablePath(parameter.variable, parameter.getType());
        var inner = shared.closure(new DBSPVariablePath("outer", b.i32()));
        var prefix = b.tuple(free, shared);
        var conflict = b.tuple(prefix, inner).closure(parameter);
        var valid = prefix.closure(parameter);
        for (boolean nativePass : java.util.List.of(false, true)) {
            if (nativePass && !Boolean.getBoolean("feldera.optimizer.compareRustValueNumbers"))
                continue;
            compiler.options.ioOptions.rustOptimizerPasses = nativePass;
            var resolver = new ResolveReferences(compiler, true);
            var error = Assert.assertThrows(InternalCompilerError.class, () -> resolver.apply(conflict));
            Assert.assertSame(shared, error.dbspNode);
            Assert.assertTrue(resolver.hasFreeVariables());
            Assert.assertSame(conflict.parameters[0], resolver.reference.get(shared));
            resolver.apply(valid);
            Assert.assertTrue(resolver.hasFreeVariables());
            Assert.assertSame(valid.parameters[0], resolver.reference.get(shared));
            resolver.apply(b.i32());
            Assert.assertFalse(resolver.hasFreeVariables());
            Assert.assertNull(resolver.reference.get(shared));
        }
    }

    @Test
    public void valueNumberingReportsBindingErrorsBeforeChoosingGroups() {
        var compiler = this.testCompiler();
        var parameter = new DBSPVariablePath("x", b.i32());
        var shared = new DBSPVariablePath("x", b.i32());
        var free = new DBSPVariablePath("missing", b.i32());
        var conflict = new DBSPLetExpression(shared, shared, shared.deepCopy()).closure(parameter);
        var empty = new DBSPAggregateList(free.getNode(), free, java.util.List.of());
        for (var root : java.util.List.of(free, conflict, empty)) {
            compiler.options.ioOptions.rustOptimizerPasses = false;
            var expected = Assert.assertThrows(InternalCompilerError.class,
                    () -> ValueNumbering.forRewrite(compiler).apply(root));
            Assert.assertTrue(expected.getMessage(), expected.getMessage().contains(
                    root == conflict ? "Changing declaration" : "Could not resolve"));
            if (Boolean.getBoolean("feldera.optimizer.compareRustValueNumbers")) {
                compiler.options.ioOptions.rustOptimizerPasses = true;
                var numbering = ValueNumbering.forRewrite(compiler);
                var actual = Assert.assertThrows(InternalCompilerError.class, () -> numbering.apply(root));
                Assert.assertEquals(expected.getMessage(), actual.getMessage());
                Assert.assertSame(expected.dbspNode, actual.dbspNode);
                Assert.assertEquals(expected.getPositionRange(), actual.getPositionRange());
                var valid = shared.closure(parameter);
                Assert.assertSame(valid, numbering.apply(valid));
            }
        }
    }

    @Test
    public void waterlineReportsTheFirstBindingErrorAtTheLiveVariable() {
        var compiler = this.testCompiler();
        var parameter = new DBSPVariablePath("x", b.i32());
        var shared = new DBSPVariablePath("x", b.i32());
        var free = new DBSPVariablePath("missing", b.i32());
        for (var initializer : java.util.List.of(shared, b.add(shared, free))) {
            // The initializer reads the parameter. Reusing its path as the LET
            // binder conflicts only after every read in the initializer has resolved.
            var let = new DBSPLetExpression(shared, initializer, shared.deepCopy());
            var closure = let.closure(parameter);
            var direct = new ResolveReferences(compiler, false);
            var error = Assert.assertThrows(InternalCompilerError.class, () -> direct.apply(closure));
            Assert.assertTrue(error.getMessage(), error.getMessage().contains(
                    initializer == shared ? "Changing declaration" : "Could not resolve"));
            if (Boolean.getBoolean("feldera.optimizer.compareRustLimiterApply")) {
                var nativeError = Assert.assertThrows(InternalCompilerError.class,
                        () -> RustCircuitPass.inspectWaterline(compiler, closure));
                Assert.assertEquals(error.getMessage(), nativeError.getMessage());
            }
        }
    }
}
