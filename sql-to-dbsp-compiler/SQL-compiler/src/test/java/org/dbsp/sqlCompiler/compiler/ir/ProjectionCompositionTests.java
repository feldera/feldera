package org.dbsp.sqlCompiler.compiler.ir;

import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.backend.RustCircuitPass;
import org.dbsp.sqlCompiler.compiler.sql.tools.ExpressionBuilder;
import org.dbsp.sqlCompiler.compiler.visitors.inner.EquivalenceContext;
import org.dbsp.sqlCompiler.ir.expression.DBSPBlockExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPLetExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPRawTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeRawTuple;
import org.dbsp.util.Maybe;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

public class ProjectionCompositionTests {
    private final ExpressionBuilder b = new ExpressionBuilder();
    private final DBSPCompiler compiler = new DBSPCompiler(new CompilerOptions());

    @Test public void projectTheResultOfABlockAndKeepItsBindings() {
        var before = b.closure(b.tup(b.i32()), row ->
                b.block(b.call("read_value", b.field(row, 0)), v -> b.tuple(v, b.lit(0))));
        var after = b.closure(before.body.getType(), row -> b.field(row, 0));
        var expected = after.applyAfter(this.compiler, before, Maybe.YES);
        Assert.assertEquals(1, expected.body.to(DBSPBlockExpression.class).contents.size());
        if (Boolean.getBoolean("feldera.optimizer.compareRustProjectionFields")) {
            var actual = RustCircuitPass.composeProjection(this.compiler, before, after);
            Assert.assertTrue(EquivalenceContext.equiv(expected, actual));
            Assert.assertSame(before.parameters[0], actual.parameters[0]);
        }
    }

    @Test public void keepAProducerWhenTheProjectionDuplicatesAnExpensiveField() {
        var before = b.closure(b.tup(b.i32()), row -> b.tuple(b.call("read_value", b.field(row, 0))));
        var after = b.closure(before.body.getType(), row -> b.tuple(b.field(row, 0), b.field(row, 0)));
        Assert.assertFalse(after.shouldInlineComposition(this.compiler, before));
        if (Boolean.getBoolean("feldera.optimizer.compareRustProjectionFields"))
            Assert.assertNull(RustCircuitPass.tryComposeProjection(this.compiler, before, after));
    }

    @Test public void inlineIndexedTupleWithBorrowedKeyAndValue() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustProjectionFields"));
        var rowType = b.tup(b.i32());
        var before = b.closure(rowType, row -> new DBSPRawTupleExpression(
                b.tuple(b.call("read_key", b.field(row, 0))), b.tuple(b.field(row, 0))));
        var after = b.lambda(new DBSPTypeRawTuple(rowType.ref(), rowType.ref()), pair ->
                new DBSPRawTupleExpression(pair.field(1).deref(), pair.field(0).deref()));
        var borrowed = new DBSPRawTupleExpression(before.body.field(0).borrow(), before.body.field(1).borrow());
        var expected = after.call(borrowed).reduce(this.compiler).closure(before.parameters);
        long next = DBSPVariablePath.nextGeneratedName();
        for (boolean onlyProjections : new boolean[]{true, false}) {
            var actual = RustCircuitPass.composeIndexedProjection(this.compiler, before, after, onlyProjections);
            Assert.assertNotNull(actual);
            Assert.assertTrue(EquivalenceContext.equiv(expected, actual));
            Assert.assertSame(before.parameters[0], actual.parameters[0]);
            Assert.assertEquals(next, DBSPVariablePath.nextGeneratedName());
        }
    }

    @Test public void bindAnExpensiveIndexedProducerOnlyOnce() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustProjectionFields"));
        var rowType = b.tup(b.i32());
        var pairType = new DBSPTypeRawTuple(rowType, rowType);
        var before = b.closure(rowType, row -> b.call(pairType, "read_pair", b.field(row, 0)));
        var after = b.lambda(new DBSPTypeRawTuple(rowType.ref(), rowType.ref()), pair ->
                new DBSPRawTupleExpression(pair.field(1).deref(), pair.field(0).deref()));
        long next = DBSPVariablePath.nextGeneratedName();
        Assert.assertNull(RustCircuitPass.composeIndexedProjection(this.compiler, before, after, true));
        Assert.assertEquals(next, DBSPVariablePath.nextGeneratedName());
        var actual = RustCircuitPass.composeIndexedProjection(this.compiler, before, after, false);
        Assert.assertNotNull(actual);
        Assert.assertEquals(next + 1, DBSPVariablePath.nextGeneratedName());
        Assert.assertSame(before.parameters[0], actual.parameters[0]);
        var outer = actual.body.to(DBSPLetExpression.class);
        Assert.assertSame(before.body, outer.initializer);
        Assert.assertSame(pairType, outer.variable.type);
        var inner = outer.consumer.to(DBSPLetExpression.class);
        Assert.assertSame(after.body, inner.consumer);
        Assert.assertSame(after.parameters[0].type, inner.variable.type);
        var borrowed = new DBSPRawTupleExpression(outer.variable.field(0).borrow(), outer.variable.field(1).borrow());
        Assert.assertTrue(EquivalenceContext.equiv(borrowed, inner.initializer));
    }
}
