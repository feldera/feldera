package org.dbsp.sqlCompiler.compiler.ir;

import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.backend.RustCircuitPass;
import org.dbsp.sqlCompiler.compiler.frontend.ExpressionCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.sql.tools.ExpressionBuilder;
import org.dbsp.sqlCompiler.compiler.visitors.inner.EquivalenceContext;
import org.dbsp.sqlCompiler.compiler.visitors.inner.Projection;
import org.dbsp.sqlCompiler.ir.aggregate.DBSPAggregateList;
import org.dbsp.sqlCompiler.ir.aggregate.LinearAggregate;
import org.dbsp.sqlCompiler.ir.expression.DBSPBlockExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPCastExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPLetExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPRawTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeRawTuple;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTupleBase;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeIndexedZSet;
import org.dbsp.util.Maybe;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import java.util.List;

public class ProjectionCompositionTests {
    private final ExpressionBuilder b = new ExpressionBuilder();
    private final DBSPCompiler compiler = new DBSPCompiler(new CompilerOptions());

    private void checkProjection(DBSPClosureExpression closure, boolean strict, boolean nested) {
        for (boolean allowNestedAndCasts : new boolean[]{false, true}) {
            var oracle = new Projection(this.compiler, allowNestedAndCasts, allowNestedAndCasts);
            oracle.apply(closure);
            Assert.assertEquals(allowNestedAndCasts ? nested : strict, oracle.isProjection);
            if (Boolean.getBoolean("feldera.optimizer.compareRustProjectionFields"))
                Assert.assertEquals(oracle.isProjection,
                        RustCircuitPass.isMapProjection(this.compiler, closure, allowNestedAndCasts));
        }
    }

    @Test public void classifyProjectionsForJoinsAndSums() {
        var rowType = b.tup(b.i32());
        this.checkProjection(b.closure(rowType, row -> b.tuple(b.field(row, 0))), true, true);
        this.checkProjection(b.closure(rowType, row -> b.tuple(b.tuple(b.field(row, 0)))), false, true);
        this.checkProjection(b.closure(rowType, row -> b.tuple(b.field(row, 0)
                .nullabilityCast(b.i32n(), DBSPCastExpression.CastType.SqlUnsafe))), false, true);
        this.checkProjection(b.closure(rowType, row -> b.tuple(b.add(b.field(row, 0), b.lit(1)))), false, false);
        var indexed = new DBSPTypeRawTuple(rowType.ref(), rowType.ref());
        this.checkProjection(b.lambda(indexed, pair -> b.tuple(pair.field(1).deref().field(0))), true, true);
    }

    @Test public void trimMiddleTopKColumnAndRebindTheReader() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustProjectionFields"));
        var key = b.tup(b.str(), b.i32());
        var row = b.tup(b.i32(), b.str(), b.i32n());
        var producer = b.closure(row, r -> b.tuple(b.field(r, 0), b.field(r, 1), b.field(r, 2)));
        var reader = b.lambda(new DBSPTypeRawTuple(key.ref(), row.ref()), pair ->
                new DBSPRawTupleExpression(b.tuple(pair.field(0).deref().field(1)),
                        b.tuple(pair.field(1).deref().field(2), pair.field(1).deref().field(0))));
        var narrowed = RustCircuitPass.trimTopKProjection(this.compiler, producer, reader);
        Assert.assertNotNull(narrowed);
        Assert.assertSame(producer.parameters[0], narrowed.left.parameters[0]);
        var expectedProducer = b.closure(row, r -> b.tuple(b.field(r, 0), b.field(r, 2)));
        var narrowRow = b.tup(b.i32(), b.i32n());
        var expectedReader = b.lambda(new DBSPTypeRawTuple(key.ref(), narrowRow.ref()), pair ->
                new DBSPRawTupleExpression(b.tuple(pair.field(0).deref().field(1)),
                        b.tuple(pair.field(1).deref().field(1), pair.field(1).deref().field(0))));
        Assert.assertTrue(EquivalenceContext.equiv(expectedProducer, narrowed.left));
        Assert.assertTrue(EquivalenceContext.equiv(expectedReader, narrowed.right));
    }

    @Test public void keepTopKWhenTheReaderUsesTheWholeValue() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustProjectionFields"));
        var row = b.tup(b.i32(), b.i32());
        var producer = b.closure(row, r -> b.tuple(b.field(r, 0), b.field(r, 1)));
        var reader = b.lambda(new DBSPTypeRawTuple(b.tup(b.i32()).ref(), row.ref()), pair ->
                new DBSPRawTupleExpression(b.tuple(), pair.field(1).deref()));
        Assert.assertNull(RustCircuitPass.trimTopKProjection(this.compiler, producer, reader));
    }

    @Test public void fillDroppedAggregateColumnsWithTheirOriginalEmptySetResults() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustProjectionFields"));
        var row = b.i32().var();
        var map = row.closure(row);
        var first = new LinearAggregate(CalciteObject.EMPTY, map, map, b.lit(7));
        var second = new LinearAggregate(CalciteObject.EMPTY, map, map, b.lit(11));
        var list = new DBSPAggregateList(CalciteObject.EMPTY, row, List.of(first, second));
        var output = new DBSPTypeIndexedZSet(CalciteObject.EMPTY, b.tup(b.str(), b.i32()), list.getEmptySetResultType());
        var all = b.lambda(output.getKVRefType(), pair -> pair.field(1).deref());
        long next = DBSPVariablePath.nextGeneratedName();
        Assert.assertNull(RustCircuitPass.trimAggregateProjection(this.compiler, list, output, all));
        Assert.assertEquals(next, DBSPVariablePath.nextGeneratedName());

        for (boolean keepSecond : new boolean[]{true, false}) {
            var reader = b.lambda(output.getKVRefType(), pair -> keepSecond ? pair.field(1).deref().field(1) : b.tuple());
            next = DBSPVariablePath.nextGeneratedName();
            var projected = RustCircuitPass.trimAggregateProjection(this.compiler, list, output, reader);
            Assert.assertNotNull(projected);
            Assert.assertEquals(next + 1, DBSPVariablePath.nextGeneratedName());
            Assert.assertEquals(keepSecond ? List.of(second) : List.of(), projected.left.aggregates);
            var filler = projected.right;
            var input = filler.parameters[0].type.to(DBSPTypeRawTuple.class);
            Assert.assertSame(output.keyType, input.tupFields[0].deref());
            Assert.assertSame(projected.left.getEmptySetResultType(), input.tupFields[1].deref());
            var values = filler.body.to(DBSPRawTupleExpression.class).fields[1].to(DBSPTupleExpression.class);
            Assert.assertSame(first.emptySetResult, values.fields[0]);
            if (keepSecond) {
                var expected = filler.parameters[0].asVariable().field(1).deref().field(0);
                Assert.assertTrue(EquivalenceContext.equiv(expected.closure(filler.parameters),
                        values.fields[1].closure(filler.parameters)));
            } else {
                Assert.assertSame(second.emptySetResult, values.fields[1]);
            }
        }
    }

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

    @Test public void deriveTheFieldTypeAfterRemovingACloneOfANullableRow() {
        var record = b.tup(b.i32(), b.str()).withMayBeNull(true);
        var before = b.closure(b.tup(record), row -> b.tuple(b.field(row, 0).applyCloneIfNeeded().field(0)));
        var after = b.closure(before.body.type, row -> b.field(row, 0));
        var expected = after.applyAfter(this.compiler, before, Maybe.YES);
        Assert.assertTrue(expected.body.type.mayBeNull);
        Assert.assertNotSame(before.body.field(0).type, expected.body.type);
        if (Boolean.getBoolean("feldera.optimizer.compareRustProjectionFields")) {
            var actual = RustCircuitPass.composeProjection(this.compiler, before, after);
            Assert.assertTrue(EquivalenceContext.equiv(expected, actual));
            Assert.assertNotSame(before.body.field(0).type, actual.body.type);
        }
    }

    @Test public void reduceAProducerCallBeforeSubstitutingItIntoAProjection() {
        var rowType = b.tup(b.i32(), b.i32());
        var swap = b.closure(rowType, row -> b.tuple(b.field(row, 1), b.field(row, 0)));
        var before = b.closure(rowType, swap::call);
        var after = b.closure(before.body.type, row -> b.tuple(b.field(row, 1), b.field(row, 0)));
        var expected = after.applyAfter(this.compiler, before, Maybe.YES);
        var input = before.parameters[0].asVariable();
        var identity = b.tuple(b.field(input, 0), b.field(input, 1)).closure(before.parameters);
        Assert.assertTrue(EquivalenceContext.equiv(identity, expected));
        if (Boolean.getBoolean("feldera.optimizer.compareRustProjectionFields")) {
            var actual = RustCircuitPass.composeProjection(this.compiler, before, after);
            Assert.assertTrue(EquivalenceContext.equiv(expected, actual));
        }
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

    @Test public void retainCompositeKeysOnBothAntijoinInputs() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustProjectionFields"));
        var key = b.tup(b.tup(b.strn()), b.i32());
        var input = new DBSPTypeIndexedZSet(CalciteObject.EMPTY, key, b.tup(b.str(), b.i32n()));
        var projection = b.lambda(input.getKVRefType(), pair -> b.tuple(pair.field(1).deref().field(1)));
        var pair = input.getKVRefType().var();
        var borrowedKey = pair.field(0).deref();
        var left = new DBSPRawTupleExpression(ExpressionCompiler.expandTuple(projection.getNode(), borrowedKey),
                projection.call(pair).reduce(this.compiler)).closure(pair);
        var right = new DBSPRawTupleExpression(
                new DBSPTupleExpression(DBSPTypeTupleBase.flatten(borrowedKey), false),
                new DBSPTupleExpression()).closure(pair);
        long next = DBSPVariablePath.nextGeneratedName();
        var actualLeft = RustCircuitPass.antijoinProjection(this.compiler, input, projection);
        var actualRight = RustCircuitPass.antijoinProjection(this.compiler, input, null);
        Assert.assertTrue(EquivalenceContext.equiv(left, actualLeft));
        Assert.assertTrue(EquivalenceContext.equiv(right, actualRight));
        Assert.assertSame(key, actualLeft.body.field(0).type);
        Assert.assertEquals(next + 2, DBSPVariablePath.nextGeneratedName());
    }

    @Test public void splitAnIndexedProjectionAtTheAntijoin() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustProjectionFields"));
        var key = b.tup(b.i32(), b.str());
        var value = b.tup(b.i32(), b.i32n());
        var input = new DBSPTypeRawTuple(key.ref(), value.ref());
        var projection = b.lambda(input, pair -> new DBSPRawTupleExpression(
                b.tuple(pair.field(0).deref().field(1).applyCloneIfNeeded()),
                b.tuple(pair.field(1).deref().field(0))));
        long next = DBSPVariablePath.nextGeneratedName();
        var split = RustCircuitPass.splitAntijoinProjection(this.compiler, projection);
        Assert.assertEquals(next, DBSPVariablePath.nextGeneratedName());
        Assert.assertSame(projection.parameters[0], split.left.parameters[0]);
        Assert.assertSame(input.tupFields[0], split.right.parameters[0].type.to(DBSPTypeRawTuple.class).tupFields[0]);
        Assert.assertTrue(key.sameType(split.left.body.field(0).type));
        Assert.assertTrue(projection.body.type.sameType(split.right.body.type));
        var argument = new DBSPRawTupleExpression(split.left.body.field(0).borrow(), split.left.body.field(1).borrow());
        var composed = split.right.call(argument).reduce(this.compiler).closure(split.left.parameters);
        Assert.assertTrue(EquivalenceContext.equiv(projection.reduce(this.compiler), composed));

        var invalid = b.lambda(input, pair -> new DBSPRawTupleExpression(
                b.tuple(pair.field(1).deref().field(0)), b.tuple()));
        var error = Assert.assertThrows(RuntimeException.class,
                () -> RustCircuitPass.splitAntijoinProjection(this.compiler, invalid));
        Assert.assertTrue(error.getMessage().contains("key projection reads the input value"));
    }
}
