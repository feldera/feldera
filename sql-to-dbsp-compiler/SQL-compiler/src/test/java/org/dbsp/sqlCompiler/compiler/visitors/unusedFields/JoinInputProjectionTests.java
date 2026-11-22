package org.dbsp.sqlCompiler.compiler.visitors.unusedFields;

import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.backend.RustCircuitPass;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.inner.EquivalenceContext;
import org.dbsp.sqlCompiler.ir.expression.DBSPDirectComparatorExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPNoComparatorExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeRawTuple;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeInteger;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeString;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import java.util.List;

public class JoinInputProjectionTests {
    @Test
    public void topKProjectionPreservesTieBreakersAndNullOrdering() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustMapDependencies"));
        var compiler = new DBSPCompiler(new CompilerOptions());
        var site = CalciteObject.EMPTY;
        var text = DBSPTypeString.varchar(true);
        var row = new DBSPTypeTuple(text, text, text, text);
        var narrowed = new DBSPTypeTuple(text, text);
        var base = new DBSPNoComparatorExpression(site, row);
        var order = base.field(3, false, true).field(1, true, false);
        var ties = base.field(3, true, false);
        Assert.assertEquals(List.of(1, 3), RustCircuitPass.topKComparedFields(compiler, order, ties));
        Assert.assertEquals(List.of(), RustCircuitPass.topKComparedFields(compiler, base, base));
        var direct = new DBSPDirectComparatorExpression(site, base, true);
        Assert.assertNull(RustCircuitPass.topKComparedFields(compiler, order, direct));

        var fields = new FieldUseMap(row, false);
        fields.setUsed(1);
        fields.setUsed(3);
        var result = RustCircuitPass.projectTopKComparators(compiler, order, ties, narrowed, fields.usageAtDepth(1));
        var expectedOrder = RemoveUnusedFields.remapComparedFields(order, fields, narrowed);
        var expectedTies = RemoveUnusedFields.remapComparedFields(ties, fields, narrowed);
        Assert.assertTrue(EquivalenceContext.equiv(expectedOrder, result.left));
        Assert.assertTrue(EquivalenceContext.equiv(expectedTies, result.right));
        Assert.assertEquals(expectedOrder.getComparatorStructName(), result.left.getComparatorStructName());
        Assert.assertEquals(expectedTies.getComparatorStructName(), result.right.getComparatorStructName());
        Assert.assertSame(narrowed, result.left.comparedValueType());
        Assert.assertSame(narrowed, result.right.comparedValueType());
    }

    @Test
    public void compressesParametersAtTheRequestedDepthAndSharesOriginalLeaves() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustMapDependencies"));
        var compiler = new DBSPCompiler(new CompilerOptions());
        var text = DBSPTypeString.varchar(false);
        var nested = new DBSPTypeTuple(text, text).withMayBeNull(true);
        var pair = new DBSPTypeRawTuple(nested.ref(), new DBSPTypeTuple().ref());
        var bits = new FieldUseMap(pair, false);
        bits.field(0).deref().setUsed(1);
        for (int depth : List.of(0, 1, 2)) {
            var expected = bits.compressedType(depth);
            Assert.assertNotNull(expected);
            var actual = RustCircuitPass.compressedParameterTypes(compiler, depth, List.of(pair, text),
                    List.of(bits.usageAtDepth(depth), List.of(true)));
            Assert.assertTrue(expected.sameType(actual.get(0)));
            Assert.assertSame(text, actual.get(1));
            if (depth == 0)
                Assert.assertSame(pair, actual.get(0));
            else {
                var row = actual.get(0).to(DBSPTypeRawTuple.class).tupFields[0].deref().to(DBSPTypeTuple.class);
                Assert.assertEquals(depth == 1 ? 2 : 1, row.size());
                Assert.assertSame(text, row.tupFields[0]);
                Assert.assertTrue(row.mayBeNull);
            }
        }
    }

    @Test
    public void indexedValueProjectionExpandsNestedKeysAndPreservesNullableRows() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustMapDependencies"));
        var compiler = new DBSPCompiler(new CompilerOptions());
        var number = new DBSPTypeInteger(CalciteObject.EMPTY, 64, true, false);
        var text = DBSPTypeString.varchar(false);
        var nested = new DBSPTypeTuple(number, text).withMayBeNull(true);
        var key = new DBSPTypeTuple(text, nested);
        for (boolean nullable : List.of(false, true)) {
            var row = new DBSPTypeTuple(number, nested, text).withMayBeNull(nullable);
            var input = new DBSPTypeRawTuple(key.ref(), row.ref());
            for (var columns : List.of(List.<Integer>of(), List.of(1, 2))) {
                var used = new FieldUseMap(row.ref(), false);
                for (int column : columns)
                    used.deref().setUsed(column);
                long start = DBSPVariablePath.nextGeneratedName();
                var projection = used.getProjection(1);
                Assert.assertNotNull(projection);
                var expected = RemoveUnusedFields.indexedValueProjection(compiler, input, projection);
                long end = DBSPVariablePath.nextGeneratedName();
                DBSPVariablePath.restoreGeneratedNames(start);
                var actual = RustCircuitPass.projectUsedFields(compiler, used.getType(), used.usageAtDepth(1), input);
                Assert.assertEquals(end, DBSPVariablePath.nextGeneratedName());
                Assert.assertTrue(expected.getResultType().sameType(actual.getResultType()));
                Assert.assertTrue(EquivalenceContext.equiv(expected, actual));
            }
        }
    }

    @Test
    public void fieldProjectionRetainsEmptyTuplesAndRawTuplePositions() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustMapDependencies"));
        var compiler = new DBSPCompiler(new CompilerOptions());
        var text = DBSPTypeString.varchar(false);
        var empty = new DBSPTypeTuple();
        var nested = new DBSPTypeTuple(text);
        for (var input : List.of(text, new DBSPTypeTuple(empty, text), new DBSPTypeRawTuple(text.ref(), nested))) {
            for (boolean read : List.of(false, true)) {
                var used = new FieldUseMap(input.ref(), read);
                long start = DBSPVariablePath.nextGeneratedName();
                var expected = used.getProjection(1);
                Assert.assertNotNull(expected);
                long end = DBSPVariablePath.nextGeneratedName();
                DBSPVariablePath.restoreGeneratedNames(start);
                var actual = RustCircuitPass.projectUsedFields(compiler, used.getType(), used.usageAtDepth(1), null);
                Assert.assertEquals(end, DBSPVariablePath.nextGeneratedName());
                Assert.assertTrue(expected.getResultType().sameType(actual.getResultType()));
                Assert.assertTrue(EquivalenceContext.equiv(expected, actual));
            }
        }
    }

    @Test
    public void keepsCompositeKeysAndDistinguishesNullFromAnEmptyValue() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustMapDependencies"));
        var compiler = new DBSPCompiler(new CompilerOptions());
        var site = CalciteObject.EMPTY;
        var number = new DBSPTypeInteger(site, 64, true, false);
        var text = DBSPTypeString.varchar(false);
        var key = new DBSPTypeTuple(number, text);
        for (boolean nullable : List.of(false, true)) {
            var row = new DBSPTypeTuple(number, number, text).withMayBeNull(nullable);
            var pair = new DBSPTypeRawTuple(key.ref(), row.ref());
            for (var columns : List.of(List.<Integer>of(), List.of(0, 2))) {
                long start = DBSPVariablePath.nextGeneratedName();
                var expected = RemoveUnusedFields.joinInputProjection(site, pair, columns);
                long end = DBSPVariablePath.nextGeneratedName();
                DBSPVariablePath.restoreGeneratedNames(start);
                var actual = RustCircuitPass.projectJoinInput(compiler, site, pair, columns);
                Assert.assertEquals(end, DBSPVariablePath.nextGeneratedName());
                Assert.assertTrue(actual.getResultType().sameType(expected.getResultType()));
                Assert.assertTrue(EquivalenceContext.equiv(expected, actual));
            }
        }
    }
}
