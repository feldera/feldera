package org.dbsp.sqlCompiler.compiler.visitors.unusedFields;

import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.backend.RustCircuitPass;
import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.inner.EquivalenceContext;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitTransform;
import org.dbsp.sqlCompiler.ir.DBSPParameter;
import org.dbsp.sqlCompiler.ir.expression.DBSPApplyExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPAssignmentExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPBlockExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPDirectComparatorExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPNoComparatorExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.statement.DBSPComment;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeRawTuple;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeInteger;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeString;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import java.util.HashSet;
import java.util.List;

public class JoinInputProjectionTests {
    @Test
    public void discardedStatementsAreCheckedBeforeTheFinalFieldRead() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustFieldUses"));
        var compiler = new DBSPCompiler(new CompilerOptions());
        var text = DBSPTypeString.varchar(false);
        var row = new DBSPTypeTuple(text, text).ref().var();
        var first = row.deref().field(0);
        var second = row.deref().field(1);
        var sameField = first.deepCopy();
        var computed = new DBSPApplyExpression("identity", text, first);
        for (var rhs : List.of(sameField, second, computed)) {
            var assignment = new DBSPAssignmentExpression(first, rhs);
            var nested = new DBSPBlockExpression(List.of(assignment.toStatement()), first);
            var body = new DBSPBlockExpression(List.of(new DBSPComment("discarded read"), nested.toStatement()), second);
            var reader = body.closure(row);
            var actual = RustCircuitPass.parameterFieldDependencies(compiler, reader);
            var visitor = new FindUsedFields(compiler);
            compiler.options.ioOptions.rustOptimizerPasses = false;
            if (rhs == sameField) {
                Assert.assertEquals(List.of(List.of(List.of(1))), actual);
                var reference = visitor.findUsedFields(reader);
                var param = reader.parameters[0];
                Assert.assertEquals(reference.get(param).toString(),
                        FindUsedFields.fromPaths(reader, actual).get(param).toString());
            } else {
                Assert.assertNull(actual);
                Assert.assertThrows(InternalCompilerError.class, () -> visitor.findUsedFields(reader));
                compiler.options.ioOptions.rustOptimizerPasses = true;
                Assert.assertThrows(InternalCompilerError.class, () -> new FindUsedFields(compiler).findUsedFields(reader));
            }
        }
    }

    @Test
    public void sqlTopKUsesRustToDiscoverAndRemapComparedFields() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustMapDependencies"));
        var options = new CompilerOptions();
        options.ioOptions.rustOptimizerPasses = true;
        var compiler = new DBSPCompiler(options);
        var answered = new HashSet<String>();
        compiler.stateObserver = new DBSPCompiler.StateObserver() {
            @Override
            public void nativeDecision(String operation, boolean success) {
                if (success)
                    answered.add(operation);
            }

            @Override
            public void before(String parent, int index, int iteration, CircuitTransform pass, DBSPCircuit input) {}

            @Override
            public void after(String parent, int index, int iteration, CircuitTransform pass,
                              DBSPCircuit output, boolean completed) {}
        };
        compiler.submitStatementsForCompilation("""
                CREATE TABLE t(id INT NOT NULL, ts INT NOT NULL, a INT, b INT, c INT);
                CREATE VIEW v AS SELECT id, a, FIRST_VALUE(b) OVER (PARTITION BY id ORDER BY ts) AS first_b FROM t;
                """);
        compiler.getFinalCircuit(false);
        compiler.throwIfErrorsOccurred();
        Assert.assertTrue(answered.contains("RemoveUnusedFields/TopKFields"));
        Assert.assertTrue(answered.contains("RemoveUnusedFields/TopKComparators"));
    }

    @Test
    public void rebindsEachJoinInputAndRetainsTheKeyDeclaration() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustMapDependencies"));
        var compiler = new DBSPCompiler(new CompilerOptions());
        var text = DBSPTypeString.varchar(false);
        var row = new DBSPTypeTuple(text, text, new DBSPTypeTuple(text));
        var key = new DBSPParameter("key", text.ref());
        var left = new DBSPParameter("left", row.ref());
        var right = new DBSPParameter("right", row.ref());
        var body = new DBSPTupleExpression(left.asVariable().deref().field(2).field(0),
                right.asVariable().deref().field(1), key.asVariable().deref());
        var closure = body.closure(key, left, right);
        var fields = new FindUsedFields(compiler).findUsedFields(closure);
        var rewriter = fields.getFieldRewriter(compiler, 1);
        rewriter.parameterFullyUsed(key);
        var expected = rewriter.rewriteClosure(closure);
        compiler.options.ioOptions.rustOptimizerPasses = true;
        var actual = rewriter.rewriteClosure(closure);
        Assert.assertTrue(EquivalenceContext.equiv(expected, actual));
        Assert.assertSame(key, actual.parameters[0]);
        Assert.assertSame(rewriter.newParam.get(left), actual.parameters[1]);
        Assert.assertSame(rewriter.newParam.get(right), actual.parameters[2]);
    }

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
