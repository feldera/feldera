package org.dbsp.sqlCompiler.compiler.visitors.outer.monotonicity;

import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.circuit.operator.DBSPConstantOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPWaterlineOperator;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.ProgramMetadata;
import org.dbsp.sqlCompiler.compiler.backend.RustCircuitPass;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteEmptyRel;
import org.dbsp.sqlCompiler.compiler.visitors.monotone.IMaybeMonotoneType;
import org.dbsp.sqlCompiler.compiler.visitors.monotone.MonotoneRefType;
import org.dbsp.sqlCompiler.compiler.visitors.monotone.MonotoneTransferFunctions;
import org.dbsp.sqlCompiler.compiler.visitors.monotone.MonotoneType;
import org.dbsp.sqlCompiler.compiler.visitors.monotone.NonMonotoneType;
import org.dbsp.sqlCompiler.compiler.visitors.monotone.PartiallyMonotoneTuple;
import org.dbsp.sqlCompiler.ir.expression.DBSPApplyExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPAssignmentExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPBinaryExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPBlockExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPCastExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPIfExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPLetExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPOpcode;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPUnaryExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPUnsignedWrapExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.expression.DBSPZSetExpression;
import org.dbsp.sqlCompiler.ir.expression.NoExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPBoolLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPDoubleLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPI32Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPISizeLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPStringLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPUSizeLiteral;
import org.dbsp.sqlCompiler.ir.statement.DBSPComment;
import org.dbsp.sqlCompiler.ir.statement.DBSPExpressionStatement;
import org.dbsp.sqlCompiler.ir.statement.DBSPLetStatement;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeRawTuple;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeBool;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeDouble;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeInteger;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeTimestamp;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeUSize;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class WaterlineProjectionTests {
    @Test
    public void projectThroughAReferenceToAPartiallyBoundRow() {
        var compiler = new DBSPCompiler(new CompilerOptions());
        var node = CalciteEmptyRel.INSTANCE;
        var integer = new DBSPTypeInteger(node, 32, true, false);
        var known = new MonotoneType(integer);
        var unknown = new NonMonotoneType(integer);
        var inner = new PartiallyMonotoneTuple(List.of(unknown, known), false, false);
        var row = new PartiallyMonotoneTuple(List.of(unknown, new MonotoneRefType(inner)), true, false);
        var input = new DBSPVariablePath("row", row.getType().ref());
        var body = new DBSPTupleExpression(input.deepCopy().deref().field(1).deref().field(0),
                input.deepCopy().deref().field(1).deref().field(1));
        var operator = new DBSPConstantOperator(node, DBSPZSetExpression.emptyWithElementType(integer));
        var direct = new MonotoneTransferFunctions(compiler, operator, MonotoneTransferFunctions.ArgumentKind.ZSet,
                row).applyAnalysis(body.closure(input)).getReducedExpression();
        var bound = new DBSPVariablePath("row", row.getProjectedType().ref());
        var expected = new DBSPTupleExpression(bound.deref().field(0).deref().field(0)).closure(bound);
        Assert.assertTrue(direct.equivalent(expected));
        if (Boolean.getBoolean("feldera.optimizer.compareRustLimiterApply")) {
            var projected = RustCircuitPass.projectRow(compiler, body, bound, row);
            Assert.assertNotNull(projected);
            Assert.assertTrue(direct.equivalent(projected.getReducedExpression().closure(bound)));
            var result = projected.getMonotoneType().to(PartiallyMonotoneTuple.class);
            Assert.assertSame(unknown, result.getFieldType(0));
            Assert.assertSame(known, result.getFieldType(1));
        }
    }

    @Test
    public void targetSizedLiteralBoundsUseTheRequiredSingletonType() {
        var compiler = new DBSPCompiler(new CompilerOptions());
        var node = CalciteEmptyRel.INSTANCE;
        var integer = new DBSPTypeInteger(node, 32, true, false);
        var row = new PartiallyMonotoneTuple(List.of(new MonotoneType(integer)), false, false);
        var body = new DBSPTupleExpression(new DBSPISizeLiteral(-1L, true),
                new DBSPUSizeLiteral(node, DBSPTypeUSize.create(true), java.math.BigInteger.ONE.shiftLeft(63)));
        var input = new DBSPVariablePath("row", row.getType().ref());
        var operator = new DBSPConstantOperator(node, DBSPZSetExpression.emptyWithElementType(integer));
        var direct = new MonotoneTransferFunctions(compiler, operator, MonotoneTransferFunctions.ArgumentKind.ZSet,
                row).applyAnalysis(body.closure(input)).getReducedExpression().to(DBSPClosureExpression.class);
        if (Boolean.getBoolean("feldera.optimizer.compareRustLimiterApply")) {
            var bound = new DBSPVariablePath("row", row.getProjectedType().ref());
            var projected = RustCircuitPass.projectRow(compiler, body, bound, row);
            Assert.assertNotNull(projected);
            Assert.assertTrue(direct.equivalent(projected.getReducedExpression().closure(bound)));
            var fields = projected.getReducedExpression().to(DBSPTupleExpression.class).fields;
            var expected = direct.body.to(DBSPTupleExpression.class).fields;
            Assert.assertSame(expected[0].getType(), fields[0].getType());
            Assert.assertSame(expected[1].getType(), fields[1].getType());
        }
    }

    @Test
    public void projectNestedNullableRow() {
        var compiler = new DBSPCompiler(new CompilerOptions());
        var node = CalciteEmptyRel.INSTANCE;
        var integer = new DBSPTypeInteger(node, 32, true, false);
        var fixed = new MonotoneType(integer);
        var unknown = new NonMonotoneType(integer);
        var nested = new PartiallyMonotoneTuple(List.of(unknown, fixed), false, true);
        var row = new PartiallyMonotoneTuple(List.of(unknown, fixed, nested), false, false);
        // The closure's SQL types can be equal to distinct upstream types.
        var localInteger = new DBSPTypeInteger(node, 32, true, false);
        var localRow = new DBSPTypeTuple(localInteger, localInteger, nested.getType());
        var original = new DBSPVariablePath("row", localRow.ref());
        var body = new DBSPTupleExpression(original.deepCopy().deref().field(2).field(1),
                original.deepCopy().deref().field(0), original.deepCopy().deref().field(1));
        var closure = body.closure(original);
        var operator = new DBSPConstantOperator(node, DBSPZSetExpression.emptyWithElementType(row.getType()));
        var analysis = new MonotoneTransferFunctions(compiler, operator,
                MonotoneTransferFunctions.ArgumentKind.ZSet, row);
        var direct = analysis.applyAnalysis(closure).getReducedExpression().to(DBSPClosureExpression.class);
        var parameter = new DBSPVariablePath("row", row.getProjectedType().ref());
        var expected = new DBSPTupleExpression(parameter.deepCopy().deref().field(1).field(0),
                parameter.deepCopy().deref().field(0)).closure(parameter);
        Assert.assertTrue(direct.equivalent(expected));
        if (Boolean.getBoolean("feldera.optimizer.compareRustLimiterApply")) {
            var result = RustCircuitPass.projectRow(compiler, body, parameter, row);
            Assert.assertNotNull(result);
            Assert.assertTrue(result.getReducedExpression().closure(parameter).equivalent(expected));
            var shape = result.getMonotoneType().to(PartiallyMonotoneTuple.class);
            Assert.assertTrue(shape.getFieldType(0).getType().mayBeNull);
            Assert.assertFalse(shape.getFieldType(1).mayBeMonotone());
            Assert.assertSame(fixed, shape.getFieldType(2));
        }
    }

    @Test
    public void projectValueWhenTheIndexHasNoBound() {
        var compiler = new DBSPCompiler(new CompilerOptions());
        var integer = new DBSPTypeInteger(CalciteEmptyRel.INSTANCE, 32, true, false);
        var field = new MonotoneType(integer);
        var unbounded = new NonMonotoneType(integer);
        var key = new PartiallyMonotoneTuple(List.of(unbounded), false, false);
        var value = new PartiallyMonotoneTuple(List.of(unbounded, field), false, false);
        var indexed = new PartiallyMonotoneTuple(List.of(key, value), true, false);
        var parameter = new DBSPVariablePath("pair", new DBSPTypeRawTuple(key.getType().ref(), value.getType().ref()));
        var body = new DBSPTupleExpression(parameter.deepCopy().field(0).deref().field(0),
                parameter.deepCopy().field(1).deref().field(1));
        var closure = body.closure(parameter);
        var operator = new DBSPConstantOperator(CalciteEmptyRel.INSTANCE, DBSPZSetExpression.emptyWithElementType(integer));
        var direct = new MonotoneTransferFunctions(compiler, operator, MonotoneTransferFunctions.ArgumentKind.IndexedZSet,
                indexed).applyAnalysis(closure);
        Assert.assertTrue(direct.mayBeMonotone());
        if (Boolean.getBoolean("feldera.optimizer.compareRustLimiterApply")) {
            var bound = new DBSPVariablePath("pair", new DBSPTypeRawTuple(value.getProjectedType().ref()));
            var nativeValue = RustCircuitPass.projectRows(compiler, body, new DBSPVariablePath[]{bound},
                    new IMaybeMonotoneType[]{key, value}, true);
            Assert.assertNotNull(nativeValue);
            var expected = new DBSPTupleExpression(bound.deepCopy().field(0).deref().field(0)).closure(bound);
            Assert.assertTrue(expected.equivalent(nativeValue.getReducedExpression().closure(bound)));
            Assert.assertSame(field, nativeValue.getMonotoneType().to(PartiallyMonotoneTuple.class).getFieldType(1));
            compiler.options.ioOptions.rustOptimizerPasses = true;
            var hybrid = new MonotoneTransferFunctions(compiler, operator, MonotoneTransferFunctions.ArgumentKind.IndexedZSet,
                    indexed).applyAnalysis(closure);
            Assert.assertTrue(direct.getReducedExpression().equivalent(hybrid.getReducedExpression()));
        }
    }

    @Test
    public void projectJoinRowsWithDifferentBoundColumns() {
        var compiler = new DBSPCompiler(new CompilerOptions());
        var node = CalciteEmptyRel.INSTANCE;
        var integer = new DBSPTypeInteger(node, 32, true, false);
        var keyField = new MonotoneType(integer);
        var leftField = new MonotoneType(integer);
        var rightField = new MonotoneType(integer);
        var unbounded = new NonMonotoneType(integer);
        var key = new PartiallyMonotoneTuple(List.of(keyField), false, false);
        var left = new PartiallyMonotoneTuple(List.of(unbounded, leftField), false, false);
        var right = new PartiallyMonotoneTuple(List.of(rightField, unbounded), false, false);
        var k = new DBSPVariablePath("k", key.getType().ref());
        var l = new DBSPVariablePath("l", left.getType().ref());
        var r = new DBSPVariablePath("r", right.getType().ref());
        var body = new DBSPTupleExpression(r.deepCopy().deref().field(0), l.deepCopy().deref().field(0),
                k.deepCopy().deref().field(0), l.deepCopy().deref().field(1), r.deepCopy().deref().field(0));
        var closure = body.closure(k, l, r);
        var inputs = new IMaybeMonotoneType[]{key, left, right};
        var operator = new DBSPConstantOperator(node, DBSPZSetExpression.emptyWithElementType(integer));
        var kind = MonotoneTransferFunctions.ArgumentKind.Join;
        var direct = new MonotoneTransferFunctions(compiler, operator, kind, inputs).applyAnalysis(closure);
        Assert.assertTrue(direct.mayBeMonotone());
        if (Boolean.getBoolean("feldera.optimizer.compareRustLimiterApply")) {
            var bounds = new DBSPVariablePath[]{new DBSPVariablePath("k", key.getProjectedType().ref()),
                    new DBSPVariablePath("l", left.getProjectedType().ref()), new DBSPVariablePath("r", right.getProjectedType().ref())};
            var projected = RustCircuitPass.projectRows(compiler, body, bounds, inputs, false);
            Assert.assertNotNull(projected);
            var tuple = projected.getMonotoneType().to(PartiallyMonotoneTuple.class);
            var expected = List.of(rightField, unbounded, keyField, leftField, rightField);
            Assert.assertEquals(expected.size(), tuple.size());
            for (int i = 0; i < expected.size(); i++)
                Assert.assertSame(expected.get(i), tuple.getFieldType(i));
            compiler.options.ioOptions.rustOptimizerPasses = true;
            var hybrid = new MonotoneTransferFunctions(compiler, operator, kind, inputs).applyAnalysis(closure);
            Assert.assertTrue(direct.getReducedExpression().equivalent(hybrid.getReducedExpression()));
        }
    }

    @Test
    public void localRowWaterlinesRespectShadowingAndShareTheirInitializer() {
        var compiler = new DBSPCompiler(new CompilerOptions());
        var node = CalciteEmptyRel.INSTANCE;
        var integer = new DBSPTypeInteger(node, 32, true, false);
        var row = new PartiallyMonotoneTuple(List.of(new MonotoneType(integer), new NonMonotoneType(integer)), false, false);
        var parameter = new DBSPVariablePath("row", row.getType().ref());
        var outer = new DBSPLetStatement("v", new DBSPTupleExpression(
                parameter.deepCopy().deref().field(0), parameter.deepCopy().deref().field(1)));
        var nested = new DBSPVariablePath("v", integer);
        var shadow = new DBSPLetExpression(nested, outer.getVarReference().field(0),
                new DBSPTupleExpression(nested.deepCopy(), nested.deepCopy(), nested.deepCopy().some()));
        var body = new DBSPBlockExpression(List.of(outer), new DBSPTupleExpression(
                shadow, outer.getVarReference(), outer.getVarReference()));
        var operator = new DBSPConstantOperator(node, DBSPZSetExpression.emptyWithElementType(integer));
        var direct = new MonotoneTransferFunctions(compiler, operator, MonotoneTransferFunctions.ArgumentKind.ZSet, row)
                .applyAnalysis(body.closure(parameter)).getReducedExpression().to(DBSPClosureExpression.class);
        if (Boolean.getBoolean("feldera.optimizer.compareRustLimiterApply")) {
            var bound = new DBSPVariablePath("row", row.getProjectedType().ref());
            var projected = RustCircuitPass.projectRow(compiler, body, bound, row);
            Assert.assertNotNull(projected);
            Assert.assertTrue(direct.equivalent(projected.getReducedExpression().closure(bound)));
            var types = projected.getMonotoneType().to(PartiallyMonotoneTuple.class);
            Assert.assertSame(types.getFieldType(1), types.getFieldType(2));
            var tuple = projected.getReducedExpression().to(DBSPTupleExpression.class);
            Assert.assertSame(tuple.fields[1], tuple.fields[2]);
            var inner = tuple.fields[0].to(DBSPTupleExpression.class);
            Assert.assertSame(inner.fields[0], inner.fields[1]);
            Assert.assertSame(inner.fields[0], inner.fields[2]);
            Assert.assertTrue(types.getFieldType(0).to(PartiallyMonotoneTuple.class).getFieldType(2).getType().mayBeNull);
        }
    }

    @Test
    public void blockWaterlineComesFromItsTail() {
        var compiler = new DBSPCompiler(new CompilerOptions());
        var node = CalciteEmptyRel.INSTANCE;
        var integer = new DBSPTypeInteger(node, 32, true, false);
        var row = new PartiallyMonotoneTuple(List.of(new MonotoneType(integer)), false, false);
        var input = new DBSPVariablePath("row", row.getType().ref());
        var local = new DBSPLetStatement("value", input.deref().field(0));
        var scratch = new DBSPLetStatement("scratch", new DBSPI32Literal(0), true);
        var empty = new DBSPBlockExpression(List.of(new DBSPComment("empty block")), null);
        var ignored = new DBSPAssignmentExpression(scratch.getVarReference(), new DBSPI32Literal(2));
        var arg = new DBSPVariablePath("n", integer);
        var indirect = new DBSPApplyExpression(arg.closure(arg), new DBSPI32Literal(1));
        var body = new DBSPBlockExpression(List.of(
                new DBSPLetStatement("unused", integer, false), local, scratch,
                new DBSPExpressionStatement(empty), new DBSPExpressionStatement(ignored)),
                new DBSPTupleExpression(local.getVarReference(), indirect));
        var operator = new DBSPConstantOperator(node, DBSPZSetExpression.emptyWithElementType(integer));
        var direct = new MonotoneTransferFunctions(compiler, operator, MonotoneTransferFunctions.ArgumentKind.ZSet, row)
                .applyAnalysis(body.closure(input)).getReducedExpression().to(DBSPClosureExpression.class);
        Assert.assertEquals(1, direct.body.to(DBSPTupleExpression.class).size());
        if (Boolean.getBoolean("feldera.optimizer.compareRustLimiterApply")) {
            var bound = new DBSPVariablePath("row", row.getProjectedType().ref());
            var projected = RustCircuitPass.projectRow(compiler, body, bound, row);
            Assert.assertNotNull(projected);
            Assert.assertTrue(direct.equivalent(projected.getReducedExpression().closure(bound)));
            var fields = projected.getMonotoneType().to(PartiallyMonotoneTuple.class);
            Assert.assertSame(row.getFieldType(0), fields.getFieldType(0));
            Assert.assertFalse(fields.getFieldType(1).mayBeMonotone());
        }
    }

    @Test
    public void readingAnUninitializedBlockLocalPreservesTheCompilerError() {
        var compiler = new DBSPCompiler(new CompilerOptions());
        var node = CalciteEmptyRel.INSTANCE;
        var integer = new DBSPTypeInteger(node, 32, true, false);
        var row = new MonotoneType(integer);
        var input = new DBSPVariablePath("input", integer.ref());
        var unused = new DBSPLetStatement("uninitialized", integer, false);
        var read = unused.getVarReference();
        var body = new DBSPBlockExpression(List.of(unused), read);
        var closure = body.closure(input);
        var operator = new DBSPConstantOperator(node, DBSPZSetExpression.emptyWithElementType(integer));
        var analyzer = new MonotoneTransferFunctions(compiler, operator, MonotoneTransferFunctions.ArgumentKind.ZSet, row);
        var direct = Assert.assertThrows(RuntimeException.class, () -> analyzer.applyAnalysis(closure));
        Assert.assertTrue(direct.getMessage(), direct.getMessage().contains(Long.toString(read.getId())));
        if (Boolean.getBoolean("feldera.optimizer.compareRustLimiterApply")) {
            compiler.options.ioOptions.rustOptimizerPasses = true;
            Assert.assertNull(RustCircuitPass.projectRow(compiler, body, input, row));
            var hybrid = new MonotoneTransferFunctions(compiler, operator, MonotoneTransferFunctions.ArgumentKind.ZSet, row);
            var error = Assert.assertThrows(RuntimeException.class, () -> hybrid.applyAnalysis(closure));
            Assert.assertEquals(direct.getClass(), error.getClass());
            Assert.assertEquals(direct.getMessage(), error.getMessage());
        }
    }

    @Test
    public void unknownCoalescedRowsRetainTheirFieldTypes() {
        var compiler = new DBSPCompiler(new CompilerOptions());
        var node = CalciteEmptyRel.INSTANCE;
        var integer = new DBSPTypeInteger(node, 32, true, false);
        var fieldType = new MonotoneType(integer);
        var row = new PartiallyMonotoneTuple(List.of(fieldType), false, false);
        var input = new DBSPVariablePath("row", row.getType().ref());
        var known = new DBSPTupleExpression(input.deref().field(0));
        var fallback = new DBSPTupleExpression(new DBSPI32Literal(0));
        var unknown = new NoExpression(known.getType().withMayBeNull(true));
        var one = new DBSPBinaryExpression(node, known.getType(), DBSPOpcode.COALESCE, unknown, fallback);
        var body = new DBSPTupleExpression(known, one, one.deepCopy().field(0));
        var operator = new DBSPConstantOperator(node, DBSPZSetExpression.emptyWithElementType(integer));
        var direct = new MonotoneTransferFunctions(compiler, operator, MonotoneTransferFunctions.ArgumentKind.ZSet, row)
                .applyAnalysis(body.closure(input));
        var closure = direct.getReducedExpression().to(DBSPClosureExpression.class);
        Assert.assertEquals(1, closure.body.to(DBSPTupleExpression.class).size());
        if (Boolean.getBoolean("feldera.optimizer.compareRustLimiterApply")) {
            var bound = new DBSPVariablePath("row", row.getProjectedType().ref());
            var projected = RustCircuitPass.projectRow(compiler, body, bound, row);
            Assert.assertNotNull(projected);
            Assert.assertTrue(closure.equivalent(projected.getReducedExpression().closure(bound)));
            var fields = projected.getMonotoneType().to(PartiallyMonotoneTuple.class);
            var directFields = Monotonicity.getBodyType(direct).to(PartiallyMonotoneTuple.class);
            for (int i = 0; i < directFields.size(); i++)
                Assert.assertEquals(directFields.getFieldType(i).getClass(), fields.getFieldType(i).getClass());
            Assert.assertFalse(fields.getFieldType(1).to(PartiallyMonotoneTuple.class).getFieldType(0).mayBeMonotone());
            Assert.assertFalse(fields.getFieldType(2).mayBeMonotone());
        }
    }

    @Test
    public void temporalFiltersKeepTheClockBoundInsideAnUnboundedComparison() {
        var compiler = new DBSPCompiler(new CompilerOptions());
        compiler.options.ioOptions.rustOptimizerPasses = Boolean.getBoolean("feldera.optimizer.compareRustLimiterApply");
        var node = CalciteEmptyRel.INSTANCE;
        var timestamp = DBSPTypeTimestamp.create(false);
        var flag = DBSPTypeBool.create(false);
        var row = new PartiallyMonotoneTuple(List.of(new NonMonotoneType(flag), new NonMonotoneType(timestamp),
                new NonMonotoneType(timestamp)), false, false);
        var input = new DBSPVariablePath("row", row.getType().ref());
        var key = new DBSPIfExpression(node, input.deepCopy().deref().field(0), input.deepCopy().deref().field(1),
                input.deepCopy().deref().field(2));
        var now = new DBSPApplyExpression("now", timestamp);
        var predicate = new DBSPBinaryExpression(node, flag, DBSPOpcode.GTE, key, now);
        var function = predicate.closure(input);
        var operator = new DBSPConstantOperator(node, DBSPZSetExpression.emptyWithElementType(timestamp));
        var facts = new MonotoneTransferFunctions(compiler, operator, MonotoneTransferFunctions.ArgumentKind.ZSet, row);
        facts.apply(function);
        Assert.assertTrue(facts.get(now).mayBeMonotone());
        Assert.assertFalse(facts.get(predicate).mayBeMonotone());
        if (compiler.options.ioOptions.rustOptimizerPasses) {
            var terms = RustCircuitPass.inferTemporalPredicates(compiler, function.parameters[0], predicate);
            Assert.assertNotNull(terms);
            Assert.assertEquals(1, terms.size());
            Assert.assertEquals(RustCircuitPass.PredicateKind.GTE, terms.get(0).kind());
            Assert.assertSame(key, terms.get(0).expression());
            Assert.assertSame(now, terms.get(0).clock());
        }
    }

    @Test
    public void conditionalsAndNestedFunctionsPreserveIndependentFieldBounds() {
        var compiler = new DBSPCompiler(new CompilerOptions());
        var node = CalciteEmptyRel.INSTANCE;
        var integer = new DBSPTypeInteger(node, 32, true, false);
        var row = new PartiallyMonotoneTuple(List.of(new MonotoneType(integer)), false, false);
        var input = new DBSPVariablePath("row", row.getType().ref());
        var field = input.deref().field(0);
        var conditional = new DBSPIfExpression(node, new DBSPBoolLiteral(true), field.deepCopy(), new DBSPI32Literal(0));
        var local = new DBSPVariablePath("inner", integer);
        var lambda = new DBSPBinaryExpression(node, integer, DBSPOpcode.ADD, field.deepCopy(), local.deepCopy()).closure(local);
        var body = new DBSPTupleExpression(conditional, lambda, field.deepCopy());
        var operator = new DBSPConstantOperator(node, DBSPZSetExpression.emptyWithElementType(integer));
        var direct = new MonotoneTransferFunctions(compiler, operator, MonotoneTransferFunctions.ArgumentKind.ZSet, row)
                .applyAnalysis(body.closure(input)).getReducedExpression().to(DBSPClosureExpression.class);
        Assert.assertEquals(1, direct.body.to(DBSPTupleExpression.class).size());
        if (Boolean.getBoolean("feldera.optimizer.compareRustLimiterApply")) {
            var bound = new DBSPVariablePath("row", row.getProjectedType().ref());
            var projected = RustCircuitPass.projectRow(compiler, body, bound, row);
            Assert.assertNotNull(projected);
            Assert.assertTrue(direct.equivalent(projected.getReducedExpression().closure(bound)));
            var types = projected.getMonotoneType().to(PartiallyMonotoneTuple.class);
            Assert.assertSame(lambda.getType(), types.getFieldType(1).getType());
        }
    }

    @Test
    public void rowReferencesSurviveLocalBindingsAndReborrowing() {
        var compiler = new DBSPCompiler(new CompilerOptions());
        var node = CalciteEmptyRel.INSTANCE;
        var integer = new DBSPTypeInteger(node, 32, true, false);
        var row = new PartiallyMonotoneTuple(List.of(new NonMonotoneType(integer), new MonotoneType(integer)), false, false);
        var input = new DBSPVariablePath("row", row.getType().ref());
        var alias = new DBSPLetStatement("alias", input.deepCopy());
        var kept = alias.getVarReference().deref().field(1).borrow();
        var lost = alias.getVarReference().deref().field(0).borrow();
        var body = new DBSPBlockExpression(List.of(alias), new DBSPTupleExpression(
                alias.getVarReference(), input.deepCopy(), kept.deref(), lost.deref()));
        var operator = new DBSPConstantOperator(node, DBSPZSetExpression.emptyWithElementType(integer));
        var direct = new MonotoneTransferFunctions(compiler, operator, MonotoneTransferFunctions.ArgumentKind.ZSet, row)
                .applyAnalysis(body.closure(input)).getReducedExpression().to(DBSPClosureExpression.class);
        Assert.assertEquals(3, direct.body.to(DBSPTupleExpression.class).size());
        if (Boolean.getBoolean("feldera.optimizer.compareRustLimiterApply")) {
            var bound = new DBSPVariablePath("row", row.getProjectedType().ref());
            var projected = RustCircuitPass.projectRow(compiler, body, bound, row);
            Assert.assertNotNull(projected);
            Assert.assertTrue(direct.equivalent(projected.getReducedExpression().closure(bound)));
            var fields = projected.getMonotoneType().to(PartiallyMonotoneTuple.class);
            Assert.assertSame(fields.getFieldType(0), fields.getFieldType(1));
            Assert.assertFalse(fields.getFieldType(3).mayBeMonotone());
        }
    }

    @Test
    public void projectedCallsSelectTheirRuntimeWaterlineAndArguments() {
        var compiler = new DBSPCompiler(new CompilerOptions());
        var node = CalciteEmptyRel.INSTANCE;
        var real = DBSPTypeDouble.create(true);
        var row = new PartiallyMonotoneTuple(List.of(new MonotoneType(real)), false, false);
        var parameter = new DBSPVariablePath("row", row.getType().ref());
        var x = parameter.deref().field(0);
        var body = new DBSPTupleExpression(
                new DBSPApplyExpression(node, "log_dN_d", real, x.deepCopy(), new DBSPDoubleLiteral(2.0, false)),
                new DBSPApplyExpression(node, "log_dN_d", real, x.deepCopy(), new DBSPDoubleLiteral(1.0, false)),
                new DBSPApplyExpression(node, "unknown", real, x.deepCopy()));
        var operator = new DBSPConstantOperator(node, DBSPZSetExpression.emptyWithElementType(real));
        var direct = new MonotoneTransferFunctions(compiler, operator, MonotoneTransferFunctions.ArgumentKind.ZSet, row)
                .applyAnalysis(body.closure(parameter)).getReducedExpression().to(DBSPClosureExpression.class);
        Assert.assertEquals(1, direct.body.to(DBSPTupleExpression.class).size());
        if (Boolean.getBoolean("feldera.optimizer.compareRustLimiterApply")) {
            var bound = new DBSPVariablePath("row", row.getProjectedType().ref());
            var projected = RustCircuitPass.projectRow(compiler, body, bound, row);
            Assert.assertNotNull(projected);
            Assert.assertTrue(direct.equivalent(projected.getReducedExpression().closure(bound)));
        }
    }

    @Test
    public void scalarRowArithmeticTracksConstantsAndNullableOperands() {
        var node = CalciteEmptyRel.INSTANCE;
        var compiler = new DBSPCompiler(new CompilerOptions());
        var integer = new DBSPTypeInteger(node, 32, true, true);
        var row = new PartiallyMonotoneTuple(List.of(new MonotoneType(integer), new NonMonotoneType(integer)), false, false);
        var input = new DBSPVariablePath("row", row.getType().ref());
        var x = input.deref().field(0);
        var y = input.deref().field(1);
        var two = new DBSPI32Literal(2, true);
        var negative = new DBSPI32Literal(-2, true);
        var zero = new DBSPI32Literal(0, true);
        var missing = new DBSPI32Literal((Integer)null, true);
        var name = new DBSPVariablePath("two", two.getType());
        var body = new DBSPTupleExpression(
                new DBSPBinaryExpression(node, integer, DBSPOpcode.ADD, x.deepCopy(), two.deepCopy()),
                new DBSPBinaryExpression(node, integer, DBSPOpcode.SUB, x.deepCopy(), two.deepCopy()),
                new DBSPLetExpression(name, two.deepCopy(),
                        new DBSPBinaryExpression(node, integer, DBSPOpcode.SUB, x.deepCopy(), name.deepCopy())),
                new DBSPBinaryExpression(node, integer, DBSPOpcode.MUL, two.deepCopy(), x.deepCopy()),
                new DBSPBinaryExpression(node, integer, DBSPOpcode.MUL, negative, x.deepCopy()),
                new DBSPBinaryExpression(node, integer, DBSPOpcode.DIV, x.deepCopy(), zero),
                new DBSPBinaryExpression(node, integer, DBSPOpcode.MAX, x.deepCopy(), y.deepCopy()),
                new DBSPBinaryExpression(node, integer, DBSPOpcode.MAX_IGNORE_NULLS, x.deepCopy(), y.deepCopy()),
                new DBSPBinaryExpression(node, integer, DBSPOpcode.COALESCE, x.deepCopy(), missing));
        var operator = new DBSPConstantOperator(node, DBSPZSetExpression.emptyWithElementType(integer));
        var direct = new MonotoneTransferFunctions(compiler, operator, MonotoneTransferFunctions.ArgumentKind.ZSet, row)
                .applyAnalysis(body.closure(input)).getReducedExpression().to(DBSPClosureExpression.class);
        Assert.assertEquals(5, direct.body.to(DBSPTupleExpression.class).size());
        if (Boolean.getBoolean("feldera.optimizer.compareRustLimiterApply")) {
            var bound = new DBSPVariablePath("row", row.getProjectedType().ref());
            var projected = RustCircuitPass.projectRow(compiler, body, bound, row);
            Assert.assertNotNull(projected);
            Assert.assertTrue(direct.equivalent(projected.getReducedExpression().closure(bound)));
        }
    }

    @Test
    public void convertScalarWaterlinesInRowClosures() {
        var compiler = new DBSPCompiler(new CompilerOptions());
        var node = CalciteEmptyRel.INSTANCE;
        var integer = new DBSPTypeInteger(node, 32, true, true);
        var wide = new DBSPTypeInteger(node, 64, true, true);
        var narrow = new DBSPTypeInteger(node, 16, true, true);
        var row = new PartiallyMonotoneTuple(List.of(new MonotoneType(integer)), false, false);
        var parameter = new DBSPVariablePath("row", row.getType().ref());
        var field = parameter.deref().field(0);
        var body = new DBSPTupleExpression(
                field.deepCopy().cast(node, wide, DBSPCastExpression.CastType.SqlUnsafe),
                field.deepCopy().cast(node, narrow, DBSPCastExpression.CastType.SqlUnsafe),
                field.deepCopy().cast(node, narrow, DBSPCastExpression.CastType.SqlSafe),
                new DBSPUnaryExpression(node, integer, DBSPOpcode.UNARY_PLUS, field.deepCopy()),
                new DBSPUnsignedWrapExpression(node, field.deepCopy(), true, true),
                new DBSPUnsignedWrapExpression(node, field.deepCopy(), true, false));
        var operator = new DBSPConstantOperator(node, DBSPZSetExpression.emptyWithElementType(integer));
        var direct = new MonotoneTransferFunctions(compiler, operator, MonotoneTransferFunctions.ArgumentKind.ZSet,
                row).applyAnalysis(body.closure(parameter)).getReducedExpression().to(DBSPClosureExpression.class);
        Assert.assertEquals(4, direct.body.to(DBSPTupleExpression.class).size());
        if (Boolean.getBoolean("feldera.optimizer.compareRustLimiterApply")) {
            var bound = new DBSPVariablePath("row", row.getProjectedType().ref());
            var result = RustCircuitPass.projectRow(compiler, body, bound, row);
            Assert.assertNotNull(result);
            Assert.assertTrue(direct.equivalent(result.getReducedExpression().closure(bound)));
            var shape = result.getMonotoneType().to(PartiallyMonotoneTuple.class);
            Assert.assertSame(wide, shape.getFieldType(0).getType());
            Assert.assertFalse(shape.getFieldType(2).mayBeMonotone());
            Assert.assertFalse(shape.getFieldType(5).mayBeMonotone());
        }
    }

    @Test
    public void unboundedFieldsKeepTheirDeclaredTypes() {
        var compiler = new DBSPCompiler(new CompilerOptions());
        var node = CalciteEmptyRel.INSTANCE;
        var integer = new DBSPTypeInteger(node, 32, true, false);
        var row = new PartiallyMonotoneTuple(List.of(new MonotoneType(integer)), false, false);
        var text = new DBSPStringLiteral("label");
        var nested = new DBSPTypeTuple(integer, text.getType());
        var nullable = nested.withMayBeNull(true).to(DBSPTypeTuple.class);
        var parameter = new DBSPVariablePath("row", row.getType().ref());
        var body = new DBSPTupleExpression(new NoExpression(nested).field(1), text,
                DBSPTupleExpression.none(nullable), parameter.deepCopy().deref().field(0));
        var closure = body.closure(parameter);
        var operator = new DBSPConstantOperator(node, DBSPZSetExpression.emptyWithElementType(integer));
        var kind = MonotoneTransferFunctions.ArgumentKind.ZSet;
        var direct = new MonotoneTransferFunctions(compiler, operator, kind, row).applyAnalysis(closure);
        var bound = new DBSPVariablePath("row", row.getProjectedType().ref());
        var expected = new DBSPTupleExpression(bound.deepCopy().deref().field(0)).closure(bound);
        Assert.assertTrue(direct.getReducedExpression().equivalent(expected));
        if (Boolean.getBoolean("feldera.optimizer.compareRustLimiterApply")) {
            var result = RustCircuitPass.projectRow(compiler, body, bound, row);
            Assert.assertNotNull(result);
            Assert.assertTrue(result.getReducedExpression().closure(bound).equivalent(expected));
            var output = result.getMonotoneType().to(PartiallyMonotoneTuple.class);
            Assert.assertSame(text.getType(), output.getFieldType(0).getType());
            Assert.assertSame(text.getType(), output.getFieldType(1).getType());
            Assert.assertTrue(output.getFieldType(2).getType().mayBeNull);
            Assert.assertFalse(output.getFieldType(2).mayBeMonotone());
        }
    }

    @Test
    public void missingEmptyFieldsKeepTheirDeclaredType() {
        var options = new CompilerOptions();
        options.ioOptions.rustOptimizerPasses = Boolean.getBoolean("feldera.optimizer.compareRustLimiterApply");
        var compiler = new DBSPCompiler(options);
        var node = CalciteEmptyRel.INSTANCE;
        var integer = new DBSPTypeInteger(node, 32, true, false);
        var field = new MonotoneType(integer);

        // Only the integer survives input compaction. Both destination fields
        // take their bound from the same declared, non-nullable empty tuple.
        var absent = new PartiallyMonotoneTuple(List.of(), false, true);
        var empty = new PartiallyMonotoneTuple(List.of(), false, false);
        var source = new PartiallyMonotoneTuple(List.of(absent, absent, field), false, false);
        var destination = new PartiallyMonotoneTuple(List.of(empty, empty, field), false, false);
        var projected = source.getProjectedType();
        var initial = projected.defaultValue();
        var left = projected.ref().var();
        var right = projected.ref().var();
        var input = new DBSPConstantOperator(node, DBSPZSetExpression.emptyWithElementType(integer));
        var waterline = new DBSPWaterlineOperator(node, initial.closure(),
                initial.closure(integer.ref().var(), DBSPTypeRawTuple.EMPTY.ref().var()),
                left.deref().closure(left, right), input.outputPort());
        var outputCircuit = new DBSPCircuit(new ProgramMetadata(), true);
        outputCircuit.addOperator(input);
        outputCircuit.addOperator(waterline);
        var pass = new InsertLimiters(compiler, outputCircuit, new Monotonicity.MonotonicityInformation(), Map.of(), Set.of()) {
            @Override
            protected void addOperator(DBSPOperator operator) {
                outputCircuit.addOperator(operator);
            }
        };

        var result = pass.project(waterline.outputPort(), source, destination);
        var body = result.simpleNode().getClosureFunction().body.to(DBSPTupleExpression.class);
        Assert.assertSame(empty.getType(), body.fields[0].getType());
        Assert.assertSame(empty.getType(), body.fields[1].getType());
    }
}
