package org.dbsp.sqlCompiler.compiler.visitors.outer.monotonicity;

import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.circuit.operator.DBSPAggregateZeroOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPBinaryDistinctOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPConstantOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDifferentiateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPWaterlineOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPViewOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPWeighOperator;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.ProgramMetadata;
import org.dbsp.sqlCompiler.compiler.ViewColumnMetadata;
import org.dbsp.sqlCompiler.compiler.ViewMetadata;
import org.dbsp.sqlCompiler.compiler.backend.RustCircuitPass;
import org.dbsp.sqlCompiler.compiler.backend.ToJsonOuterVisitor.OptimizerSnapshots;
import org.dbsp.sqlCompiler.compiler.errors.SourcePosition;
import org.dbsp.sqlCompiler.compiler.errors.SourcePositionRange;
import org.dbsp.sqlCompiler.compiler.errors.UnsupportedException;
import org.dbsp.sqlCompiler.compiler.frontend.ExpressionCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.ProgramIdentifier;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteEmptyRel;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.frontend.parser.SqlCreateView;
import org.dbsp.sqlCompiler.compiler.visitors.monotone.CombinedBounds;
import org.dbsp.sqlCompiler.compiler.visitors.monotone.IMaybeMonotoneType;
import org.dbsp.sqlCompiler.compiler.visitors.monotone.MonotoneRefType;
import org.dbsp.sqlCompiler.compiler.visitors.monotone.MonotoneTransferFunctions;
import org.dbsp.sqlCompiler.compiler.visitors.monotone.MonotoneType;
import org.dbsp.sqlCompiler.compiler.visitors.monotone.NonMonotoneType;
import org.dbsp.sqlCompiler.compiler.visitors.monotone.PartiallyMonotoneTuple;
import org.dbsp.sqlCompiler.ir.aggregate.DBSPFold;
import org.dbsp.sqlCompiler.ir.aggregate.DBSPMinMax;
import org.dbsp.sqlCompiler.ir.expression.DBSPApplyExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPAssignmentExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPBinaryExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPBorrowExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPDerefExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPFieldExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPBlockExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPCastExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPCloneExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPUnwrapExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPFailExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPIfExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPLetExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPOpcode;
import org.dbsp.sqlCompiler.ir.expression.DBSPRawTupleExpression;
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
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeCode;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeFunction;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeRawTuple;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeStruct;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeBool;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeDouble;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeInteger;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeTimestamp;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeUSize;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeIndexedZSet;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeUser;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class WaterlineProjectionTests {
    @Test
    public void combinesDeepWaterlinesForUnionsAndJoins() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustLimiterApply"));
        var compiler = new DBSPCompiler(new CompilerOptions());
        var type = new DBSPI32Literal(1).getType();
        IMaybeMonotoneType bounded = new MonotoneType(type);
        IMaybeMonotoneType unbounded = new NonMonotoneType(type);
        IMaybeMonotoneType left = bounded;
        IMaybeMonotoneType right = unbounded;
        for (int i = 0; i < 144; i++) {
            left = new PartiallyMonotoneTuple(List.of(unbounded, left), false, false);
            right = new PartiallyMonotoneTuple(List.of(bounded, right), false, false);
        }
        var leftPair = new PartiallyMonotoneTuple(List.of(left, right), true, false);
        var rightPair = new PartiallyMonotoneTuple(List.of(right, left), true, false);
        for (boolean hybrid : List.of(false, true)) {
            compiler.options.ioOptions.rustOptimizerPasses = hybrid;
            var union = CombinedBounds.union(compiler, right, left);
            var intersection = CombinedBounds.intersection(compiler, List.of(left, left, right));
            var join = CombinedBounds.joinArguments(compiler, leftPair, rightPair);
            var anti = CombinedBounds.commonKey(compiler, leftPair, rightPair);
            Assert.assertSame(right, join[1]);
            Assert.assertSame(left, join[2]);
            Assert.assertFalse(anti.getFieldType(1).mayBeMonotone());
            IMaybeMonotoneType[] keys = {union, intersection, join[0], anti.getFieldType(0)};
            for (int depth = 0; depth < 144; depth++) {
                for (int i = 0; i < keys.length; i++) {
                    var tuple = keys[i].to(PartiallyMonotoneTuple.class);
                    Assert.assertSame(i % 2 == 0 ? bounded : unbounded, tuple.getFieldType(0));
                    keys[i] = tuple.getFieldType(1);
                }
            }
            Assert.assertArrayEquals(new IMaybeMonotoneType[]{bounded, unbounded, bounded, unbounded}, keys);
        }
    }

    @Test
    public void deepAggregatePayloadsRetainOnlyTheComparedLeaf() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustLimiterApply"));
        var compiler = new DBSPCompiler(new CompilerOptions());
        var site = CalciteEmptyRel.INSTANCE;
        var integer = new DBSPTypeInteger(site, 32, true, false);
        DBSPType value = integer;
        for (int i = 0; i < 144; i++)
            value = new DBSPTypeTuple(value, integer.withMayBeNull(true));
        var input = new DBSPTypeRawTuple(integer, value);
        var grouped = RustCircuitPass.aggregateTransfer(compiler, input, new DBSPTypeIndexedZSet(input), false);
        var groupedValue = grouped.body.to(DBSPRawTupleExpression.class).get(1);
        Assert.assertTrue(groupedValue.equivalent(Monotonicity.makeNoExpression(value)));
        Assert.assertTrue(groupedValue.getType().sameType(value));

        var minimum = RustCircuitPass.minimumTransfer(compiler, site, input, null);
        var compared = minimum.body.to(DBSPRawTupleExpression.class).get(1);
        DBSPExpression expected = minimum.parameters[0].asVariable().field(1).deref();
        for (int i = 0; i < 144; i++) {
            var tuple = compared.to(DBSPTupleExpression.class);
            Assert.assertTrue(tuple.get(1).is(NoExpression.class));
            Assert.assertTrue(tuple.get(1).getType().mayBeNull);
            compared = tuple.get(0);
            expected = expected.field(0);
        }
        Assert.assertTrue(compared.closure(minimum.parameters).equivalent(expected.closure(minimum.parameters)));
    }

    @Test
    public void mixedCircuitRebuildsOutputTypesWhenLimitersReplaceInputs() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustLimiterApply"));
        for (boolean hybrid : List.of(false, true)) {
            var compiler = new DBSPCompiler(new CompilerOptions());
            compiler.options.ioOptions.rustOptimizerPasses = hybrid;
            var site = CalciteEmptyRel.INSTANCE;
            var circuit = new DBSPCircuit(new ProgramMetadata(), false);
            var name = DBSPCompiler.ERROR_VIEW_NAME;
            var col = new ProgramIdentifier("k");
            var integer = new DBSPI32Literal(1).getType();
            var schema = new DBSPTypeStruct(site, name, List.of(new DBSPTypeStruct.Field(site, col, 0, integer)), false);
            var rowType = schema.toTuple();
            var source = new DBSPConstantOperator(site, DBSPZSetExpression.emptyWithElementType(rowType));
            circuit.addOperator(source);
            var metadata = new ViewMetadata(name, List.of(new ViewColumnMetadata(site, name, col, integer, null)),
                    SqlCreateView.ViewKind.STANDARD, -1, false, false, null);
            var view = new DBSPViewOperator(site, name, "", schema, metadata, source.outputPort());
            circuit.addOperator(view);
            var row = rowType.ref().var();
            var indexed = new DBSPMapIndexOperator(site,
                    new DBSPRawTupleExpression(row.deref().field(0), row.deref()).closure(row), view.outputPort());
            circuit.addOperator(indexed);
            var weigh = new DBSPWeighOperator(site, new DBSPISizeLiteral(1).closure(
                    indexed.getOutputIndexedZSetType().keyType.ref().var(), rowType.ref().var()), indexed.outputPort());
            circuit.addOperator(weigh);
            var zero = new DBSPAggregateZeroOperator(site, rowType.defaultValue(), weigh.outputPort());
            circuit.addOperator(zero);
            var delta = new DBSPDifferentiateOperator(site, zero.outputPort());
            circuit.addOperator(delta);
            var distinct = new DBSPBinaryDistinctOperator(site, source.outputPort(), delta.outputPort(), false);
            circuit.addOperator(distinct);
            var limiters = new InsertLimiters(compiler, circuit, new Monotonicity.MonotonicityInformation(),
                    Map.of(), Set.of());
            var result = hybrid ? new RustCircuitPass(compiler, limiters, new OptimizerSnapshots(compiler)).apply(circuit)
                    : limiters.apply(circuit);
            var rebuiltDistinct = result.allOperators.getLast().to(DBSPBinaryDistinctOperator.class);
            var rebuiltDelta = rebuiltDistinct.right().node().to(DBSPDifferentiateOperator.class);
            var rebuiltZero = rebuiltDelta.input().node().to(DBSPAggregateZeroOperator.class);
            var rebuilt = rebuiltZero.input().node().to(DBSPWeighOperator.class);
            Assert.assertNotSame(zero.getType(), rebuiltZero.getType());
            Assert.assertSame(rebuiltZero.getFunction().getType(), rebuiltZero.getOutputZSetElementType());
            Assert.assertSame(rebuiltZero.getType(), rebuiltDelta.getType());
            Assert.assertNotSame(rebuiltDistinct.left().outputType(), rebuiltDistinct.getType());
            Assert.assertSame(rebuiltDelta.getType(), rebuiltDistinct.getType());
            Assert.assertNotSame(indexed, rebuilt.input().node());
            Assert.assertNotSame(weigh.getType(), rebuilt.getType());
            Assert.assertSame(rebuilt.input().getOutputIndexedZSetType().elementType, rebuilt.getOutputZSetElementType());
        }
    }

    @Test
    public void wholeClosureProjectionPreservesBindingAndOccurrenceDiagnostics() {
        var compiler = new DBSPCompiler(new CompilerOptions());
        var site = CalciteEmptyRel.INSTANCE;
        var integer = new DBSPTypeInteger(site, 32, true, false);
        var row = new PartiallyMonotoneTuple(List.of(new MonotoneType(integer)), false, false);
        var input = new DBSPVariablePath("row", row.getType().ref());
        var inner = new DBSPVariablePath("inner", integer);
        var value = input.deref().field(0);
        var missing = new DBSPVariablePath("missing", integer);
        var operator = new DBSPConstantOperator(site, DBSPZSetExpression.emptyWithElementType(integer));
        for (var body : List.of(new DBSPTupleExpression(value, value),
                new DBSPTupleExpression(value, missing.closure(inner)),
                // Resolution must finish before the visitor reports a repeated node.
                new DBSPTupleExpression(value, value, missing.closure(inner)),
                new DBSPTupleExpression(new DBSPVariablePath("row", integer)))) {
            var closure = body.closure(input);
            RuntimeException direct = null;
            for (boolean hybrid : new boolean[]{false, true}) {
                if (hybrid && !Boolean.getBoolean("feldera.optimizer.compareRustLimiterApply"))
                    break;
                compiler.options.ioOptions.rustOptimizerPasses = hybrid;
                // The type diagnostic includes the caller's stack. Exercise both modes at the same site.
                var failure = Assert.assertThrows(RuntimeException.class, () ->
                        new MonotoneTransferFunctions(compiler, operator, MonotoneTransferFunctions.ArgumentKind.ZSet,
                                row).applyAnalysis(closure));
                if (direct == null) {
                    direct = failure;
                } else {
                    Assert.assertEquals(direct.getClass(), failure.getClass());
                    // The projected parameter is fresh on each invocation.
                    Assert.assertEquals(direct.getMessage().replaceAll("\\(\\d+\\) with type", "(id) with type"),
                            failure.getMessage().replaceAll("\\(\\d+\\) with type", "(id) with type"));
                }
            }
        }
    }

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
            assertGeneralWaterlineClosure(operator, MonotoneTransferFunctions.ArgumentKind.ZSet, closure, direct, row);
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
            assertGeneralWaterlineClosure(operator, MonotoneTransferFunctions.ArgumentKind.IndexedZSet, closure,
                    direct.getReducedExpression(), indexed);
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
            assertGeneralWaterlineClosure(operator, kind, closure, direct.getReducedExpression(), inputs);
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

    private static void assertGeneralWaterlineClosure(DBSPSimpleOperator operator, MonotoneTransferFunctions.ArgumentKind kind,
            DBSPClosureExpression closure, DBSPExpression expected, IMaybeMonotoneType... inputs) {
        var compiler = new DBSPCompiler(new CompilerOptions());
        compiler.options.ioOptions.rustOptimizerPasses = true;
        var analysis = new MonotoneTransferFunctions(compiler, operator, kind, inputs);
        analysis.apply(closure);
        var reduced = analysis.get(closure).getReducedExpression().to(DBSPClosureExpression.class);
        Assert.assertTrue(expected.equivalent(reduced));
        var body = analysis.get(closure.body).getReducedExpression();
        if (kind == MonotoneTransferFunctions.ArgumentKind.ZSet)
            Assert.assertSame(body, reduced.body);
        else
            Assert.assertSame(body, reduced.body.to(DBSPBlockExpression.class).lastExpression);
        var signature = reduced.getType().to(DBSPTypeFunction.class);
        Assert.assertSame(body.getType(), signature.resultType);
        Assert.assertSame(reduced.parameters[0].getType(), signature.parameterTypes[0]);
    }

    @Test
    public void waterlineTreeReportsTheFirstRepeatedSubexpressionAndItsParents() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustLimiterApply"));
        var compiler = new DBSPCompiler(new CompilerOptions());
        var site = CalciteEmptyRel.INSTANCE;
        var integer = new DBSPTypeInteger(site, 32, true, false);
        var parameter = new DBSPVariablePath("row", integer.ref());
        var read = parameter.deref();
        var shared = new DBSPTupleExpression(read);
        // The first duplicate in visitor order is the whole row inside the
        // second tuple. A later read also shares a node with the first tuple.
        var nested = new DBSPTupleExpression(shared, read);
        var body = new DBSPTupleExpression(shared, nested);
        var closure = body.closure(parameter);
        var operator = new DBSPConstantOperator(site, DBSPZSetExpression.emptyWithElementType(integer));
        var kind = MonotoneTransferFunctions.ArgumentKind.ZSet;
        var input = new MonotoneType(integer);
        var error = Assert.assertThrows(RuntimeException.class,
                () -> new MonotoneTransferFunctions(compiler, operator, kind, input).apply(closure));
        Assert.assertTrue(error.getMessage(), error.getMessage().contains(
                "contains multiple instances of " + shared + " " + shared.id + " context:\n" + nested.id));
        compiler.options.ioOptions.rustOptimizerPasses = true;
        var directVisit = Assert.assertThrows(RuntimeException.class,
                () -> new MonotoneTransferFunctions(compiler, operator, kind, input).apply(closure));
        var closureVisit = Assert.assertThrows(RuntimeException.class,
                () -> new MonotoneTransferFunctions(compiler, operator, kind, input).applyAnalysis(closure));
        Assert.assertEquals(error.getMessage(), directVisit.getMessage());
        Assert.assertEquals(error.getMessage(), closureVisit.getMessage());
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
    public void projectAClosureOverDeepInputRows() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustLimiterApply"));
        var compiler = new DBSPCompiler(new CompilerOptions());
        var integer = new DBSPI32Literal(1).getType();
        var kept = new MonotoneType(integer);
        var dropped = new NonMonotoneType(integer);
        IMaybeMonotoneType nested = kept;
        for (int i = 0; i < 144; i++)
            nested = new PartiallyMonotoneTuple(List.of(dropped, nested), false, false);
        var input = new DBSPVariablePath("row", nested.getType().ref());
        DBSPExpression body = input.deref();
        for (int i = 0; i < 144; i++)
            body = body.field(1);
        var closure = body.closure(input);
        var operator = new DBSPConstantOperator(CalciteEmptyRel.INSTANCE, DBSPZSetExpression.emptyWithElementType(integer));
        var kind = MonotoneTransferFunctions.ArgumentKind.ZSet;
        var direct = new MonotoneTransferFunctions(compiler, operator, kind, nested).applyAnalysis(closure);
        var projected = (RustCircuitPass.ProjectedWaterline) RustCircuitPass.projectWaterlineClosure(compiler, body, closure.parameters,
                new DBSPType[]{nested.getProjectedType()}, new IMaybeMonotoneType[]{new MonotoneRefType(nested)}, false);
        Assert.assertNotNull(projected);
        Assert.assertSame(kept, projected.bodyType());
        Assert.assertTrue(direct.getReducedExpression().equivalent(projected.closure()));
        compiler.options.ioOptions.rustOptimizerPasses = true;
        var hybrid = new MonotoneTransferFunctions(compiler, operator, kind, nested).applyAnalysis(closure);
        Assert.assertTrue(direct.getReducedExpression().equivalent(hybrid.getReducedExpression()));
    }

    @Test
    public void deepArithmeticRetainsItsWaterlineThroughTheLiveCompiler() {
        var compiler = new DBSPCompiler(new CompilerOptions());
        var site = CalciteEmptyRel.INSTANCE;
        var integer = new DBSPTypeInteger(site, 32, true, false);
        var field = new MonotoneType(integer);
        var input = new DBSPVariablePath("row", integer.ref());
        DBSPExpression body = input.deref();
        for (int i = 1; i <= 180; i++) {
            var literal = new DBSPI32Literal(i);
            body = i % 2 == 0
                    ? new DBSPBinaryExpression(site, integer, DBSPOpcode.SUB, body, literal)
                    : new DBSPBinaryExpression(site, integer, DBSPOpcode.ADD, literal, body);
            body = new DBSPUnaryExpression(site, integer, DBSPOpcode.UNARY_PLUS, body);
        }
        var closure = body.closure(input);
        var operator = new DBSPConstantOperator(site, DBSPZSetExpression.emptyWithElementType(integer));
        var kind = MonotoneTransferFunctions.ArgumentKind.ZSet;
        var direct = new MonotoneTransferFunctions(compiler, operator, kind, field)
                .applyAnalysis(closure).getReducedExpression();
        if (Boolean.getBoolean("feldera.optimizer.compareRustLimiterApply")) {
            var projected = (RustCircuitPass.ProjectedWaterline) RustCircuitPass.projectWaterlineClosure(compiler, body, closure.parameters,
                    new DBSPType[]{integer}, new IMaybeMonotoneType[]{new MonotoneRefType(field)}, false);
            Assert.assertNotNull(projected);
            Assert.assertSame(integer, projected.bodyType().getType());
            Assert.assertTrue(direct.equivalent(projected.closure()));
            compiler.options.ioOptions.rustOptimizerPasses = true;
            var hybrid = new MonotoneTransferFunctions(compiler, operator, kind, field).applyAnalysis(closure);
            Assert.assertTrue(direct.equivalent(hybrid.getReducedExpression()));
        }
    }

    @Test
    public void deepSqlProjectionCompilesThroughTheHybridOptimizer() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustLimiterApply"));
        var expression = new StringBuilder("v");
        for (int i = 1; i <= 160; i++)
            expression.append(i % 2 == 0 ? " - " : " + ").append(i);
        String sql = "CREATE TABLE t(v BIGINT NOT NULL LATENESS 10);"
                + "CREATE VIEW result AS SELECT " + expression + " AS x FROM t;";
        var direct = compileIncrementally(sql, false);
        var hybrid = compileIncrementally(sql, true);
        Assert.assertEquals(direct.getOutputViews(), hybrid.getOutputViews());
        Assert.assertTrue(direct.getSingleOutputType().sameType(hybrid.getSingleOutputType()));
    }

    private static DBSPCircuit compileIncrementally(String sql, boolean hybrid) {
        var options = new CompilerOptions();
        options.languageOptions.incrementalize = true;
        options.ioOptions.rustOptimizerPasses = hybrid;
        var compiler = new DBSPCompiler(options);
        compiler.submitStatementsForCompilation(sql);
        var circuit = compiler.getFinalCircuit(false);
        compiler.throwIfErrorsOccurred();
        Assert.assertNotNull(circuit);
        return circuit;
    }

    @Test
    public void nestedFunctionArgumentsBindEachInitializerInItsOuterScope() {
        var compiler = new DBSPCompiler(new CompilerOptions());
        var site = CalciteEmptyRel.INSTANCE;
        var integer = new DBSPTypeInteger(site, 32, true, false);
        var field = new MonotoneType(integer);
        var input = new DBSPVariablePath("row", integer.ref());
        DBSPExpression body = input.deref();
        for (int i = 0; i < 180; i++) {
            var alias = new DBSPLetStatement("v", body);
            var sign = new DBSPApplyExpression(site, "sign_i32", integer, alias.getVarReference());
            body = new DBSPBlockExpression(List.of(alias), new DBSPTupleExpression(sign).field(0));
        }
        var closure = body.closure(input);
        var operator = new DBSPConstantOperator(site, DBSPZSetExpression.emptyWithElementType(integer));
        var kind = MonotoneTransferFunctions.ArgumentKind.ZSet;
        var direct = new MonotoneTransferFunctions(compiler, operator, kind, field)
                .applyAnalysis(closure).getReducedExpression();
        if (Boolean.getBoolean("feldera.optimizer.compareRustLimiterApply")) {
            var projected = RustCircuitPass.projectRow(compiler, body, input, field);
            Assert.assertNotNull(projected);
            Assert.assertTrue(direct.equivalent(projected.getReducedExpression().closure(input)));
        }
    }

    @Test
    public void deepBindingsRetainTheWaterlineOfTheOuterInput() {
        var compiler = new DBSPCompiler(new CompilerOptions());
        var node = CalciteEmptyRel.INSTANCE;
        var integer = new DBSPTypeInteger(node, 32, true, false);
        var row = new MonotoneType(integer);
        var input = new DBSPVariablePath("row", integer.ref());
        int count = 180;
        DBSPExpression body = new DBSPVariablePath("v" + (count - 1), integer);
        for (int i = count - 1; i >= 0; i--) {
            var name = "v" + i;
            var init = i == 0 ? input.deref() : new DBSPVariablePath("v" + (i - 1), integer);
            body = i % 2 == 0
                    ? new DBSPLetExpression(new DBSPVariablePath(name, integer), init, body)
                    : new DBSPBlockExpression(List.of(new DBSPLetStatement(name, init)), body);
        }
        var operator = new DBSPConstantOperator(node, DBSPZSetExpression.emptyWithElementType(integer));
        var kind = MonotoneTransferFunctions.ArgumentKind.ZSet;
        var closure = body.closure(input);
        var direct = new MonotoneTransferFunctions(compiler, operator, kind, row)
                .applyAnalysis(closure).getReducedExpression();
        if (Boolean.getBoolean("feldera.optimizer.compareRustLimiterApply")) {
            var projected = RustCircuitPass.projectRow(compiler, body, input, row);
            Assert.assertNotNull(projected);
            Assert.assertSame(row, projected.getMonotoneType());
            Assert.assertTrue(direct.equivalent(projected.getReducedExpression().closure(input)));
            compiler.options.ioOptions.rustOptimizerPasses = true;
            var hybrid = new MonotoneTransferFunctions(compiler, operator, kind, row).applyAnalysis(closure);
            Assert.assertTrue(direct.equivalent(hybrid.getReducedExpression()));
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
    public void countIndicatorDropsOnlyItsOwnWaterline() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustLimiterApply"));
        var compiler = new DBSPCompiler(new CompilerOptions());
        var site = CalciteEmptyRel.INSTANCE;
        var integer = new DBSPI32Literal(0).getType();
        var kept = new MonotoneType(integer);
        var nullable = integer.withMayBeNull(true);
        for (var indicatorInput : List.of(new MonotoneType(nullable), new NonMonotoneType(nullable))) {
            var shape = new PartiallyMonotoneTuple(List.of(kept, indicatorInput), false, false);
            var row = new DBSPVariablePath("row", shape.getType().ref());
            var indicator = ExpressionCompiler.makeIndicator(site, integer, row.deref().field(1));
            Assert.assertEquals(DBSPOpcode.INDICATOR, indicator.to(DBSPUnaryExpression.class).opcode);
            var body = new DBSPTupleExpression(indicator, row.deepCopy().deref().field(0));
            var closure = body.closure(row);
            var operator = new DBSPConstantOperator(site, DBSPZSetExpression.emptyWithElementType(integer));
            var direct = new MonotoneTransferFunctions(compiler, operator,
                    MonotoneTransferFunctions.ArgumentKind.ZSet, shape).applyAnalysis(closure);
            var result = (RustCircuitPass.ProjectedWaterline) RustCircuitPass.projectWaterlineClosure(compiler, body, closure.parameters,
                    new DBSPType[]{shape.getProjectedType()}, new IMaybeMonotoneType[]{new MonotoneRefType(shape)}, false);
            Assert.assertNotNull(result);
            var fields = result.bodyType().to(PartiallyMonotoneTuple.class);
            Assert.assertFalse(fields.getFieldType(0).mayBeMonotone());
            Assert.assertSame(kept, fields.getFieldType(1));
            Assert.assertTrue(direct.getReducedExpression().equivalent(result.closure()));
        }
    }

    @Test
    public void nullableEmptyTupleDropsItsWaterlineAndKeepsTheSiblingRead() {
        var compiler = new DBSPCompiler(new CompilerOptions());
        var site = CalciteEmptyRel.INSTANCE;
        var integer = new DBSPI32Literal(0).getType();
        var kept = new MonotoneType(integer);
        var unit = new PartiallyMonotoneTuple(List.of(), false, false);
        var input = new PartiallyMonotoneTuple(List.of(unit, kept), false, false);
        var row = new DBSPVariablePath("row", input.getType().ref());
        var body = new DBSPTupleExpression(row.deref().field(0).some(), row.deepCopy().deref().field(1));
        var closure = body.closure(row);
        var operator = new DBSPConstantOperator(site, DBSPZSetExpression.emptyWithElementType(integer));
        var direct = new MonotoneTransferFunctions(compiler, operator, MonotoneTransferFunctions.ArgumentKind.ZSet,
                input).applyAnalysis(closure);
        var expected = Monotonicity.getBodyType(direct).to(PartiallyMonotoneTuple.class);
        Assert.assertFalse(expected.getFieldType(0).mayBeMonotone());
        Assert.assertSame(kept, expected.getFieldType(1));
        if (Boolean.getBoolean("feldera.optimizer.compareRustLimiterApply")) {
            var projected = (RustCircuitPass.ProjectedWaterline) RustCircuitPass.projectWaterlineClosure(compiler, body, closure.parameters,
                    new DBSPType[]{input.getProjectedType()}, new IMaybeMonotoneType[]{new MonotoneRefType(input)}, false);
            Assert.assertNotNull(projected);
            var result = projected.bodyType().to(PartiallyMonotoneTuple.class);
            Assert.assertFalse(result.getFieldType(0).mayBeMonotone());
            Assert.assertTrue(result.getFieldType(0).getType().sameType(expected.getFieldType(0).getType()));
            Assert.assertSame(kept, result.getFieldType(1));
            Assert.assertTrue(direct.getReducedExpression().equivalent(projected.closure()));
        }
    }

    @Test
    public void castingAPartiallyBoundRowKeepsOnlyTheSiblingWaterline() {
        var compiler = new DBSPCompiler(new CompilerOptions());
        var site = CalciteEmptyRel.INSTANCE;
        var integer = new DBSPTypeInteger(site, 32, true, false);
        var inputShape = new PartiallyMonotoneTuple(
                List.of(new MonotoneType(integer), new NonMonotoneType(integer)), false, false);
        var row = new DBSPVariablePath("row", inputShape.getType().ref());
        var target = inputShape.getType().withMayBeNull(true);
        var cast = row.deref().cast(site, target, DBSPCastExpression.CastType.SqlUnsafe);
        var body = new DBSPTupleExpression(cast, row.deepCopy().deref().field(0));
        var closure = body.closure(row);
        var op = new DBSPConstantOperator(site, DBSPZSetExpression.emptyWithElementType(integer));
        var direct = new MonotoneTransferFunctions(compiler, op, MonotoneTransferFunctions.ArgumentKind.ZSet, inputShape)
                .applyAnalysis(closure);
        var fields = Monotonicity.getBodyType(direct).to(PartiallyMonotoneTuple.class);
        Assert.assertFalse(fields.getFieldType(0).mayBeMonotone());
        Assert.assertTrue(fields.getFieldType(1).mayBeMonotone());
        if (Boolean.getBoolean("feldera.optimizer.compareRustLimiterApply")) {
            var projected = (RustCircuitPass.ProjectedWaterline) RustCircuitPass.projectWaterlineClosure(compiler, body, closure.parameters,
                    new DBSPType[]{inputShape.getProjectedType()},
                    new IMaybeMonotoneType[]{new MonotoneRefType(inputShape)}, false);
            Assert.assertNotNull(projected);
            var result = projected.bodyType().to(PartiallyMonotoneTuple.class);
            Assert.assertFalse(result.getFieldType(0).mayBeMonotone());
            Assert.assertTrue(result.getFieldType(0).getType().sameType(target));
            Assert.assertSame(inputShape.getFieldType(0), result.getFieldType(1));
            Assert.assertTrue(direct.getReducedExpression().equivalent(projected.closure()));
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
    public void temporalComparisonsProjectEmptyRowsInAnUnorderedInput() {
        var compiler = new DBSPCompiler(new CompilerOptions());
        var site = CalciteEmptyRel.INSTANCE;
        var timestamp = DBSPTypeTimestamp.create(false);
        var unit = new DBSPTypeTuple();
        var nested = new DBSPTypeTuple(unit.withMayBeNull(true), unit);
        var row = new DBSPTypeTuple(timestamp, nested);
        var input = new DBSPVariablePath("row", row.ref());
        var key = input.deepCopy().deref().field(0);
        var kept = input.deepCopy().deref().field(1).field(1);
        var clock = new DBSPBlockExpression(List.of(new DBSPLetStatement("unit", kept)),
                new DBSPApplyExpression("now", timestamp));
        var predicate = new DBSPBinaryExpression(site, DBSPTypeBool.create(false), DBSPOpcode.GTE, key, clock);
        var function = predicate.closure(input);
        var operator = new DBSPConstantOperator(site, DBSPZSetExpression.emptyWithElementType(timestamp));
        var direct = new MonotoneTransferFunctions(compiler, operator, MonotoneTransferFunctions.ArgumentKind.ZSet,
                PartiallyMonotoneTuple.noMonotoneFields(row));
        direct.apply(function);
        Assert.assertTrue(direct.get(kept).mayBeMonotone());
        Assert.assertTrue(direct.get(clock).mayBeMonotone());
        if (Boolean.getBoolean("feldera.optimizer.compareRustLimiterApply")) {
            var terms = RustCircuitPass.inferTemporalPredicates(compiler, function.parameters[0], predicate);
            Assert.assertNotNull(terms);
            Assert.assertEquals(1, terms.size());
            Assert.assertEquals(RustCircuitPass.PredicateKind.GTE, terms.get(0).kind());
            Assert.assertSame(key, terms.get(0).expression());
            Assert.assertSame(clock, terms.get(0).clock());
        }
    }

    @Test
    public void tupleWaterlineKeepsLiveFieldsAndClockIdentity() {
        var span = new SourcePositionRange(new SourcePosition(2, 3), new SourcePosition(2, 8));
        var site = CalciteObject.create(span);
        var time = DBSPTypeTimestamp.create(false);
        var row = new PartiallyMonotoneTuple(List.of(new NonMonotoneType(time)), false, false);
        var input = new DBSPVariablePath("row", row.getType().ref());
        var clock = new DBSPApplyExpression(site, "now", time);
        var unit = new DBSPRawTupleExpression();
        var dropped = input.deref().field(0);
        var tuple = new DBSPTupleExpression(site, List.of(unit, dropped, clock));
        var operator = new DBSPConstantOperator(CalciteEmptyRel.INSTANCE, DBSPZSetExpression.emptyWithElementType(time));
        for (boolean nativePass : List.of(false, true)) {
            if (nativePass && !Boolean.getBoolean("feldera.optimizer.compareRustLimiterApply"))
                continue;
            var compiler = new DBSPCompiler(new CompilerOptions());
            compiler.options.ioOptions.rustOptimizerPasses = nativePass;
            var analysis = new MonotoneTransferFunctions(compiler, operator, MonotoneTransferFunctions.ArgumentKind.ZSet, row);
            analysis.apply(tuple.closure(input));
            var reduced = analysis.get(tuple).getReducedExpression().to(DBSPTupleExpression.class);
            Assert.assertEquals(2, reduced.size());
            Assert.assertEquals(span, reduced.getNode().getPositionRange());
            Assert.assertSame(analysis.get(unit).getReducedExpression(), reduced.get(0));
            Assert.assertEquals(0, reduced.get(0).to(DBSPRawTupleExpression.class).size());
            Assert.assertSame(analysis.get(clock).getReducedExpression(), reduced.get(1));
            var reducedClock = reduced.get(1).to(DBSPApplyExpression.class);
            Assert.assertNotSame(clock, reducedClock);
            Assert.assertSame(clock.function, reducedClock.function);
            Assert.assertSame(time, reducedClock.getType());
            Assert.assertFalse(reducedClock.getNode().getPositionRange().isValid());
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
    public void scalarCallWaterlineReusesTheBoundAndCopiesTheConstant() {
        var site = CalciteEmptyRel.INSTANCE;
        var real = DBSPTypeDouble.create(true);
        var row = new PartiallyMonotoneTuple(List.of(new MonotoneType(real)), false, false);
        var parameter = new DBSPVariablePath("row", row.getType().ref());
        var field = parameter.deref().field(0);
        var precision = new DBSPApplyExpression("precision", new DBSPI32Literal(0).getType());
        var call = new DBSPApplyExpression(site, "round_dN_i32", real, field, precision);
        var closure = call.closure(parameter);
        var operator = new DBSPConstantOperator(site, DBSPZSetExpression.emptyWithElementType(real));
        for (boolean nativePass : List.of(false, true)) {
            if (nativePass && !Boolean.getBoolean("feldera.optimizer.compareRustLimiterApply"))
                continue;
            var compiler = new DBSPCompiler(new CompilerOptions());
            compiler.options.ioOptions.rustOptimizerPasses = nativePass;
            var analysis = new MonotoneTransferFunctions(compiler, operator, MonotoneTransferFunctions.ArgumentKind.ZSet, row);
            analysis.apply(closure);
            var reduced = analysis.get(call).getReducedExpression().to(DBSPApplyExpression.class);
            Assert.assertEquals("round_d_i32", reduced.getFunctionName());
            Assert.assertFalse(reduced.getType().mayBeNull);
            Assert.assertSame(analysis.get(field).getReducedExpression(), reduced.arguments[0]);
            Assert.assertNotSame(precision, reduced.arguments[1]);
            Assert.assertTrue(precision.equivalent(reduced.arguments[1]));
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
    public void literalWaterlinesDistinguishNullsFromPositiveConstants() {
        var site = CalciteEmptyRel.INSTANCE;
        var real = DBSPTypeDouble.create(true);
        var row = new PartiallyMonotoneTuple(List.of(new MonotoneType(real)), false, false);
        var input = new DBSPVariablePath("row", row.getType().ref());
        var factor = new DBSPDoubleLiteral(1.25, true);
        var nan = new DBSPDoubleLiteral(Double.NaN, true);
        var absent = new DBSPDoubleLiteral(null, true);
        var text = new DBSPStringLiteral("literal");
        var scale = new DBSPBinaryExpression(site, real, DBSPOpcode.MUL, input.deepCopy().deref().field(0), factor);
        var invalidScale = new DBSPBinaryExpression(site, real, DBSPOpcode.MUL, input.deepCopy().deref().field(0), nan);
        var difference = new DBSPBinaryExpression(site, real, DBSPOpcode.SUB, input.deepCopy().deref().field(0), absent);
        var closure = new DBSPTupleExpression(scale, invalidScale, difference, text).closure(input);
        var operator = new DBSPConstantOperator(site, DBSPZSetExpression.emptyWithElementType(real));
        for (boolean nativePass : List.of(false, true)) {
            if (nativePass && !Boolean.getBoolean("feldera.optimizer.compareRustLimiterApply"))
                continue;
            var compiler = new DBSPCompiler(new CompilerOptions());
            compiler.options.ioOptions.rustOptimizerPasses = nativePass;
            var analysis = new MonotoneTransferFunctions(compiler, operator, MonotoneTransferFunctions.ArgumentKind.ZSet, row);
            analysis.apply(closure);
            Assert.assertTrue(analysis.get(scale).mayBeMonotone());
            Assert.assertFalse(analysis.get(invalidScale).mayBeMonotone());
            Assert.assertFalse(analysis.get(difference).mayBeMonotone());
            Assert.assertFalse(analysis.get(text).mayBeMonotone());
            var maximum = analysis.get(absent).getReducedExpression().to(DBSPDoubleLiteral.class);
            Assert.assertFalse(maximum.getType().mayBeNull);
            Assert.assertTrue(Double.isNaN(maximum.value));
            var target = new DBSPUSizeLiteral(null, true);
            var unsupported = new MonotoneTransferFunctions(compiler, operator, MonotoneTransferFunctions.ArgumentKind.ZSet, row);
            var error = Assert.assertThrows(UnsupportedException.class, () -> unsupported.apply(target.closure(input)));
            Assert.assertSame(target.getType().withMayBeNull(false).getNode(), error.calciteObject);
        }
    }

    @Test
    public void generalValueWrappersKeepTheOperandBoundAndCloneLocation() {
        var range = new SourcePositionRange(new SourcePosition(5, 3), new SourcePosition(5, 19));
        var site = CalciteObject.create(range);
        var scalar = new MonotoneType(new DBSPI32Literal(0).getType());
        var unit = new PartiallyMonotoneTuple(List.of(), false, false);
        var shape = new PartiallyMonotoneTuple(List.of(unit, scalar), false, false);
        var row = new DBSPVariablePath("row", shape.getType().ref());
        var empty = row.deref().field(0);
        var nullableEmpty = empty.some();
        var field = row.deepCopy().deref().field(1);
        var copy = new DBSPCloneExpression(site, field);
        var some = copy.some();
        var unwrap = new DBSPUnwrapExpression(site, "expected value", some);
        var closure = new DBSPTupleExpression(nullableEmpty, unwrap).closure(row);
        var op = new DBSPConstantOperator(CalciteEmptyRel.INSTANCE, DBSPZSetExpression.emptyWithElementType(shape.getType()));
        for (boolean hybrid : List.of(false, true)) {
            if (hybrid && !Boolean.getBoolean("feldera.optimizer.compareRustLimiterApply"))
                continue;
            var compiler = new DBSPCompiler(new CompilerOptions());
            compiler.options.ioOptions.rustOptimizerPasses = hybrid;
            var analysis = new MonotoneTransferFunctions(compiler, op, MonotoneTransferFunctions.ArgumentKind.ZSet, shape);
            analysis.apply(closure);
            var cloned = analysis.get(copy).getReducedExpression().to(DBSPCloneExpression.class);
            Assert.assertEquals(range, cloned.getNode().getPositionRange());
            Assert.assertSame(analysis.get(field).getReducedExpression(), cloned.expression);
            Assert.assertSame(cloned, analysis.get(some).getReducedExpression());
            Assert.assertSame(cloned, analysis.get(unwrap).getReducedExpression());
            Assert.assertFalse(analysis.get(nullableEmpty).mayBeMonotone());
            Assert.assertSame(analysis.get(empty).getReducedExpression(), analysis.get(nullableEmpty).getReducedExpression());
        }
    }

    @Test
    public void generalFieldReadsUseTheCompressedIndexAndRetainBoundIdentity() {
        var integer = new DBSPI32Literal(1).getType();
        var fieldType = new MonotoneType(integer);
        var row = new PartiallyMonotoneTuple(List.of(new NonMonotoneType(integer), fieldType), false, false);
        var input = new DBSPVariablePath("row", row.getType().ref());
        var dropped = input.deref().field(0);
        var kept = input.deepCopy().deref().field(1);
        var tuple = new DBSPTupleExpression(dropped, kept);
        var field = new DBSPFieldExpression(CalciteObject.EMPTY, tuple, 1);
        var borrow = field.borrow();
        var deref = borrow.deref();
        var operator = new DBSPConstantOperator(CalciteEmptyRel.INSTANCE, DBSPZSetExpression.emptyWithElementType(row.getType()));
        for (boolean hybrid : List.of(false, true)) {
            if (hybrid && !Boolean.getBoolean("feldera.optimizer.compareRustLimiterApply"))
                continue;
            var compiler = new DBSPCompiler(new CompilerOptions());
            compiler.options.ioOptions.rustOptimizerPasses = hybrid;
            var analysis = new MonotoneTransferFunctions(compiler, operator, MonotoneTransferFunctions.ArgumentKind.ZSet, row);
            analysis.apply(deref.closure(input));
            Assert.assertFalse(analysis.get(dropped).mayBeMonotone());
            var retained = analysis.get(kept).getReducedExpression().to(DBSPFieldExpression.class);
            Assert.assertEquals(0, retained.fieldNo);
            Assert.assertSame(retained, analysis.get(field).getReducedExpression());
            var reference = analysis.get(borrow).getReducedExpression().to(DBSPBorrowExpression.class);
            Assert.assertSame(retained, reference.expression);
            Assert.assertSame(reference, analysis.get(deref).getReducedExpression().to(DBSPDerefExpression.class).expression);
            Assert.assertSame(fieldType, analysis.get(deref).getMonotoneType());
        }
    }

    @Test
    public void generalCastWaterlinesRetainTheLiveBoundAndCastLocation() {
        var span = new SourcePositionRange(new SourcePosition(3, 4), new SourcePosition(3, 21));
        var site = CalciteObject.create(span);
        var integer = new DBSPTypeInteger(site, 32, true, true);
        var row = new PartiallyMonotoneTuple(List.of(new MonotoneType(integer)), false, false);
        var parameter = new DBSPVariablePath("row", row.getType().ref());
        var operator = new DBSPConstantOperator(CalciteEmptyRel.INSTANCE, DBSPZSetExpression.emptyWithElementType(integer));
        for (boolean hybrid : List.of(false, true)) {
            if (hybrid && !Boolean.getBoolean("feldera.optimizer.compareRustLimiterApply"))
                continue;
            var compiler = new DBSPCompiler(new CompilerOptions());
            compiler.options.ioOptions.rustOptimizerPasses = hybrid;
            for (int width : List.of(16, 32, 64)) {
                for (var mode : List.of(DBSPCastExpression.CastType.SqlUnsafe, DBSPCastExpression.CastType.SqlSafe)) {
                    var target = new DBSPTypeInteger(site, width, true, true);
                    var field = parameter.deref().field(0);
                    var cast = new DBSPCastExpression(site, field, target, mode);
                    var analysis = new MonotoneTransferFunctions(compiler, operator,
                            MonotoneTransferFunctions.ArgumentKind.ZSet, row);
                    analysis.apply(cast.closure(parameter));
                    var result = analysis.get(cast);
                    if (width == 16 && mode.isSafe()) {
                        Assert.assertFalse(result.mayBeMonotone());
                        continue;
                    }
                    var bound = analysis.get(field).getReducedExpression();
                    var reduced = result.getReducedExpression();
                    Assert.assertTrue(reduced.getType().sameType(target.withMayBeNull(false)));
                    if (width == 32) {
                        Assert.assertSame(bound, reduced);
                    } else {
                        var projected = reduced.to(DBSPCastExpression.class);
                        Assert.assertSame(bound, projected.source);
                        Assert.assertEquals(span, projected.getNode().getPositionRange());
                        Assert.assertEquals(width == 16 ? DBSPCastExpression.CastType.Saturating
                                : DBSPCastExpression.CastType.SqlUnsafe, projected.safe);
                    }
                }
            }
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
    public void aggregateImplementationsHaveNoWaterline() {
        var compiler = new DBSPCompiler(new CompilerOptions());
        var node = CalciteEmptyRel.INSTANCE;
        var integer = new DBSPI32Literal(0).getType();
        var field = new MonotoneType(integer);
        var row = new PartiallyMonotoneTuple(List.of(field), false, false);
        var input = new DBSPVariablePath("input", row.getType().ref());
        var accumulator = new DBSPVariablePath("accumulator", integer.ref());
        var element = new DBSPVariablePath("element", integer.ref());
        var weight = new DBSPVariablePath("weight", integer);
        var increment = accumulator.deref().closure(accumulator, element, weight);
        var finish = element.deref().closure(element);
        var fold = new DBSPFold(node, new DBSPTypeUser(node, DBSPTypeCode.USER, "DefaultSemigroup", false, integer),
                new DBSPI32Literal(0), increment, finish);
        var minimum = new DBSPMinMax(node, fold.getType(), finish.deepCopy().to(DBSPClosureExpression.class),
                DBSPMinMax.Aggregation.Min);
        var body = new DBSPTupleExpression(fold, minimum, input.deref().field(0));
        var bound = new DBSPVariablePath("input", row.getProjectedType().ref());
        var expected = new DBSPTupleExpression(bound.deref().field(0)).closure(bound);
        var operator = new DBSPConstantOperator(node, DBSPZSetExpression.emptyWithElementType(integer));
        var direct = new MonotoneTransferFunctions(compiler, operator, MonotoneTransferFunctions.ArgumentKind.ZSet,
                row).applyAnalysis(body.closure(input));
        Assert.assertTrue(direct.getReducedExpression().equivalent(expected));
        if (Boolean.getBoolean("feldera.optimizer.compareRustLimiterApply")) {
            var projected = RustCircuitPass.projectRow(compiler, body, bound, row);
            Assert.assertNotNull(projected);
            Assert.assertTrue(projected.getReducedExpression().closure(bound).equivalent(expected));
            var result = projected.getMonotoneType().to(PartiallyMonotoneTuple.class);
            Assert.assertFalse(result.getFieldType(0).mayBeMonotone());
            Assert.assertFalse(result.getFieldType(1).mayBeMonotone());
            Assert.assertSame(field, result.getFieldType(2));
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
                DBSPTupleExpression.none(nullable), new DBSPFailExpression(node, nested, "null row cast"),
                parameter.deepCopy().deref().field(0));
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
            var failedRow = output.getFieldType(3).to(PartiallyMonotoneTuple.class);
            Assert.assertFalse(failedRow.mayBeMonotone());
            Assert.assertSame(integer, failedRow.getFieldType(0).getType());
            Assert.assertSame(row.getFieldType(0), output.getFieldType(4));
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
