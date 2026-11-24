package org.dbsp.sqlCompiler.compiler.visitors.outer.monotonicity;

import org.apache.calcite.util.TimestampWithTimeZoneString;
import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.circuit.operator.DBSPAggregateZeroOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPBinaryDistinctOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPConstantOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPChainAggregateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDifferentiateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPPartitionedRollingAggregateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPWaterlineOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPViewOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPWeighOperator;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitTransform;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.ProgramMetadata;
import org.dbsp.sqlCompiler.compiler.ViewColumnMetadata;
import org.dbsp.sqlCompiler.compiler.ViewMetadata;
import org.dbsp.sqlCompiler.compiler.backend.RustCircuitPass;
import org.dbsp.sqlCompiler.compiler.backend.ToJsonOuterVisitor.OptimizerSnapshots;
import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
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
import org.dbsp.sqlCompiler.compiler.visitors.monotone.MonotoneClosureType;
import org.dbsp.sqlCompiler.compiler.visitors.monotone.MonotoneExpression;
import org.dbsp.sqlCompiler.compiler.visitors.monotone.MonotoneRefType;
import org.dbsp.sqlCompiler.compiler.visitors.monotone.MonotoneTransferFunctions;
import org.dbsp.sqlCompiler.compiler.visitors.monotone.MonotoneType;
import org.dbsp.sqlCompiler.compiler.visitors.monotone.NonMonotoneType;
import org.dbsp.sqlCompiler.compiler.visitors.monotone.PartiallyMonotoneTuple;
import org.dbsp.sqlCompiler.ir.DBSPFunction;
import org.dbsp.sqlCompiler.ir.DBSPParameter;
import org.dbsp.sqlCompiler.ir.aggregate.DBSPAggregateList;
import org.dbsp.sqlCompiler.ir.aggregate.DBSPFold;
import org.dbsp.sqlCompiler.ir.aggregate.DBSPMinMax;
import org.dbsp.sqlCompiler.ir.aggregate.IAggregate;
import org.dbsp.sqlCompiler.ir.aggregate.LinearAggregate;
import org.dbsp.sqlCompiler.ir.aggregate.MinMaxAggregate;
import org.dbsp.sqlCompiler.ir.aggregate.NonLinearAggregate;
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
import org.dbsp.sqlCompiler.ir.expression.DBSPConditionalIncrementExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPCloneExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPUnwrapExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPFailExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPForExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPIfExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPNoComparatorExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPStaticExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPLetExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPOpcode;
import org.dbsp.sqlCompiler.ir.expression.DBSPRawTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTimeAddSub;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPUnaryExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPUnsignedWrapExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.expression.DBSPWindowBoundExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPZSetExpression;
import org.dbsp.sqlCompiler.ir.expression.NoExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPBoolLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPTimestampTzLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPDoubleLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPI32Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPISizeLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPStringLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPUSizeLiteral;
import org.dbsp.sqlCompiler.ir.statement.DBSPComment;
import org.dbsp.sqlCompiler.ir.statement.DBSPComparatorItem;
import org.dbsp.sqlCompiler.ir.statement.DBSPExpressionStatement;
import org.dbsp.sqlCompiler.ir.statement.DBSPFunctionItem;
import org.dbsp.sqlCompiler.ir.statement.DBSPLetStatement;
import org.dbsp.sqlCompiler.ir.statement.DBSPStaticItem;
import org.dbsp.sqlCompiler.ir.statement.DBSPStructItem;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeCode;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeFunction;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeRawTuple;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeStruct;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeBool;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeDouble;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeInteger;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeTimestampTz;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeTimestamp;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeUSize;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeTypedBox;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeIndexedZSet;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeUser;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeWeight;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import java.util.HashMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class WaterlineProjectionTests {
    private static void assertFailure(RustCircuitPass.WaterlineAnalysis analysis,
            RustCircuitPass.WaterlineError error, DBSPExpression expression) {
        Assert.assertTrue("Expected a native waterline error", analysis instanceof RustCircuitPass.FailedWaterline);
        var failure = (RustCircuitPass.FailedWaterline) analysis;
        Assert.assertEquals(error, failure.error());
        Assert.assertSame(expression, failure.expression());
    }

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
    public void incompatibleWaterlinesKeepTheFirstDiagnosticWithoutRepeatingTheCombination() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustLimiterApply"));
        var compiler = new DBSPCompiler(new CompilerOptions());
        var type = new DBSPI32Literal(1).getType();
        class CountedField extends MonotoneType {
            int visits;

            CountedField(DBSPType type) {
                super(type);
            }

            @Override
            public IMaybeMonotoneType intersection(IMaybeMonotoneType other) {
                this.visits++;
                return super.intersection(other);
            }

            @Override
            public IMaybeMonotoneType union(IMaybeMonotoneType other) {
                this.visits++;
                return super.union(other);
            }
        }
        var first = new CountedField(type);
        var scalar = new NonMonotoneType(type);
        var tuple = new PartiallyMonotoneTuple(List.of(scalar), false, false);
        var reference = new MonotoneRefType(scalar);
        var closure = new MonotoneClosureType(scalar, new DBSPParameter[0], new DBSPParameter[0]);
        for (var incompatible : List.of(tuple, reference, closure)) {
            // Two fields fail. Java visits the middle field before the last one.
            var left = new PartiallyMonotoneTuple(List.of(first, incompatible, closure), false, false);
            var right = new PartiallyMonotoneTuple(List.of(scalar, scalar, scalar), false, false);
            var leftPair = new PartiallyMonotoneTuple(List.of(left, tuple), true, false);
            var rightPair = new PartiallyMonotoneTuple(List.of(right, tuple), true, false);
            for (Runnable operation : List.<Runnable>of(
                    () -> CombinedBounds.union(compiler, left, right),
                    // The first input selects the second row before the last input fails.
                    () -> CombinedBounds.intersection(compiler, List.of(new MonotoneType(type), left, right)),
                    () -> CombinedBounds.joinArguments(compiler, leftPair, rightPair),
                    () -> CombinedBounds.commonKey(compiler, leftPair, rightPair))) {
                compiler.options.ioOptions.rustOptimizerPasses = false;
                first.visits = 0;
                var direct = Assert.assertThrows(RuntimeException.class, operation::run);
                Assert.assertEquals(1, first.visits);
                compiler.options.ioOptions.rustOptimizerPasses = true;
                first.visits = 0;
                var hybrid = Assert.assertThrows(direct.getClass(), operation::run);
                Assert.assertEquals(direct.getMessage(), hybrid.getMessage());
                Assert.assertEquals(0, first.visits);
            }
        }
    }

    @Test
    public void rollingFrameChecksNullOffsetWithoutAnInputWaterline() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustLimiterApply"));
        var compiler = new DBSPCompiler(new CompilerOptions());
        var site = CalciteEmptyRel.INSTANCE;
        var integer = new DBSPI32Literal(0).getType();
        var row = integer.ref().var();
        var partition = new DBSPRawTupleExpression(row.deref(), row.deref()).closure(row);
        var input = new DBSPConstantOperator(site, DBSPZSetExpression.emptyWithElementType(integer));
        var index = new DBSPMapIndexOperator(site, partition, input.outputPort());
        var aggregates = new DBSPAggregateList(site, row, List.of());
        var lower = new DBSPWindowBoundExpression(site, true, new DBSPI32Literal(1));
        for (boolean preceding : List.of(true, false)) {
            var upper = new DBSPWindowBoundExpression(site, preceding, new DBSPI32Literal(null, true));
            var rolling = new DBSPPartitionedRollingAggregateOperator(site, partition, null, aggregates,
                    lower, upper, index.getOutputIndexedZSetType(), index.outputPort());
            var circuit = new DBSPCircuit(new ProgramMetadata(), true);
            circuit.addOperator(input);
            circuit.addOperator(index);
            circuit.addOperator(rolling);
            for (boolean hybrid : List.of(false, true)) {
                compiler.options.ioOptions.rustOptimizerPasses = hybrid;
                var analysis = new Monotonicity(compiler);
                if (preceding) {
                    analysis.apply(circuit);
                    Assert.assertNull(analysis.getMonotoneExpression(rolling));
                } else {
                    var error = Assert.assertThrows(InternalCompilerError.class, () -> analysis.apply(circuit));
                    Assert.assertTrue(error.getMessage(), error.getMessage().contains("DBSPI32Literal.gt0"));
                }
            }
        }
    }

    @Test
    public void chainMinimumChecksEachUpdateBeforeTheInitializerAndInputWaterline() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustLimiterApply"));
        var site = CalciteEmptyRel.INSTANCE;
        var ts = DBSPTypeTimestampTz.INSTANCE;
        var good = new DBSPTimestampTzLiteral(site, ts, new TimestampWithTimeZoneString("2020-01-01 00:00:00 UTC"));
        var bad = new DBSPTimestampTzLiteral(site, ts, new TimestampWithTimeZoneString("2020-01-01 00:00:00 GMT+19:00"));
        var integer = new DBSPI32Literal(0).getType();
        var input = new DBSPConstantOperator(site, DBSPZSetExpression.emptyWithElementType(integer));
        var row = integer.ref().var();
        var index = new DBSPMapIndexOperator(site, new DBSPRawTupleExpression(row.deref(), row.deref()).closure(row),
                input.outputPort());
        var state = new DBSPTypeTuple(ts, ts);
        var output = new DBSPTypeIndexedZSet(site, integer, state);
        var weight = DBSPTypeWeight.INSTANCE.var();
        var acc = state.var();
        var initRow = integer.ref().var();
        var shared = new DBSPVariablePath(row.variable, row.getType());
        var conflict = new DBSPLetExpression(shared, shared, shared.deepCopy());
        for (DBSPExpression compared : List.of(bad, new DBSPTupleExpression(), conflict)) {
            for (var firstInit : List.of(DBSPOpcode.AGG_MIN, DBSPOpcode.AGG_MAX)) {
                var init = new DBSPTupleExpression(
                        new DBSPConditionalIncrementExpression(site, firstInit, ts, good, good.deepCopy(), null),
                        new DBSPConditionalIncrementExpression(site, DBSPOpcode.AGG_MAX, ts, good, good.deepCopy(), null)
                ).closure(initRow, weight);
                var updates = new DBSPTupleExpression(
                        new DBSPConditionalIncrementExpression(site, DBSPOpcode.AGG_MIN, ts, acc.field(0), good, null),
                        new DBSPConditionalIncrementExpression(site, DBSPOpcode.AGG_MIN, ts, acc.field(1), compared, null)
                ).closure(acc, row, weight);
                var chain = new DBSPChainAggregateOperator(site, init, updates, output, index.outputPort());
                var circuit = new DBSPCircuit(new ProgramMetadata(), true);
                circuit.addOperator(input);
                circuit.addOperator(index);
                circuit.addOperator(chain);
                var compiler = new DBSPCompiler(new CompilerOptions());
                var direct = new Monotonicity(compiler);
                if (firstInit == DBSPOpcode.AGG_MAX) {
                    direct.apply(circuit);
                    Assert.assertNull(direct.getMonotoneExpression(chain));
                    compiler.options.ioOptions.rustOptimizerPasses = true;
                    Assert.assertEquals(false, RustCircuitPass.isMinimumChain(compiler, chain));
                    var hybrid = new Monotonicity(compiler);
                    hybrid.apply(circuit);
                    Assert.assertNull(hybrid.getMonotoneExpression(chain));
                } else {
                    var expected = Assert.assertThrows(RuntimeException.class, () -> direct.apply(circuit));
                    compiler.options.ioOptions.rustOptimizerPasses = true;
                    var checked = Assert.assertThrows(expected.getClass(), () -> RustCircuitPass.isMinimumChain(compiler, chain));
                    var scheduled = Assert.assertThrows(expected.getClass(), () -> new Monotonicity(compiler).apply(circuit));
                    Assert.assertEquals(expected.getMessage().lines().findFirst(), checked.getMessage().lines().findFirst());
                    Assert.assertEquals(expected.getMessage().lines().findFirst(), scheduled.getMessage().lines().findFirst());
                }
            }
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
                    if (direct.getMessage().startsWith("Variable ")) {
                        Assert.assertEquals(variableTypeDiagnostic(direct), variableTypeDiagnostic(failure));
                        Assert.assertEquals(((InternalCompilerError) direct).getPositionRange(),
                                ((InternalCompilerError) failure).getPositionRange());
                    } else {
                        Assert.assertEquals(direct.getMessage(), failure.getMessage());
                    }
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
    public void invalidClosureBindingsAndRepeatedReadsAreFinalRustDecisions() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustLimiterApply"));
        var compiler = new DBSPCompiler(new CompilerOptions());
        var integer = new DBSPI32Literal(1).getType();
        var input = integer.ref().var();
        var shape = new MonotoneType(integer);
        var repeated = input.deref();
        var path = new DBSPVariablePath(input.variable, input.getType());
        var free = new DBSPVariablePath("missing", integer);
        var conflict = new DBSPLetExpression(path, path, path.deepCopy());
        var op = new DBSPConstantOperator(CalciteEmptyRel.INSTANCE, DBSPZSetExpression.emptyWithElementType(integer));
        var decisions = new ArrayList<Boolean>();
        compiler.stateObserver = new DBSPCompiler.StateObserver() {
            @Override
            public void nativeDecision(String operation, boolean answered) {
                if (operation.startsWith("Monotonicity/"))
                    decisions.add(answered);
            }

            @Override
            public void before(String parent, int index, int iteration, CircuitTransform pass, DBSPCircuit circuit) {}

            @Override
            public void after(String parent, int index, int iteration, CircuitTransform pass,
                              DBSPCircuit circuit, boolean completed) {}
        };
        for (var body : List.of(new DBSPTupleExpression(repeated, repeated),
                new DBSPTupleExpression(repeated, repeated, free), conflict)) {
            var closure = body.closure(input);
            var direct = new MonotoneTransferFunctions(compiler, op, MonotoneTransferFunctions.ArgumentKind.ZSet, shape);
            var expected = Assert.assertThrows(RuntimeException.class, () -> direct.apply(closure));
            for (boolean resultOnly : List.of(false, true)) {
                decisions.clear();
                var facts = new HashMap<DBSPExpression, MonotoneExpression>();
                var error = Assert.assertThrows(expected.getClass(), () ->
                        RustCircuitPass.projectWaterlineClosure(compiler, body, closure.parameters,
                                new DBSPType[]{integer}, new IMaybeMonotoneType[]{new MonotoneRefType(shape)},
                                false, resultOnly, facts::put));
                Assert.assertEquals(expected.getMessage(), error.getMessage());
                Assert.assertTrue(facts.isEmpty());
                Assert.assertEquals(List.of(true), decisions);
            }
        }
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
    public void expressionFactsPreserveLocalAliasesAndSharedWaterlines() {
        var compiler = new DBSPCompiler(new CompilerOptions());
        var integer = new DBSPTypeInteger(CalciteEmptyRel.INSTANCE, 32, true, false);
        var row = new PartiallyMonotoneTuple(List.of(new MonotoneType(integer)), false, false);
        var input = new DBSPVariablePath("row", row.getType().ref());
        var field = input.deref().field(0);
        var local = new DBSPLetStatement("value", field);
        var first = local.getVarReference();
        var second = local.getVarReference();
        var pair = new DBSPTupleExpression(first, second);
        var block = new DBSPBlockExpression(List.of(local), pair);
        var closure = block.closure(input);
        var operator = new DBSPConstantOperator(CalciteEmptyRel.INSTANCE, DBSPZSetExpression.emptyWithElementType(integer));
        var analyzer = new MonotoneTransferFunctions(compiler, operator, MonotoneTransferFunctions.ArgumentKind.ZSet, row);
        analyzer.apply(closure);
        Assert.assertSame(analyzer.get(field), analyzer.get(first));
        Assert.assertSame(analyzer.get(first), analyzer.get(second));
        Assert.assertSame(analyzer.get(pair), analyzer.get(block));

        if (Boolean.getBoolean("feldera.optimizer.compareRustLimiterApply")) {
            var facts = new HashMap<DBSPExpression, MonotoneExpression>();
            var nativeResult = RustCircuitPass.projectWaterlineClosure(compiler, block, closure.parameters,
                    new DBSPType[]{row.getProjectedType()}, new IMaybeMonotoneType[]{new MonotoneRefType(row)},
                    false, facts::put);
            Assert.assertTrue(nativeResult instanceof RustCircuitPass.ProjectedWaterline);
            Assert.assertSame(facts.get(field), facts.get(first));
            Assert.assertSame(facts.get(first), facts.get(second));
            Assert.assertSame(facts.get(pair), facts.get(block));
            var reduced = ((RustCircuitPass.ProjectedWaterline) nativeResult).closure();
            Assert.assertTrue(analyzer.get(closure).getReducedExpression().equivalent(reduced));
            Assert.assertSame(facts.get(pair).getReducedExpression(), reduced.body);
            var fields = reduced.body.to(DBSPTupleExpression.class);
            Assert.assertSame(facts.get(first).getReducedExpression(), fields.get(0));
            Assert.assertSame(fields.get(0), fields.get(1));
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
    public void assignmentToAnUninitializedLocalNeedsNoDestinationWaterline() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustLimiterApply"));
        var compiler = new DBSPCompiler(new CompilerOptions());
        var literal = new DBSPI32Literal(7);
        var integer = literal.getType();
        var input = integer.ref().var();
        var scratch = new DBSPLetStatement("scratch", integer, true);
        var destination = scratch.getVarReference();
        var assignment = new DBSPAssignmentExpression(destination, literal);
        var tail = input.deref();
        var body = new DBSPBlockExpression(List.of(scratch, assignment.toStatement()), tail);
        var closure = body.closure(input);
        var op = new DBSPConstantOperator(CalciteEmptyRel.INSTANCE, DBSPZSetExpression.emptyWithElementType(integer));
        var shape = new MonotoneType(integer);
        var direct = new MonotoneTransferFunctions(compiler, op, MonotoneTransferFunctions.ArgumentKind.ZSet, shape);
        direct.apply(closure);
        Assert.assertNull(direct.maybeGet(destination));
        for (boolean observe : List.of(false, true)) {
            var facts = new HashMap<DBSPExpression, MonotoneExpression>();
            var result = RustCircuitPass.projectWaterlineClosure(compiler, body, closure.parameters,
                    new DBSPType[]{integer}, new IMaybeMonotoneType[]{new MonotoneRefType(shape)},
                    false, observe ? facts::put : null);
            Assert.assertTrue(result instanceof RustCircuitPass.ProjectedWaterline);
            Assert.assertTrue(direct.get(closure).getReducedExpression().equivalent(
                    ((RustCircuitPass.ProjectedWaterline) result).closure()));
            if (observe) {
                Assert.assertFalse(facts.containsKey(destination));
                Assert.assertFalse(facts.get(assignment).mayBeMonotone());
                Assert.assertTrue(facts.get(literal).mayBeMonotone());
                Assert.assertSame(facts.get(tail), facts.get(body));
            }
        }
        // Reading the same declaration as the block result demands its absent fact.
        var required = new DBSPBlockExpression(List.of(scratch), scratch.getVarReference());
        var failure = RustCircuitPass.projectWaterlineClosure(compiler, required, closure.parameters,
                new DBSPType[]{integer}, new IMaybeMonotoneType[]{new MonotoneRefType(shape)}, false);
        assertFailure(failure, RustCircuitPass.WaterlineError.MissingOperand, required.lastExpression);
    }

    @Test
    public void rowEqualityDoesNotDemandOperandWaterlines() {
        var empty = new NoExpression(DBSPTypeRawTuple.EMPTY);
        var equality = new DBSPBinaryExpression(CalciteEmptyRel.INSTANCE, DBSPTypeBool.create(false), DBSPOpcode.EQ,
                empty, new DBSPRawTupleExpression());
        assertUnusedOperandWaterline(empty, equality);
    }

    @Test
    public void aggregateDescriptorsExposeChildrenWithoutAResultFact() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustLimiterApply"));
        var compiler = new DBSPCompiler(new CompilerOptions());
        var site = CalciteEmptyRel.INSTANCE;
        var integer = new DBSPI32Literal(0).getType();
        var shape = new MonotoneType(integer);
        var operator = new DBSPConstantOperator(site, DBSPZSetExpression.emptyWithElementType(integer));
        var row = integer.var();
        var map = row.closure(row);
        var acc = integer.var();
        var weight = DBSPTypeWeight.INSTANCE.var();
        var increment = acc.closure(acc, row, weight);
        var post = acc.deepCopy().closure(acc);
        for (IAggregate descriptor : List.of(
                new LinearAggregate(site, map, post, new DBSPI32Literal(0)),
                new NonLinearAggregate(site, new DBSPI32Literal(1), increment, new DBSPI32Literal(0),
                        DBSPTypeWeight.INSTANCE),
                new MinMaxAggregate(site, new DBSPI32Literal(1), increment, new DBSPI32Literal(0),
                        DBSPTypeWeight.INSTANCE, map, post, MinMaxAggregate.Operation.Min))) {
            var input = integer.ref().var();
            var body = new DBSPBlockExpression(List.of(descriptor.toStatement()), input.deref());
            var closure = body.closure(input);
            var direct = new MonotoneTransferFunctions(compiler, operator, MonotoneTransferFunctions.ArgumentKind.ZSet, shape);
            direct.apply(closure);
            Assert.assertNull(direct.maybeGet(descriptor));
            Assert.assertTrue(direct.get(descriptor.getEmptySetResult()).mayBeMonotone());
            var required = descriptor.closure(input);
            Assert.assertThrows(RuntimeException.class, () -> new MonotoneTransferFunctions(compiler, operator,
                    MonotoneTransferFunctions.ArgumentKind.ZSet, shape).apply(required));
            for (boolean observe : List.of(false, true)) {
                var facts = new HashMap<DBSPExpression, MonotoneExpression>();
                var result = RustCircuitPass.projectWaterlineClosure(compiler, body, closure.parameters,
                        new DBSPType[]{integer}, new IMaybeMonotoneType[]{new MonotoneRefType(shape)},
                        false, observe ? facts::put : null);
                Assert.assertTrue(result instanceof RustCircuitPass.ProjectedWaterline);
                Assert.assertTrue(direct.get(closure).getReducedExpression()
                        .equivalent(((RustCircuitPass.ProjectedWaterline) result).closure()));
                if (observe) {
                    Assert.assertFalse(facts.containsKey(descriptor));
                    Assert.assertTrue(facts.get(descriptor.getEmptySetResult()).mayBeMonotone());
                }
                var failure = RustCircuitPass.projectWaterlineClosure(compiler, descriptor, required.parameters,
                        new DBSPType[]{integer}, new IMaybeMonotoneType[]{new MonotoneRefType(shape)},
                        false, observe ? facts::put : null);
                assertFailure(failure, RustCircuitPass.WaterlineError.MissingOperand, descriptor);
            }
        }
    }

    @Test
    public void projectedReadsDemandOnlyTheSelectedWaterlineBound() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustLimiterApply"));
        var compiler = new DBSPCompiler(new CompilerOptions());
        var site = CalciteEmptyRel.INSTANCE;
        var integer = new DBSPI32Literal(1).getType();
        var bounded = new MonotoneType(integer);
        var unbounded = new NonMonotoneType(integer);
        var shape = new PartiallyMonotoneTuple(List.of(bounded, unbounded), false, false);
        var row = shape.getType().var();
        var operator = new DBSPConstantOperator(site, DBSPZSetExpression.emptyWithElementType(row.type));
        var oracle = new MonotoneTransferFunctions(compiler, operator, MonotoneTransferFunctions.ArgumentKind.ZSet, shape) {
            void seed(DBSPExpression value, IMaybeMonotoneType type) {
                this.set(value, new MonotoneExpression(value, type, null));
            }
        };
        oracle.seed(row, shape);
        var selected = new DBSPFieldExpression(site, row, 0);
        Assert.assertThrows(NullPointerException.class, () -> oracle.postorder(selected));
        Assert.assertThrows(NullPointerException.class, () -> RustCircuitPass.projectRead(compiler, selected, shape, null));

        var skipped = new DBSPFieldExpression(site, row, 1);
        oracle.postorder(skipped);
        var result = RustCircuitPass.projectRead(compiler, skipped, shape, null);
        Assert.assertNotNull(result);
        Assert.assertSame(oracle.get(skipped).getMonotoneType(), result.getMonotoneType());
        Assert.assertFalse(result.mayBeMonotone());

        var copy = new DBSPCloneExpression(site, row);
        Assert.assertThrows(NullPointerException.class, () -> oracle.postorder(copy));
        Assert.assertThrows(NullPointerException.class, () -> RustCircuitPass.projectRead(compiler, copy, shape, null));
    }

    @Test
    public void nullableUnitFieldNeedsNoBoundFromItsParent() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustLimiterApply"));
        var compiler = new DBSPCompiler(new CompilerOptions());
        var site = CalciteEmptyRel.INSTANCE;
        var unit = new PartiallyMonotoneTuple(List.of(), false, false);
        var shape = new PartiallyMonotoneTuple(List.of(unit), false, true);
        var row = shape.getType().var();
        var field = new DBSPFieldExpression(site, row, 0);
        var operator = new DBSPConstantOperator(site, DBSPZSetExpression.emptyWithElementType(row.type));
        var oracle = new MonotoneTransferFunctions(compiler, operator, MonotoneTransferFunctions.ArgumentKind.ZSet, shape) {
            void seed() { this.set(row, new MonotoneExpression(row, shape, null)); }
        };
        oracle.seed();
        oracle.postorder(field);
        var projected = RustCircuitPass.projectRead(compiler, field, shape, null);
        Assert.assertNotNull(projected);
        Assert.assertFalse(projected.mayBeMonotone());
        Assert.assertTrue(projected.getMonotoneType().getType().sameType(oracle.get(field).getMonotoneType().getType()));
    }

    @Test
    public void discardedNullableRawTupleReportsItsEarlierFacts() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustLimiterApply"));
        var compiler = new DBSPCompiler(new CompilerOptions());
        var integer = new DBSPI32Literal(1).getType();
        var shape = new MonotoneType(integer);
        var input = integer.ref().var();
        var prefix = new DBSPI32Literal(7);
        var raw = new DBSPRawTupleExpression(input.deref());
        var invalid = raw.some();
        var later = new DBSPI32Literal(8);
        var body = new DBSPBlockExpression(List.of(prefix.toStatement(), invalid.toStatement()), later);
        var closure = body.closure(input);
        var op = new DBSPConstantOperator(CalciteEmptyRel.INSTANCE, DBSPZSetExpression.emptyWithElementType(integer));
        var direct = new MonotoneTransferFunctions(compiler, op, MonotoneTransferFunctions.ArgumentKind.ZSet, shape);
        var expected = Assert.assertThrows(InternalCompilerError.class, () -> direct.apply(closure));
        for (boolean resultOnly : List.of(false, true)) {
            var facts = new HashMap<DBSPExpression, MonotoneExpression>();
            var error = Assert.assertThrows(InternalCompilerError.class, () ->
                    RustCircuitPass.projectWaterlineClosure(compiler, body, closure.parameters,
                            new DBSPType[]{integer}, new IMaybeMonotoneType[]{new MonotoneRefType(shape)},
                            false, resultOnly, facts::put));
            Assert.assertEquals(expected.getMessage().lines().findFirst(), error.getMessage().lines().findFirst());
            Assert.assertTrue(facts.get(prefix).mayBeMonotone());
            Assert.assertTrue(facts.get(raw).mayBeMonotone());
            Assert.assertFalse(facts.containsKey(invalid));
            Assert.assertFalse(facts.containsKey(later));
        }
    }

    @Test
    public void singleReadReportsInvalidTypeProjections() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustLimiterApply"));
        var compiler = new DBSPCompiler(new CompilerOptions());
        var site = CalciteEmptyRel.INSTANCE;
        var integer = new DBSPI32Literal(1).getType();
        var scalar = new MonotoneType(integer);
        var reference = new MonotoneRefType(scalar);
        var function = new MonotoneClosureType(scalar, new DBSPParameter[0], new DBSPParameter[0]);
        // Supply the declared read separately, as the general visitor does.
        // The closure's SQL type itself rejects lifting while constructing a field expression.
        var read = new DBSPTypeTuple(site, true, List.of(integer)).var().field(0);
        for (var component : List.of(reference, function)) {
            var shape = new PartiallyMonotoneTuple(List.of(component), false, true);
            var expected = Assert.assertThrows(UnsupportedException.class, () -> shape.getFieldExpressionType(0));
            var actual = Assert.assertThrows(UnsupportedException.class,
                    () -> RustCircuitPass.projectRead(compiler, read, shape, shape.getProjectedType().var()));
            Assert.assertEquals(expected.getMessage(), actual.getMessage());
            Assert.assertEquals(expected.getPositionRange(), actual.getPositionRange());
        }
        var expected = Assert.assertThrows(RuntimeException.class, () -> scalar.to(PartiallyMonotoneTuple.class));
        var error = Assert.assertThrows(expected.getClass(), () -> RustCircuitPass.projectRead(compiler, read, scalar, null));
        Assert.assertEquals(expected.getMessage(), error.getMessage());
    }

    @Test
    public void binaryWaterlineReportsTheFirstSelectedMissingBound() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustLimiterApply"));
        var compiler = new DBSPCompiler(new CompilerOptions());
        var integer = new DBSPI32Literal(1).getType();
        var input = integer.ref().var();
        var site = CalciteEmptyRel.INSTANCE;
        var source = new DBSPConstantOperator(site, DBSPZSetExpression.emptyWithElementType(integer));
        var shape = new MonotoneType(integer);
        for (int missingSide = 0; missingSide < 3; missingSide++) {
            // An empty tuple can be monotone while lacking a reduced expression.
            // An opaque scalar on the opposite side of MAX contributes no bound.
            var unknown = new NoExpression(integer);
            var left = missingSide == 1 ? unknown : new NoExpression(DBSPTypeRawTuple.EMPTY);
            var right = missingSide == 0 ? unknown : new NoExpression(DBSPTypeRawTuple.EMPTY);
            var maximum = new DBSPBinaryExpression(site, integer, DBSPOpcode.MAX, left, right);
            var later = new DBSPI32Literal(4);
            var body = new DBSPBlockExpression(List.of(maximum.toStatement()), later);
            var closure = body.closure(input);
            var kind = MonotoneTransferFunctions.ArgumentKind.ZSet;
            var direct = new MonotoneTransferFunctions(compiler, source, kind, shape);
            Assert.assertThrows(NullPointerException.class, () -> direct.apply(closure));
            for (boolean resultOnly : List.of(false, true)) {
                var facts = new HashMap<DBSPExpression, MonotoneExpression>();
                var failure = RustCircuitPass.projectWaterlineClosure(compiler, body, closure.parameters,
                        new DBSPType[]{integer}, new IMaybeMonotoneType[]{new MonotoneRefType(shape)},
                        false, resultOnly, facts::put);
                assertFailure(failure, RustCircuitPass.WaterlineError.MissingOperand, missingSide == 1 ? right : left);
                Assert.assertTrue(facts.containsKey(left));
                Assert.assertTrue(facts.containsKey(right));
                Assert.assertFalse(facts.containsKey(maximum));
                Assert.assertFalse(facts.containsKey(later));
            }
        }
    }

    @Test
    public void nullableRawTupleFailsBeforeConsumingItsFieldsBounds() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustLimiterApply"));
        var site = CalciteEmptyRel.INSTANCE;
        var integer = new DBSPI32Literal(1).getType();
        var input = integer.ref().var();
        var shape = new MonotoneType(integer);
        var compiler = new DBSPCompiler(new CompilerOptions());
        var operator = new DBSPConstantOperator(site, DBSPZSetExpression.emptyWithElementType(integer));
        var unit = DBSPTypeRawTuple.EMPTY;
        var nullable = new DBSPTypeRawTuple(site, true, List.of(unit));
        var unsupported = new DBSPTypeStruct(site, new ProgramIdentifier("Unsupported"), List.of(), false);
        for (var invalid : List.of(new DBSPRawTupleExpression(site, nullable, new NoExpression(unit)),
                new NoExpression(new DBSPTypeTuple(nullable)),
                new NoExpression(new DBSPTypeRawTuple(site, true, List.of(unsupported))))) {
            var before = new DBSPI32Literal(7);
            var later = new DBSPI32Literal(8);
            var body = new DBSPBlockExpression(List.of(before.toStatement(), invalid.toStatement()), later);
            var closure = body.closure(input);
            var oracle = new MonotoneTransferFunctions(compiler, operator, MonotoneTransferFunctions.ArgumentKind.ZSet, shape);
            var expected = Assert.assertThrows(RuntimeException.class, () -> oracle.apply(closure));
            Assert.assertFalse(expected instanceof NullPointerException);
            for (boolean resultOnly : List.of(false, true)) {
                var facts = new HashMap<DBSPExpression, MonotoneExpression>();
                var failure = Assert.assertThrows(expected.getClass(), () -> RustCircuitPass.projectWaterlineClosure(
                        compiler, body, closure.parameters, new DBSPType[]{integer},
                        new IMaybeMonotoneType[]{new MonotoneRefType(shape)}, false, resultOnly, facts::put));
                Assert.assertEquals(expected.getMessage().lines().findFirst(), failure.getMessage().lines().findFirst());
                Assert.assertTrue(facts.containsKey(before));
                Assert.assertFalse(facts.containsKey(invalid));
                Assert.assertFalse(facts.containsKey(later));
            }
        }
    }

    @Test
    public void scalarTransfersReportTheFirstInvalidTypeOperation() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustLimiterApply"));
        var site = CalciteEmptyRel.INSTANCE;
        var integer = new DBSPI32Literal(1).getType();
        var scalar = new MonotoneType(integer);
        var input = integer.ref().var();
        var missing = new NoExpression(DBSPTypeRawTuple.EMPTY);
        var tuple = new DBSPTupleExpression(new DBSPI32Literal(3));
        var struct = new DBSPTypeStruct(site, new ProgramIdentifier("Result"), List.of(), false);
        var function = new DBSPTypeFunction(integer);
        var operator = new DBSPConstantOperator(site, DBSPZSetExpression.emptyWithElementType(integer));
        var compiler = new DBSPCompiler(new CompilerOptions());
        for (var invalid : List.of(
                new DBSPBinaryExpression(site, integer, DBSPOpcode.MUL, tuple, new DBSPI32Literal(2)),
                new DBSPBinaryExpression(site, integer, DBSPOpcode.COALESCE, tuple, missing),
                new DBSPBinaryExpression(site, struct, DBSPOpcode.MAX, new DBSPI32Literal(1), missing),
                new DBSPUnaryExpression(site, integer, DBSPOpcode.UNARY_PLUS, tuple),
                new DBSPUnaryExpression(site, struct, DBSPOpcode.UNARY_PLUS, missing),
                new DBSPUnaryExpression(site, function, DBSPOpcode.UNARY_PLUS, missing),
                new DBSPTimeAddSub(site, tuple.getType(), DBSPOpcode.SUB, new NoExpression(integer), new NoExpression(integer)),
                new DBSPApplyExpression("sign_i32", function, new DBSPI32Literal(1)))) {
            var before = new DBSPI32Literal(7);
            var later = new DBSPI32Literal(8);
            var body = new DBSPBlockExpression(List.of(before.toStatement(), invalid.toStatement()), later);
            var closure = body.closure(input);
            var oracle = new MonotoneTransferFunctions(compiler, operator, MonotoneTransferFunctions.ArgumentKind.ZSet, scalar);
            var expected = Assert.assertThrows(RuntimeException.class, () -> oracle.apply(closure));
            Assert.assertFalse(expected instanceof NullPointerException);
            for (boolean resultOnly : List.of(false, true)) {
                var facts = new HashMap<DBSPExpression, MonotoneExpression>();
                var failure = Assert.assertThrows(expected.getClass(), () ->
                        RustCircuitPass.projectWaterlineClosure(compiler, body, closure.parameters,
                                new DBSPType[]{integer}, new IMaybeMonotoneType[]{new MonotoneRefType(scalar)},
                                false, resultOnly, facts::put));
                if (expected.getMessage() == null)
                    Assert.assertNull(failure.getMessage());
                else
                    Assert.assertEquals(expected.getMessage().lines().findFirst(), failure.getMessage().lines().findFirst());
                Assert.assertTrue(facts.get(before).mayBeMonotone());
                Assert.assertFalse(facts.containsKey(invalid));
                Assert.assertFalse(facts.containsKey(later));
            }
        }
    }

    @Test
    public void oneSidedMaximumSkipsAnUnboundedStructuredOperand() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustLimiterApply"));
        var compiler = new DBSPCompiler(new CompilerOptions());
        var site = CalciteEmptyRel.INSTANCE;
        var integer = new DBSPI32Literal(1).getType();
        var shape = new MonotoneType(integer);
        var input = integer.ref().var();
        var other = new NoExpression(new DBSPTypeTuple(integer));
        var left = new DBSPBinaryExpression(site, integer, DBSPOpcode.MAX, input.deref(), other);
        var right = new DBSPBinaryExpression(site, integer, DBSPOpcode.MAX, other.deepCopy(), input.deepCopy().deref());
        var body = new DBSPTupleExpression(left, right);
        var closure = body.closure(input);
        var operator = new DBSPConstantOperator(site, DBSPZSetExpression.emptyWithElementType(integer));
        var oracle = new MonotoneTransferFunctions(compiler, operator, MonotoneTransferFunctions.ArgumentKind.ZSet, shape);
        var expected = oracle.applyAnalysis(closure).getReducedExpression();
        for (boolean resultOnly : List.of(false, true)) {
            var projected = RustCircuitPass.projectWaterlineClosure(compiler, body, closure.parameters,
                    new DBSPType[]{integer}, new IMaybeMonotoneType[]{new MonotoneRefType(shape)},
                    false, resultOnly, (expression, fact) -> {});
            Assert.assertTrue(projected instanceof RustCircuitPass.ProjectedWaterline);
            Assert.assertTrue(expected.equivalent(((RustCircuitPass.ProjectedWaterline) projected).closure()));
        }
    }

    @Test
    public void unaryWaterlineReadsOnlyOrderPreservingOperands() {
        var empty = new NoExpression(DBSPTypeRawTuple.EMPTY);
        var integer = new DBSPI32Literal(1).getType();
        var indicator = new DBSPUnaryExpression(CalciteEmptyRel.INSTANCE, integer, DBSPOpcode.INDICATOR, empty);
        assertUnusedOperandWaterline(empty, indicator);

        var boxed = DBSPTypeTypedBox.wrapTypedBox(empty, true);
        var compiler = new DBSPCompiler(new CompilerOptions());
        var op = new DBSPConstantOperator(CalciteEmptyRel.INSTANCE, DBSPZSetExpression.emptyWithElementType(integer));
        var shape = new MonotoneType(integer);
        for (boolean hybrid : List.of(false, true)) {
            compiler.options.ioOptions.rustOptimizerPasses = hybrid;
            var visitor = new MonotoneTransferFunctions(compiler, op, MonotoneTransferFunctions.ArgumentKind.ZSet, shape);
            Assert.assertThrows(NullPointerException.class, () -> visitor.apply(boxed));
        }
        var input = integer.ref().var();
        var body = new DBSPBlockExpression(List.of(boxed.toStatement()), input.deref());
        for (boolean observe : List.of(false, true)) {
            var facts = new HashMap<DBSPExpression, MonotoneExpression>();
            var failure = RustCircuitPass.projectWaterlineClosure(compiler, body, body.closure(input).parameters,
                    new DBSPType[]{integer}, new IMaybeMonotoneType[]{new MonotoneRefType(shape)},
                    false, observe ? facts::put : null);
            assertFailure(failure, RustCircuitPass.WaterlineError.MissingOperand, empty);
        }
    }

    private static void assertUnusedOperandWaterline(DBSPExpression operand, DBSPExpression operation) {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustLimiterApply"));
        var compiler = new DBSPCompiler(new CompilerOptions());
        var integer = new DBSPI32Literal(1).getType();
        var input = integer.ref().var();
        var shape = new MonotoneType(integer);
        var op = new DBSPConstantOperator(CalciteEmptyRel.INSTANCE, DBSPZSetExpression.emptyWithElementType(integer));
        var kind = MonotoneTransferFunctions.ArgumentKind.ZSet;

        // Visiting the expression directly also exercises the general visitor.
        for (boolean hybrid : List.of(false, true)) {
            compiler.options.ioOptions.rustOptimizerPasses = hybrid;
            var visitor = new MonotoneTransferFunctions(compiler, op, kind, shape);
            visitor.apply(operation);
            Assert.assertTrue(visitor.get(operand).mayBeMonotone());
            Assert.assertThrows(NullPointerException.class, () -> visitor.get(operand).getReducedExpression());
            Assert.assertFalse(visitor.get(operation).mayBeMonotone());
        }
        var tail = input.deref();
        var body = new DBSPBlockExpression(List.of(operation.toStatement()), tail);
        var closure = body.closure(input);
        for (boolean observe : List.of(false, true)) {
            var facts = new HashMap<DBSPExpression, MonotoneExpression>();
            var result = RustCircuitPass.projectWaterlineClosure(compiler, body, closure.parameters,
                    new DBSPType[]{integer}, new IMaybeMonotoneType[]{new MonotoneRefType(shape)},
                    false, observe ? facts::put : null);
            Assert.assertTrue(result instanceof RustCircuitPass.ProjectedWaterline);
            Assert.assertTrue(tail.closure(input).equivalent(((RustCircuitPass.ProjectedWaterline) result).closure()));
            if (observe) {
                Assert.assertFalse(facts.get(operation).mayBeMonotone());
                Assert.assertSame(facts.get(tail), facts.get(body));
            }
        }
    }

    @Test
    public void onlyRegisteredFunctionsRequireArgumentWaterlines() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustLimiterApply"));
        var compiler = new DBSPCompiler(new CompilerOptions());
        var integer = new DBSPI32Literal(1).getType();
        var input = integer.ref().var();
        var shape = new MonotoneType(integer);
        var op = new DBSPConstantOperator(CalciteEmptyRel.INSTANCE, DBSPZSetExpression.emptyWithElementType(integer));
        var kind = MonotoneTransferFunctions.ArgumentKind.ZSet;
        var local = new DBSPLetStatement("scratch", integer, true);
        var read = local.getVarReference();
        var known = new DBSPApplyExpression("sign_i32", integer, read);
        var required = new DBSPBlockExpression(List.of(local, known.toStatement()), input.deref()).closure(input);
        var expected = Assert.assertThrows(RuntimeException.class,
                () -> new MonotoneTransferFunctions(compiler, op, kind, shape).apply(required));
        compiler.options.ioOptions.rustOptimizerPasses = true;
        var hybrid = new MonotoneTransferFunctions(compiler, op, kind, shape);
        var actual = Assert.assertThrows(RuntimeException.class, () -> hybrid.apply(required));
        Assert.assertEquals(expected.getClass(), actual.getClass());
        Assert.assertEquals(expected.getMessage(), actual.getMessage());

        compiler.options.ioOptions.rustOptimizerPasses = false;
        // The empty tuple has a monotone shape but no bound. Neither it nor
        // the uninitialized local is demanded by an unregistered function.
        var unit = new NoExpression(DBSPTypeRawTuple.EMPTY);
        var unknown = new DBSPApplyExpression("opaque", integer, read, unit);
        var tail = input.deref();
        var body = new DBSPBlockExpression(List.of(local, unknown.toStatement()), tail);
        var closure = body.closure(input);
        var direct = new MonotoneTransferFunctions(compiler, op, kind, shape);
        direct.apply(closure);
        for (boolean observe : List.of(false, true)) {
            var facts = new HashMap<DBSPExpression, MonotoneExpression>();
            var result = RustCircuitPass.projectWaterlineClosure(compiler, body, closure.parameters,
                    new DBSPType[]{integer}, new IMaybeMonotoneType[]{new MonotoneRefType(shape)},
                    false, observe ? facts::put : null);
            Assert.assertTrue(result instanceof RustCircuitPass.ProjectedWaterline);
            Assert.assertTrue(direct.get(closure).getReducedExpression().equivalent(
                    ((RustCircuitPass.ProjectedWaterline) result).closure()));
            if (observe) {
                Assert.assertFalse(facts.containsKey(read));
                Assert.assertTrue(facts.get(unit).mayBeMonotone());
                Assert.assertFalse(facts.get(unknown).mayBeMonotone());
                Assert.assertSame(facts.get(tail), facts.get(body));
            }
        }
    }

    @Test
    public void nullableReferencesStopWaterlineAnalysisAtTheRead() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustLimiterApply"));
        var compiler = new DBSPCompiler(new CompilerOptions());
        var site = CalciteEmptyRel.INSTANCE;
        var integer = new DBSPI32Literal(1).getType();
        var op = new DBSPConstantOperator(site, DBSPZSetExpression.emptyWithElementType(integer));
        for (boolean nullableRow : List.of(false, true)) {
            var reference = new MonotoneRefType(new MonotoneType(integer));
            var shape = new PartiallyMonotoneTuple(List.of(reference), false, nullableRow);
            var input = shape.getType().ref().var();
            var read = input.deref().field(0);
            var invalid = nullableRow ? read : read.some();
            var before = new DBSPI32Literal(2);
            var later = new DBSPI32Literal(3);
            var body = new DBSPBlockExpression(List.of(before.toStatement(), invalid.toStatement()), later);
            var closure = body.closure(input);
            var direct = new MonotoneTransferFunctions(compiler, op, MonotoneTransferFunctions.ArgumentKind.ZSet, shape);
            var expected = Assert.assertThrows(UnsupportedException.class, () -> direct.apply(closure));

            for (boolean resultOnly : List.of(false, true)) {
                var facts = new HashMap<DBSPExpression, MonotoneExpression>();
                var failure = Assert.assertThrows(UnsupportedException.class, () ->
                        RustCircuitPass.projectWaterlineClosure(compiler, body, closure.parameters,
                                new DBSPType[]{shape.getProjectedType()}, new IMaybeMonotoneType[]{new MonotoneRefType(shape)},
                                false, resultOnly, facts::put));
                Assert.assertEquals(expected.getMessage(), failure.getMessage());
                Assert.assertEquals(expected.getPositionRange(), failure.getPositionRange());
                Assert.assertTrue(facts.get(before).mayBeMonotone());
                Assert.assertEquals(!nullableRow, facts.containsKey(read));
                Assert.assertFalse(facts.containsKey(invalid));
                Assert.assertFalse(facts.containsKey(later));
            }
        }
    }

    @Test
    public void unsupportedWaterlineTypesPreserveTheFirstFieldAndEarlierFacts() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustLimiterApply"));
        var compiler = new DBSPCompiler(new CompilerOptions());
        var site = CalciteEmptyRel.INSTANCE;
        var before = new DBSPI32Literal(4);
        var integer = before.getType();
        var shape = new MonotoneType(integer);
        var input = integer.ref().var();
        var first = new DBSPTypeStruct(site, new ProgramIdentifier("First"), List.of(), false);
        var second = new DBSPTypeStruct(site, new ProgramIdentifier("Second"), List.of(), false);
        var nested = new DBSPTypeTuple(integer, first.ref(), second);
        var local = new DBSPLetStatement("scratch", integer, true);
        var op = new DBSPConstantOperator(site, DBSPZSetExpression.emptyWithElementType(integer));
        var kind = MonotoneTransferFunctions.ArgumentKind.ZSet;
        // A registered call checks its result type before demanding its argument's absent fact.
        for (var invalid : List.of(new NoExpression(nested),
                new DBSPApplyExpression("sign_i32", nested, local.getVarReference()))) {
            var later = input.deref();
            var body = new DBSPBlockExpression(List.of(local, before.toStatement(), invalid.toStatement()), later);
            var closure = body.closure(input);
            compiler.options.ioOptions.rustOptimizerPasses = false;
            var direct = new MonotoneTransferFunctions(compiler, op, kind, shape);
            var expected = Assert.assertThrows(UnsupportedException.class, () -> direct.apply(closure));
            Assert.assertEquals(Assert.assertThrows(UnsupportedException.class,
                    () -> NonMonotoneType.nonMonotone(first)).getMessage(), expected.getMessage());
            compiler.options.ioOptions.rustOptimizerPasses = true;
            var hybrid = new MonotoneTransferFunctions(compiler, op, kind, shape);
            var error = Assert.assertThrows(UnsupportedException.class, () -> hybrid.apply(closure));
            Assert.assertEquals(expected.getMessage(), error.getMessage());
            for (boolean resultOnly : List.of(false, true)) {
                var facts = new HashMap<DBSPExpression, MonotoneExpression>();
                var failure = Assert.assertThrows(UnsupportedException.class, () ->
                        RustCircuitPass.projectWaterlineClosure(compiler, body, closure.parameters,
                                new DBSPType[]{integer}, new IMaybeMonotoneType[]{new MonotoneRefType(shape)},
                                false, resultOnly, facts::put));
                Assert.assertEquals(expected.getMessage(), failure.getMessage());
                Assert.assertTrue(facts.get(before).mayBeMonotone());
                Assert.assertFalse(facts.containsKey(invalid));
                Assert.assertFalse(facts.containsKey(later));
            }
        }
    }

    @Test
    public void blockDeclarationsExposeIntermediateBoundsAndPreserveTheTail() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustLimiterApply"));
        var compiler = new DBSPCompiler(new CompilerOptions());
        var site = CalciteEmptyRel.INSTANCE;
        var literal = new DBSPI32Literal(4);
        var integer = literal.getType();
        var row = new MonotoneType(integer);
        var input = integer.ref().var();
        var tail = input.deref();
        var saved = new DBSPStaticExpression(site, literal, "saved");
        var comparator = new DBSPNoComparatorExpression(site, integer);
        var struct = new DBSPTypeStruct(site, new ProgramIdentifier("S"), List.of(), false);
        var body = new DBSPI32Literal(7);
        var function = new DBSPFunction(site, "f", List.of(integer.var().asParameter()), integer, body, List.of());
        var block = new DBSPBlockExpression(List.of(new DBSPStaticItem(saved), new DBSPComparatorItem(comparator),
                new DBSPStructItem(struct, null), new DBSPFunctionItem(function)), tail);
        var closure = block.closure(input);
        var op = new DBSPConstantOperator(site, DBSPZSetExpression.emptyWithElementType(integer));
        var direct = new MonotoneTransferFunctions(compiler, op, MonotoneTransferFunctions.ArgumentKind.ZSet, row);
        direct.apply(closure);
        var facts = new HashMap<DBSPExpression, MonotoneExpression>();
        var result = RustCircuitPass.projectWaterlineClosure(compiler, block, closure.parameters,
                new DBSPType[]{row.getProjectedType()}, new IMaybeMonotoneType[]{new MonotoneRefType(row)},
                false, facts::put);
        Assert.assertTrue(result instanceof RustCircuitPass.ProjectedWaterline);
        for (var expression : List.of(literal, saved, comparator, body, tail, block))
            Assert.assertEquals(direct.get(expression).mayBeMonotone(), facts.get(expression).mayBeMonotone());
        Assert.assertSame(facts.get(tail), facts.get(block));
        Assert.assertTrue(direct.get(closure).getReducedExpression().equivalent(
                ((RustCircuitPass.ProjectedWaterline) result).closure()));
    }

    @Test
    public void missingOperandsRetainFactsUpToTheFailingLookup() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustLimiterApply"));
        var compiler = new DBSPCompiler(new CompilerOptions());
        var integer = new DBSPI32Literal(1).getType();
        var input = integer.ref().var();
        var local = new DBSPLetStatement("uninitialized", integer, false);
        var read = local.getVarReference();
        var empty = new NoExpression(DBSPTypeRawTuple.EMPTY);
        var visited = input.deref();
        var later = new DBSPI32Literal(99);
        var op = new DBSPConstantOperator(CalciteEmptyRel.INSTANCE, DBSPZSetExpression.emptyWithElementType(integer));
        var shape = new MonotoneType(integer);
        var kind = MonotoneTransferFunctions.ArgumentKind.ZSet;

        // The tuple looks up every field's fact before reading any reduced
        // expressions. A registered function consumes one argument at a time.
        for (var consumer : List.of(new DBSPTupleExpression(empty, read),
                new DBSPApplyExpression("sign_i32", integer, empty, read))) {
            var body = new DBSPBlockExpression(List.of(local, visited.toStatement(), consumer.toStatement()), later);
            var closure = body.closure(input);
            var expectedOperand = consumer instanceof DBSPTupleExpression ? read : empty;
            var directVisitor = new MonotoneTransferFunctions(compiler, op, kind, shape);
            compiler.options.ioOptions.rustOptimizerPasses = false;
            var expected = Assert.assertThrows(RuntimeException.class, () -> directVisitor.apply(closure));
            for (boolean resultOnly : List.of(false, true)) {
                var facts = new HashMap<DBSPExpression, MonotoneExpression>();
                var failure = RustCircuitPass.projectWaterlineClosure(compiler, body, closure.parameters,
                        new DBSPType[]{integer}, new IMaybeMonotoneType[]{new MonotoneRefType(shape)},
                        false, resultOnly, facts::put);
                assertFailure(failure, RustCircuitPass.WaterlineError.MissingOperand, expectedOperand);
                Assert.assertTrue(directVisitor.get(visited).getReducedExpression().closure(input)
                        .equivalent(facts.get(visited).getReducedExpression().closure(input)));
                Assert.assertTrue(facts.containsKey(empty));
                Assert.assertFalse(facts.containsKey(read));
                Assert.assertFalse(facts.containsKey(later));
                compiler.options.ioOptions.rustOptimizerPasses = true;
                var hybrid = new MonotoneTransferFunctions(compiler, op, kind, shape);
                var actual = Assert.assertThrows(RuntimeException.class, () -> {
                    if (resultOnly)
                        hybrid.applyAnalysis(closure);
                    else
                        hybrid.apply(closure);
                });
                Assert.assertEquals(expected.getClass(), actual.getClass());
                Assert.assertEquals(expected.getMessage(), actual.getMessage());
                Assert.assertTrue(hybrid.get(empty).mayBeMonotone());
                Assert.assertNull(hybrid.maybeGet(later));
            }
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
    public void parameterTypesAreCheckedOnVisitedWaterlineReads() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustLimiterApply"));
        var compiler = new DBSPCompiler(new CompilerOptions());
        var site = CalciteEmptyRel.INSTANCE;
        var timestamp = DBSPTypeTimestamp.INSTANCE;
        var row = new PartiallyMonotoneTuple(List.of(new MonotoneType(timestamp)), false, false);
        var input = new DBSPVariablePath("row", row.getType().ref());
        var mistyped = new DBSPVariablePath("row", DBSPTypeBool.create(false));
        var nested = mistyped.closure();
        var key = input.deref().field(0);
        var body = new DBSPBlockExpression(List.of(nested.toStatement()), key);
        var closure = body.closure(input);
        var operator = new DBSPConstantOperator(site, DBSPZSetExpression.emptyWithElementType(row.getType()));
        var kind = MonotoneTransferFunctions.ArgumentKind.ZSet;
        var direct = new MonotoneTransferFunctions(compiler, operator, kind, row);
        direct.apply(closure);
        for (boolean observe : List.of(false, true)) {
            var facts = new HashMap<DBSPExpression, MonotoneExpression>();
            var result = RustCircuitPass.projectWaterlineClosure(compiler, body, closure.parameters,
                    new DBSPType[]{row.getProjectedType()}, new IMaybeMonotoneType[]{new MonotoneRefType(row)},
                    false, observe ? facts::put : null);
            Assert.assertTrue(result instanceof RustCircuitPass.ProjectedWaterline);
            Assert.assertTrue(direct.get(closure).getReducedExpression().equivalent(
                    ((RustCircuitPass.ProjectedWaterline) result).closure()));
            if (observe) {
                Assert.assertFalse(facts.get(nested).mayBeMonotone());
                Assert.assertFalse(facts.containsKey(mistyped));
                Assert.assertSame(facts.get(key), facts.get(body));
            }
        }

        var clock = new DBSPApplyExpression("now", timestamp);
        var comparison = new DBSPBinaryExpression(site, DBSPTypeBool.create(false), DBSPOpcode.GTE, body, clock);
        var terms = RustCircuitPass.inferTemporalPredicates(compiler, closure.parameters[0], comparison);
        Assert.assertNotNull(terms);
        Assert.assertEquals(1, terms.size());
        Assert.assertSame(body, terms.get(0).expression());
        Assert.assertSame(clock, terms.get(0).clock());

        var visited = mistyped.closure(input);
        for (boolean hybrid : List.of(false, true)) {
            compiler.options.ioOptions.rustOptimizerPasses = hybrid;
            var analyzer = new MonotoneTransferFunctions(compiler, operator, kind, row);
            var error = Assert.assertThrows(RuntimeException.class, () -> analyzer.apply(visited));
            Assert.assertTrue(error.getMessage().contains("Variable row(" + mistyped.getId() + ") type"));
            Assert.assertTrue(error.getMessage().contains("does not match expected type"));
        }
    }

    private static String variableTypeDiagnostic(RuntimeException error) {
        // The assertion appends the executing visitor's Java stack, and each
        // analysis constructs a fresh variable to represent a closure parameter.
        return error.getMessage().lines().findFirst().orElseThrow().replaceAll("\\(\\d+\\) with type", "(id) with type");
    }

    @Test
    public void mistypedWaterlineReadsPublishTheirDeclarationFactBeforeFailing() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustLimiterApply"));
        var compiler = new DBSPCompiler(new CompilerOptions());
        var initial = new DBSPI32Literal(5);
        var shape = new MonotoneType(initial.getType());
        var input = new DBSPVariablePath("row", initial.getType().ref());
        var local = new DBSPLetStatement("local", initial);
        var op = new DBSPConstantOperator(CalciteEmptyRel.INSTANCE, DBSPZSetExpression.emptyWithElementType(initial.getType()));
        for (String name : List.of("local", "row")) {
            var bad = new DBSPVariablePath(name, DBSPTypeBool.create(false));
            var later = new DBSPI32Literal(7);
            var body = new DBSPBlockExpression(List.of(local, bad.toStatement()), later);
            var closure = body.closure(input);
            var direct = new MonotoneTransferFunctions(compiler, op, MonotoneTransferFunctions.ArgumentKind.ZSet, shape);
            var expected = Assert.assertThrows(RuntimeException.class, () -> direct.apply(closure));
            Assert.assertTrue(expected.getMessage().contains("Variable " + name + "(" + bad.getId() + ") type"));
            for (boolean resultOnly : List.of(false, true)) {
                var facts = new HashMap<DBSPExpression, MonotoneExpression>();
                var failure = Assert.assertThrows(expected.getClass(), () -> RustCircuitPass.projectWaterlineClosure(
                        compiler, body, closure.parameters, new DBSPType[]{initial.getType()},
                        new IMaybeMonotoneType[]{new MonotoneRefType(shape)}, false, resultOnly, facts::put));
                Assert.assertTrue(facts.containsKey(initial));
                Assert.assertFalse(facts.containsKey(later));
                var recorded = facts.get(bad);
                Assert.assertNotNull(recorded);
                Assert.assertTrue(direct.get(bad).getMonotoneType().getType().sameType(recorded.getMonotoneType().getType()));
                String message = failure.getMessage().lines().findFirst().orElseThrow();
                if (name.equals("local")) {
                    Assert.assertSame(facts.get(initial), recorded);
                    Assert.assertEquals(expected.getMessage().lines().findFirst().orElseThrow(), message);
                } else {
                    Assert.assertEquals(variableTypeDiagnostic(expected), variableTypeDiagnostic(failure));
                }
            }
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
    public void tupleReturningCallsFailBeforePublishingTheirWaterline() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustLimiterApply"));
        var integer = new DBSPI32Literal(1).getType();
        var row = new MonotoneType(integer);
        var input = new DBSPVariablePath("input", integer.ref());
        var operator = new DBSPConstantOperator(CalciteEmptyRel.INSTANCE, DBSPZSetExpression.emptyWithElementType(integer));
        for (var tuple : List.of(new DBSPTypeTuple(integer), DBSPTypeRawTuple.EMPTY)) {
            for (var call : List.of(new DBSPApplyExpression("sign_i32", tuple, input.deref()),
                    new DBSPApplyExpression("now", tuple))) {
                var prefix = new DBSPI32Literal(7);
                var after = new DBSPI32Literal(8);
                var body = new DBSPBlockExpression(List.of(prefix.toStatement(), call.toStatement()), after);
                var closure = body.closure(input);
                var compiler = new DBSPCompiler(new CompilerOptions());
                var direct = new MonotoneTransferFunctions(compiler, operator, MonotoneTransferFunctions.ArgumentKind.ZSet, row);
                var expected = Assert.assertThrows(InternalCompilerError.class, () -> direct.apply(closure));
                Assert.assertTrue(direct.get(prefix).mayBeMonotone());
                var diagnostic = expected.getMessage().lines().findFirst().orElseThrow();
                Assert.assertTrue(diagnostic.contains("Type should have been scalar"));
                for (boolean resultOnly : List.of(false, true)) {
                    var facts = new HashMap<DBSPExpression, MonotoneExpression>();
                    var failure = Assert.assertThrows(InternalCompilerError.class,
                            () -> RustCircuitPass.projectWaterlineClosure(compiler, body, closure.parameters,
                                    new DBSPType[]{integer}, new IMaybeMonotoneType[]{new MonotoneRefType(row)},
                                    false, resultOnly, facts::put));
                    Assert.assertEquals(diagnostic, failure.getMessage().lines().findFirst().orElseThrow());
                    Assert.assertTrue(facts.get(prefix).mayBeMonotone());
                    Assert.assertFalse(facts.containsKey(call));
                    Assert.assertFalse(facts.containsKey(after));
                }
            }
        }
    }

    @Test
    public void referenceReturningCallsKeepTheirScalarWaterline() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustLimiterApply"));
        var compiler = new DBSPCompiler(new CompilerOptions());
        var integer = new DBSPI32Literal(1).getType();
        var row = new MonotoneType(integer);
        var input = new DBSPVariablePath("input", integer.ref());
        var call = new DBSPApplyExpression("sign_i32", integer.ref(), input.deref());
        var closure = call.closure(input);
        var operator = new DBSPConstantOperator(CalciteEmptyRel.INSTANCE, DBSPZSetExpression.emptyWithElementType(integer));
        var direct = new MonotoneTransferFunctions(compiler, operator, MonotoneTransferFunctions.ArgumentKind.ZSet, row);
        var expected = direct.applyAnalysis(closure).getReducedExpression();
        for (boolean resultOnly : List.of(false, true)) {
            var facts = new HashMap<DBSPExpression, MonotoneExpression>();
            var result = RustCircuitPass.projectWaterlineClosure(compiler, call, closure.parameters,
                    new DBSPType[]{integer}, new IMaybeMonotoneType[]{new MonotoneRefType(row)},
                    false, resultOnly, facts::put);
            Assert.assertTrue(result instanceof RustCircuitPass.ProjectedWaterline);
            var projected = (RustCircuitPass.ProjectedWaterline) result;
            Assert.assertTrue(projected.bodyType() instanceof MonotoneType);
            Assert.assertSame(call.getType(), projected.bodyType().getType());
            Assert.assertTrue(expected.equivalent(projected.closure()));
            if (!resultOnly)
                Assert.assertSame(call.getType(), facts.get(call).getMonotoneType().getType());
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
            for (DBSPExpression target : List.of(new DBSPUSizeLiteral(null, true), new DBSPISizeLiteral(null, true))) {
                var visited = input.deref();
                var block = new DBSPBlockExpression(List.of(visited.toStatement()), target);
                var function = block.closure(input);
                var unsupported = new MonotoneTransferFunctions(compiler, operator, MonotoneTransferFunctions.ArgumentKind.ZSet, row);
                var error = Assert.assertThrows(UnsupportedException.class, () -> unsupported.applyAnalysis(function));
                Assert.assertSame(target.getType().withMayBeNull(false).getNode(), error.calciteObject);
                Assert.assertTrue(unsupported.get(visited).mayBeMonotone());
                if (nativePass) {
                    for (boolean resultOnly : List.of(false, true)) {
                        var facts = new HashMap<DBSPExpression, MonotoneExpression>();
                        var failure = RustCircuitPass.projectWaterlineClosure(compiler, block, function.parameters,
                                new DBSPType[]{row.getProjectedType()}, new IMaybeMonotoneType[]{new MonotoneRefType(row)},
                                false, resultOnly, facts::put);
                        assertFailure(failure, RustCircuitPass.WaterlineError.TargetMaximum, target);
                        Assert.assertTrue(facts.containsKey(visited));
                        Assert.assertFalse(facts.containsKey(target));
                    }
                }
            }
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
    public void functionFieldsCountTowardsTheCompressedWaterline() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustLimiterApply"));
        var compiler = new DBSPCompiler(new CompilerOptions());
        var node = CalciteEmptyRel.INSTANCE;
        var integer = new DBSPI32Literal(0).getType();
        var bounded = new MonotoneType(integer);
        var operator = new DBSPConstantOperator(node, DBSPZSetExpression.emptyWithElementType(integer));
        for (IMaybeMonotoneType functionResult : List.of(bounded, new NonMonotoneType(integer))) {
            var function = new MonotoneClosureType(functionResult, new DBSPParameter[0], new DBSPParameter[0]);
            var row = new PartiallyMonotoneTuple(List.of(function, bounded), false, false);
            var input = new DBSPVariablePath("row", row.getType().ref());
            var bound = new DBSPVariablePath("row", row.getProjectedType().ref());
            for (int column : List.of(0, 1)) {
                var body = input.deref().field(column);
                var direct = new MonotoneTransferFunctions(compiler, operator, MonotoneTransferFunctions.ArgumentKind.ZSet,
                        row).applyAnalysis(body.closure(input));
                var projected = RustCircuitPass.projectRow(compiler, body, bound, row);
                Assert.assertNotNull(projected);
                Assert.assertSame(row.getFieldType(column), projected.getMonotoneType());
                if (column == 0 && !function.mayBeMonotone()) {
                    Assert.assertNull(direct);
                    Assert.assertFalse(projected.mayBeMonotone());
                } else {
                    var actual = projected.getReducedExpression().closure(bound);
                    Assert.assertTrue(direct.getReducedExpression().equivalent(actual));
                    int index = column == 1 && function.mayBeMonotone() ? 1 : 0;
                    Assert.assertTrue(bound.deref().field(index).closure(bound).equivalent(actual));
                }
            }
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
    public void emptyFieldsDoNotRequireAWaterlineUntilRead() {
        var compiler = new DBSPCompiler(new CompilerOptions());
        var site = CalciteEmptyRel.INSTANCE;
        var integer = new DBSPTypeInteger(site, 32, true, false);
        var row = new PartiallyMonotoneTuple(List.of(new MonotoneType(integer)), false, false);
        var input = new DBSPVariablePath("row", row.getType().ref());
        var unit = DBSPTypeRawTuple.EMPTY;
        var unknown = new NoExpression(new DBSPTypeTuple(unit, integer));
        var unboundedField = unknown.field(1);
        var loop = new DBSPForExpression("unused", input.deepCopy().deref(),
                new DBSPBlockExpression(List.of(), new NoExpression(unit)));
        var result = new DBSPTupleExpression(unboundedField, input.deepCopy().deref().field(0));
        var body = new DBSPBlockExpression(List.of(new DBSPExpressionStatement(loop)), result);
        var closure = body.closure(input);
        var operator = new DBSPConstantOperator(site, DBSPZSetExpression.emptyWithElementType(integer));
        var kind = MonotoneTransferFunctions.ArgumentKind.ZSet;
        var direct = new MonotoneTransferFunctions(compiler, operator, kind, row);
        direct.apply(closure);
        Assert.assertTrue(direct.get(unknown).mayBeMonotone());
        Assert.assertTrue(direct.get(loop).mayBeMonotone());
        Assert.assertFalse(direct.get(unboundedField).mayBeMonotone());
        Assert.assertThrows(NullPointerException.class, () -> direct.get(unknown).getReducedExpression());

        if (Boolean.getBoolean("feldera.optimizer.compareRustLimiterApply")) {
            var facts = new HashMap<DBSPExpression, MonotoneExpression>();
            var nativeResult = RustCircuitPass.projectWaterlineClosure(compiler, body, closure.parameters,
                    new DBSPType[]{row.getProjectedType()}, new IMaybeMonotoneType[]{new MonotoneRefType(row)},
                    false, facts::put);
            Assert.assertTrue(nativeResult instanceof RustCircuitPass.ProjectedWaterline);
            Assert.assertTrue(direct.get(closure).getReducedExpression().equivalent(
                    ((RustCircuitPass.ProjectedWaterline) nativeResult).closure()));
            Assert.assertTrue(facts.get(loop).mayBeMonotone());
            Assert.assertFalse(facts.get(unboundedField).mayBeMonotone());
            Assert.assertThrows(NullPointerException.class, () -> facts.get(unknown).getReducedExpression());
        }

        // Selecting the empty field itself does demand the absent operand bound.
        var demanding = unknown.field(0).closure(input);
        for (boolean hybrid : List.of(false, true)) {
            if (hybrid && !Boolean.getBoolean("feldera.optimizer.compareRustLimiterApply"))
                continue;
            compiler.options.ioOptions.rustOptimizerPasses = hybrid;
            Assert.assertThrows(NullPointerException.class, () ->
                    new MonotoneTransferFunctions(compiler, operator, kind, row).applyAnalysis(demanding));
            if (hybrid) {
                for (boolean resultOnly : List.of(false, true)) {
                    var facts = new HashMap<DBSPExpression, MonotoneExpression>();
                    var failure = RustCircuitPass.projectWaterlineClosure(compiler, demanding.body, demanding.parameters,
                            new DBSPType[]{row.getProjectedType()}, new IMaybeMonotoneType[]{new MonotoneRefType(row)},
                            false, resultOnly, facts::put);
                    assertFailure(failure, RustCircuitPass.WaterlineError.MissingOperand, unknown);
                    Assert.assertTrue(facts.get(unknown).mayBeMonotone());
                }
            }
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
