package org.dbsp.sqlCompiler.compiler.visitors.outer.temporal;

import org.apache.calcite.util.TimestampWithTimeZoneString;
import org.dbsp.sqlCompiler.compiler.visitors.inner.EquivalenceContext;
import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.circuit.operator.DBSPConstantOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPFilterOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapOperator;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.ProgramMetadata;
import org.dbsp.sqlCompiler.compiler.backend.RustCircuitPass;
import org.dbsp.sqlCompiler.compiler.backend.ToJsonOuterVisitor.OptimizerSnapshots;
import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.errors.UnsupportedException;
import org.dbsp.sqlCompiler.compiler.errors.SourcePosition;
import org.dbsp.sqlCompiler.compiler.errors.SourcePositionRange;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteEmptyRel;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.ProgramIdentifier;
import org.dbsp.sqlCompiler.compiler.visitors.monotone.IMaybeMonotoneType;
import org.dbsp.sqlCompiler.compiler.visitors.monotone.MonotoneExpression;
import org.dbsp.sqlCompiler.compiler.visitors.monotone.MonotoneRefType;
import org.dbsp.sqlCompiler.compiler.visitors.monotone.MonotoneTransferFunctions;
import org.dbsp.sqlCompiler.compiler.visitors.monotone.MonotoneType;
import org.dbsp.sqlCompiler.compiler.visitors.monotone.PartiallyMonotoneTuple;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitTransform;
import org.dbsp.sqlCompiler.compiler.visitors.outer.Graph;
import org.dbsp.sqlCompiler.compiler.visitors.outer.WindowInputStats;
import org.dbsp.sqlCompiler.compiler.visitors.outer.expandCasts.ExpandCasts;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.sql.tools.ExpressionBuilder;
import org.dbsp.sqlCompiler.ir.aggregate.DBSPMinMax;
import org.dbsp.sqlCompiler.ir.expression.DBSPApplyExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPFailExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPArrayExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPAssignmentExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPAsymmetricFieldComparatorExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPCustomOrdExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPBinaryExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPBlockExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPCastExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPConditionalIncrementExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPConstructorExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPDirectComparatorExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPEqualityComparatorExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPFieldComparatorExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPNoComparatorExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPSortExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPPathExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPUnaryExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPUnsignedUnwrapExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPUnsignedWrapExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPFlatmap;
import org.dbsp.sqlCompiler.ir.expression.DBSPMapExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPLazyExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPStaticExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPForExpression;
import org.dbsp.sqlCompiler.ir.expression.NoExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPHandleErrorExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPIsNullExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPUnwrapExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPLetExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPOpcode;
import org.dbsp.sqlCompiler.ir.expression.DBSPQualifyTypeExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPRawTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPGeoPointConstructor;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPReturnExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.expression.DBSPWindowBoundExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPZSetExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPBoolLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPI32Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPUSizeLiteral;
import org.dbsp.sqlCompiler.ir.path.DBSPPath;
import org.dbsp.sqlCompiler.ir.path.DBSPSimplePathSegment;
import org.dbsp.sqlCompiler.ir.statement.DBSPComment;
import org.dbsp.sqlCompiler.ir.statement.DBSPLetStatement;
import org.dbsp.sqlCompiler.ir.statement.DBSPStaticItem;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeCode;
import org.dbsp.sqlCompiler.ir.type.ZSetShape;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeFunction;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeRawTuple;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeStruct;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPTimestampTzLiteral;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeInteger;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeTimestamp;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeStr;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeVoid;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeTimestampTz;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeArray;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeMap;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeSqlResult;
import org.dbsp.sqlCompiler.ir.type.user.DBSPComparatorType;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeStream;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeUser;
import org.dbsp.sqlCompiler.ir.type.user.StreamKind;
import org.dbsp.util.IdShuffle;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.function.BiFunction;
import java.util.function.Consumer;

public class TemporalKeyTests {
    private static List<BooleanExpression> longPredicate() {
        var b = new ExpressionBuilder();
        var shared = b.call(b.bool(), "shared");
        var terms = new ArrayList<BooleanExpression>();
        for (int i = 0; i < 192; i++)
            terms.add(new NoNow(i % 2 == 0 ? shared : b.call(b.bool(), "predicate", b.lit(i))));
        return terms;
    }

    private static NoNow combinedPredicate(List<BooleanExpression> terms) {
        Assert.assertEquals(1, terms.size());
        return terms.get(0).to(NoNow.class);
    }

    private static void assertSameConjuncts(DBSPExpression expected, DBSPExpression actual) {
        var left = expected.conjuncts();
        var right = actual.conjuncts();
        Assert.assertEquals(left.size(), right.size());
        for (int i = 0; i < left.size(); i++)
            Assert.assertSame(left.get(i), right.get(i));
    }

    @Test
    public void groupingReturnsEveryLeafOfALongTemporalConjunction() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustTemporalKeys"));
        var compiler = new DBSPCompiler(new CompilerOptions());
        var terms = longPredicate();
        var direct = combinedPredicate(RewriteNow.combineExpressions(terms));
        var hybrid = combinedPredicate(NativeTemporalComparisons.combine(compiler, terms));
        assertSameConjuncts(direct.noNow(), hybrid.noNow());
    }

    @Test
    public void classificationRebuildsALongClockFreeSuffix() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustTemporalKeys"));
        var compiler = new DBSPCompiler(new CompilerOptions());
        var body = combinedPredicate(RewriteNow.combineExpressions(longPredicate())).noNow();
        var parts = RustCircuitPass.classifyTemporalPredicates(compiler, body, Set.of());
        Assert.assertEquals(1, parts.size());
        Assert.assertEquals(RustCircuitPass.PredicateKind.NoNow, parts.get(0).kind());
        assertSameConjuncts(body, parts.get(0).expression());
    }

    private static MergeFilters nativeMergeOnly(DBSPCompiler compiler) {
        return new MergeFilters(compiler) {
            @Override
            public String getName() { return "MergeFilters"; }

            @Override
            public boolean canBeMerged(DBSPFilterOperator source, DBSPClosureExpression upstream,
                                       DBSPFilterOperator filter, DBSPClosureExpression downstream) {
                throw new AssertionError("delegated a temporal merge decision to Java");
            }

            @Override
            public void postorder(DBSPFilterOperator filter) { Assert.fail("restarted the Java filter pass"); }
        };
    }

    private static DBSPCircuit temporalFilterChain(DBSPExpression lastClock) {
        var b = new ExpressionBuilder();
        var site = CalciteEmptyRel.INSTANCE;
        var circuit = new DBSPCircuit(new ProgramMetadata(), false);
        var rowType = b.tup(b.i32(), b.i32(), new DBSPTypeArray(b.i32(), false));
        var input = new DBSPConstantOperator(site, DBSPZSetExpression.emptyWithElementType(rowType));
        circuit.addOperator(input);
        var rows = input.outputPort();
        for (var clock : List.of(b.call(DBSPTypeTimestamp.INSTANCE, "now"),
                b.call(DBSPTypeTimestamp.INSTANCE, "now"), lastClock)) {
            var row = b.refVar(rowType);
            var key = b.call(DBSPTypeTimestamp.INSTANCE, "timestamp_key", b.field(row, 0));
            var filter = new DBSPFilterOperator(site, b.binary(DBSPOpcode.GTE, key, clock).closure(row), rows);
            circuit.addOperator(filter);
            rows = filter.outputPort();
        }
        return circuit;
    }

    @Test
    public void clockFilterLoweringReportsTheFailingConjunctAfterEarlierWindows() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustTemporalKeys"));
        var b = new ExpressionBuilder();
        var time = DBSPTypeTimestamp.INSTANCE;
        var invalid = b.call(b.i32(), "sign_i32");
        var clock = new DBSPBlockExpression(List.of(invalid.toStatement()), b.call(time, "now"));
        var circuit = temporalFilterChain(clock);
        var compiler = new DBSPCompiler(new CompilerOptions());
        compiler.options.ioOptions.nowStream = false;
        long start = DBSPVariablePath.nextGeneratedName();
        var direct = Assert.assertThrows(IndexOutOfBoundsException.class, () -> new RewriteNow(compiler).apply(circuit));
        long names = DBSPVariablePath.nextGeneratedName() - start;
        compiler.options.ioOptions.rustOptimizerPasses = true;
        var pass = new RewriteNow(compiler) {
            @Override
            public List<BooleanExpression> findTemporalFilters(DBSPFilterOperator filter, DBSPClosureExpression predicate) {
                throw new AssertionError("reclassified a clock predicate in Java");
            }
        };
        start = DBSPVariablePath.nextGeneratedName();
        var hybrid = Assert.assertThrows(direct.getClass(), () -> pass.apply(circuit));
        Assert.assertEquals(direct.getMessage(), hybrid.getMessage());
        Assert.assertEquals(names, DBSPVariablePath.nextGeneratedName() - start);
        Assert.assertEquals(2, pass.nativeClockFilters());
    }

    @Test
    public void windowScanReportsTheFirstInvalidClockOperand() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustTemporalKeys"));
        var b = new ExpressionBuilder();
        var site = CalciteEmptyRel.INSTANCE;
        var time = DBSPTypeTimestamp.INSTANCE;
        for (boolean scalarInput : List.of(false, true)) {
            var invalid = b.call(b.i32(), "sign_i32");
            var clock = new DBSPBlockExpression(List.of(invalid.toStatement()), b.call(time, "now"));
            var circuit = temporalFilterChain(scalarInput ? b.call(time, "now") : clock);
            if (scalarInput) {
                var source = new DBSPConstantOperator(site, DBSPZSetExpression.emptyWithElementType(time));
                circuit.addOperator(source);
                var row = b.refVar(time);
                var predicate = b.binary(DBSPOpcode.GTE, row.deref(), b.call(time, "now"));
                circuit.addOperator(new DBSPFilterOperator(site, predicate.closure(row), source.outputPort()));
            }
            var compiler = new DBSPCompiler(new CompilerOptions());
            var decisions = new ArrayList<Boolean>();
            var expected = scalarInput ? NullPointerException.class : IndexOutOfBoundsException.class;
            long start = DBSPVariablePath.nextGeneratedName();
            var direct = Assert.assertThrows(expected, () -> scanWindows(compiler, circuit, decisions));
            long names = DBSPVariablePath.nextGeneratedName() - start;
            compiler.options.ioOptions.rustOptimizerPasses = true;
            start = DBSPVariablePath.nextGeneratedName();
            var hybrid = Assert.assertThrows(expected, () -> scanWindows(compiler, circuit, decisions));
            Assert.assertEquals(direct.getMessage(), hybrid.getMessage());
            Assert.assertEquals(names, DBSPVariablePath.nextGeneratedName() - start);
            Assert.assertEquals(List.of(true), decisions);
        }
    }

    @Test
    public void reorderChecksTheSharedConjunctBeforeTheOtherClockOperands() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustTemporalKeys"));
        var b = new ExpressionBuilder();
        var site = CalciteEmptyRel.INSTANCE;
        var time = DBSPTypeTimestamp.INSTANCE;
        for (boolean scalar : List.of(false, true)) {
            var circuit = new DBSPCircuit(new ProgramMetadata(), false);
            var rowType = scalar ? time : b.tup(time);
            var input = new DBSPConstantOperator(site, DBSPZSetExpression.emptyWithElementType(rowType));
            circuit.addOperator(input);
            var row = b.refVar(rowType);
            var key = scalar ? row.deref() : b.field(row, 0);
            var allocates = flatmap(new IdShuffle(2), 0, b.lit(1));
            var firstClock = new DBSPBlockExpression(List.of(allocates.toStatement()), b.call(time, "now"));
            var secondClock = new DBSPBlockExpression(List.of(b.call(b.i32(), "sign_i32").toStatement()), b.call(time, "now"));
            var first = b.binary(DBSPOpcode.GTE, key, firstClock);
            var second = b.binary(DBSPOpcode.LT, key.deepCopy(), secondClock);
            var filter = new DBSPFilterOperator(site, b.binary(DBSPOpcode.AND, first, second).closure(row), input.outputPort());
            circuit.addOperator(filter);
            var counts = new ArrayList<Long>();
            for (var chosen : List.of(Map.<DBSPFilterOperator, Integer>of(), Map.of(filter, 0), Map.of(filter, 1))) {
                var compiler = new DBSPCompiler(new CompilerOptions());
                var pass = new ReorderTemporalFilters.Reorder(compiler, chosen);
                long start = DBSPVariablePath.nextGeneratedName();
                Class<? extends RuntimeException> error = scalar ? NullPointerException.class : IndexOutOfBoundsException.class;
                var expected = Assert.assertThrows(error, () -> pass.apply(circuit));
                long names = DBSPVariablePath.nextGeneratedName() - start;
                counts.add(names);
                compiler.options.ioOptions.rustOptimizerPasses = true;
                var decisions = new LinkedHashMap<String, Boolean>();
                compiler.stateObserver = new DBSPCompiler.StateObserver() {
                    @Override public void nativeDecision(String op, boolean success) { decisions.put(op, success); }
                    @Override public void before(String p, int i, int n, CircuitTransform t, DBSPCircuit graph) {}
                    @Override public void after(String p, int i, int n, CircuitTransform t, DBSPCircuit graph, boolean done) {}
                };
                var hybrid = new RustCircuitPass(compiler, pass, new OptimizerSnapshots(compiler));
                start = DBSPVariablePath.nextGeneratedName();
                var actual = Assert.assertThrows(expected.getClass(), () -> hybrid.apply(circuit));
                Assert.assertEquals(expected.getMessage(), actual.getMessage());
                Assert.assertEquals(names, DBSPVariablePath.nextGeneratedName() - start);
                Assert.assertEquals(Boolean.TRUE, decisions.get("WindowGroups/Reorder"));
            }
            if (!scalar) {
                Assert.assertTrue(counts.get(1) > counts.get(0));
                Assert.assertTrue(counts.get(0) > counts.get(2));
            }
        }
    }

    @Test
    public void unsharedReorderReportsALaterClockBeforePrintingWindowKeys() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustTemporalKeys"));
        var b = new ExpressionBuilder();
        var site = CalciteEmptyRel.INSTANCE;
        var time = DBSPTypeTimestamp.INSTANCE;
        var circuit = new DBSPCircuit(new ProgramMetadata(), false);
        var rowType = b.tup(time);
        var input = new DBSPConstantOperator(site, DBSPZSetExpression.emptyWithElementType(rowType));
        circuit.addOperator(input);
        var row = b.refVar(rowType);
        // Printing this key would fail while renaming a declaration in a static initializer.
        var initializer = new DBSPBlockExpression(List.of(new DBSPLetStatement("offset", b.lit(1))), b.lit(2));
        var key = b.call(time, "shift_timestamp", b.field(row, 0), new DBSPStaticExpression(site, initializer, "saved"));
        var first = b.binary(DBSPOpcode.GTE, key, b.call(time, "now"));
        var invalid = new DBSPBlockExpression(List.of(b.call(b.i32(), "sign_i32").toStatement()), b.call(time, "now"));
        var second = b.binary(DBSPOpcode.LT, b.field(row, 0), invalid);
        circuit.addOperator(new DBSPFilterOperator(site, b.binary(DBSPOpcode.AND, first, second).closure(row), input.outputPort()));
        var compiler = new DBSPCompiler(new CompilerOptions());
        var pass = new ReorderTemporalFilters.Reorder(compiler, Map.of()) {
            @Override public String getName() { return "Reorder"; }
            @Override public TemporalOrder classify(DBSPFilterOperator filter) {
                throw new AssertionError("delegated an unshared partition before it needed a fingerprint");
            }
        };
        long start = DBSPVariablePath.nextGeneratedName();
        var expected = Assert.assertThrows(IndexOutOfBoundsException.class, () -> pass.apply(circuit));
        long names = DBSPVariablePath.nextGeneratedName() - start;
        compiler.options.ioOptions.rustOptimizerPasses = true;
        var hybrid = new RustCircuitPass(compiler, pass, new OptimizerSnapshots(compiler));
        start = DBSPVariablePath.nextGeneratedName();
        var actual = Assert.assertThrows(expected.getClass(), () -> hybrid.apply(circuit));
        Assert.assertEquals(expected.getMessage(), actual.getMessage());
        Assert.assertEquals(names, DBSPVariablePath.nextGeneratedName() - start);
    }

    @Test
    public void liveFilterClassificationRequiresAProjectedRow() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustTemporalKeys"));
        var b = new ExpressionBuilder();
        var time = DBSPTypeTimestamp.INSTANCE;
        for (boolean scalar : List.of(false, true)) {
            var rowType = scalar ? time : b.tup(time);
            var row = b.refVar(rowType);
            var key = scalar ? row.deref() : b.field(row, 0);
            var clock = b.call(time, "now");
            var condition = b.binary(DBSPOpcode.GTE, key, clock).closure(row);
            var site = CalciteEmptyRel.INSTANCE;
            var input = new DBSPConstantOperator(site, DBSPZSetExpression.emptyWithElementType(rowType));
            var filter = new DBSPFilterOperator(site, condition, input.outputPort());
            var compiler = new DBSPCompiler(new CompilerOptions());
            var detached = RustCircuitPass.inferTemporalPredicates(compiler, condition.parameters[0], condition.body);
            Assert.assertNotNull(detached);
            Assert.assertSame(key, detached.get(0).expression());
            for (boolean hybrid : List.of(false, true)) {
                compiler.options.ioOptions.rustOptimizerPasses = hybrid;
                long start = DBSPVariablePath.nextGeneratedName();
                if (scalar) {
                    Assert.assertThrows(NullPointerException.class,
                            () -> FindComparisons.decomposeIntoTemporalFilters(compiler, filter, condition));
                } else {
                    var parts = FindComparisons.decomposeIntoTemporalFilters(compiler, filter, condition);
                    Assert.assertEquals(1, parts.size());
                    var bound = parts.get(0).to(TemporalFilter.class);
                    Assert.assertSame(key, bound.noNow());
                    Assert.assertSame(clock, bound.withNow());
                }
                Assert.assertEquals(start, DBSPVariablePath.nextGeneratedName());
            }
        }
    }

    @Test
    public void windowScanKeepsAnEarlierConjunctsCastWarning() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustTemporalKeys"));
        var b = new ExpressionBuilder();
        var site = CalciteEmptyRel.INSTANCE;
        var time = DBSPTypeTimestamp.INSTANCE;
        var range = new SourcePositionRange(new SourcePosition(2, 3), new SourcePosition(2, 18));
        var circuit = new DBSPCircuit(new ProgramMetadata(), false);
        var rowType = b.tup(time);
        var input = new DBSPConstantOperator(site, DBSPZSetExpression.emptyWithElementType(rowType));
        circuit.addOperator(input);
        var value = b.tuple(b.lit("invalid timestamp"));
        var map = new DBSPMapOperator(site, value.closure(b.refVar(rowType)), input.outputPort());
        circuit.addOperator(map);
        var row = b.refVar(value.getType());
        var key = new DBSPCastExpression(CalciteObject.create(range), b.field(row, 0), time,
                DBSPCastExpression.CastType.SqlUnsafe);
        var first = b.binary(DBSPOpcode.GTE, key, b.call(time, "now"));
        var invalid = new DBSPBlockExpression(List.of(b.call(b.i32(), "sign_i32").toStatement()), b.call(time, "now"));
        var second = b.binary(DBSPOpcode.LT, key.deepCopy(), invalid);
        var filter = new DBSPFilterOperator(site, b.binary(DBSPOpcode.AND, first, second).closure(row), map.outputPort());
        circuit.addOperator(filter);
        for (boolean hybrid : List.of(false, true)) {
            var compiler = new DBSPCompiler(new CompilerOptions());
            compiler.options.ioOptions.rustOptimizerPasses = hybrid;
            var decisions = new ArrayList<Boolean>();
            Assert.assertThrows(IndexOutOfBoundsException.class, () -> scanWindows(compiler, circuit, decisions));
            Assert.assertEquals(hybrid ? List.of(true) : List.of(), decisions);
            Assert.assertEquals(1, compiler.messages.messages.size());
            var warning = compiler.messages.messages.get(0);
            Assert.assertEquals(range, warning.range);
            Assert.assertEquals("Not a TIMESTAMP", warning.errorType);
        }
    }

    @Test
    public void temporalMergeChecksScalarInputBeforeEvaluatingClockBounds() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustTemporalKeys"));
        var b = new ExpressionBuilder();
        var time = DBSPTypeTimestamp.INSTANCE;
        var circuit = temporalFilterChain(b.call(time, "now"));
        var source = new DBSPConstantOperator(CalciteEmptyRel.INSTANCE, DBSPZSetExpression.emptyWithElementType(time));
        circuit.addOperator(source);
        var rows = source.outputPort();
        for (int i = 0; i < 2; i++) {
            var row = b.refVar(time);
            var predicate = b.binary(DBSPOpcode.GTE, row.deref(), b.call(time, "now"));
            var filter = new DBSPFilterOperator(CalciteEmptyRel.INSTANCE, predicate.closure(row), rows);
            circuit.addOperator(filter);
            rows = filter.outputPort();
        }
        var compiler = new DBSPCompiler(new CompilerOptions());
        long start = DBSPVariablePath.nextGeneratedName();
        var direct = Assert.assertThrows(NullPointerException.class, () -> new MergeFilters(compiler).apply(circuit));
        long names = DBSPVariablePath.nextGeneratedName() - start;
        Assert.assertTrue(names > 0);
        compiler.options.ioOptions.rustOptimizerPasses = true;
        var pass = new RustCircuitPass(compiler, nativeMergeOnly(compiler), new OptimizerSnapshots(compiler));
        start = DBSPVariablePath.nextGeneratedName();
        var hybrid = Assert.assertThrows(direct.getClass(), () -> pass.apply(circuit));
        Assert.assertEquals(direct.getMessage(), hybrid.getMessage());
        Assert.assertEquals(names, DBSPVariablePath.nextGeneratedName() - start);
    }

    @Test
    public void temporalMergeValidatesTheInputRowBeforeExaminingItsClock() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustTemporalKeys"));
        var b = new ExpressionBuilder();
        var site = CalciteEmptyRel.INSTANCE;
        var invalid = new DBSPTypeRawTuple(site, true, List.of(b.i32()));
        var rowType = b.tup(b.i32(), invalid);
        var circuit = new DBSPCircuit(new ProgramMetadata(), false);
        var input = new DBSPConstantOperator(site, DBSPZSetExpression.emptyWithElementType(rowType));
        circuit.addOperator(input);
        var rows = input.outputPort();
        for (int i = 0; i < 2; i++) {
            var row = b.refVar(rowType);
            var key = b.call(DBSPTypeTimestamp.INSTANCE, "timestamp_key", b.field(row, 0));
            var clock = b.call(DBSPTypeTimestamp.INSTANCE, "now");
            var filter = new DBSPFilterOperator(site, b.binary(DBSPOpcode.GTE, key, clock).closure(row), rows);
            circuit.addOperator(filter);
            rows = filter.outputPort();
        }
        var compiler = new DBSPCompiler(new CompilerOptions());
        long start = DBSPVariablePath.nextGeneratedName();
        var direct = Assert.assertThrows(InternalCompilerError.class, () -> new MergeFilters(compiler).apply(circuit));
        long names = DBSPVariablePath.nextGeneratedName() - start;
        compiler.options.ioOptions.rustOptimizerPasses = true;
        var pass = new RustCircuitPass(compiler, nativeMergeOnly(compiler), new OptimizerSnapshots(compiler));
        start = DBSPVariablePath.nextGeneratedName();
        var hybrid = Assert.assertThrows(direct.getClass(), () -> pass.apply(circuit));
        Assert.assertEquals(direct.getMessage().lines().findFirst(), hybrid.getMessage().lines().findFirst());
        Assert.assertEquals(names, DBSPVariablePath.nextGeneratedName() - start);
    }

    @Test
    public void temporalMergeReportsATupleClockAfterAnEarlierMerge() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustTemporalKeys"));
        var b = new ExpressionBuilder();
        var bad = b.call(DBSPTypeRawTuple.EMPTY, "now");
        var clock = new DBSPBlockExpression(List.of(bad.toStatement()), b.call(DBSPTypeTimestamp.INSTANCE, "now"));
        var circuit = temporalFilterChain(clock);
        var compiler = new DBSPCompiler(new CompilerOptions());
        long start = DBSPVariablePath.nextGeneratedName();
        var direct = Assert.assertThrows(InternalCompilerError.class, () -> new MergeFilters(compiler).apply(circuit));
        long names = DBSPVariablePath.nextGeneratedName() - start;
        compiler.options.ioOptions.rustOptimizerPasses = true;
        start = DBSPVariablePath.nextGeneratedName();
        var pass = new RustCircuitPass(compiler, nativeMergeOnly(compiler), new OptimizerSnapshots(compiler));
        var hybrid = Assert.assertThrows(InternalCompilerError.class, () -> pass.apply(circuit));
        var diagnostic = direct.getMessage().lines().findFirst().orElseThrow();
        Assert.assertTrue(diagnostic.contains("Type should have been scalar"));
        Assert.assertEquals(diagnostic, hybrid.getMessage().lines().findFirst().orElseThrow());
        Assert.assertEquals(names, DBSPVariablePath.nextGeneratedName() - start);
    }

    @Test
    public void temporalMergeChecksTheCopiedPredicateBeforeReadingItsClock() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustTemporalKeys"));
        var b = new ExpressionBuilder();
        var clock = b.call(DBSPTypeTimestamp.INSTANCE, "now");
        var returned = new DBSPReturnExpression(CalciteEmptyRel.INSTANCE, clock);
        var lastClock = new DBSPBlockExpression(List.of(returned.toStatement(), returned.toStatement()),
                b.call(DBSPTypeTimestamp.INSTANCE, "now"));
        var circuit = temporalFilterChain(lastClock);
        var compiler = new DBSPCompiler(new CompilerOptions());
        long directNames = 0;
        for (boolean hybrid : List.of(false, true)) {
            compiler.options.ioOptions.rustOptimizerPasses = hybrid;
            var reference = new MergeFilters(compiler);
            var pass = hybrid ? new RustCircuitPass(compiler, nativeMergeOnly(compiler),
                    new OptimizerSnapshots(compiler)) : reference;
            long start = DBSPVariablePath.nextGeneratedName();
            var failure = Assert.assertThrows(RuntimeException.class, () -> pass.apply(circuit));
            Assert.assertTrue(failure.getMessage(), failure.getMessage().contains(
                    "contains multiple instances of " + clock + " " + clock.getId() + " context:"));
            long names = DBSPVariablePath.nextGeneratedName() - start;
            if (hybrid)
                Assert.assertEquals(directNames, names);
            directNames = names;
        }
    }

    @Test
    public void temporalMergePreservesAnAbsentBoundAfterAnEarlierMerge() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustTemporalKeys"));
        var b = new ExpressionBuilder();
        var empty = new NoExpression(DBSPTypeRawTuple.EMPTY);
        var missing = new DBSPBinaryExpression(CalciteEmptyRel.INSTANCE, b.i32(), DBSPOpcode.MAX,
                new NoExpression(b.i32()), empty);
        var lastClock = new DBSPBlockExpression(List.of(missing.toStatement()), b.call(DBSPTypeTimestamp.INSTANCE, "now"));
        var circuit = temporalFilterChain(lastClock);
        var compiler = new DBSPCompiler(new CompilerOptions());
        long start = DBSPVariablePath.nextGeneratedName();
        var direct = Assert.assertThrows(NullPointerException.class, () -> new MergeFilters(compiler).apply(circuit));
        long names = DBSPVariablePath.nextGeneratedName() - start;
        compiler.options.ioOptions.rustOptimizerPasses = true;
        start = DBSPVariablePath.nextGeneratedName();
        var pass = new RustCircuitPass(compiler, nativeMergeOnly(compiler), new OptimizerSnapshots(compiler));
        var hybrid = Assert.assertThrows(direct.getClass(), () -> pass.apply(circuit));
        Assert.assertEquals(direct.getMessage(), hybrid.getMessage());
        Assert.assertEquals(names, DBSPVariablePath.nextGeneratedName() - start);
    }

    @Test
    public void temporalMergeReportsAnUninitializedClockWithoutReplayingTheGraph() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustTemporalKeys"));
        var b = new ExpressionBuilder();
        var time = DBSPTypeTimestamp.INSTANCE;
        var missing = new DBSPLetStatement("missing", time, true);
        var lastClock = new DBSPBlockExpression(List.of(missing, new DBSPLetStatement("clock", b.call(time, "now"))),
                missing.getVarReference());
        var circuit = temporalFilterChain(lastClock);
        var compiler = new DBSPCompiler(new CompilerOptions());
        long start = DBSPVariablePath.nextGeneratedName();
        var direct = Assert.assertThrows(RuntimeException.class, () -> new MergeFilters(compiler).apply(circuit));
        long names = DBSPVariablePath.nextGeneratedName() - start;
        compiler.options.ioOptions.rustOptimizerPasses = true;
        start = DBSPVariablePath.nextGeneratedName();
        var pass = new RustCircuitPass(compiler, nativeMergeOnly(compiler), new OptimizerSnapshots(compiler));
        var hybrid = Assert.assertThrows(direct.getClass(), () -> pass.apply(circuit));
        Assert.assertEquals(direct.getMessage(), hybrid.getMessage());
        Assert.assertEquals(names, DBSPVariablePath.nextGeneratedName() - start);
    }

    @Test
    public void temporalMergeReportsInvalidCallArgumentsAndLiteralMaximaInOneVisit() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustTemporalKeys"));
        var b = new ExpressionBuilder();
        var time = DBSPTypeTimestamp.INSTANCE;
        for (var invalid : List.of(b.call(b.i32(), "sign_i32"),
                b.call(b.i32(), "sign_i32", b.lit(1), new NoExpression(b.i32())), new DBSPUSizeLiteral(null, true))) {
            var nested = flatmap(new IdShuffle(2), 0, b.lit(1));
            var clock = new DBSPBlockExpression(List.of(nested.toStatement(), invalid.toStatement()), b.call(time, "now"));
            var circuit = temporalFilterChain(clock);
            var compiler = new DBSPCompiler(new CompilerOptions());
            long start = DBSPVariablePath.nextGeneratedName();
            var expected = Assert.assertThrows(RuntimeException.class, () -> new MergeFilters(compiler).apply(circuit));
            long names = DBSPVariablePath.nextGeneratedName() - start;
            if (invalid instanceof DBSPApplyExpression call) {
                if (call.arguments.length == 0)
                    Assert.assertEquals(IndexOutOfBoundsException.class, expected.getClass());
                else
                    Assert.assertTrue(expected.getMessage(), expected.getMessage().contains("is not constant"));
            } else {
                Assert.assertEquals(UnsupportedException.class, expected.getClass());
            }
            var decisions = new LinkedHashMap<String, Boolean>();
            compiler.stateObserver = new DBSPCompiler.StateObserver() {
                @Override public void nativeDecision(String op, boolean success) { decisions.put(op, success); }
                @Override public void before(String p, int i, int n, CircuitTransform pass, DBSPCircuit graph) {}
                @Override public void after(String p, int i, int n, CircuitTransform pass, DBSPCircuit graph, boolean done) {}
            };
            compiler.options.ioOptions.rustOptimizerPasses = true;
            var pass = new RustCircuitPass(compiler, nativeMergeOnly(compiler), new OptimizerSnapshots(compiler));
            start = DBSPVariablePath.nextGeneratedName();
            var actual = Assert.assertThrows(expected.getClass(), () -> pass.apply(circuit));
            Assert.assertEquals(expected.getMessage().lines().findFirst(), actual.getMessage().lines().findFirst());
            Assert.assertEquals(names, DBSPVariablePath.nextGeneratedName() - start);
            Assert.assertEquals(Boolean.TRUE, decisions.get("MergeFilters"));
            Assert.assertFalse(decisions.containsKey("Monotonicity/ProjectClosure"));
            Assert.assertFalse(decisions.containsKey("Monotonicity/ExpressionFacts"));
        }
    }

    @Test
    public void temporalMergeMaterializesOnlyTheFailedPredicate() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustTemporalKeys"));
        var b = new ExpressionBuilder();
        var site = CalciteEmptyRel.INSTANCE;
        var integer = b.i32();
        var nullable = new DBSPTypeTuple(site, true, List.of(integer, new DBSPTypeArray(integer, false)));
        var source = nullable.ref().var();
        var badFlatmap = new DBSPFlatmap(site, nullable, source.deref().field(1).closure(source),
                List.of(0), null, null, new IdShuffle(2));
        var scalar = new DBSPBinaryExpression(site, integer, DBSPOpcode.MUL, b.tuple(b.lit(1)), b.lit(2));
        var declaration = new DBSPLetStatement("local", b.lit(3));
        var badRead = new DBSPBlockExpression(List.of(declaration), new DBSPVariablePath("local", b.bool()));
        for (var bad : List.of(badFlatmap, scalar, badRead, new DBSPUSizeLiteral(null, true),
                new DBSPVariablePath("free_clock_value", integer))) {
            var clock = new DBSPBlockExpression(List.of(bad.toStatement()), b.call(DBSPTypeTimestamp.INSTANCE, "now"));
            var circuit = temporalFilterChain(clock);
            var compiler = new DBSPCompiler(new CompilerOptions());
            long start = DBSPVariablePath.nextGeneratedName();
            var direct = Assert.assertThrows(RuntimeException.class, () -> new MergeFilters(compiler).apply(circuit));
            long names = DBSPVariablePath.nextGeneratedName() - start;
            Assert.assertTrue(names > 0);
            compiler.options.ioOptions.rustOptimizerPasses = true;
            var pass = new RustCircuitPass(compiler, nativeMergeOnly(compiler), new OptimizerSnapshots(compiler));
            start = DBSPVariablePath.nextGeneratedName();
            var hybrid = Assert.assertThrows(direct.getClass(), () -> pass.apply(circuit));
            Assert.assertEquals(direct.getMessage().lines().findFirst(), hybrid.getMessage().lines().findFirst());
            Assert.assertEquals(names, DBSPVariablePath.nextGeneratedName() - start);
        }
    }

    @Test
    public void temporalMergeAnalyzesNestedFlatmapsInRust() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustTemporalKeys"));
        var b = new ExpressionBuilder();
        var nested = flatmap(new IdShuffle(2), 0, b.lit(1));
        var lastClock = new DBSPBlockExpression(List.of(new DBSPLetStatement("unused", nested),
                new DBSPLetStatement("second", flatmap(new IdShuffle(2), 1, b.lit(2)))),
                b.call(DBSPTypeTimestamp.INSTANCE, "now"));
        var circuit = temporalFilterChain(lastClock);
        var compiler = new DBSPCompiler(new CompilerOptions());
        long start = DBSPVariablePath.nextGeneratedName();
        var direct = new MergeFilters(compiler).apply(circuit);
        long names = DBSPVariablePath.nextGeneratedName() - start;
        compiler.options.ioOptions.rustOptimizerPasses = true;
        start = DBSPVariablePath.nextGeneratedName();
        var pass = new RustCircuitPass(compiler, nativeMergeOnly(compiler), new OptimizerSnapshots(compiler));
        var hybrid = pass.apply(circuit);
        var expected = direct.allOperators.get(direct.allOperators.size() - 1).to(DBSPFilterOperator.class);
        var actual = hybrid.allOperators.get(hybrid.allOperators.size() - 1).to(DBSPFilterOperator.class);
        Assert.assertEquals(direct.allOperators.indexOf(expected.input().node()),
                hybrid.allOperators.indexOf(actual.input().node()));
        Assert.assertTrue(expected.getClosureFunction().equivalent(actual.getClosureFunction()));
        Assert.assertEquals(names, DBSPVariablePath.nextGeneratedName() - start);
    }

    @Test
    public void temporalMergeReportsACopiedClockReferenceWithoutRepeatingThePair() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustTemporalKeys"));
        var b = new ExpressionBuilder();
        var clock = new DBSPLetStatement("saved_clock", b.call(DBSPTypeTimestamp.INSTANCE, "now"));
        var shared = b.add(b.lit(1), b.lit(2));
        var nested = flatmap(new IdShuffle(2), 0, b.lit(1));
        // Sharing forces a tree copy. Visiting the flatmap then replaces the
        // reference table before the copied read of saved_clock.
        var lastClock = new DBSPBlockExpression(List.of(clock, shared.toStatement(), shared.toStatement(),
                nested.toStatement()), clock.getVarReference());
        var circuit = temporalFilterChain(lastClock);
        var compiler = new DBSPCompiler(new CompilerOptions());
        long start = DBSPVariablePath.nextGeneratedName();
        var direct = Assert.assertThrows(RuntimeException.class, () -> new MergeFilters(compiler).apply(circuit));
        long names = DBSPVariablePath.nextGeneratedName() - start;
        Assert.assertTrue(direct.getMessage(), direct.getMessage().contains("saved_clock"));
        compiler.options.ioOptions.rustOptimizerPasses = true;
        var pass = new RustCircuitPass(compiler, nativeMergeOnly(compiler), new OptimizerSnapshots(compiler));
        start = DBSPVariablePath.nextGeneratedName();
        var hybrid = Assert.assertThrows(direct.getClass(), () -> pass.apply(circuit));
        Assert.assertEquals(direct.getMessage(), hybrid.getMessage());
        Assert.assertEquals(names, DBSPVariablePath.nextGeneratedName() - start);
    }

    @Test
    public void temporalMergePreservesTheReferenceErrorAfterANestedFlatmap() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustTemporalKeys"));
        var b = new ExpressionBuilder();
        var clock = new DBSPLetStatement("saved_clock", b.call(DBSPTypeTimestamp.INSTANCE, "now"));
        var nested = flatmap(new IdShuffle(2), 0, b.lit(1));
        var lastClock = new DBSPBlockExpression(List.of(clock, new DBSPLetStatement("unused", nested)),
                clock.getVarReference());
        var circuit = temporalFilterChain(lastClock);
        var compiler = new DBSPCompiler(new CompilerOptions());
        long start = DBSPVariablePath.nextGeneratedName();
        var direct = Assert.assertThrows(RuntimeException.class, () -> new MergeFilters(compiler).apply(circuit));
        long names = DBSPVariablePath.nextGeneratedName() - start;
        Assert.assertTrue(direct.getMessage(), direct.getMessage().contains("saved_clock"));
        compiler.options.ioOptions.rustOptimizerPasses = true;
        var pass = new RustCircuitPass(compiler, nativeMergeOnly(compiler), new OptimizerSnapshots(compiler));
        start = DBSPVariablePath.nextGeneratedName();
        var hybrid = Assert.assertThrows(direct.getClass(), () -> pass.apply(circuit));
        Assert.assertEquals(direct.getMessage(), hybrid.getMessage());
        Assert.assertEquals(names, DBSPVariablePath.nextGeneratedName() - start);
    }

    @Test
    public void reorderCountsNestedFlatmapsAtEachWindowClassification() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustTemporalKeys"));
        var b = new ExpressionBuilder();
        var site = CalciteEmptyRel.INSTANCE;
        var time = DBSPTypeTimestamp.INSTANCE;
        for (boolean single : List.of(false, true)) {
            var circuit = new DBSPCircuit(new ProgramMetadata(), false);
            var rowType = b.tup(time, time);
            var input = new DBSPConstantOperator(site, DBSPZSetExpression.emptyWithElementType(rowType));
            circuit.addOperator(input);
            var row = b.refVar(rowType);
            var clock = new DBSPBlockExpression(List.of(flatmap(new IdShuffle(2), 0, b.lit(1)).toStatement()),
                    b.call(time, "now"));
            var upper = b.binary(DBSPOpcode.LT, b.field(row, 0), clock);
            var lower = b.binary(DBSPOpcode.GTE, b.field(row, 1), b.call(time, "now"));
            var condition = single ? upper : b.binary(DBSPOpcode.AND, upper, lower);
            var filter = new DBSPFilterOperator(site, condition.closure(row), input.outputPort());
            circuit.addOperator(filter);
            var compiler = new DBSPCompiler(new CompilerOptions());
            long start = DBSPVariablePath.nextGeneratedName();
            scanWindows(compiler, circuit, new ArrayList<>());
            long scanNames = DBSPVariablePath.nextGeneratedName() - start;
            compiler.options.ioOptions.rustOptimizerPasses = true;
            var decisions = new ArrayList<Boolean>();
            start = DBSPVariablePath.nextGeneratedName();
            scanWindows(compiler, circuit, decisions);
            Assert.assertEquals(scanNames, DBSPVariablePath.nextGeneratedName() - start);
            Assert.assertEquals(List.of(true), decisions);

            for (boolean shared : List.of(false, true)) {
                Map<DBSPFilterOperator, Integer> chosen = shared ? Map.of(filter, 0) : Map.of();
                compiler.options.ioOptions.rustOptimizerPasses = false;
                var directPass = new ReorderTemporalFilters.Reorder(compiler, chosen);
                start = DBSPVariablePath.nextGeneratedName();
                var direct = directPass.apply(circuit);
                long names = DBSPVariablePath.nextGeneratedName() - start;
                Assert.assertTrue(names > 0);
                compiler.options.ioOptions.rustOptimizerPasses = true;
                var reorder = new ReorderTemporalFilters.Reorder(compiler, chosen);
                var hybridPass = new RustCircuitPass(compiler, reorder, new OptimizerSnapshots(compiler));
                start = DBSPVariablePath.nextGeneratedName();
                var hybrid = hybridPass.apply(circuit);
                Assert.assertEquals(names, DBSPVariablePath.nextGeneratedName() - start);
                var expected = direct.allOperators.get(1).to(DBSPFilterOperator.class);
                var actual = hybrid.allOperators.get(1).to(DBSPFilterOperator.class);
                Assert.assertTrue(expected.getClosureFunction().equivalent(actual.getClosureFunction()));
            }
        }
    }

    @Test
    public void nestedFlatmapValidationKeepsEarlierFactsAndAllocatedNames() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustTemporalKeys"));
        var integer = new DBSPI32Literal(0).getType();
        var scalar = new MonotoneType(integer);
        var operator = new DBSPConstantOperator(CalciteEmptyRel.INSTANCE, DBSPZSetExpression.emptyWithElementType(integer));
        for (var kind : MonotoneTransferFunctions.ArgumentKind.values()) {
            // Reading a passthrough field from a nullable row lifts its type.
            var row = new DBSPTypeTuple(CalciteObject.EMPTY, true, List.of(integer, new DBSPTypeArray(integer, false)));
            var source = row.ref().var();
            var nested = new DBSPFlatmap(CalciteObject.EMPTY, row, source.deref().field(1).closure(source),
                    List.of(0), null, null, new IdShuffle(2));
            var prefix = new DBSPI32Literal(7);
            var body = new DBSPBlockExpression(List.of(prefix.toStatement()), nested);
            var pair = new PartiallyMonotoneTuple(List.of(scalar, scalar), true, false);
            var inputs = switch (kind) {
                case IndexedZSet -> new IMaybeMonotoneType[]{pair};
                case Join -> new IMaybeMonotoneType[]{scalar, scalar, scalar};
                case ZSet -> new IMaybeMonotoneType[]{scalar};
            };
            var closure = switch (kind) {
                case IndexedZSet -> body.closure(new DBSPTypeRawTuple(integer.ref(), integer.ref()).var());
                case Join -> body.closure(integer.ref().var(), integer.ref().var(), integer.ref().var());
                case ZSet -> body.closure(integer.ref().var());
            };
            var compiler = new DBSPCompiler(new CompilerOptions());
            var direct = new MonotoneTransferFunctions(compiler, operator, kind, inputs);
            long start = DBSPVariablePath.nextGeneratedName();
            var expected = Assert.assertThrows(InternalCompilerError.class, () -> direct.apply(closure));
            long names = DBSPVariablePath.nextGeneratedName() - start;
            Assert.assertEquals(kind == MonotoneTransferFunctions.ArgumentKind.ZSet ? 1 : 0, names);
            compiler.options.ioOptions.rustOptimizerPasses = true;
            var decisions = new LinkedHashMap<String, Boolean>();
            compiler.stateObserver = new DBSPCompiler.StateObserver() {
                @Override public void nativeDecision(String op, boolean success) { decisions.put(op, success); }
                @Override public void before(String p, int i, int n, CircuitTransform pass, DBSPCircuit graph) {}
                @Override public void after(String p, int i, int n, CircuitTransform pass, DBSPCircuit graph, boolean done) {}
            };
            for (boolean resultOnly : List.of(false, true)) {
                var hybrid = new MonotoneTransferFunctions(compiler, operator, kind, inputs);
                start = DBSPVariablePath.nextGeneratedName();
                var actual = Assert.assertThrows(expected.getClass(), () -> {
                    if (resultOnly)
                        hybrid.applyAnalysis(closure);
                    else
                        hybrid.apply(closure);
                });
                Assert.assertEquals(expected.getMessage().lines().findFirst(), actual.getMessage().lines().findFirst());
                Assert.assertEquals(names, DBSPVariablePath.nextGeneratedName() - start);
                Assert.assertTrue(hybrid.get(prefix).mayBeMonotone());
                Assert.assertNull(hybrid.maybeGet(nested));
                Assert.assertEquals(Boolean.TRUE, decisions.get(resultOnly ? "Monotonicity/ProjectClosure" : "Monotonicity/ExpressionFacts"));
            }
        }
    }

    @Test
    public void nestedFlatmapFactsRetainTheSyntheticClosureAndItsGeneratedName() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustTemporalKeys"));
        var compiler = new DBSPCompiler(new CompilerOptions());
        var b = new ExpressionBuilder();
        var integer = b.i32();
        var parameter = b.refVar(integer);
        var shape = new MonotoneType(integer);
        var input = new DBSPConstantOperator(CalciteEmptyRel.INSTANCE, DBSPZSetExpression.emptyWithElementType(integer));
        for (boolean fail : List.of(false, true)) {
            var nested = flatmap(new IdShuffle(2), 0, b.lit(1));
            var read = parameter.deepCopy();
            var body = fail ? new DBSPBlockExpression(List.of(nested.toStatement()), read) : nested;
            var closure = body.closure(parameter);
            var direct = new MonotoneTransferFunctions(compiler, input, MonotoneTransferFunctions.ArgumentKind.ZSet, shape);
            long start = DBSPVariablePath.nextGeneratedName();
            RuntimeException expected = null;
            if (fail)
                expected = Assert.assertThrows(RuntimeException.class, () -> direct.apply(closure));
            else
                direct.apply(closure);
            long names = DBSPVariablePath.nextGeneratedName() - start;
            Assert.assertEquals(1, names);
            var flatmapType = direct.maybeGet(nested).getMonotoneType();
            for (boolean resultOnly : List.of(true, false)) {
                var facts = new LinkedHashMap<DBSPExpression, MonotoneExpression>();
                start = DBSPVariablePath.nextGeneratedName();
                java.util.function.Supplier<RustCircuitPass.WaterlineAnalysis> analyze = () ->
                        RustCircuitPass.projectWaterlineClosure(compiler, body, closure.parameters,
                                new DBSPType[]{integer}, new IMaybeMonotoneType[]{new MonotoneRefType(shape)},
                                false, resultOnly, facts::put);
                if (fail) {
                    var error = Assert.assertThrows(expected.getClass(), analyze::get);
                    Assert.assertEquals(expected.getMessage(), error.getMessage());
                    Assert.assertFalse(facts.containsKey(read));
                } else {
                    var result = (RustCircuitPass.ProjectedWaterline) analyze.get();
                    Assert.assertTrue(flatmapType.getType().sameType(result.bodyType().getType()));
                    Assert.assertNull(result.closure());
                }
                Assert.assertEquals(names, DBSPVariablePath.nextGeneratedName() - start);
                if (resultOnly && !fail) {
                    Assert.assertTrue(facts.isEmpty());
                } else {
                    Assert.assertEquals(2, facts.size());
                    var synthetic = facts.keySet().iterator().next().to(DBSPClosureExpression.class);
                    Assert.assertEquals("t_" + start, synthetic.parameters[0].getName());
                    Assert.assertSame(facts.get(synthetic), facts.get(nested));
                    Assert.assertSame(synthetic.getType(), facts.get(nested).getMonotoneType().getType());
                }
            }
        }
    }

    @Test
    public void clockSitesUseTheLastOccurrenceInEachExpression() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustTemporalKeys"));
        var b = new ExpressionBuilder();
        var time = DBSPTypeTimestamp.INSTANCE;
        var first = b.call(time, "NOW");
        var second = b.call(time, "now");
        var ordinary = b.call(time, "now", b.lit(1));
        var root = b.tuple(first, second, first, ordinary);
        Iterable<DBSPExpression> roots = () -> List.<DBSPExpression>of(root, second, ordinary).iterator();
        var sites = RustCircuitPass.temporalClockSites(new DBSPCompiler(new CompilerOptions()), roots);
        Assert.assertEquals(2, sites.size());
        Assert.assertSame(first, sites.get(root.id));
        Assert.assertSame(second, sites.get(second.id));
    }

    @Test
    public void windowScanReportsCastWarningAtItsOriginalRange() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustTemporalKeys"));
        var b = new ExpressionBuilder();
        var range = new SourcePositionRange(new SourcePosition(3, 2), new SourcePosition(3, 17));
        var rel = CalciteEmptyRel.INSTANCE;
        var time = DBSPTypeTimestamp.INSTANCE;
        var inputType = b.tup(time.withMayBeNull(true));
        for (boolean withComment : List.of(false, true)) {
            var circuit = new DBSPCircuit(new ProgramMetadata(), false);
            var input = new DBSPConstantOperator(rel, DBSPZSetExpression.emptyWithElementType(inputType));
            circuit.addOperator(input);
            var param = b.refVar(inputType);
            var values = b.tuple(b.lit("invalid timestamp"));
            var map = new DBSPMapOperator(rel, values.closure(param), input.outputPort());
            circuit.addOperator(map);
            var row = b.refVar(values.getType());
            var cast = new DBSPCastExpression(CalciteObject.create(range), b.field(row, 0), time,
                    DBSPCastExpression.CastType.SqlUnsafe);
            var clock = new DBSPApplyExpression("now", time);
            circuit.addOperator(new DBSPFilterOperator(rel, b.binary(DBSPOpcode.GTE, cast, clock).closure(row),
                    map.outputPort()));
            var second = b.refVar(inputType);
            var initializer = new DBSPBlockExpression(
                    withComment ? List.of(new DBSPComment("offset")) : List.of(b.call("prepare_offset").toStatement()),
                    b.call("get_offset", b.lit(1)));
            var offset = new DBSPStaticExpression(rel, initializer, "offset");
            var key = b.call(time, "shift_timestamp", b.unwrap(b.field(second, 0)), offset);
            var condition = b.binary(DBSPOpcode.GTE, key, clock.deepCopy());
            circuit.addOperator(new DBSPFilterOperator(rel, condition.closure(second), input.outputPort()));

            var decisions = new ArrayList<Boolean>();
            for (boolean hybrid : List.of(false, true)) {
                var compiler = new DBSPCompiler(new CompilerOptions());
                compiler.options.ioOptions.rustOptimizerPasses = hybrid;
                scanWindows(compiler, circuit, decisions);
                Assert.assertEquals(1, compiler.messages.messages.size());
                var warning = compiler.messages.messages.get(0);
                Assert.assertEquals(range, warning.range);
                Assert.assertEquals("Not a TIMESTAMP", warning.errorType);
                Assert.assertTrue(warning.message.contains("invalid timestamp"));
            }
            Assert.assertEquals(List.of(true), decisions);
        }
    }

    @Test
    public void windowSharingComparesPrintedCommentLines() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustTemporalKeys"));
        var b = new ExpressionBuilder();
        var offsets = new ArrayList<DBSPExpression>();
        for (String comment : List.of("first\n\nsecond", "first\n\nsecond\n\n", "first\nsecond", "", "\n", "\n\n"))
            offsets.add(new DBSPBlockExpression(List.of(new DBSPComment(comment)), b.lit(1)));
        assertStaticWindowGroups(offsets, Set.of(0, 1, 4, 5));
    }

    @Test
    public void windowSharingUsesTheFieldsOfStaticCollectionInitializers() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustTemporalKeys"));
        var b = new ExpressionBuilder();
        var offsets = new ArrayList<DBSPExpression>();
        for (int value : List.of(3, 3, 4)) {
            var number = b.call(b.lit(0).getType(), "get_offset");
            var selected = b.ifThenElse(b.binary(DBSPOpcode.LT, number, b.lit(1)),
                    b.neg(b.lit(2)), b.add(number.deepCopy(), b.lit(value)));
            var array = new DBSPArrayExpression(selected);
            var string = b.lit("key");
            var map = new DBSPMapExpression(new DBSPTypeMap(string.getType(), array.getType(), false),
                    List.of(string), List.of(array));
            offsets.add(b.tuple(map, b.lit(0)));
        }
        assertStaticWindowGroups(offsets, Set.of(0, 1));
    }

    private static void assertStaticWindowGroups(List<DBSPExpression> offsets, Set<Integer> shared) {
        var b = new ExpressionBuilder();
        var site = CalciteEmptyRel.INSTANCE;
        var time = DBSPTypeTimestamp.INSTANCE;
        var rowType = b.tup(time);
        var circuit = new DBSPCircuit(new ProgramMetadata(), false);
        var input = new DBSPConstantOperator(site, DBSPZSetExpression.emptyWithElementType(rowType));
        circuit.addOperator(input);
        var expected = new ArrayList<DBSPFilterOperator>();
        for (int i = 0; i < offsets.size(); i++) {
            var row = b.refVar(rowType);
            var key = b.call(time, "shift_timestamp", b.field(row, 0), new DBSPStaticExpression(site, offsets.get(i), "offset"));
            var condition = b.binary(DBSPOpcode.GTE, key, b.call(time, "now"));
            var filter = new DBSPFilterOperator(site, condition.closure(row), input.outputPort());
            circuit.addOperator(filter);
            if (shared.contains(i))
                expected.add(filter);
        }
        for (boolean hybrid : List.of(false, true)) {
            var compiler = new DBSPCompiler(new CompilerOptions());
            compiler.metadata.set(ProgramMetadata.WINDOW_SHARING_THRESHOLD, b.lit(2), SourcePositionRange.INVALID, compiler);
            compiler.options.ioOptions.rustOptimizerPasses = hybrid;
            var decisions = new ArrayList<Boolean>();
            var scan = scanWindows(compiler, circuit, decisions);
            Assert.assertEquals(expected, List.copyOf(scan.chosenConjunct.keySet()));
            Assert.assertEquals(hybrid ? List.of(true) : List.of(), decisions);
        }
    }

    @Test
    public void windowScanPreservesTheErrorWhenRenamingChangesAStaticBlock() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustTemporalKeys"));
        var b = new ExpressionBuilder();
        var site = CalciteEmptyRel.INSTANCE;
        var time = DBSPTypeTimestamp.INSTANCE;
        var inputType = b.tup(time);
        var circuit = new DBSPCircuit(new ProgramMetadata(), false);
        var input = new DBSPConstantOperator(site, DBSPZSetExpression.emptyWithElementType(inputType));
        circuit.addOperator(input);
        var row = b.refVar(inputType);
        var initializer = new DBSPBlockExpression(List.of(new DBSPLetStatement("offset", b.lit(1))), b.lit(2));
        var offset = new DBSPStaticExpression(site, initializer, "saved");
        var key = b.call(time, "shift_timestamp", b.field(row, 0), offset);
        var predicate = b.binary(DBSPOpcode.GTE, key, b.call(time, "now"));
        circuit.addOperator(new DBSPFilterOperator(site, predicate.closure(row), input.outputPort()));

        var compiler = new DBSPCompiler(new CompilerOptions());
        var direct = Assert.assertThrows(RuntimeException.class, () -> scanWindows(compiler, circuit, new ArrayList<>()));
        Assert.assertTrue(direct.getMessage().contains("Changing static expression"));
        compiler.options.ioOptions.rustOptimizerPasses = true;
        var hybrid = Assert.assertThrows(RuntimeException.class, () -> scanWindows(compiler, circuit, new ArrayList<>()));
        Assert.assertEquals(direct.getClass(), hybrid.getClass());
        Assert.assertEquals(direct.getMessage(), hybrid.getMessage());
    }

    @Test
    public void qualifiedCallsAndConstructorsGroupByPrintedInitializer() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustTemporalKeys"));
        var b = new ExpressionBuilder();
        var site = CalciteEmptyRel.INSTANCE;
        var time = DBSPTypeTimestamp.INSTANCE;
        var inputType = b.tup(time);
        var circuit = new DBSPCircuit(new ProgramMetadata(), false);
        var input = new DBSPConstantOperator(site, DBSPZSetExpression.emptyWithElementType(inputType));
        circuit.addOperator(input);
        var filters = new ArrayList<DBSPFilterOperator>();
        for (int i = 0; i < 4; i++) {
            var row = b.refVar(inputType);
            var field = b.field(row, 0);
            var literal = b.lit(i == 2 ? 2 : 1);
            var offset = b.call("get_offset", b.call("offset_unit", literal), literal.deepCopy());
            var namespace = i == 3 ? "other" : "clock";
            var path = new DBSPPathExpression(time, new DBSPPath(namespace, "shift"));
            DBSPExpression key = i == 0 ? b.call(time, "clock::shift", field, offset)
                    : new DBSPConstructorExpression(path, time, field, new DBSPStaticExpression(site, offset, "offset"));
            var predicate = b.binary(DBSPOpcode.GTE, key, b.call(time, "now"));
            var filter = new DBSPFilterOperator(site, predicate.closure(row), input.outputPort());
            circuit.addOperator(filter);
            filters.add(filter);
        }
        for (boolean hybrid : List.of(false, true)) {
            var compiler = new DBSPCompiler(new CompilerOptions());
            compiler.metadata.set(ProgramMetadata.WINDOW_SHARING_THRESHOLD, b.lit(2), SourcePositionRange.INVALID, compiler);
            compiler.options.ioOptions.rustOptimizerPasses = hybrid;
            var decisions = new ArrayList<Boolean>();
            var scan = scanWindows(compiler, circuit, decisions);
            Assert.assertEquals(filters.subList(0, 2), List.copyOf(scan.chosenConjunct.keySet()));
            Assert.assertEquals(hybrid ? List.of(true) : List.of(), decisions);
        }
    }

    @Test
    public void windowSharingProjectsFieldsFromConstructedRows() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustTemporalKeys"));
        var b = new ExpressionBuilder();
        assertWindowGroups(3, (row, i) -> {
            var first = b.field(row, 0);
            var last = b.field(row, 1);
            // The located constructor retains the field access in the optimizer input.
            return i == 0 ? first :
                    new DBSPTupleExpression(last, first).field(CalciteEmptyRel.INSTANCE, i == 1 ? 1 : 0);
        }, List.of(0, 1));
    }

    @Test
    public void windowSharingCanonicalizesCastsAndNullTests() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustTemporalKeys"));
        var b = new ExpressionBuilder();
        var site = CalciteEmptyRel.INSTANCE;
        var time = DBSPTypeTimestamp.INSTANCE;
        assertWindowGroups(6, (row, i) -> {
            var field = b.field(row, i == 5 ? 1 : 0);
            var key = i == 4 ? new DBSPCastExpression(site, field, time, DBSPCastExpression.CastType.SqlUnsafe) : field;
            var flag = switch (i) {
                case 2 -> new DBSPIsNullExpression(site, new DBSPI32Literal(1, true));
                case 3 -> new DBSPIsNullExpression(site, new DBSPI32Literal(null, true));
                default -> new DBSPBoolLiteral(i == 1);
            };
            return b.call(time, "shift_timestamp", key, flag);
        }, List.of(0, 2, 4, 1, 3));
    }

    @Test
    public void windowSharingFoldsSharedCalculationsInsideClosures() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustTemporalKeys"));
        var b = new ExpressionBuilder();
        var site = CalciteEmptyRel.INSTANCE;
        var time = DBSPTypeTimestamp.INSTANCE;
        assertWindowGroups(3, (row, i) -> {
            var parameter = b.refVar(b.tup(time));
            DBSPExpression field = b.field(row, i == 2 ? 1 : 0);
            if (i == 0)
                field = new DBSPCastExpression(site, field, time, DBSPCastExpression.CastType.SqlUnsafe);
            var shared = b.call(time, "read_timestamp", field);
            var body = b.call(time, "combine", shared, shared, b.field(parameter, 0));
            return b.call(time, "choose", body.closure(parameter));
        }, List.of(0, 1));
    }

    @Test
    public void windowKeysKeepNamesAllocatedInDiscardedTupleFields() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustTemporalKeys"));
        var b = new ExpressionBuilder();
        assertWindowGroups(5, (row, i) -> {
            var retained = new DBSPLetStatement("kept" + i, b.field(row, i == 4 ? 1 : 0));
            var key = new DBSPBlockExpression(List.of(retained), retained.getVarReference());
            if (i == 1 || i == 4)
                return key;
            DBSPExpression discarded = b.lit(0);
            if (i == 0 || i == 2) {
                var local = new DBSPLetStatement("scratch" + i, b.lit(i));
                discarded = new DBSPBlockExpression(List.of(local), local.getVarReference());
            }
            return new DBSPTupleExpression(discarded, key).field(CalciteEmptyRel.INSTANCE, 1);
        }, List.of(0, 2, 1, 3));
    }

    @Test
    public void windowSharingKeepsRepeatedReadsOfAnExpensiveMapBoundOnce() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustTemporalKeys"));
        var b = new ExpressionBuilder();
        var site = CalciteEmptyRel.INSTANCE;
        var time = DBSPTypeTimestamp.INSTANCE;
        var inputType = b.tup(time, time);
        var circuit = new DBSPCircuit(new ProgramMetadata(), false);
        var input = new DBSPConstantOperator(site, DBSPZSetExpression.emptyWithElementType(inputType));
        circuit.addOperator(input);
        var filters = new ArrayList<DBSPFilterOperator>();
        for (int column : List.of(0, 1, 0)) {
            var source = b.refVar(inputType);
            var values = b.tuple(b.call(time, "derive_timestamp", b.field(source, column)));
            var map = new DBSPMapOperator(site, values.closure(source), input.outputPort());
            circuit.addOperator(map);
            var row = b.refVar(values.getType());
            var key = b.call(time, "combine_timestamps", b.field(row, 0), b.field(row, 0));
            var predicate = b.binary(DBSPOpcode.GTE, key, b.call(time, "now"));
            var filter = new DBSPFilterOperator(site, predicate.closure(row), map.outputPort());
            circuit.addOperator(filter);
            filters.add(filter);
        }
        for (boolean hybrid : List.of(false, true)) {
            var compiler = new DBSPCompiler(new CompilerOptions());
            compiler.options.ioOptions.rustOptimizerPasses = hybrid;
            compiler.metadata.set(ProgramMetadata.WINDOW_SHARING_THRESHOLD, b.lit(2), SourcePositionRange.INVALID, compiler);
            var decisions = new ArrayList<Boolean>();
            var scan = scanWindows(compiler, circuit, decisions);
            Assert.assertEquals(List.of(filters.get(0), filters.get(2)), List.copyOf(scan.chosenConjunct.keySet()));
            Assert.assertEquals(hybrid ? List.of(true) : List.of(), decisions);
        }
    }

    @Test
    public void windowSharingDistinguishesMutableBorrows() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustTemporalKeys"));
        var b = new ExpressionBuilder();
        assertWindowGroups(3, (row, index) -> b.call(DBSPTypeTimestamp.INSTANCE,
                "read_timestamp", b.field(row, 0).borrow(index == 1)), List.of(0, 2));
    }

    @Test
    public void windowSharingComparesFrameOffsetsWithTheirPrintedConstructors() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustTemporalKeys"));
        var b = new ExpressionBuilder();
        var offsets = new ArrayList<DBSPExpression>();
        for (boolean before : List.of(true, false)) {
            var distance = b.lit(1);
            offsets.add(new DBSPWindowBoundExpression(CalciteObject.EMPTY, before, distance));
            offsets.add(b.call(distance.getType(), before ? "RelOffset::Before" : "RelOffset::After", distance));
        }
        offsets.add(new DBSPWindowBoundExpression(CalciteObject.EMPTY, true, b.lit(2)));
        assertStaticWindowGroups(offsets, Set.of(0, 1, 2, 3));
    }

    @Test
    public void windowSharingPreservesTheAggregateOpcodeAndFilter() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustTemporalKeys"));
        var b = new ExpressionBuilder();
        var value = b.lit(1);
        var weight = b.lit(2);
        var enabled = new DBSPBoolLiteral(true);
        var sum = new DBSPConditionalIncrementExpression(CalciteObject.EMPTY, DBSPOpcode.AGG_ADD,
                value.getType(), value, weight, enabled);
        var max = new DBSPConditionalIncrementExpression(CalciteObject.EMPTY, DBSPOpcode.AGG_MAX,
                value.getType(), value, weight, enabled);
        assertStaticWindowGroups(List.of(sum, b.call(value.getType(), "agg_plus", value, weight, enabled),
                max, b.call(value.getType(), "agg_plus", value, weight),
                b.call(value.getType(), "agg_plus", weight, value, enabled)), Set.of(0, 1));
    }

    @Test
    public void failureKeysFollowThePrintedPanicMessage() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustTemporalKeys"));
        var time = DBSPTypeTimestamp.INSTANCE;
        var site = CalciteObject.EMPTY;
        String ascii = "bad\t\"value\"\n";
        String unicode = "bad\u0007😀";
        assertStaticWindowGroups(List.of(
                new DBSPFailExpression(site, time, ascii),
                DBSPApplyExpression.panic(site, time, ascii),
                new DBSPFailExpression(site, time, unicode),
                DBSPApplyExpression.panic(site, time, unicode),
                new DBSPFailExpression(site, time, unicode),
                new DBSPFailExpression(site, time, "different")), Set.of(0, 1, 2, 4));
    }

    @Test
    public void windowKeysCompareClosureBodiesAndCallArguments() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustTemporalKeys"));
        var b = new ExpressionBuilder();
        var time = DBSPTypeTimestamp.INSTANCE;
        assertWindowGroups(5, (row, i) -> {
            var argument = new DBSPVariablePath("timestamp" + i, time);
            var captured = b.field(row, i == 2 ? 1 : 0);
            var body = i == 3 ? b.call(time, "shift", captured, argument)
                    : b.call(time, "shift", argument, captured);
            return new DBSPApplyExpression(body.closure(argument), b.field(row, i == 4 ? 0 : 1));
        }, List.of(0, 1));
    }

    @Test
    public void windowKeysCompareLoopInputsAndAssignedValues() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustTemporalKeys"));
        var b = new ExpressionBuilder();
        var time = DBSPTypeTimestamp.INSTANCE;
        assertWindowGroups(5, (row, i) -> {
            var latest = new DBSPLetStatement("latest" + i, b.field(row, 0), true);
            var assign = new DBSPAssignmentExpression(latest.getVarReference(), b.field(row, i == 2 ? 1 : 0));
            var batch = b.call(new DBSPTypeArray(time, false), "batch", b.field(row, i == 3 ? 1 : 0));
            var body = new DBSPBlockExpression(List.of(assign.toStatement()), null);
            var loop = new DBSPForExpression(i == 4 ? "other" : "event", batch, body);
            return new DBSPBlockExpression(List.of(latest, loop.toStatement()), latest.getVarReference());
        }, List.of(0, 1));
    }

    @Test
    public void orderedWindowKeysMatchTheConversionCallAndItsFlags() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustTemporalKeys"));
        var b = new ExpressionBuilder();
        var site = CalciteObject.EMPTY;
        var time = DBSPTypeTimestamp.INSTANCE;
        for (DBSPType type : List.of(time, new DBSPTypeInteger(site, 32, true, false),
                new DBSPTypeInteger(site, 32, false, false))) {
            for (boolean nullable : List.of(false, true)) {
                var valueType = type.withMayBeNull(nullable);
                var value = b.call(valueType, "sort_key");
                var wrap = new DBSPUnsignedWrapExpression(site, value, true, false);
                var unwrapped = new DBSPUnsignedUnwrapExpression(site, wrap, valueType, true, false);
                var ascending = new DBSPBoolLiteral(true);
                var nullsLast = new DBSPBoolLiteral(false);
                assertStaticWindowGroups(List.of(
                        wrap, b.call(wrap.getType(), "UnsignedWrappers::" + wrap.getMethod(), value, ascending, nullsLast),
                        unwrapped, b.call(valueType, "UnsignedWrappers::" + unwrapped.getMethod(), wrap, ascending, nullsLast),
                        new DBSPUnsignedWrapExpression(site, value, false, false),
                        new DBSPUnsignedUnwrapExpression(site, wrap, valueType, true, true)), Set.of(0, 1, 2, 3));
            }
        }
    }

    @Test
    public void windowKeysUseTheComparatorPrintedBySortAndTheValuePrintedByCustomOrd() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustTemporalKeys"));
        var b = new ExpressionBuilder();
        var site = CalciteObject.EMPTY;
        var value = b.tuple(b.lit(1));
        var base = new DBSPNoComparatorExpression(site, value.getType());
        var ascending = new DBSPFieldComparatorExpression(site, base, 0, true, false);
        var descending = new DBSPFieldComparatorExpression(site, base, 0, false, false);
        var ordered = new DBSPCustomOrdExpression(site, value, ascending);
        assertStaticWindowGroups(List.of(ordered,
                new DBSPCustomOrdExpression(site, value, descending),
                b.call(ordered.getType(), "WithCustomOrd", value),
                new DBSPCustomOrdExpression(site, b.tuple(b.lit(2)), ascending)), Set.of(0, 1, 2));
        assertStaticWindowGroups(List.of(
                new DBSPSortExpression(site, value.getType(), ascending),
                new DBSPSortExpression(site, b.tup(b.bool()), ascending),
                new DBSPSortExpression(site, value.getType(), descending)), Set.of(0, 1));
    }

    @Test
    public void windowKeysCompareThePrintedOrderingChain() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustTemporalKeys"));
        var b = new ExpressionBuilder();
        var site = CalciteObject.EMPTY;
        var time = DBSPTypeTimestamp.INSTANCE;
        assertWindowGroups(7, (row, i) -> {
            var base = new DBSPNoComparatorExpression(site, b.tup(time, i == 5 ? b.bool() : time));
            var field = new DBSPFieldComparatorExpression(site, base, i == 3 ? 1 : 0, i != 4, i == 1);
            var order = new DBSPDirectComparatorExpression(site, field, i != 6);
            return b.call(time, "ordered_time", b.field(row, 0),
                    i == 2 ? new DBSPEqualityComparatorExpression(site, order) : order);
        }, List.of(0, 1, 2));
    }

    @Test
    public void windowKeysCompareAsymmetricCollationsInOrder() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustTemporalKeys"));
        var b = new ExpressionBuilder();
        var site = CalciteObject.EMPTY;
        var time = DBSPTypeTimestamp.INSTANCE;
        assertWindowGroups(7, (row, i) -> {
            var type = i == 1 ? b.tup(b.bool(), b.bool()) : b.tup(time, time);
            var first = new DBSPAsymmetricFieldComparatorExpression.Collation(
                    i == 2 ? 1 : 0, i == 3 ? 0 : 1, i != 4, i == 5);
            var second = new DBSPAsymmetricFieldComparatorExpression.Collation(1, 0, true, true);
            var order = new DBSPAsymmetricFieldComparatorExpression(site, type, type,
                    i == 6 ? List.of(second, first) : List.of(first, second));
            return b.call(time, "ordered_time", b.field(row, 0), order);
        }, List.of(0, 1));
    }

    @Test
    public void windowSharingUsesThePrintedGenericSignature() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustTemporalKeys"));
        var b = new ExpressionBuilder();
        var site = CalciteObject.EMPTY;
        var time = DBSPTypeTimestamp.INSTANCE;
        assertWindowGroups(7, (row, i) -> {
            var field = new DBSPTypeStruct.Field(site, new ProgramIdentifier("event time"), 0,
                    i == 2 ? b.bool() : time);
            var rowType = new DBSPTypeStruct(site, new ProgramIdentifier("Event row", i == 1),
                    List.of(field), i == 3);
            DBSPType argument = switch (i) {
                case 4 -> DBSPTypeStr.INSTANCE;
                case 5 -> new DBSPTypeUser(site, DBSPTypeCode.USER, "str", false);
                case 6 -> DBSPTypeVoid.INSTANCE;
                default -> rowType;
            };
            var function = new DBSPPathExpression(time, new DBSPPath(new DBSPSimplePathSegment("stamp", argument)));
            return new DBSPApplyExpression(function, time, b.field(row, 0));
        }, List.of(0, 1, 4, 5));
    }

    @Test
    public void windowSharingComparesLazyAndEarlyExitExpressions() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustTemporalKeys"));
        var b = new ExpressionBuilder();
        var time = DBSPTypeTimestamp.INSTANCE;
        assertWindowGroups(4, (row, index) -> {
            var value = b.call(time.withMayBeNull(true), "read_time", b.field(row, index == 2 ? 1 : 0));
            DBSPExpression result = index == 3 ? b.unwrap(value) : value.question();
            var returned = new DBSPReturnExpression(CalciteObject.EMPTY, result);
            return b.call(time, "choose_time", new DBSPLazyExpression(returned));
        }, List.of(0, 1));
    }

    @Test
    public void windowScanReportsAConflictingDeclarationInItsSourceMap() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustTemporalKeys"));
        var b = new ExpressionBuilder();
        var site = CalciteEmptyRel.INSTANCE;
        var time = DBSPTypeTimestamp.INSTANCE;
        var rowType = b.tup(time);
        var circuit = new DBSPCircuit(new ProgramMetadata(), false);
        var input = new DBSPConstantOperator(site, DBSPZSetExpression.emptyWithElementType(rowType));
        circuit.addOperator(input);
        var inner = b.refVar(rowType);
        var shared = b.field(inner, 0);
        var callbacks = b.call(time, "combine", shared.closure(inner), shared.closure(inner));
        var map = new DBSPMapOperator(site, b.tuple(callbacks).closure(b.refVar(rowType)), input.outputPort());
        circuit.addOperator(map);
        var outer = b.refVar(rowType);
        var condition = b.binary(DBSPOpcode.GTE, b.field(outer, 0), b.call(time, "now"));
        circuit.addOperator(new DBSPFilterOperator(site, condition.closure(outer), map.outputPort()));

        var compiler = new DBSPCompiler(new CompilerOptions());
        var decisions = new ArrayList<Boolean>();
        var direct = Assert.assertThrows(InternalCompilerError.class, () -> scanWindows(compiler, circuit, decisions));
        Assert.assertTrue(direct.getMessage(), direct.getMessage().contains("Changing declaration"));
        compiler.options.ioOptions.rustOptimizerPasses = true;
        var hybrid = Assert.assertThrows(InternalCompilerError.class, () -> scanWindows(compiler, circuit, decisions));
        Assert.assertEquals(direct.getMessage(), hybrid.getMessage());
        Assert.assertEquals(List.of(true), decisions);
    }

    private static void assertWindowGroups(int count,
            BiFunction<DBSPVariablePath, Integer, DBSPExpression> timestamp,
            List<Integer> shared) {
        var b = new ExpressionBuilder();
        var site = CalciteEmptyRel.INSTANCE;
        var time = DBSPTypeTimestamp.INSTANCE;
        var rowType = b.tup(time, time);
        var circuit = new DBSPCircuit(new ProgramMetadata(), false);
        var input = new DBSPConstantOperator(site, DBSPZSetExpression.emptyWithElementType(rowType));
        circuit.addOperator(input);
        var filters = new ArrayList<DBSPFilterOperator>();
        for (int i = 0; i < count; i++) {
            var row = b.refVar(rowType);
            var predicate = b.binary(DBSPOpcode.GTE, timestamp.apply(row, i), b.call(time, "now"));
            var filter = new DBSPFilterOperator(site, predicate.closure(row), input.outputPort());
            circuit.addOperator(filter);
            filters.add(filter);
        }
        var expected = shared.stream().map(filters::get).toList();
        for (boolean hybrid : List.of(false, true)) {
            var compiler = new DBSPCompiler(new CompilerOptions());
            compiler.metadata.set(ProgramMetadata.WINDOW_SHARING_THRESHOLD, b.lit(2), SourcePositionRange.INVALID, compiler);
            compiler.options.ioOptions.rustOptimizerPasses = hybrid;
            var decisions = new ArrayList<Boolean>();
            var scan = scanWindows(compiler, circuit, decisions);
            Assert.assertEquals(expected, List.copyOf(scan.chosenConjunct.keySet()));
            Assert.assertEquals(hybrid ? List.of(true) : List.of(), decisions);
        }
    }

    private static ReorderTemporalFilters.Scan scanWindows(
            DBSPCompiler compiler, DBSPCircuit circuit, List<Boolean> decisions) {
        compiler.stateObserver = new DBSPCompiler.StateObserver() {
            @Override
            public void nativeDecision(String operation, boolean success) {
                if (operation.equals("WindowGroups/Scan"))
                    decisions.add(success);
            }
            @Override
            public void before(String parent, int index, int iteration, CircuitTransform pass, DBSPCircuit graph) {}
            @Override
            public void after(String parent, int index, int iteration, CircuitTransform pass, DBSPCircuit graph, boolean done) {}
        };
        var graph = new Graph(compiler);
        graph.apply(circuit);
        var scan = new ReorderTemporalFilters.Scan(compiler, graph.getGraphs());
        if (compiler.options.ioOptions.rustOptimizerPasses)
            new RustCircuitPass(compiler, scan, new OptimizerSnapshots(compiler)).apply(circuit);
        else
            scan.apply(circuit);
        return scan;
    }

    @Test
    public void windowKeysKeepUnwrapMessagesAndGroupThePrintedErrorHandler() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustTemporalKeys"));
        var b = new ExpressionBuilder();
        var site = CalciteEmptyRel.INSTANCE;
        var time = DBSPTypeTimestamp.INSTANCE;
        var rowType = b.tup(time.withMayBeNull(true));
        var circuit = new DBSPCircuit(new ProgramMetadata(), false);
        var input = new DBSPConstantOperator(site, DBSPZSetExpression.emptyWithElementType(rowType));
        circuit.addOperator(input);
        var expected = new ArrayList<DBSPFilterOperator>();
        for (var behavior : DBSPHandleErrorExpression.RuntimeBehavior.values()) {
            for (boolean namedCall : List.of(false, true))
                expected.add(addCheckedWindow(circuit, input, behavior, namedCall, ""));
        }
        for (String message : List.of("missing time", "missing date"))
            addCheckedWindow(circuit, input, DBSPHandleErrorExpression.RuntimeBehavior.Panic, false, message);
        for (boolean hybrid : List.of(false, true)) {
            var compiler = new DBSPCompiler(new CompilerOptions());
            compiler.metadata.set(ProgramMetadata.WINDOW_SHARING_THRESHOLD, b.lit(2), SourcePositionRange.INVALID, compiler);
            compiler.options.ioOptions.rustOptimizerPasses = hybrid;
            var decisions = new ArrayList<Boolean>();
            var scan = scanWindows(compiler, circuit, decisions);
            Assert.assertEquals(expected, List.copyOf(scan.chosenConjunct.keySet()));
            Assert.assertEquals(hybrid ? List.of(true) : List.of(), decisions);
        }
    }

    private static DBSPFilterOperator addCheckedWindow(DBSPCircuit circuit, DBSPConstantOperator input,
            DBSPHandleErrorExpression.RuntimeBehavior behavior, boolean namedCall, String message) {
        var b = new ExpressionBuilder();
        var time = DBSPTypeTimestamp.INSTANCE;
        var site = CalciteEmptyRel.INSTANCE;
        var row = b.refVar(input.outputPort().getOutputRowType());
        var value = new DBSPUnwrapExpression(site, message, b.field(row, 0));
        var checked = b.call(new DBSPTypeSqlResult(time), "check_time", value);
        var handler = new DBSPHandleErrorExpression(site, 0, behavior, checked, false);
        var key = namedCall ? b.call(time, handler.getFunction(), checked) : handler;
        var condition = b.binary(DBSPOpcode.GTE, key, b.call(time, "now"));
        var filter = new DBSPFilterOperator(site, condition.closure(row), input.outputPort());
        circuit.addOperator(filter);
        return filter;
    }

    @Test
    public void unaryConversionAndItsNamedCallShareAWindowKey() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustTemporalKeys"));
        var b = new ExpressionBuilder();
        var site = CalciteEmptyRel.INSTANCE;
        var time = DBSPTypeTimestamp.INSTANCE;
        var inputType = b.tup(b.bool(), b.bool());
        var circuit = new DBSPCircuit(new ProgramMetadata(), false);
        var input = new DBSPConstantOperator(site, DBSPZSetExpression.emptyWithElementType(inputType));
        circuit.addOperator(input);
        var filters = new ArrayList<DBSPFilterOperator>();
        for (int i = 0; i < 3; i++) {
            var param = b.refVar(inputType);
            var flag = b.field(param, i == 2 ? 1 : 0);
            var byteType = b.integer(DBSPTypeCode.INT8, false);
            var offset = i == 0 ? new DBSPUnaryExpression(site, byteType, DBSPOpcode.BOOL_TO_INTEGER, flag)
                    : b.call(byteType, "bool_to_i8", flag);
            var timestamp = b.call(time, "timestamp_from_offset", offset);
            var condition = b.binary(DBSPOpcode.GTE, timestamp, b.call(time, "now"));
            var filter = new DBSPFilterOperator(site, condition.closure(param), input.outputPort());
            filters.add(filter);
            circuit.addOperator(filter);
        }
        for (boolean hybrid : List.of(false, true)) {
            var compiler = new DBSPCompiler(new CompilerOptions());
            compiler.metadata.set(ProgramMetadata.WINDOW_SHARING_THRESHOLD, b.lit(2), SourcePositionRange.INVALID, compiler);
            compiler.options.ioOptions.rustOptimizerPasses = hybrid;
            var decisions = new ArrayList<Boolean>();
            var scan = scanWindows(compiler, circuit, decisions);
            Assert.assertEquals(filters.subList(0, 2), List.copyOf(scan.chosenConjunct.keySet()));
            Assert.assertEquals(List.of(0, 0), List.copyOf(scan.chosenConjunct.values()));
            Assert.assertEquals(hybrid ? List.of(true) : List.of(), decisions);
        }
    }

    @Test
    public void numericOffsetsShareOnlyMatchingWindowKeys() {
        checkOffsetKeys("id + 1", "id + 2");
    }

    @Test
    public void decimalThresholdsAndFloatingCastsDistinguishWindowKeys() {
        checkOffsetKeys("CASE WHEN CAST(id AS DECIMAL(18,2)) > 1.25 THEN id ELSE 1 END",
                "CASE WHEN CAST(id AS DECIMAL(18,2)) > 1.75 THEN id ELSE 1 END");
        checkOffsetKeys("CASE WHEN CAST(id AS DOUBLE) > CAST(id + 1 AS DOUBLE) THEN id ELSE 1 END",
                "CASE WHEN CAST(id AS REAL) > CAST(id + 1 AS REAL) THEN id ELSE 1 END");
    }

    @Test
    public void temporalThresholdsShareOnlyMatchingWindowKeys() {
        checkTemporalThreshold("TIME", "12:34:56.123", "12:34:56.124");
        checkTemporalThreshold("DATE", "1969-12-31", "1970-01-01");
        checkTemporalThreshold("TIMESTAMP", "1969-12-31 23:59:59.999999", "1970-01-01 00:00:00");
        checkTemporalThreshold("TIMESTAMP WITH TIME ZONE",
                "2020-01-02 10:11:12 UTC", "2020-01-02 10:11:12 America/New_York");
    }

    @Test
    public void windowKeysPreserveIntervalUnits() {
        String columns = "id INT NOT NULL, ts TIMESTAMP, i INT";
        checkSharedWindowKeys(columns, "ts + CAST(i AS INTERVAL DAY)", "ts + CAST(i AS INTERVAL HOUR)");
        checkSharedWindowKeys(columns, "ts + CAST(i AS INTERVAL MONTH)", "ts + CAST(i AS INTERVAL YEAR)");
    }

    @Test
    public void externalFunctionsShareMatchingWindowKeys() {
        for (boolean nativePasses : List.of(false, true)) {
            checkSharedWindowKeys("id INT NOT NULL, ts TIMESTAMP", "stamp(ts)", "stamp(ts)", "other_stamp(ts)", compiler -> {
                compiler.options.ioOptions.rustOptimizerPasses = nativePasses;
                compiler.submitStatementsForCompilation("""
                        CREATE FUNCTION stamp(t TIMESTAMP) RETURNS TIMESTAMP;
                        CREATE FUNCTION other_stamp(t TIMESTAMP) RETURNS TIMESTAMP;
                        """);
            });
        }
    }

    @Test
    public void windowKeysWithNullIntervalsShareByTheirTimestampExpression() {
        String columns = "id INT NOT NULL, ts TIMESTAMP, alt TIMESTAMP";
        for (String unit : List.of("SECOND", "MONTH")) {
            String offset = "(CASE WHEN id > 0 THEN INTERVAL '1' %1$s ELSE CAST(NULL AS INTERVAL %1$s) END)"
                    .formatted(unit);
            checkSharedWindowKeys(columns, "ts + " + offset, "alt + " + offset);
        }
    }

    @Test
    public void expandedCollectionCastsShareOnlyMatchingWindowKeys() {
        String columns = "id INT NOT NULL, ts TIMESTAMP, arr VARCHAR ARRAY, m MAP<INT,VARCHAR>";
        for (boolean nativePasses : List.of(false, true)) {
            Consumer<DBSPCompiler> expand = compiler -> {
                compiler.options.ioOptions.rustOptimizerPasses = nativePasses;
                // Exercise the window pass on the lower-level IR produced by cast expansion.
                compiler.optimizerHook = optimizer -> optimizer.insertBefore(ImplementNow.class, new ExpandCasts(compiler));
            };
            for (String offset : List.of("ARRAY_MAX(%s(arr AS INT ARRAY))", "%s(m AS MAP<INT,INT>)[id]")) {
                String timestamp = "TIMESTAMPADD(SECOND, " + offset + ", ts)";
                for (String cast : List.of("CAST", "SAFE_CAST")) {
                    String shared = timestamp.formatted(cast);
                    String different = timestamp.formatted(cast.equals("CAST") ? "SAFE_CAST" : "CAST");
                    checkSharedWindowKeys(columns, shared, shared, different, expand);
                }
            }
        }
    }

    @Test
    public void windowKeysPreservePointCoordinates() {
        String key = "TIMESTAMPADD(SECOND, CAST(ST_DISTANCE(ST_POINT(x,y), %s) AS INTEGER), ts)";
        checkSharedWindowKeys("id INT NOT NULL, ts TIMESTAMP, x DOUBLE, y DOUBLE",
                key.formatted("ST_POINT(1.0, 2.0)"), key.formatted("ST_POINT(2.0, 1.0)"));
    }

    private static void checkTemporalThreshold(String type, String first, String second) {
        var offset = "CASE WHEN CAST(ts AS %1$s) > %1$s '%2$s' THEN id ELSE 1 END";
        checkOffsetKeys(offset.formatted(type, first), offset.formatted(type, second));
    }

    @Test
    public void conditionalTimestampKeysDistinguishBranchOrderAndStringValues() {
        checkOffsetKeys("CASE WHEN flag THEN id ELSE 1 END", "CASE WHEN flag THEN 1 ELSE id END");
        checkOffsetKeys("CASE WHEN label = 'déjà' THEN id ELSE 1 END",
                "CASE WHEN label = 'deja' THEN id ELSE 1 END");
        checkOffsetKeys("CASE WHEN label IS NULL THEN id ELSE 1 END",
                "CASE WHEN label IS NOT NULL THEN id ELSE 1 END");
    }

    @Test
    public void collectionWindowKeysPreserveTheSelectedElement() {
        String fields = "id INT NOT NULL, ts TIMESTAMP, alt TIMESTAMP, times TIMESTAMP ARRAY";
        checkSharedWindowKeys(fields, "ARRAY[ts, alt][id]", "ARRAY[alt, ts][id]");
        checkSharedWindowKeys(fields, "MAP[1, ts, 2, alt][id]", "MAP[1, alt, 2, ts][id]");
        checkSharedWindowKeys(fields, "times[id]", "times[id + 1]");
    }

    @Test
    public void windowKeysPreserveNullableCollections() {
        String fields = "id INT NOT NULL, ts TIMESTAMP, times TIMESTAMP ARRAY, by_id MAP<INT, TIMESTAMP>";
        String array = "(CASE WHEN id > 0 THEN times ELSE CAST(NULL AS TIMESTAMP ARRAY) END)";
        String map = "(CASE WHEN id > 0 THEN by_id ELSE CAST(NULL AS MAP<INT, TIMESTAMP>) END)";
        checkSharedWindowKeys(fields, array + "[id]", array + "[id + 1]");
        checkSharedWindowKeys(fields, map + "[id]", map + "[id + 1]");
    }

    @Test
    public void literalComparisonsPreserveBinaryAndUuidWindowKeys() {
        String key = "TIMESTAMPADD(SECOND, CASE WHEN tag = %s THEN id ELSE 1 END, ts)";
        String columns = "id INT NOT NULL, ts TIMESTAMP, tag ";
        checkSharedWindowKeys(columns + "UUID",
                key.formatted("UUID '123e4567-e89b-12d3-a456-426655440000'"),
                key.formatted("UUID '123e4567-e89b-12d3-a456-426655440001'"));
        checkSharedWindowKeys(columns + "VARBINARY", key.formatted("X'00FF'"), key.formatted("X'FF00'"));
    }

    @Test
    public void floatingThresholdsDistinguishSharedTimestamps() {
        String key = "TIMESTAMPADD(SECOND, CASE WHEN tag > %s THEN id ELSE 1 END, ts)";
        checkSharedWindowKeys("id INT NOT NULL, ts TIMESTAMP, tag REAL",
                key.formatted("CAST(1.25 AS REAL)"), key.formatted("CAST(1.75 AS REAL)"));
        checkSharedWindowKeys("id INT NOT NULL, ts TIMESTAMP, tag DOUBLE",
                key.formatted("1.25e0"), key.formatted("1.75e0"));
    }

    @Test
    public void variantAndRowValuesDistinguishSharedTimestamps() {
        String key = "TIMESTAMPADD(SECOND, CASE WHEN %s THEN id ELSE 1 END, ts)";
        String variants = "id INT NOT NULL, ts TIMESTAMP, tag VARIANT";
        checkSharedWindowKeys(variants, key.formatted("tag = CAST('blue' AS VARIANT)"),
                key.formatted("tag = CAST('red' AS VARIANT)"));
        checkSharedWindowKeys(variants, key.formatted("tag['x'] = CAST(id AS VARIANT)"),
                key.formatted("tag['y'] = CAST(id AS VARIANT)"));
        checkSharedWindowKeys("id INT NOT NULL, ts TIMESTAMP, tag INT",
                key.formatted("ARRAY_CONTAINS(ARRAY[ROW(id, tag)], ROW(tag, id))"),
                key.formatted("ARRAY_CONTAINS(ARRAY[ROW(id, tag)], ROW(id, tag))"));
    }

    @Test
    public void lambdaWindowKeysCompareBindingsAndBodies() {
        String key = "TIMESTAMPADD(SECOND, ARRAY_MAX(TRANSFORM(arr, %s)), ts)";
        checkSharedWindowKeys("id INT NOT NULL, ts TIMESTAMP, arr INT ARRAY",
                key.formatted("x -> x + x"), key.formatted("y -> y + y"), key.formatted("y -> y + 1"));
    }

    private static void checkOffsetKeys(String shared, String different) {
        String key = "TIMESTAMPADD(SECOND, %s, ts)";
        checkSharedWindowKeys("id INT NOT NULL, ts TIMESTAMP, flag BOOLEAN, label VARCHAR",
                key.formatted(shared), key.formatted(different));
    }

    private static void checkSharedWindowKeys(String fields, String shared, String different) {
        checkSharedWindowKeys(fields, shared, shared, different);
    }

    private static void checkSharedWindowKeys(String fields, String first, String second, String different) {
        checkSharedWindowKeys(fields, first, second, different, compiler -> {});
    }

    private static void checkSharedWindowKeys(String fields, String first, String second, String different,
                                               Consumer<DBSPCompiler> configure) {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustTemporalKeys"));
        for (boolean gen2 : List.of(false, true)) {
            var options = new CompilerOptions();
            options.ioOptions.gen2 = gen2;
            options.ioOptions.rustOptimizerPasses = true;
            var compiler = new DBSPCompiler(options);
            configure.accept(compiler);
            var analyses = Set.of("WindowGroups/Scan", "WindowGroups/Reorder", "WindowGroups/Indexes");
            var answered = new HashSet<String>();
            compiler.stateObserver = new DBSPCompiler.StateObserver() {
                @Override
                public void nativeDecision(String operation, boolean success) {
                    if (analyses.contains(operation)) {
                        Assert.assertTrue(operation, success);
                        answered.add(operation);
                    }
                }

                @Override
                public void before(String parent, int index, int iteration, CircuitTransform pass, DBSPCircuit circuit) {}

                @Override
                public void after(String parent, int index, int iteration, CircuitTransform pass,
                                  DBSPCircuit circuit, boolean completed) {}
            };
            compiler.submitStatementsForCompilation("""
                    SET feldera_window_sharing_threshold = 2;
                    CREATE TABLE t (%s);
                    CREATE VIEW a AS SELECT id FROM t WHERE (%s) >= NOW() - INTERVAL '1' DAY AND id > 0;
                    CREATE VIEW b AS SELECT id FROM t WHERE (%s) >= NOW() - INTERVAL '2' DAY AND id > 1;
                    CREATE VIEW c AS SELECT id FROM t WHERE (%s) >= NOW() - INTERVAL '3' DAY AND id > 2;
                    """.formatted(fields, first, second, different));
            var circuit = compiler.getFinalCircuit(false);
            compiler.throwIfErrorsOccurred();
            if (options.ioOptions.rustOptimizerPasses)
                Assert.assertEquals(analyses, answered);
            var windows = new WindowInputStats(compiler);
            windows.apply(circuit);
            Assert.assertEquals(3, windows.leftInputIds.size());
            Assert.assertEquals(2, windows.distinctInputCount());
        }
    }

    @Test
    public void negativeSharingThresholdPreservesTheWarningAndSharedIndex() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustTemporalKeys"));
        for (boolean nativePasses : List.of(false, true)) {
            var options = new CompilerOptions();
            options.ioOptions.rustOptimizerPasses = nativePasses;
            options.languageOptions.incrementalize = true;
            var compiler = new DBSPCompiler(options);
            compiler.submitStatementsForCompilation("""
                    SET feldera_window_sharing_threshold = -1;
                    CREATE TABLE t (a INT, b INT, ts TIMESTAMP);
                    CREATE VIEW v1 AS SELECT a FROM t WHERE ts >= NOW() - INTERVAL 1 HOUR AND b > 2;
                    CREATE VIEW v2 AS SELECT b FROM t WHERE ts >= NOW() - INTERVAL 2 HOURS AND a < 9;
                    """);
            var circuit = compiler.getFinalCircuit(false);
            Assert.assertNotNull(compiler.messages.toString(), circuit);
            Assert.assertTrue(compiler.messages.toString(), compiler.messages.toString().contains("Invalid setting"));
            var windows = new WindowInputStats(compiler);
            windows.apply(circuit);
            Assert.assertEquals(2, windows.leftInputIds.size());
            Assert.assertEquals(1, windows.distinctInputCount());
        }
    }

    @Test
    public void windowSharingFindsBoundsAfterAResidualAndRetainsTheOriginalKeys() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustTemporalKeys"));
        var compiler = new DBSPCompiler(new CompilerOptions());
        var b = new ExpressionBuilder();
        var ts = DBSPTypeTimestamp.INSTANCE;
        var row = b.refVar(b.tup(ts, ts));
        var first = b.field(row, 0);
        var second = b.field(row, 1);
        var upper = b.binary(DBSPOpcode.GT, new DBSPApplyExpression("now", ts), first);
        var residual = b.binary(DBSPOpcode.NEQ, b.field(row, 0), new DBSPApplyExpression("now", ts));
        var lower = b.binary(DBSPOpcode.GTE, second, new DBSPApplyExpression("now", ts));
        var predicate = b.binary(DBSPOpcode.AND, upper, b.binary(DBSPOpcode.AND, residual, lower));
        var windows = RustCircuitPass.conjunctWindows(compiler, predicate.closure(row));
        Assert.assertNotNull(windows);
        Assert.assertEquals(3, windows.size());
        Assert.assertSame(first, windows.get(0).key());
        Assert.assertFalse(windows.get(0).lower());
        Assert.assertNull(windows.get(1));
        Assert.assertSame(second, windows.get(2).key());
        Assert.assertTrue(windows.get(2).lower());
    }

    private static TemporalFilter key(String rowName, String localName, String aliasName, boolean readRow) {
        var zero = new DBSPI32Literal(0);
        var row = new DBSPVariablePath(rowName, zero.getType());
        var local = new DBSPLetStatement(localName, row);
        var alias = new DBSPVariablePath(aliasName, zero.getType());
        var bind = new DBSPLetExpression(alias, local.getVarReference(), readRow ? row.deepCopy() : alias);
        var body = new DBSPBlockExpression(List.of(local), bind);
        return new TemporalFilter(row.asParameter(), body, zero, DBSPOpcode.GTE);
    }

    @Test
    public void deepClockBindingsKeepTheOriginalWindowKey() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustTemporalKeys"));
        var compiler = new DBSPCompiler(new CompilerOptions());
        var builder = new ExpressionBuilder();
        var timestamp = DBSPTypeTimestamp.INSTANCE;
        var row = builder.refVar(builder.tup(timestamp));
        var key = builder.field(row, 0);
        DBSPExpression clock = new DBSPApplyExpression("now", timestamp);
        for (int i = 0; i < 180; i++) {
            var alias = new DBSPVariablePath("t" + i, timestamp);
            clock = new DBSPLetExpression(alias, clock, alias.deepCopy());
        }
        var predicate = builder.binary(DBSPOpcode.GTE, key, clock);
        var filter = predicate.closure(row);
        var windows = RustCircuitPass.conjunctWindows(compiler, filter);
        Assert.assertNotNull(windows);
        Assert.assertEquals(1, windows.size());
        Assert.assertSame(key, windows.get(0).key());
        Assert.assertTrue(windows.get(0).lower());
        var terms = RustCircuitPass.inferTemporalPredicates(compiler, filter.parameters[0], predicate);
        Assert.assertNotNull(terms);
        Assert.assertEquals(1, terms.size());
        Assert.assertSame(key, terms.get(0).expression());
        Assert.assertSame(clock, terms.get(0).clock());
    }

    @Test
    public void temporalBoundsCompareLocalDeclarationsInScope() {
        var compiler = new DBSPCompiler(new CompilerOptions());
        var first = key("r", "r", "x", false);
        for (boolean readRow : List.of(false, true)) {
            var next = key("row", "local", "alias", readRow);
            assertCompatible(compiler, first, next, !readRow);
        }
    }

    private static void assertCompatible(DBSPCompiler compiler, TemporalFilter first, TemporalFilter next,
                                         boolean expected) {
        Assert.assertEquals(expected, first.compatible(next));
        if (Boolean.getBoolean("feldera.optimizer.compareRustTemporalKeys")) {
            var result = RustCircuitPass.acceptsTemporalBound(compiler, List.of("GTE", "GTE"),
                    List.of(first.parameter(), first.noNow(), next.parameter(), next.noNow()));
            Assert.assertEquals(Boolean.valueOf(expected), result);
        }
    }

    private static TemporalFilter bound(DBSPExpression key) {
        var zero = new DBSPI32Literal(0);
        return new TemporalFilter(new DBSPVariablePath("row", zero.getType()).asParameter(),
                key, zero, DBSPOpcode.GTE);
    }

    @Test
    public void temporalKeysReportTheFirstMissingDeclaration() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustTemporalKeys"));
        var type = new DBSPI32Literal(0).getType();
        var compiler = new DBSPCompiler(new CompilerOptions());
        var decisions = new ArrayList<String>();
        compiler.stateObserver = new DBSPCompiler.StateObserver() {
            @Override
            public void nativeDecision(String operation, boolean success) {
                Assert.assertTrue(operation, success);
                decisions.add(operation);
            }
            @Override
            public void before(String parent, int index, int iteration, CircuitTransform pass, DBSPCircuit graph) {}
            @Override
            public void after(String parent, int index, int iteration, CircuitTransform pass, DBSPCircuit graph, boolean done) {}
        };
        for (String leftName : List.of("left", "row")) {
            var left = new DBSPVariablePath(leftName, type);
            var right = new DBSPVariablePath("right", type);
            // A cast compares its source before either its safety or its result type.
            var first = bound(new DBSPCastExpression(CalciteObject.EMPTY, left, type,
                    DBSPCastExpression.CastType.SqlUnsafe));
            var second = bound(new DBSPCastExpression(CalciteObject.EMPTY, right, type.withMayBeNull(true),
                    DBSPCastExpression.CastType.SqlSafe));
            var direct = Assert.assertThrows(InternalCompilerError.class, () -> first.compatible(second));
            var message = direct.getMessage().lines().findFirst().orElseThrow();
            Assert.assertEquals("Declaration for variable '" + (leftName.equals("row") ? "right" : "left") +
                    "' not found", message);
            decisions.clear();
            var merge = Assert.assertThrows(InternalCompilerError.class,
                    () -> NativeTemporalComparisons.canMerge(compiler, List.of(first), second));
            var group = Assert.assertThrows(InternalCompilerError.class,
                    () -> NativeTemporalComparisons.combine(compiler, List.of(first, second)));
            Assert.assertEquals(message, merge.getMessage().lines().findFirst().orElseThrow());
            Assert.assertEquals(message, group.getMessage().lines().findFirst().orElseThrow());
            Assert.assertEquals(List.of("MergeTemporalBounds", "TemporalGroups"), decisions);
        }
    }

    @Test
    public void zonedLiteralKeysUseTheJavaShortIdsInWinterAndSummer() {
        var compiler = new DBSPCompiler(new CompilerOptions());
        for (var alias : ZoneId.SHORT_IDS.keySet()) {
            for (var local : List.of("2020-01-15 12:00:00", "2020-07-15 12:00:00")) {
                var timestamp = new TimestampWithTimeZoneString(local + " " + alias);
                var utc = timestamp.withTimeZone(TimeZone.getTimeZone("UTC"));
                var type = DBSPTypeTimestampTz.INSTANCE;
                var site = CalciteObject.EMPTY;
                assertCompatible(compiler, bound(new DBSPTimestampTzLiteral(site, type, timestamp)),
                        bound(new DBSPTimestampTzLiteral(site, type, utc)), true);
            }
        }
    }

    @Test
    public void zonedLiteralKeysUseJdkSystemVAndCustomOffsets() {
        var compiler = new DBSPCompiler(new CompilerOptions());
        var zones = new ArrayList<>(List.of("GMT+5", "GMT-330", "GMT+0545", "GMT-0:30", "GMT+18:00"));
        ZoneId.getAvailableZoneIds().stream().filter(name -> name.startsWith("SystemV/")).sorted()
                .forEach(zones::add);
        var format = DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss");
        for (var zone : zones) {
            for (var local : List.of("1899-07-15 12:00:00", "1900-04-29 01:59:59", "1900-04-29 03:00:00",
                    "1974-01-06 01:59:59", "1974-01-06 03:00:00", "1974-11-24 01:30:00", "1974-11-24 02:00:00",
                    "1975-02-23 03:00:00", "2020-03-15 12:00:00", "2020-07-15 12:00:00", "2020-10-25 01:30:00")) {
                var timestamp = new TimestampWithTimeZoneString(local + " " + zone);
                var utc = LocalDateTime.parse(local, format).atZone(timestamp.getTimeZone().toZoneId())
                        .withZoneSameInstant(ZoneOffset.UTC);
                var canonical = new TimestampWithTimeZoneString(format.format(utc) + " UTC");
                var type = DBSPTypeTimestampTz.INSTANCE;
                assertCompatible(compiler, bound(new DBSPTimestampTzLiteral(CalciteObject.EMPTY, type, timestamp)),
                        bound(new DBSPTimestampTzLiteral(CalciteObject.EMPTY, type, canonical)), true);
            }
        }
    }

    @Test
    public void temporalKeyEqualityUsesTheInstantEmittedByTheCompiler() {
        var compiler = new DBSPCompiler(new CompilerOptions());
        var type = DBSPTypeTimestampTz.INSTANCE;
        var format = DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss");
        for (var spelling : List.of("1974-02-01 12:30:01 America/Tijuana", "2040-07-15 12:30:01 Africa/Casablanca")) {
            var local = new TimestampWithTimeZoneString(spelling);
            var value = new DBSPTimestampTzLiteral(CalciteObject.EMPTY, type, local);
            long second = value.getMicros() / 1_000_000;
            for (int shift : List.of(-3600, 0, 3600)) {
                var utc = LocalDateTime.ofEpochSecond(second + shift, 0, ZoneOffset.UTC);
                var expected = new TimestampWithTimeZoneString(format.format(utc) + " UTC");
                var other = new DBSPTimestampTzLiteral(CalciteObject.EMPTY, type, expected);
                assertCompatible(compiler, bound(value), bound(other), shift == 0);
                assertCompatible(compiler, bound(other), bound(value), shift == 0);
            }
        }
        // The JDK accepts this custom TimeZone, but java.time cannot represent
        // its offset. An earlier opcode mismatch must still avoid comparing it.
        var invalid = new DBSPTimestampTzLiteral(CalciteObject.EMPTY, type,
                new TimestampWithTimeZoneString("2020-01-01 00:00:00 GMT+19:00"));
        var first = bound(invalid);
        var strict = new TemporalFilter(first.parameter(), invalid.deepCopy(), first.withNow(), DBSPOpcode.GT);
        Assert.assertFalse(first.compatible(strict));
        if (Boolean.getBoolean("feldera.optimizer.compareRustTemporalKeys"))
            Assert.assertFalse(NativeTemporalComparisons.canMerge(compiler, List.of(first), strict));
    }

    @Test
    public void temporalMergeReportsTheFirstInvalidKeyWithoutReanalyzingThePair() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustTemporalKeys"));
        var b = new ExpressionBuilder();
        var time = DBSPTypeTimestamp.INSTANCE;
        var east = new DBSPTimestampTzLiteral(CalciteObject.EMPTY, DBSPTypeTimestampTz.INSTANCE,
                new TimestampWithTimeZoneString("2020-01-01 00:00:00 GMT+19:00"));
        var west = new DBSPTimestampTzLiteral(CalciteObject.EMPTY, DBSPTypeTimestampTz.INSTANCE,
                new TimestampWithTimeZoneString("2020-01-01 00:00:00 GMT-20:00"));
        for (var pair : List.of(List.of(east, west), List.of(west, east))) {
            // An earlier, independent chain has already merged and allocated names.
            var circuit = temporalFilterChain(b.call(time, "now"));
            var rows = circuit.allOperators.getFirst().to(DBSPConstantOperator.class).outputPort();
            for (var offset : pair) {
                var row = b.refVar(rows.getOutputZSetElementType());
                var key = b.call(time, "shift_timestamp", b.field(row, 0), offset);
                var predicate = b.binary(DBSPOpcode.GTE, key, b.call(time, "now"));
                var filter = new DBSPFilterOperator(CalciteEmptyRel.INSTANCE, predicate.closure(row), rows);
                circuit.addOperator(filter);
                rows = filter.outputPort();
            }
            var compiler = new DBSPCompiler(new CompilerOptions());
            var invalid = Assert.assertThrows(java.time.DateTimeException.class, pair.getFirst()::getMicros);
            long start = DBSPVariablePath.nextGeneratedName();
            var direct = Assert.assertThrows(invalid.getClass(), () -> new MergeFilters(compiler).apply(circuit));
            long names = DBSPVariablePath.nextGeneratedName() - start;
            Assert.assertTrue(names > 0);
            Assert.assertEquals(invalid.getMessage(), direct.getMessage());
            compiler.options.ioOptions.rustOptimizerPasses = true;
            var pass = new RustCircuitPass(compiler, nativeMergeOnly(compiler), new OptimizerSnapshots(compiler));
            start = DBSPVariablePath.nextGeneratedName();
            var hybrid = Assert.assertThrows(direct.getClass(), () -> pass.apply(circuit));
            Assert.assertEquals(direct.getMessage(), hybrid.getMessage());
            Assert.assertEquals(names, DBSPVariablePath.nextGeneratedName() - start);
        }
    }

    @Test
    public void temporalKeysReportInvalidOffsetsInOperandOrder() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustTemporalKeys"));
        var compiler = new DBSPCompiler(new CompilerOptions());
        var type = DBSPTypeTimestampTz.INSTANCE;
        var valid = bound(new DBSPTimestampTzLiteral(CalciteObject.EMPTY, type,
                new TimestampWithTimeZoneString("2020-01-01 00:00:00 UTC")));
        var east = bound(new DBSPTimestampTzLiteral(CalciteObject.EMPTY, type,
                new TimestampWithTimeZoneString("2020-01-01 00:00:00 GMT+19:00")));
        var west = bound(new DBSPTimestampTzLiteral(CalciteObject.EMPTY, type,
                new TimestampWithTimeZoneString("2020-01-01 00:00:00 GMT-20:00")));
        for (var pair : List.of(List.of(valid, east), List.of(west, valid), List.of(east, west), List.of(west, east))) {
            var left = pair.get(0);
            var right = pair.get(1);
            var direct = Assert.assertThrows(java.time.DateTimeException.class, () -> left.compatible(right));
            var hybrid = Assert.assertThrows(direct.getClass(), () -> RustCircuitPass.acceptsTemporalBound(
                    compiler, List.of("GTE", "GTE"), List.of(left.parameter(), left.noNow(), right.parameter(), right.noNow()), true));
            Assert.assertEquals(direct.getMessage(), hybrid.getMessage());
        }
        assertCompatible(compiler, east, east, true);
    }

    @Test
    public void blockItemsCompareUnequalAfterCheckingEarlierInitializers() {
        var compiler = new DBSPCompiler(new CompilerOptions());
        var literal = new DBSPI32Literal(1);
        var item = new DBSPStaticItem(new DBSPStaticExpression(CalciteObject.EMPTY, literal, "saved"));
        var single = bound(new DBSPBlockExpression(List.of(item), literal));
        assertCompatible(compiler, single, single, false);

        var missing = new DBSPVariablePath("missing", literal.getType());
        var first = bound(new DBSPBlockExpression(List.of(new DBSPLetStatement("x", missing, false), item), literal));
        var second = bound(new DBSPBlockExpression(List.of(new DBSPLetStatement("y", missing.deepCopy(), true), item), literal));
        Assert.assertThrows(RuntimeException.class, () -> first.compatible(second));
        if (Boolean.getBoolean("feldera.optimizer.compareRustTemporalKeys")) {
            Assert.assertNull(RustCircuitPass.acceptsTemporalBound(compiler, List.of("GTE", "GTE"),
                    List.of(first.parameter(), first.noNow(), second.parameter(), second.noNow())));
        }
    }

    private static TemporalFilter loop(String variable, String read, int input) {
        var value = new DBSPI32Literal(input);
        var body = new DBSPBlockExpression(List.of(), new DBSPVariablePath(read, value.getType()));
        return bound(new DBSPForExpression(variable, value, body));
    }

    @Test
    public void loopComparesItsInputBeforeBindingTheBody() {
        var compiler = new DBSPCompiler(new CompilerOptions());
        var first = loop("item", "item", 1);
        assertCompatible(compiler, first, loop("item", "item", 1), true);
        assertCompatible(compiler, first, loop("item", "row", 1), false);
        assertCompatible(compiler, first, loop("item", "item", 2), false);
        var renamed = loop("other", "other", 1);
        // The Java visitor enters the left name on both sides and diagnoses the unbound read.
        Assert.assertThrows(RuntimeException.class, () -> first.compatible(renamed));
        if (Boolean.getBoolean("feldera.optimizer.compareRustTemporalKeys"))
            Assert.assertNull(RustCircuitPass.acceptsTemporalBound(compiler, List.of("GTE", "GTE"),
                    List.of(first.parameter(), first.noNow(), renamed.parameter(), renamed.noNow())));
    }

    private static DBSPFlatmap flatmap(IdShuffle shuffle, int passthrough, DBSPExpression projection) {
        var type = new DBSPI32Literal(0).getType();
        var rowType = new DBSPTypeTuple(type, type, new DBSPTypeArray(type, false));
        var row = rowType.ref().var();
        var element = new DBSPVariablePath("element", type);
        return new DBSPFlatmap(CalciteObject.EMPTY, rowType, row.deref().field(2).closure(row),
                List.of(passthrough), List.of(projection.closure(element)), null, shuffle);
    }

    @Test
    public void candidateKeysOpenAnEmptyScopeForParameterlessProjections() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustTemporalKeys"));
        var compiler = new DBSPCompiler(new CompilerOptions());
        var first = new DBSPForExpression("item", new DBSPI32Literal(1), new DBSPBlockExpression(List.of(), null));
        var same = new DBSPForExpression("item", new DBSPI32Literal(1), new DBSPBlockExpression(List.of(), null));
        var other = new DBSPForExpression("item", new DBSPI32Literal(2), new DBSPBlockExpression(List.of(), null));
        var context = new EquivalenceContext();
        context.leftDeclaration.newContext();
        context.rightDeclaration.newContext();
        Assert.assertTrue(context.equivalent(first, same));
        Assert.assertFalse(context.equivalent(first, other));
        var shape = new ZSetShape(3);
        var function = new DBSPTupleExpression(first, other, same).closure();
        Assert.assertEquals(List.of(List.of(shape.output(0), shape.output(2))),
                RustCircuitPass.outputColumns(compiler, function, shape, false).groups());
    }

    @Test
    public void candidateKeysReportTheFirstInvalidComputedColumn() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustTemporalKeys"));
        var compiler = new DBSPCompiler(new CompilerOptions());
        var type = DBSPTypeTimestampTz.INSTANCE;
        var east = new DBSPTimestampTzLiteral(CalciteObject.EMPTY, type,
                new TimestampWithTimeZoneString("2020-01-01 00:00:00 GMT+19:00"));
        var west = new DBSPTimestampTzLiteral(CalciteObject.EMPTY, type,
                new TimestampWithTimeZoneString("2020-01-01 00:00:00 GMT-20:00"));
        var shape = new ZSetShape(2);
        for (var pair : List.of(List.of(east, west), List.of(west, east))) {
            var first = pair.get(0);
            var second = pair.get(1);
            var direct = Assert.assertThrows(java.time.DateTimeException.class, () -> first.equivalent(second));
            var closure = new DBSPTupleExpression(first, second).closure();
            var hybrid = Assert.assertThrows(direct.getClass(), () -> RustCircuitPass.outputColumns(compiler, closure, shape, false));
            Assert.assertEquals(direct.getMessage(), hybrid.getMessage());
        }
    }

    @Test
    public void candidateKeysGroupFlatmapsByTheirLiveShuffle() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustTemporalKeys"));
        var compiler = new DBSPCompiler(new CompilerOptions());
        var b = new ExpressionBuilder();
        var shared = new IdShuffle(2);
        var row = b.tup(b.i32());
        var parameter = b.refVar(row);
        var first = flatmap(shared, 0, b.lit(1));
        var distinct = flatmap(new IdShuffle(2), 0, b.lit(1));
        var same = flatmap(shared, 0, b.lit(1));
        Assert.assertTrue(first.equivalent(same));
        Assert.assertFalse(first.equivalent(distinct));
        var body = new DBSPTupleExpression(first, distinct, same);
        var function = body.closure(parameter);
        var shape = new ZSetShape(3);
        var columns = RustCircuitPass.outputColumns(compiler, function, shape, false);
        Assert.assertEquals(List.of(List.of(shape.output(0), shape.output(2))), columns.groups());

        var site = CalciteEmptyRel.INSTANCE;
        var input = new DBSPConstantOperator(site, DBSPZSetExpression.emptyWithElementType(row));
        var map = new DBSPMapOperator(site, function, input.outputPort());
        var circuit = new DBSPCircuit(new ProgramMetadata(), false);
        circuit.addOperator(input);
        circuit.addOperator(map);
        Assert.assertTrue(RustCircuitPass.circuitKeys(compiler, circuit, false, (op, facts) -> {}));
    }

    @Test
    public void flatmapKeysCompareShuffleIdentityAndPostprocessingClosures() {
        var compiler = new DBSPCompiler(new CompilerOptions());
        var shuffle = new IdShuffle(2);
        var one = new DBSPI32Literal(1);
        var first = bound(flatmap(shuffle, 0, one));
        assertCompatible(compiler, first, bound(flatmap(shuffle, 0, one.deepCopy())), true);
        assertCompatible(compiler, first, bound(flatmap(new IdShuffle(2), 0, one)), false);
        assertCompatible(compiler, first, bound(flatmap(shuffle, 1, one)), false);
        assertCompatible(compiler, first, bound(flatmap(shuffle, 0, new DBSPI32Literal(2))), false);
        var free = bound(flatmap(shuffle, 0, new DBSPVariablePath("row", one.getType())));
        var other = bound(flatmap(shuffle, 0, new DBSPVariablePath("row", one.getType())));
        Assert.assertThrows(RuntimeException.class, () -> free.compatible(other));
        if (Boolean.getBoolean("feldera.optimizer.compareRustTemporalKeys"))
            Assert.assertNull(RustCircuitPass.acceptsTemporalBound(compiler, List.of("GTE", "GTE"),
                    List.of(free.parameter(), free.noNow(), other.parameter(), other.noNow())));
    }

    @Test
    public void comparatorAndGenericKeyTypesCompareByNameAndArguments() {
        var compiler = new DBSPCompiler(new CompilerOptions());
        var value = new DBSPI32Literal(1);
        var comparator = DBSPComparatorType.generateType(value.getType());
        var key = bound(new DBSPQualifyTypeExpression(value, comparator));
        var named = new DBSPTypeUser(CalciteObject.EMPTY, DBSPTypeCode.USER, comparator.name, false);
        var generic = bound(new DBSPQualifyTypeExpression(value, named));
        assertCompatible(compiler, key, generic, true);
        assertCompatible(compiler, generic, key, true);
        var extra = new DBSPTypeUser(CalciteObject.EMPTY, DBSPTypeCode.USER, comparator.name, false, value.getType());
        assertCompatible(compiler, key, bound(new DBSPQualifyTypeExpression(value, extra)), false);
        var descending = DBSPComparatorType.generateType(comparator, false);
        assertCompatible(compiler, key, bound(new DBSPQualifyTypeExpression(value, descending)), false);
    }

    @Test
    public void keyTypeArgumentsDistinguishFunctionSignaturesAndStreams() {
        var compiler = new DBSPCompiler(new CompilerOptions());
        var value = new DBSPI32Literal(1);
        var type = value.getType();
        var nullable = type.withMayBeNull(true);
        var signature = new DBSPTypeFunction(type, type, nullable);
        var stream = new DBSPTypeStream(type, StreamKind.DELTA, 0);
        var first = bound(new DBSPQualifyTypeExpression(value, signature, stream));
        var same = new DBSPTypeFunction(type, type, nullable);
        var delta = new DBSPTypeStream(type, StreamKind.DELTA, 0);
        assertCompatible(compiler, first, bound(new DBSPQualifyTypeExpression(value, same, delta)), true);
        for (var other : List.of(new DBSPTypeFunction(nullable, type, nullable),
                new DBSPTypeFunction(type, nullable, type), new DBSPTypeFunction(type, type))) {
            assertCompatible(compiler, first, bound(new DBSPQualifyTypeExpression(value, other, delta)), false);
        }
        for (var other : List.of(new DBSPTypeStream(nullable, StreamKind.DELTA, 0),
                new DBSPTypeStream(type, StreamKind.COLLECTION, 0), new DBSPTypeStream(type, StreamKind.DELTA, 1))) {
            assertCompatible(compiler, first, bound(new DBSPQualifyTypeExpression(value, same, other)), false);
        }
    }

    private static DBSPZSetExpression nestedConstantRow(int leaf) {
        DBSPExpression row = new DBSPI32Literal(leaf);
        for (int i = 0; i < 144; i++)
            row = new DBSPTupleExpression(row);
        return new DBSPZSetExpression(row);
    }

    @Test
    public void deeplyNestedConstantRowsCompareByValueInWindowKeys() {
        var compiler = new DBSPCompiler(new CompilerOptions());
        var one = bound(nestedConstantRow(1));
        assertCompatible(compiler, one, bound(nestedConstantRow(1)), true);
        assertCompatible(compiler, one, bound(nestedConstantRow(2)), false);
    }

    private static TemporalFilter arrayRowKey(DBSPExpression element) {
        return bound(new DBSPZSetExpression(new DBSPArrayExpression(element)));
    }

    @Test
    public void constantRowKeysReportTheFirstInvalidTimestamp() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustTemporalKeys"));
        var compiler = new DBSPCompiler(new CompilerOptions());
        var type = DBSPTypeTimestampTz.INSTANCE;
        var east = new DBSPTimestampTzLiteral(CalciteObject.EMPTY, type,
                new TimestampWithTimeZoneString("2020-01-01 00:00:00 GMT+19:00"));
        var west = new DBSPTimestampTzLiteral(CalciteObject.EMPTY, type,
                new TimestampWithTimeZoneString("2020-01-01 00:00:00 GMT-20:00"));
        for (var pair : List.of(List.of(east, west), List.of(west, east))) {
            for (boolean array : List.of(false, true)) {
                var a = pair.get(0);
                var b = pair.get(1);
                var left = array ? arrayRowKey(a) : bound(new DBSPZSetExpression(a));
                var right = array ? arrayRowKey(b) : bound(new DBSPZSetExpression(b));
                var direct = Assert.assertThrows(java.time.DateTimeException.class, () -> left.compatible(right));
                var hybrid = Assert.assertThrows(direct.getClass(), () -> RustCircuitPass.acceptsTemporalBound(
                        compiler, List.of("GTE", "GTE"),
                        List.of(left.parameter(), left.noNow(), right.parameter(), right.noNow()), true));
                Assert.assertEquals(direct.getMessage(), hybrid.getMessage());
            }
        }
    }

    @Test
    public void constantArrayKeysCompareSqlTupleFieldIdentities() {
        var compiler = new DBSPCompiler(new CompilerOptions());
        var one = new DBSPI32Literal(1);
        var key = arrayRowKey(new DBSPTupleExpression(one));
        assertCompatible(compiler, key, arrayRowKey(new DBSPTupleExpression(one)), true);
        assertCompatible(compiler, key, arrayRowKey(new DBSPTupleExpression(new DBSPI32Literal(1))), false);
    }

    @Test
    public void constantArrayKeysCompareRawTupleElementsAndPoints() {
        var compiler = new DBSPCompiler(new CompilerOptions());
        var key = arrayRowKey(new DBSPRawTupleExpression(new DBSPI32Literal(1), new DBSPGeoPointConstructor()));
        assertCompatible(compiler, key,
                arrayRowKey(new DBSPRawTupleExpression(new DBSPI32Literal(1), new DBSPGeoPointConstructor())), true);
        assertCompatible(compiler, key,
                arrayRowKey(new DBSPRawTupleExpression(new DBSPI32Literal(2), new DBSPGeoPointConstructor())), false);
    }

    @Test
    public void computedRowKeysReplaceWeightsWithoutCancellation() {
        var compiler = new DBSPCompiler(new CompilerOptions());
        var value = new DBSPApplyExpression("opaque", new DBSPI32Literal(0).getType());
        var set = bound(new DBSPZSetExpression(value));
        assertCompatible(compiler, set, bound(new DBSPZSetExpression(value)), false);
        var tuple = new DBSPTupleExpression(value);
        var rows = bound(new DBSPZSetExpression(tuple));
        assertCompatible(compiler, rows, bound(new DBSPZSetExpression(new DBSPTupleExpression(value))), false);
        assertCompatible(compiler, rows, bound(new DBSPZSetExpression(tuple)), true);
    }

    @Test
    public void collectionWeightsQualifiersAndAggregateOperationsAffectKeyEquality() {
        var compiler = new DBSPCompiler(new CompilerOptions());
        var one = new DBSPI32Literal(1);
        var two = new DBSPI32Literal(2);
        var type = one.getType();
        var rows = new DBSPZSetExpression(one, two, one);
        var reordered = new DBSPZSetExpression(two, one, one);
        var key = bound(new DBSPQualifyTypeExpression(rows, type));
        assertCompatible(compiler, key, bound(new DBSPQualifyTypeExpression(reordered, type)), true);
        assertCompatible(compiler, key, bound(new DBSPQualifyTypeExpression(reordered, type.withMayBeNull(true))), false);
        reordered.append(two, -2);
        assertCompatible(compiler, key, bound(new DBSPQualifyTypeExpression(reordered, type)), false);

        var min = bound(new DBSPMinMax(CalciteObject.EMPTY, type, null, DBSPMinMax.Aggregation.Min));
        for (var op : List.of(DBSPMinMax.Aggregation.Min, DBSPMinMax.Aggregation.Max)) {
            var other = bound(new DBSPMinMax(CalciteObject.EMPTY, type, null, op));
            assertCompatible(compiler, min, other, op == DBSPMinMax.Aggregation.Min);
        }
    }
}
