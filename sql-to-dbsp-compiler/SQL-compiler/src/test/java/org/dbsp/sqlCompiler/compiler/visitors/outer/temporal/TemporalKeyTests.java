package org.dbsp.sqlCompiler.compiler.visitors.outer.temporal;

import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.circuit.operator.DBSPConstantOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPFilterOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapOperator;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.ProgramMetadata;
import org.dbsp.sqlCompiler.compiler.backend.RustCircuitPass;
import org.dbsp.sqlCompiler.compiler.backend.ToJsonOuterVisitor.OptimizerSnapshots;
import org.dbsp.sqlCompiler.compiler.errors.SourcePosition;
import org.dbsp.sqlCompiler.compiler.errors.SourcePositionRange;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteEmptyRel;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitTransform;
import org.dbsp.sqlCompiler.compiler.visitors.outer.Graph;
import org.dbsp.sqlCompiler.compiler.visitors.outer.WindowInputStats;
import org.dbsp.sqlCompiler.compiler.visitors.outer.expandCasts.ExpandCasts;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.sql.tools.ExpressionBuilder;
import org.dbsp.sqlCompiler.ir.aggregate.DBSPMinMax;
import org.dbsp.sqlCompiler.ir.expression.DBSPApplyExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPBlockExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPCastExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPUnaryExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPFlatmap;
import org.dbsp.sqlCompiler.ir.expression.DBSPForExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPLetExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPOpcode;
import org.dbsp.sqlCompiler.ir.expression.DBSPQualifyTypeExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.expression.DBSPZSetExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPI32Literal;
import org.dbsp.sqlCompiler.ir.statement.DBSPLetStatement;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeCode;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeFunction;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeTimestamp;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeArray;
import org.dbsp.sqlCompiler.ir.type.user.DBSPComparatorType;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeStream;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeUser;
import org.dbsp.sqlCompiler.ir.type.user.StreamKind;
import org.dbsp.util.IdShuffle;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

public class TemporalKeyTests {
    @Test
    public void windowScanReportsCastWarningAtItsOriginalRange() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustTemporalKeys"));
        var b = new ExpressionBuilder();
        var range = new SourcePositionRange(new SourcePosition(3, 2), new SourcePosition(3, 17));
        var rel = CalciteEmptyRel.INSTANCE;
        var time = DBSPTypeTimestamp.INSTANCE;
        var inputType = b.tup(time.withMayBeNull(true));
        for (boolean replayPaths : List.of(false, true)) {
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
            if (replayPaths) {
                // This key needs the canonical printer after the preceding map has emitted its warning.
                var second = b.refVar(inputType);
                var condition = b.binary(DBSPOpcode.GTE, b.unwrap(b.field(second, 0)), clock.deepCopy());
                circuit.addOperator(new DBSPFilterOperator(rel, condition.closure(second), input.outputPort()));
            }

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
            Assert.assertEquals(List.of(!replayPaths), decisions);
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
