package org.dbsp.sqlCompiler.compiler.sql.tools;

import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.functions.CustomFunctions;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.functions.FunctionDocumentation;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

public class S1BuiltinOperatorRegistryTests {
    @Test public void spatialOverloadsUseExactObjectsAndMethodSignatures() throws IOException {
        var registry = new S1BuiltinOperatorRegistry();
        var points = registry.entries().entrySet().stream().filter(entry ->
                entry.getKey().registry().equals(S1SpatialOperatorRegistry.NAME)
                        && entry.getValue().getName().equals("ST_POINT")).toList();
        Assert.assertTrue(points.size() > 1);
        Assert.assertEquals(points.size(), points.stream().map(entry -> entry.getKey().key()).distinct().count());
        for (var entry: points) Assert.assertSame(entry.getValue(), registry.resolve(entry.getKey()));
        Assert.assertThrows(IOException.class, () -> registry.resolve(new S1BuiltinOperatorRegistry.Identity(
                S1SpatialOperatorRegistry.NAME, S1SpatialOperatorRegistry.VERSION, "java.lang.Runtime#exec")));
    }
    @Test public void internalUnicodeWrapperKeepsTheExactLoweringSingleton() throws IOException {
        var registry = new S1BuiltinOperatorRegistry();
        var wrapper = org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.NonBmpLiterals.Wrapper.INSTANCE;
        var identity = registry.identify(wrapper);
        Assert.assertEquals(S1BuiltinOperatorRegistry.INTERNAL_NAME, identity.registry());
        Assert.assertSame(wrapper, registry.resolve(identity));
        Assert.assertThrows(IOException.class,
                () -> registry.identify(new SqlSpecialOperator("NON_BMP_LITERAL", SqlKind.OTHER)));
    }
    @Test public void arrayAggregateRetainsFelderasReplacementSingleton() throws IOException {
        var registry = new S1BuiltinOperatorRegistry();
        var operator = org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.functions.SqlArrayAggFunction.INSTANCE;
        var identity = registry.identify(operator);
        Assert.assertEquals(S1BuiltinOperatorRegistry.INTERNAL_NAME, identity.registry());
        Assert.assertSame(operator, registry.resolve(identity));
        Assert.assertNotEquals(registry.identify(org.apache.calcite.sql.fun.SqlLibraryOperators.ARRAY_AGG), identity);
    }

    @Test public void datetimeSubtractionRetainsItsInternalSingleton() throws IOException {
        var registry = new S1BuiltinOperatorRegistry();
        var operator = org.apache.calcite.sql.fun.SqlInternalOperators.MINUS_DATE2;
        var identity = registry.identify(operator);
        Assert.assertEquals("SqlInternalOperators.MINUS_DATE2", identity.key());
        Assert.assertSame(operator, registry.resolve(identity));
        Assert.assertNotEquals(registry.identify(SqlStdOperatorTable.MINUS), identity);
    }

    @Test public void everyBuiltinResolvesToItsExactObject() throws IOException {
        var registry = new S1BuiltinOperatorRegistry();
        var fresh = new S1BuiltinOperatorRegistry();
        for (var entry: registry.entries().entrySet()) {
            Assert.assertSame(entry.getValue(), registry.resolve(entry.getKey()));
            Assert.assertSame(entry.getValue(), fresh.resolve(entry.getKey()));
            Assert.assertSame(entry.getValue(), fresh.resolve(registry.identify(entry.getValue())));
        }
    }

    @Test public void felderaClonesRetainTheirIdentityRatherThanUsingCalciteNames() throws IOException {
        var registry = new S1BuiltinOperatorRegistry();
        var description = new CustomFunctions().getDescriptions().stream()
                .filter(function -> function.functionName().equals("abs")).findFirst().orElseThrow();
        var custom = (SqlOperator) description;
        Assert.assertNotSame(SqlStdOperatorTable.ABS, custom);
        var identity = registry.identify(custom);
        Assert.assertEquals(S1BuiltinOperatorRegistry.CUSTOM_NAME, identity.registry());
        Assert.assertSame(custom, registry.resolve(identity));
        Assert.assertNotEquals(registry.identify(SqlStdOperatorTable.ABS), identity);
        Assert.assertThrows(IOException.class,
                () -> registry.identify(new SqlSpecialOperator("abs", SqlKind.OTHER_FUNCTION)));
    }

    @Test public void unknownCustomKeysAndVersionsFail() {
        var registry = new S1BuiltinOperatorRegistry();
        Assert.assertThrows(IOException.class, () -> registry.resolve(new S1BuiltinOperatorRegistry.Identity(
                S1BuiltinOperatorRegistry.CUSTOM_NAME, "future", "anything")));
        Assert.assertThrows(IOException.class, () -> registry.resolve(new S1BuiltinOperatorRegistry.Identity(
                S1BuiltinOperatorRegistry.CUSTOM_NAME, S1BuiltinOperatorRegistry.CUSTOM_VERSION,
                "java.lang.Runtime#getRuntime")));
    }

    private static final class Duplicate extends SqlSpecialOperator
            implements FunctionDocumentation.FunctionDescription {
        Duplicate() { super("same_name", SqlKind.OTHER_FUNCTION); }
        @Override public String functionName() { return this.getName(); }
        @Override public String documentation() { return ""; }
        @Override public boolean aggregate() { return false; }
        @Override public String testedBy() { return ""; }
    }

    @Test public void ambiguousObjectsCannotShareARegistryKey() {
        Assert.assertThrows(IllegalStateException.class,
                () -> new S1BuiltinOperatorRegistry(List.of(new Duplicate(), new Duplicate())));
    }
}
