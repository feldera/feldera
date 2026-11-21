package org.dbsp.sqlCompiler.compiler.sql.tools;

import org.apache.calcite.sql.SqlOperator;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.functions.CustomFunctions;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.functions.FunctionDocumentation;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.NonBmpLiterals;

import java.io.IOException;
import java.util.Collection;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/** Trusted builtins for the S1 expression adapter. Custom function descriptors
 * contain the actual singleton operators, including Feldera's deliberate clones
 * of Calcite functions. No display-name fallback or wire-driven reflection occurs. */
final class S1BuiltinOperatorRegistry {
    static final String CUSTOM_NAME = "feldera-s1-builtins";
    static final String CUSTOM_VERSION = "1";
    static final String INTERNAL_NAME = "feldera-s1-internals";
    static final String INTERNAL_VERSION = "1";
    private static final Map<String, SqlOperator> INTERNALS = Map.of(
            "non-bmp-literal", NonBmpLiterals.Wrapper.INSTANCE,
            "array-aggregate", org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.functions.SqlArrayAggFunction.INSTANCE);

    record Identity(String registry, String version, String key) { }

    private final S1OperatorRegistry calcite = new S1OperatorRegistry();
    private final S1SpatialOperatorRegistry spatial = new S1SpatialOperatorRegistry();
    private final IdentityHashMap<SqlOperator, String> customKeys = new IdentityHashMap<>();
    private final Map<String, SqlOperator> customOperators;

    S1BuiltinOperatorRegistry() {
        this(new CustomFunctions().getDescriptions());
    }

    S1BuiltinOperatorRegistry(Collection<? extends FunctionDocumentation.FunctionDescription> trusted) {
        Map<String, SqlOperator> entries = new LinkedHashMap<>();
        for (var description: trusted) {
            if (!(description instanceof SqlOperator operator))
                throw new IllegalStateException("Custom function descriptor does not expose its operator object");
            // Both class and function name are necessary: several JSON builtins
            // deliberately use one implementation class for different functions.
            String key = operator.getClass().getName() + "#" + operator.getName();
            SqlOperator previous = entries.putIfAbsent(key, operator);
            if (previous != null && previous != operator)
                throw new IllegalStateException("Ambiguous custom operator identity needs an explicit overload key: " + key);
            this.customKeys.putIfAbsent(operator, key);
        }
        this.customOperators = Map.copyOf(entries);
    }

    Identity identify(SqlOperator operator) throws IOException {
        if (operator == null) throw new IOException("Missing S1 builtin operator");
        if (this.calcite.contains(operator))
            return new Identity(S1OperatorRegistry.NAME, S1OperatorRegistry.VERSION, this.calcite.key(operator));
        for (var entry: INTERNALS.entrySet())
            if (entry.getValue() == operator) return new Identity(INTERNAL_NAME, INTERNAL_VERSION, entry.getKey());
        String spatialKey = this.spatial.key(operator);
        if (spatialKey != null) return new Identity(S1SpatialOperatorRegistry.NAME, S1SpatialOperatorRegistry.VERSION, spatialKey);
        String key = this.customKeys.get(operator);
        if (key == null) throw new IOException("Operator is outside the S1 builtin registries: " + operator.getName());
        return new Identity(CUSTOM_NAME, CUSTOM_VERSION, key);
    }

    boolean contains(SqlOperator operator) {
        return this.calcite.contains(operator) || this.customKeys.containsKey(operator)
                || INTERNALS.values().stream().anyMatch(value -> value == operator) || this.spatial.key(operator) != null;
    }

    SqlOperator resolve(Identity identity) throws IOException {
        if (identity == null) throw new IOException("Missing S1 builtin identity");
        if (S1OperatorRegistry.NAME.equals(identity.registry()))
            return this.calcite.resolve(identity.registry(), identity.version(), identity.key());
        if (INTERNAL_NAME.equals(identity.registry())) {
            if (!INTERNAL_VERSION.equals(identity.version()) || identity.key() == null || !INTERNALS.containsKey(identity.key()))
                throw new IOException("Unsupported internal operator key or version");
            return INTERNALS.get(identity.key());
        }
        if (S1SpatialOperatorRegistry.NAME.equals(identity.registry())) {
            if (!S1SpatialOperatorRegistry.VERSION.equals(identity.version()) || identity.key() == null)
                throw new IOException("Unsupported spatial registry version or key");
            SqlOperator operator = this.spatial.resolve(identity.key());
            if (operator == null) throw new IOException("Unknown spatial registry key");
            return operator;
        }
        if (!CUSTOM_NAME.equals(identity.registry()) || !CUSTOM_VERSION.equals(identity.version()))
            throw new IOException("Unsupported S1 builtin registry or version");
        if (identity.key() == null) throw new IOException("Missing custom S1 operator key");
        SqlOperator operator = this.customOperators.get(identity.key());
        if (operator == null) throw new IOException("Unknown custom S1 operator key: " + identity.key());
        return operator;
    }

    Map<Identity, SqlOperator> entries() {
        Map<Identity, SqlOperator> result = new LinkedHashMap<>();
        this.calcite.entries().forEach((key, operator) -> result.put(
                new Identity(S1OperatorRegistry.NAME, S1OperatorRegistry.VERSION, key), operator));
        this.customOperators.forEach((key, operator) -> result.put(
                new Identity(CUSTOM_NAME, CUSTOM_VERSION, key), operator));
        INTERNALS.forEach((key, operator) -> result.put(new Identity(INTERNAL_NAME, INTERNAL_VERSION, key), operator));
        this.spatial.entries().forEach((key, operator) -> result.put(new Identity(S1SpatialOperatorRegistry.NAME,
                S1SpatialOperatorRegistry.VERSION, key), operator));
        return Map.copyOf(result);
    }
}
