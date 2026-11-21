package org.dbsp.sqlCompiler.compiler.sql.tools;

import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.fun.SqlInternalOperators;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

import java.io.IOException;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Comparator;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Test-only identities for static Calcite operators. Reflection enumerates three
 * fixed trusted classes at initialization; wire keys only consult the frozen map.
 * Custom, catalog, spatial and dynamically created operators need other registries.
 * Producer provenance must additionally pin the actual Calcite dependency. */
final class S1OperatorRegistry {
    static final String NAME = "calcite-s1-fixture";
    static final String VERSION = "1";
    private final IdentityHashMap<SqlOperator, String> keys = new IdentityHashMap<>();
    private final Map<String, SqlOperator> operators;

    S1OperatorRegistry() {
        Map<String, SqlOperator> entries = new LinkedHashMap<>();
        for (Class<?> table: List.of(SqlStdOperatorTable.class, SqlLibraryOperators.class, SqlInternalOperators.class)) {
            var fields = Arrays.stream(table.getFields())
                    .filter(field -> Modifier.isStatic(field.getModifiers()) && Modifier.isFinal(field.getModifiers())
                            && SqlOperator.class.isAssignableFrom(field.getType()))
                    .sorted(Comparator.comparing(java.lang.reflect.Field::getName))
                    .toList();
            for (var field: fields) {
                String key = table.getSimpleName() + "." + field.getName();
                try {
                    SqlOperator operator = (SqlOperator) field.get(null);
                    if (operator == null) throw new IllegalStateException("Null static operator " + key);
                    entries.put(key, operator);
                    // Prefer the standard table and then the first field by
                    // name. Equal display names never merge operator objects.
                    this.keys.putIfAbsent(operator, key);
                } catch (IllegalAccessException exception) {
                    throw new IllegalStateException("Cannot enumerate trusted operator " + key, exception);
                }
            }
        }
        this.operators = Map.copyOf(entries);
    }

    boolean contains(SqlOperator operator) { return this.keys.containsKey(operator); }

    String key(SqlOperator operator) throws IOException {
        if (operator == null) throw new IOException("Missing S1 operator object");
        String key = this.keys.get(operator);
        if (key == null) throw new IOException("Operator is outside the static S1 registry: " + operator.getName());
        return key;
    }

    SqlOperator resolve(String registry, String version, String key) throws IOException {
        if (!NAME.equals(registry) || !VERSION.equals(version))
            throw new IOException("Unsupported S1 operator registry or version");
        if (key == null) throw new IOException("Missing static S1 operator key");
        SqlOperator operator = this.operators.get(key);
        if (operator == null) throw new IOException("Unknown static S1 operator key: " + key);
        return operator;
    }

    Map<String, SqlOperator> entries() { return this.operators; }
}
