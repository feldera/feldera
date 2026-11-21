package org.dbsp.sqlCompiler.compiler.sql.tools;

import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.impl.AggregateFunctionImpl;
import org.apache.calcite.schema.impl.ReflectiveFunctionBase;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.validate.SqlUserDefinedAggFunction;
import org.apache.calcite.sql.validate.SqlUserDefinedFunction;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.functions.SpatialOperatorTable;

import java.lang.reflect.Method;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/** Identity registry for the compiler's fixed spatial table. Keys include Java
 * method signatures to distinguish SQL overloads. The input only indexes this
 * map; it cannot request method lookup, invocation, or class loading. */
final class S1SpatialOperatorRegistry {
    static final String NAME = "feldera-s1-spatial";
    static final String VERSION = "1";
    private final IdentityHashMap<SqlOperator, String> keys = new IdentityHashMap<>();
    private final Map<String, SqlOperator> entries;

    S1SpatialOperatorRegistry() {
        var values = new LinkedHashMap<String, SqlOperator>();
        for (SqlOperator operator: SpatialOperatorTable.instance().getOperatorList()) {
            Function function;
            if (operator instanceof SqlUserDefinedFunction scalar) function = scalar.getFunction();
            else if (operator instanceof SqlUserDefinedAggFunction aggregate) function = aggregate.function;
            else throw new IllegalStateException("Unclassified trusted spatial operator: " + operator.getClass());
            String implementation;
            if (function instanceof ReflectiveFunctionBase reflective) implementation = reflective.method.toGenericString();
            else if (function instanceof AggregateFunctionImpl aggregate) {
                implementation = method(aggregate.initMethod) + "/" + method(aggregate.addMethod)
                        + "/" + method(aggregate.mergeMethod) + "/" + method(aggregate.resultMethod);
            } else throw new IllegalStateException("Unclassified trusted spatial function: " + function.getClass());
            String key = operator.getClass().getName() + "#" + operator.getName() + "#" + implementation;
            SqlOperator previous = values.putIfAbsent(key, operator);
            if (previous != null && previous != operator) throw new IllegalStateException("Ambiguous trusted spatial key: " + key);
            this.keys.putIfAbsent(operator, key);
        }
        this.entries = Map.copyOf(values);
    }

    private static String method(Method method) { return method == null ? "absent" : method.toGenericString(); }
    String key(SqlOperator operator) { return this.keys.get(operator); }
    SqlOperator resolve(String key) { return this.entries.get(key); }
    Map<String, SqlOperator> entries() { return this.entries; }
}
