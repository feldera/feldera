package org.dbsp.sqlCompiler.compiler.sql.tools;

import com.fasterxml.jackson.databind.node.ArrayNode;
import org.dbsp.util.Utilities;

import java.lang.reflect.Modifier;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.TreeMap;

/** Declaration inventory only. Reading field definitions needs no reflective
 * access to values and cannot evaluate a pass, predicate, or initializer.
 * A field's lifetime and its reachable state still require source review. */
final class S4Fields {
    private S4Fields() { }

    static ArrayNode declarations(Class<?> implementation) {
        var classes = new TreeMap<String, Class<?>>();
        var visited = new HashSet<Class<?>>();
        var pending = new ArrayDeque<Class<?>>(); pending.add(implementation);
        while (!pending.isEmpty()) {
            var type = pending.removeFirst();
            if (!visited.add(type) || type == Object.class) continue;
            classes.put(type.getName(), type);
            if (type.getSuperclass() != null) pending.add(type.getSuperclass());
            pending.addAll(Arrays.asList(type.getInterfaces()));
        }
        var result = Utilities.deterministicObjectMapper().createArrayNode();
        for (var type: classes.values()) {
            var fields = type.getDeclaredFields(); Arrays.sort(fields, Comparator.comparing(java.lang.reflect.Field::getName));
            for (var field: fields) {
                var entry = result.addObject().put("declaring_class", type.getName()).put("name", field.getName())
                        .put("type", field.getGenericType().getTypeName())
                        .put("modifiers", Modifier.toString(field.getModifiers()))
                        .put("static", Modifier.isStatic(field.getModifiers()))
                        .put("synthetic", field.isSynthetic());
                var owner = type;
                while (owner.getEnclosingClass() != null) owner = owner.getEnclosingClass();
                entry.put("source", owner.getName().replace('.', '/') + ".java");
            }
        }
        return result;
    }
}
