package org.dbsp.sqlCompiler.compiler.visitors.monotone;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.dbsp.sqlCompiler.ir.type.DBSPType;

import java.util.ArrayList;
import java.util.List;
import java.util.function.LongFunction;

/** Exchange field monotonicity with Rust while retaining the original SQL types. */
public final class WaterlineProjection {
    private WaterlineProjection() {}

    public static JsonNode encode(IMaybeMonotoneType type) {
        ObjectNode node = JsonNodeFactory.instance.objectNode();
        if (type instanceof PartiallyMonotoneTuple tuple) {
            node.put("kind", "tuple");
            node.put("raw", tuple.raw);
            node.put("nullable", tuple.mayBeNull);
            var fields = node.putArray("fields");
            for (int i = 0; i < tuple.size(); i++)
                fields.add(encode(tuple.getFieldType(i)));
        } else if (type instanceof MonotoneRefType reference) {
            node.put("kind", "reference").set("base", encode(reference.base));
        } else if (type instanceof ScalarMonotoneType) {
            node.put("kind", "scalar");
            node.put("monotone", type.mayBeMonotone());
        } else {
            node.put("kind", "other");
            if (type instanceof MonotoneClosureType)
                node.put("closure", true);
            node.put("monotone", type.mayBeMonotone());
        }
        return node;
    }

    /** ZSet and join parameters are references. An indexed parameter is a pair of references. */
    public static List<JsonNode> inputShapes(IMaybeMonotoneType[] parameters, boolean indexed) {
        var references = indexed ? parameters[0].to(PartiallyMonotoneTuple.class).fields : List.of(parameters);
        return references.stream().map(ref -> encode(ref.to(MonotoneRefType.class).base)).toList();
    }

    /** Resolve the Rust projection against the same analyzed types that fed the closure. */
    public static IMaybeMonotoneType resolveType(JsonNode projection, LongFunction<DBSPType> declaredType,
                                                 IMaybeMonotoneType... parameters) {
        return resolveType(projection, declaredType, parameters, new ArrayList<>(), List.of());
    }

    public static IMaybeMonotoneType resolveFactType(JsonNode projection, LongFunction<DBSPType> declaredType,
                                                    IMaybeMonotoneType[] parameters, List<MonotoneExpression> facts) {
        return resolveType(projection, declaredType, parameters, new ArrayList<>(), facts);
    }

    private static IMaybeMonotoneType resolveType(JsonNode projection, LongFunction<DBSPType> declaredType,
                                                   IMaybeMonotoneType[] parameters, ArrayList<IMaybeMonotoneType> locals,
                                                   List<MonotoneExpression> facts) {
        return switch (projection.required("kind").textValue()) {
            case "unbounded" -> NonMonotoneType.nonMonotone(declaredType.apply(projection.required("type_id").longValue()));
            case "bounded_scalar" -> new MonotoneType(declaredType.apply(projection.required("type_id").longValue()));
            case "prior" -> facts.get(projection.required("index").intValue()).getMonotoneType();
            case "local" -> locals.get(projection.required("index").intValue());
            case "scope" -> {
                int outer = locals.size();
                for (var initializer : projection.required("initializers"))
                    locals.add(initializer.isNull() ? null : resolveType(initializer, declaredType, parameters, locals, facts));
                var result = resolveType(projection.required("result"), declaredType, parameters, locals, facts);
                locals.subList(outer, locals.size()).clear();
                yield result;
            }
            case "parameter" -> parameters[projection.required("index").intValue()];
            case "borrow" -> new MonotoneRefType(resolveType(projection.required("value"), declaredType, parameters, locals, facts));
            case "deref" -> resolveType(projection.required("value"), declaredType, parameters, locals, facts).to(MonotoneRefType.class).base;
            case "field" -> resolveType(projection.required("row"), declaredType, parameters, locals, facts).to(PartiallyMonotoneTuple.class)
                    .getFieldExpressionType(projection.required("index").intValue());
            case "with_nullability" -> resolveType(projection.required("value"), declaredType, parameters, locals, facts)
                    .withMaybeNull(projection.required("nullable").booleanValue());
            case "tuple" -> {
                var members = new ArrayList<IMaybeMonotoneType>();
                for (var field : projection.required("fields"))
                    members.add(resolveType(field, declaredType, parameters, locals, facts));
                yield new PartiallyMonotoneTuple(members, projection.required("raw").booleanValue(),
                        projection.required("nullable").booleanValue());
            }
            default -> throw new IllegalStateException("Unknown row projection " + projection);
        };
    }
}
