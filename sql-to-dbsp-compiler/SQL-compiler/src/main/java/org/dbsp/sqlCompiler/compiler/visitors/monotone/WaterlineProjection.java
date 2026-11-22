package org.dbsp.sqlCompiler.compiler.visitors.monotone;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.dbsp.sqlCompiler.ir.type.DBSPType;

import java.util.ArrayList;
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
        } else if (type instanceof ScalarMonotoneType) {
            node.put("kind", "scalar");
            node.put("monotone", type.mayBeMonotone());
        } else {
            node.put("kind", "other");
            node.put("monotone", type.mayBeMonotone());
        }
        return node;
    }

    /** Resolve the Rust projection against the same analyzed types that fed the closure. */
    public static IMaybeMonotoneType resolveType(JsonNode projection, LongFunction<DBSPType> declaredType,
                                                 IMaybeMonotoneType... inputs) {
        return resolveType(projection, declaredType, inputs, new ArrayList<>());
    }

    private static IMaybeMonotoneType resolveType(JsonNode projection, LongFunction<DBSPType> declaredType,
                                                   IMaybeMonotoneType[] inputs, ArrayList<IMaybeMonotoneType> locals) {
        return switch (projection.required("kind").textValue()) {
            case "unbounded" -> NonMonotoneType.nonMonotone(declaredType.apply(projection.required("type_id").longValue()));
            case "bounded_scalar" -> new MonotoneType(declaredType.apply(projection.required("type_id").longValue()));
            case "local" -> locals.get(projection.required("index").intValue());
            case "scope" -> {
                int outer = locals.size();
                for (var initializer : projection.required("initializers"))
                    locals.add(resolveType(initializer, declaredType, inputs, locals));
                var result = resolveType(projection.required("result"), declaredType, inputs, locals);
                locals.subList(outer, locals.size()).clear();
                yield result;
            }
            case "input" -> inputs[projection.required("index").intValue()];
            case "field" -> resolveType(projection.required("row"), declaredType, inputs, locals).to(PartiallyMonotoneTuple.class)
                    .getFieldExpressionType(projection.required("index").intValue());
            case "with_nullability" -> resolveType(projection.required("value"), declaredType, inputs, locals)
                    .withMaybeNull(projection.required("nullable").booleanValue());
            case "tuple" -> {
                var members = new ArrayList<IMaybeMonotoneType>();
                for (var field : projection.required("fields"))
                    members.add(resolveType(field, declaredType, inputs, locals));
                yield new PartiallyMonotoneTuple(members, projection.required("raw").booleanValue(),
                        projection.required("nullable").booleanValue());
            }
            default -> throw new IllegalStateException("Unknown row projection " + projection);
        };
    }
}
