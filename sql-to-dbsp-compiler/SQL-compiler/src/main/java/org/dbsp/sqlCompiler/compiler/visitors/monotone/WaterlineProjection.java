package org.dbsp.sqlCompiler.compiler.visitors.monotone;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.ArrayList;

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
    public static IMaybeMonotoneType resolveType(JsonNode projection, IMaybeMonotoneType... inputs) {
        return switch (projection.required("kind").textValue()) {
            case "input" -> inputs[projection.required("index").intValue()];
            case "field" -> resolveType(projection.required("row"), inputs).to(PartiallyMonotoneTuple.class)
                    .getFieldExpressionType(projection.required("index").intValue());
            case "with_nullability" -> resolveType(projection.required("value"), inputs)
                    .withMaybeNull(projection.required("nullable").booleanValue());
            case "tuple" -> {
                var members = new ArrayList<IMaybeMonotoneType>();
                for (var field : projection.required("fields"))
                    members.add(resolveType(field, inputs));
                yield new PartiallyMonotoneTuple(members, projection.required("raw").booleanValue(),
                        projection.required("nullable").booleanValue());
            }
            default -> throw new IllegalStateException("Unknown row projection " + projection);
        };
    }
}
