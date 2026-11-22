package org.dbsp.sqlCompiler.compiler.visitors.monotone;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.backend.RustCircuitPass;

import java.util.ArrayList;
import java.util.List;

/** Retain live SQL types while Rust combines their monotonicity projections. */
public final class CombinedBounds {
    private final List<IMaybeMonotoneType> types = new ArrayList<>();

    private ObjectNode input(IMaybeMonotoneType type) {
        var result = JsonNodeFactory.instance.objectNode().put("slot", this.types.size());
        this.types.add(type);
        if (type instanceof PartiallyMonotoneTuple tuple) {
            var fields = result.put("kind", "tuple").putArray("fields");
            tuple.fields.forEach(field -> fields.add(this.input(field)));
        } else if (type instanceof MonotoneRefType reference) {
            result.put("kind", "reference").set("base", this.input(reference.base));
        } else if (type instanceof ScalarMonotoneType) {
            result.put("kind", "scalar").put("monotone", type.mayBeMonotone());
        } else {
            result.put("kind", "opaque");
        }
        return result;
    }

    private IMaybeMonotoneType result(JsonNode selection) {
        if (selection.isIntegralNumber())
            return this.types.get(selection.intValue());
        var template = this.types.get(selection.required("template").intValue());
        var fields = selection.required("fields");
        if (template instanceof MonotoneRefType)
            return new MonotoneRefType(this.result(fields.required(0)));
        var tuple = template.to(PartiallyMonotoneTuple.class);
        var members = new ArrayList<IMaybeMonotoneType>(fields.size());
        fields.forEach(field -> members.add(this.result(field)));
        return new PartiallyMonotoneTuple(members, tuple.raw, tuple.mayBeNull);
    }

    public static IMaybeMonotoneType union(DBSPCompiler compiler, IMaybeMonotoneType left, IMaybeMonotoneType right) {
        if (!compiler.options.ioOptions.rustOptimizerPasses)
            return left.union(right);
        return combine(List.of(left, right), true);
    }

    public static IMaybeMonotoneType intersection(DBSPCompiler compiler, List<IMaybeMonotoneType> inputs) {
        if (!compiler.options.ioOptions.rustOptimizerPasses)
            return IMaybeMonotoneType.intersection(inputs);
        return combine(inputs, false);
    }

    private static IMaybeMonotoneType combine(List<IMaybeMonotoneType> inputs, boolean union) {
        if (inputs.size() == 1)
            return inputs.get(0);
        var bounds = new CombinedBounds();
        var request = JsonNodeFactory.instance.arrayNode();
        inputs.forEach(type -> request.add(bounds.input(type)));
        var reply = RustCircuitPass.combineBounds(request, union);
        if (!reply.isNull())
            return bounds.result(reply);
        // Preserve the original diagnostic when a closure or incompatible tuple is combined.
        return union ? inputs.get(0).union(inputs.get(1)) : IMaybeMonotoneType.intersection(inputs);
    }
}
