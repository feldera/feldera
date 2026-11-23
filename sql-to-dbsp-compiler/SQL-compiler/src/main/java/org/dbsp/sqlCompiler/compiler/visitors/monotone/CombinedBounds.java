package org.dbsp.sqlCompiler.compiler.visitors.monotone;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.backend.RustCircuitPass;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTupleBase;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;

/** Retain live SQL types while Rust combines their monotonicity projections. */
public final class CombinedBounds {
    private final List<IMaybeMonotoneType> types = new ArrayList<>();

    private int slot(IMaybeMonotoneType type) {
        int slot = this.types.size();
        this.types.add(type);
        return slot;
    }

    /** Record the types in preorder. Tuple arity delimits children even for deep row types. */
    private ArrayNode input(IMaybeMonotoneType root) {
        var nodes = JsonNodeFactory.instance.arrayNode();
        var pending = new ArrayDeque<IMaybeMonotoneType>();
        pending.push(root);
        while (!pending.isEmpty()) {
            var type = pending.pop();
            var node = nodes.addObject().put("slot", this.slot(type));
            if (type instanceof PartiallyMonotoneTuple tuple) {
                node.put("kind", "tuple").put("arity", tuple.size());
                for (int i = tuple.size() - 1; i >= 0; i--)
                    pending.push(tuple.getFieldType(i));
            } else if (type instanceof MonotoneRefType reference) {
                node.put("kind", "reference");
                pending.push(reference.base);
            } else if (type instanceof ScalarMonotoneType) {
                node.put("kind", "scalar").put("monotone", type.mayBeMonotone());
            } else {
                node.put("kind", "opaque");
            }
        }
        return nodes;
    }

    private IMaybeMonotoneType result(JsonNode steps) {
        var stack = new ArrayList<IMaybeMonotoneType>();
        for (var step : steps) {
            if (step.isIntegralNumber()) {
                stack.add(this.types.get(step.intValue()));
            } else if (step.has("unbounded_fields")) {
                var value = this.types.get(step.required("unbounded_fields").intValue());
                stack.add(PartiallyMonotoneTuple.noMonotoneFields(value.getType().to(DBSPTypeTupleBase.class)));
            } else {
                var template = this.types.get(step.required("template").intValue());
                int arity = step.required("arity").intValue();
                var fields = stack.subList(stack.size() - arity, stack.size());
                IMaybeMonotoneType type;
                if (template instanceof MonotoneRefType) {
                    type = new MonotoneRefType(fields.get(0));
                } else {
                    var tuple = template.to(PartiallyMonotoneTuple.class);
                    type = new PartiallyMonotoneTuple(new ArrayList<>(fields), tuple.raw, tuple.mayBeNull);
                }
                fields.clear();
                stack.add(type);
            }
        }
        if (stack.size() != 1)
            throw new IllegalStateException("Expected one combined waterline");
        return stack.get(0);
    }

    public static IMaybeMonotoneType union(DBSPCompiler compiler, IMaybeMonotoneType left, IMaybeMonotoneType right) {
        if (!compiler.options.ioOptions.rustOptimizerPasses)
            return left.union(right);
        return combine(compiler, List.of(left, right), true);
    }

    public static IMaybeMonotoneType intersection(DBSPCompiler compiler, List<IMaybeMonotoneType> inputs) {
        if (!compiler.options.ioOptions.rustOptimizerPasses)
            return IMaybeMonotoneType.intersection(inputs);
        return combine(compiler, inputs, false);
    }

    private static IMaybeMonotoneType combine(DBSPCompiler compiler, List<IMaybeMonotoneType> inputs, boolean union) {
        if (inputs.size() == 1)
            return inputs.get(0);
        var bounds = new CombinedBounds();
        var request = JsonNodeFactory.instance.arrayNode();
        inputs.forEach(type -> request.add(bounds.input(type)));
        var reply = RustCircuitPass.combineBounds(compiler, request, union);
        if (!reply.isNull())
            return bounds.result(reply);
        // Preserve the original diagnostic when a closure or incompatible tuple is combined.
        return union ? inputs.get(0).union(inputs.get(1)) : IMaybeMonotoneType.intersection(inputs);
    }

    private JsonNode indexedInputs(PartiallyMonotoneTuple left, PartiallyMonotoneTuple right) {
        var inputs = JsonNodeFactory.instance.arrayNode();
        for (var pair : List.of(left, right)) {
            var input = inputs.addObject();
            input.set("key", this.input(pair.getFieldType(0)));
            input.put("value", this.slot(pair.getFieldType(1)));
        }
        return inputs;
    }

    public static PartiallyMonotoneTuple commonKey(DBSPCompiler compiler,
            PartiallyMonotoneTuple left, PartiallyMonotoneTuple right) {
        if (compiler.options.ioOptions.rustOptimizerPasses) {
            var bounds = new CombinedBounds();
            var reply = RustCircuitPass.indexedBounds(compiler, bounds.indexedInputs(left, right), true);
            if (!reply.isNull())
                return new PartiallyMonotoneTuple(List.of(bounds.result(reply.required(0)),
                        bounds.result(reply.required(1))), true, false);
        }
        var key = left.getFieldType(0).intersection(right.getFieldType(0));
        var value = left.getFieldType(1).getType().to(DBSPTypeTupleBase.class);
        return new PartiallyMonotoneTuple(List.of(key, PartiallyMonotoneTuple.noMonotoneFields(value)), true, false);
    }

    public static IMaybeMonotoneType[] joinArguments(DBSPCompiler compiler,
            PartiallyMonotoneTuple left, PartiallyMonotoneTuple right) {
        if (compiler.options.ioOptions.rustOptimizerPasses) {
            var bounds = new CombinedBounds();
            var reply = RustCircuitPass.indexedBounds(compiler, bounds.indexedInputs(left, right), false);
            if (!reply.isNull()) {
                return new IMaybeMonotoneType[]{
                        bounds.result(reply.required(0)), bounds.result(reply.required(1)), bounds.result(reply.required(2))};
            }
        }
        return new IMaybeMonotoneType[]{left.getFieldType(0).union(right.getFieldType(0)),
                left.getFieldType(1), right.getFieldType(1)};
    }
}
