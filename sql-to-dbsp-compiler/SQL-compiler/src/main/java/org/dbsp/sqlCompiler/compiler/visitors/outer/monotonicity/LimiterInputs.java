package org.dbsp.sqlCompiler.compiler.visitors.outer.monotonicity;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.compiler.visitors.outer.expansion.ReplacementDeltaExpansion;
import org.dbsp.sqlCompiler.compiler.visitors.monotone.IMaybeMonotoneType;
import org.dbsp.sqlCompiler.compiler.visitors.monotone.PartiallyMonotoneTuple;
import org.dbsp.sqlCompiler.compiler.visitors.monotone.ScalarMonotoneType;
import org.dbsp.util.Utilities;

/** Entry facts from the expanded circuit, keyed by their corresponding original operators. */
public final class LimiterInputs {
    private LimiterInputs() {}

    private static JsonNode projection(IMaybeMonotoneType type) {
        ObjectNode node = Utilities.deterministicObjectMapper().createObjectNode();
        if (type instanceof PartiallyMonotoneTuple tuple) {
            node.put("kind", "tuple");
            node.put("raw", tuple.raw);
            node.put("nullable", tuple.mayBeNull);
            var fields = node.putArray("fields");
            for (int i = 0; i < tuple.size(); i++)
                fields.add(projection(tuple.getFieldType(i)));
        } else if (type instanceof ScalarMonotoneType) {
            node.put("kind", "scalar");
            node.put("monotone", type.mayBeMonotone());
        } else {
            node.put("kind", type.getClass().getSimpleName());
        }
        return node;
    }

    public static ObjectNode capture(InsertLimiters pass, DBSPCircuit original) {
        ObjectNode facts = Utilities.deterministicObjectMapper().createObjectNode();
        for (var operator : original.allOperators) {
            var entry = facts.putObject(Long.toString(operator.getId()));
            entry.set("position",operator.getNode().getPositionRange().asJson());
            entry.put("reachable_from_error", pass.reachableFromError.contains(operator));
            var kinds = entry.putArray("input_kinds");
            for (var input : operator.inputs)
                kinds.add(input.kind().name());
            if (operator instanceof DBSPSimpleOperator simple
                    && pass.expandedInto.get(simple) instanceof ReplacementDeltaExpansion expansion) {
                var replacement = expansion.replacement;
                var value = pass.expansionMonotoneValues.get(replacement);
                if (value != null)
                    entry.set("projection", projection(Monotonicity.getBodyType(value)));
                entry.put("expanded_id", replacement.getId());
            }
        }
        return facts;
    }
}
