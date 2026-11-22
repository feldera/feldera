package org.dbsp.sqlCompiler.compiler.visitors.outer.monotonicity;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.circuit.operator.DBSPAtomicSumOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPConstantOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPFilterOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPFlatMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPHopOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIncrementalConstantOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSinkOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSourceMultisetOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSubtractOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSumOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPViewOperator;
import org.dbsp.sqlCompiler.compiler.backend.ToJsonOuterVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.monotone.IMaybeMonotoneType;
import org.dbsp.sqlCompiler.compiler.visitors.monotone.PartiallyMonotoneTuple;
import org.dbsp.sqlCompiler.compiler.visitors.monotone.ScalarMonotoneType;
import org.dbsp.sqlCompiler.compiler.visitors.outer.expansion.ReplacementDeltaExpansion;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.util.Utilities;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/** Entry facts from the expanded circuit, keyed by their corresponding original operators. */
public final class LimiterInputs {
    private LimiterInputs() {}

    private static final Set<Class<?>> NATIVE_OPERATORS = Set.of(
            DBSPSourceMultisetOperator.class, DBSPViewOperator.class, DBSPSinkOperator.class,
            DBSPMapOperator.class, DBSPMapIndexOperator.class, DBSPFilterOperator.class,
            DBSPHopOperator.class, DBSPFlatMapOperator.class, DBSPSumOperator.class,
            DBSPAtomicSumOperator.class, DBSPSubtractOperator.class, DBSPConstantOperator.class,
            DBSPIncrementalConstantOperator.class);

    /** Whole-pass lowering uses the expansion facts for these operators. */
    public static boolean canLowerCircuit(DBSPCircuit circuit) {
        return circuit.allOperators.stream().allMatch(operator -> NATIVE_OPERATORS.contains(operator.getClass()));
    }

    public static JsonNode projection(IMaybeMonotoneType type) {
        ObjectNode node = JsonNodeFactory.instance.objectNode();
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
            node.put("kind", "other");
            node.put("monotone", type.mayBeMonotone());
        }
        return node;
    }

    /** The reduced closures belong to the matching expansion of this circuit. */
    public static Map<Long, DBSPClosureExpression> transferFunctions(InsertLimiters pass, DBSPCircuit original) {
        var functions = new LinkedHashMap<Long, DBSPClosureExpression>();
        for (var operator : original.allOperators) {
            if (!(operator instanceof DBSPMapOperator || operator instanceof DBSPMapIndexOperator
                    || operator instanceof DBSPFilterOperator || operator instanceof DBSPFlatMapOperator
                    || operator instanceof DBSPHopOperator))
                continue;
            var expansion = pass.getReplacement(operator.to(DBSPSimpleOperator.class));
            if (expansion == null)
                continue;
            var value = pass.expansionMonotoneValues.get(expansion.replacement);
            if (value != null)
                functions.put(operator.id, value.getReducedExpression().to(DBSPClosureExpression.class));
        }
        return functions;
    }

    public static String encodeTransfers(ToJsonOuterVisitor circuit, Map<Long, DBSPClosureExpression> functions) {
        var writer = circuit.additionalInnerNodes();
        writer.stream.beginObject();
        functions.forEach((id, function) -> {
            writer.stream.label(Long.toString(id));
            function.accept(writer);
        });
        writer.stream.endObject();
        return writer.stream.toString();
    }

    public static ObjectNode capture(InsertLimiters pass, DBSPCircuit original) {
        ObjectNode facts = Utilities.deterministicObjectMapper().createObjectNode();
        for (var operator : original.allOperators) {
            var entry = facts.putObject(Long.toString(operator.getId()));
            entry.set("position",operator.getNode().getPositionRange().asJson());
            entry.put("reachable_from_error", pass.reachableFromError.contains(operator));
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
