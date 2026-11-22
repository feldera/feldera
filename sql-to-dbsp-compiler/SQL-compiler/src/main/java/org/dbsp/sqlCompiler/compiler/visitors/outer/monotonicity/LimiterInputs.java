package org.dbsp.sqlCompiler.compiler.visitors.outer.monotonicity;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.circuit.annotation.AlwaysMonotone;
import org.dbsp.sqlCompiler.circuit.operator.DBSPAggregateLinearPostprocessOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPAggregateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPAntiJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPApplyOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPAtomicSumOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPChainAggregateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPConstantOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDeindexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDifferentiateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDistinctOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPFilterOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPFlatMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPHopOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIncrementalConstantOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIndexedTopKOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIntegrateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinBaseOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPLagOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPNegateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPNoopOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPNowOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPPartitionedRollingAggregateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPPositiveOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPRankOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPRowNumberOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSinkOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSourceMultisetOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStarJoinFilterMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStarJoinIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStarJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamAggregateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamAntiJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamDistinctOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamJoinIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSubtractOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSumOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPViewOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPWaterlineOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPWindowOperator;
import org.dbsp.sqlCompiler.compiler.backend.ToJsonOuterVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.monotone.IMaybeMonotoneType;
import org.dbsp.sqlCompiler.compiler.visitors.monotone.PartiallyMonotoneTuple;
import org.dbsp.sqlCompiler.compiler.visitors.monotone.ScalarMonotoneType;
import org.dbsp.sqlCompiler.compiler.visitors.outer.expansion.AggregateDeltaExpansion;
import org.dbsp.sqlCompiler.compiler.visitors.outer.expansion.DistinctDeltaExpansion;
import org.dbsp.sqlCompiler.compiler.visitors.outer.expansion.ReplacementDeltaExpansion;
import org.dbsp.sqlCompiler.compiler.visitors.outer.expansion.StarJoinDeltaExpansion;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.util.Utilities;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/** Entry facts from the expanded circuit, keyed by their corresponding original operators. */
public final class LimiterInputs {
    private LimiterInputs() {}

    private static final Set<Class<?>> TRANSFER_OPERATORS = Set.of(
            DBSPMapOperator.class, DBSPMapIndexOperator.class, DBSPFilterOperator.class,
            DBSPHopOperator.class, DBSPFlatMapOperator.class, DBSPNoopOperator.class,
            DBSPNegateOperator.class, DBSPDeindexOperator.class, DBSPStreamAntiJoinOperator.class,
            DBSPAggregateLinearPostprocessOperator.class, DBSPChainAggregateOperator.class, DBSPLagOperator.class);

    private static final Set<Class<?>> OTHER_NATIVE_OPERATORS = Set.of(
            DBSPSourceMultisetOperator.class, DBSPViewOperator.class, DBSPSinkOperator.class, DBSPSumOperator.class,
            DBSPAtomicSumOperator.class, DBSPSubtractOperator.class, DBSPConstantOperator.class,
            DBSPIncrementalConstantOperator.class, DBSPDistinctOperator.class, DBSPPositiveOperator.class,
            DBSPApplyOperator.class, DBSPWaterlineOperator.class, DBSPWindowOperator.class,
            DBSPDifferentiateOperator.class, DBSPIntegrateOperator.class, DBSPIndexedTopKOperator.class,
            DBSPRankOperator.class, DBSPRowNumberOperator.class, DBSPAggregateOperator.class,
            DBSPAntiJoinOperator.class, DBSPNowOperator.class, DBSPPartitionedRollingAggregateOperator.class,
            DBSPStreamAggregateOperator.class, DBSPStreamDistinctOperator.class,
            DBSPStreamJoinOperator.class, DBSPStreamJoinIndexOperator.class, DBSPStarJoinOperator.class,
            DBSPStarJoinIndexOperator.class, DBSPStarJoinFilterMapOperator.class);

    /** Whole-pass lowering uses the expansion facts for these operators. */
    public static boolean canLowerCircuit(DBSPCircuit circuit) {
        return circuit.allOperators.stream().allMatch(operator -> TRANSFER_OPERATORS.contains(operator.getClass())
                || OTHER_NATIVE_OPERATORS.contains(operator.getClass()) || BinaryJoinInputs.supports(operator.getClass()));
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

    static void recordTransfer(Map<Long, IDBSPInnerNode> functions, long id,
                                       InsertLimiters pass, DBSPSimpleOperator source) {
        var value = pass.expansionMonotoneValues.get(source);
        if (value != null)
            functions.put(id, value.getReducedExpression().to(DBSPClosureExpression.class));
    }

    /** A stateful aggregate has an intermediate reduction and an output upsert.
     * A rolling aggregate needs the analyzed input type to bind its ordering key.
     * Preserve identities shared by those roots and the circuit. */
    public static Map<Long, IDBSPInnerNode> transferRoots(InsertLimiters pass, DBSPCircuit original) {
        var functions = new LinkedHashMap<Long, IDBSPInnerNode>();
        for (var operator : original.allOperators) {
            if (operator instanceof DBSPSimpleOperator simple
                    && pass.expandedInto.get(simple) instanceof StarJoinDeltaExpansion star) {
                StarJoinInputs.recordTransfers(pass, star, functions);
                continue;
            }
            if (BinaryJoinInputs.supports(operator.getClass())) {
                var join = BinaryJoinInputs.from(pass.expandedInto.get(operator.to(DBSPSimpleOperator.class)));
                if (join != null)
                    join.recordTransfers(pass, functions);
                continue;
            }
            if (operator instanceof DBSPAggregateOperator aggregate
                    && pass.expandedInto.get(aggregate) instanceof AggregateDeltaExpansion expansion) {
                recordTransfer(functions, expansion.aggregator.id, pass, expansion.aggregator);
                recordTransfer(functions, aggregate.id, pass, expansion.upsert);
                continue;
            }
            if (operator instanceof DBSPPartitionedRollingAggregateOperator rolling) {
                var expansion = pass.getReplacement(rolling);
                if (expansion != null) {
                    var input = pass.expansionMonotoneValues.get(expansion.replacement.inputs.get(0));
                    if (input != null)
                        functions.put(operator.id, Monotonicity.getBodyType(input).getType());
                }
                continue;
            }
            if (operator instanceof DBSPStreamJoinOperator || operator instanceof DBSPStreamJoinIndexOperator) {
                var expansion = pass.getReplacement(operator.to(DBSPSimpleOperator.class));
                if (expansion != null)
                    recordTransfer(functions, expansion.replacement.id, pass, expansion.replacement);
                continue;
            }
            if (!TRANSFER_OPERATORS.contains(operator.getClass()))
                continue;
            if (operator instanceof DBSPChainAggregateOperator && operator.annotations.first(AlwaysMonotone.class) != null)
                continue;
            var expansion = pass.getReplacement(operator.to(DBSPSimpleOperator.class));
            if (expansion == null)
                continue;
            recordTransfer(functions, operator.id, pass, expansion.replacement);
        }
        return functions;
    }

    public static String encodeTransferRoots(ToJsonOuterVisitor circuit, Map<Long, IDBSPInnerNode> functions) {
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
                    && pass.expandedInto.get(simple) instanceof StarJoinDeltaExpansion star)
                StarJoinInputs.capture(pass, star, entry.putObject("star_join"));
            if (BinaryJoinInputs.supports(operator.getClass())) {
                var state = entry.putObject("join");
                var join = BinaryJoinInputs.from(pass.expandedInto.get(operator.to(DBSPSimpleOperator.class)));
                if (join != null)
                    join.capture(pass, state.putObject("expansion"), entry);
            }
            if (operator instanceof DBSPSimpleOperator simple
                    && pass.expandedInto.get(simple) instanceof ReplacementDeltaExpansion expansion) {
                var replacement = expansion.replacement;
                var value = pass.expansionMonotoneValues.get(replacement);
                if (value != null)
                    entry.set("projection", projection(Monotonicity.getBodyType(value)));
                entry.put("expanded_id", replacement.getId());
                if (operator instanceof DBSPStreamJoinOperator || operator instanceof DBSPStreamJoinIndexOperator)
                    BinaryJoinInputs.captureTerm(pass, replacement.to(DBSPJoinBaseOperator.class), entry.putObject("snapshot_join"));
                if (operator instanceof DBSPLagOperator || operator instanceof DBSPPartitionedRollingAggregateOperator) {
                    var input = pass.expansionMonotoneValues.get(replacement.inputs.get(0));
                    if (input != null)
                        entry.set("input_projection", projection(Monotonicity.getBodyType(input)));
                }
            }
            if (operator instanceof DBSPAggregateOperator aggregate) {
                var aggregateFacts = entry.putObject("aggregate");
                if (pass.expandedInto.get(aggregate) instanceof AggregateDeltaExpansion expansion) {
                    aggregateFacts.put("primitive", expansion.aggregator.id);
                    var input = pass.expansionMonotoneValues.get(expansion.aggregator.inputs.get(0));
                    if (input != null)
                        aggregateFacts.set("input_projection", projection(Monotonicity.getBodyType(input)));
                    var reduction = pass.expansionMonotoneValues.get(expansion.aggregator);
                    if (reduction != null)
                        aggregateFacts.set("result_projection", projection(Monotonicity.getBodyType(reduction)));
                    var output = pass.expansionMonotoneValues.get(expansion.upsert);
                    if (output != null)
                        entry.set("projection", projection(Monotonicity.getBodyType(output)));
                }
            }
            if (operator instanceof DBSPDistinctOperator || operator instanceof DBSPPositiveOperator) {
                var expansion = pass.expandedInto.get(operator.to(DBSPSimpleOperator.class));
                var distinct = entry.putObject("distinct").put("expanded", expansion != null);
                if (expansion != null) {
                    var value = pass.expansionMonotoneValues.get(expansion.to(DistinctDeltaExpansion.class).distinct.right());
                    if (value != null)
                        distinct.set("input_projection", projection(Monotonicity.getBodyType(value)));
                }
            }
        }
        return facts;
    }
}
