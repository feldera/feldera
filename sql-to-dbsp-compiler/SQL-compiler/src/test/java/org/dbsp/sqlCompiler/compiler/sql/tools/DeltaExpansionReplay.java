package org.dbsp.sqlCompiler.compiler.sql.tools;

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.circuit.operator.DBSPBinaryDistinctOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDelayedIntegralOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIntegrateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSumOperator;
import org.dbsp.sqlCompiler.compiler.ProgramMetadata;
import org.dbsp.sqlCompiler.compiler.backend.JsonDecoder;
import org.dbsp.sqlCompiler.compiler.visitors.outer.expansion.DistinctDeltaExpansion;
import org.dbsp.sqlCompiler.compiler.visitors.outer.expansion.JoinDeltaExpansion;
import org.dbsp.sqlCompiler.compiler.visitors.outer.expansion.OperatorDeltaExpansion;
import org.dbsp.sqlCompiler.compiler.visitors.outer.expansion.ReplacementDeltaExpansion;

import java.util.HashMap;
import java.util.Map;

/** Restore the analysis graph and its original-to-expanded correspondence. */
final class DeltaExpansionReplay {
    private DeltaExpansionReplay() {}

    static DBSPCircuit restore(DBSPCircuit input, JsonNode before, JsonNode result,
                               Map<DBSPSimpleOperator, OperatorDeltaExpansion> correspondences) {
        JsonNode wire = result.required("circuit");
        var decoder = new JsonDecoder() {
            @Override
            public ProgramMetadata decodeProgramMetadata(JsonNode metadata) {
                if (!metadata.equals(before.required("metadata")))
                    throw new IllegalArgumentException("Delta expansion changed schema metadata");
                return input.getMetadata();
            }
        };
        DBSPCircuit expanded = decoder.decodeOuter(wire, DBSPCircuit.class);
        JsonNode saved = wire.required("allOperators");
        if (!saved.isArray() || saved.size() != expanded.allOperators.size())
            throw new IllegalArgumentException("Delta expansion lost operators on decode");

        Map<Long, DBSPOperator> oldIds = new HashMap<>();
        for (DBSPOperator operator : input.allOperators)
            oldIds.put(operator.getId(), operator);
        Map<Long, DBSPOperator> newIds = new HashMap<>();
        for (int index = 0; index < saved.size(); index++)
            if (newIds.put(id(saved.get(index), "id"), expanded.allOperators.get(index)) != null)
                throw new IllegalArgumentException("Duplicate delta expansion output ID");

        JsonNode items = result.required("expansions");
        if (!items.isArray())
            throw new IllegalArgumentException("Expected delta expansion correspondences");
        correspondences.clear();
        for (JsonNode item : items) {
            DBSPSimpleOperator original = operator(oldIds, item, "original").to(DBSPSimpleOperator.class);
            DBSPOperator terminal = operator(newIds, item, "operator");
            OperatorDeltaExpansion expansion = switch (item.required("kind").asText()) {
                case "replacement" -> new ReplacementDeltaExpansion(terminal.to(DBSPSimpleOperator.class));
                case "distinct" -> new DistinctDeltaExpansion(
                        operator(newIds, item, "integrator").to(DBSPIntegrateOperator.class),
                        terminal.to(DBSPBinaryDistinctOperator.class));
                case "join" -> new JoinDeltaExpansion(
                        operator(newIds, item, "left_integrator").to(DBSPDelayedIntegralOperator.class),
                        operator(newIds, item, "right_integrator").to(DBSPDelayedIntegralOperator.class),
                        operator(newIds, item, "left_delta").to(DBSPStreamJoinOperator.class),
                        operator(newIds, item, "right_delta").to(DBSPStreamJoinOperator.class),
                        operator(newIds, item, "both").to(DBSPStreamJoinOperator.class),
                        terminal.to(DBSPSumOperator.class));
                default -> throw new IllegalArgumentException("Unsupported delta expansion " + item);
            };
            if (correspondences.put(original, expansion) != null)
                throw new IllegalArgumentException("Duplicate delta expansion for " + original);
        }
        return expanded;
    }

    private static DBSPOperator operator(Map<Long, DBSPOperator> operators, JsonNode item, String field) {
        long id = id(item, field);
        DBSPOperator operator = operators.get(id);
        if (operator == null)
            throw new IllegalArgumentException("Unknown delta operator " + id);
        return operator;
    }

    private static long id(JsonNode item, String field) {
        JsonNode id = item.required(field);
        if (!id.isIntegralNumber() || !id.canConvertToLong() || id.longValue() < 0)
            throw new IllegalArgumentException("Invalid delta operator ID " + id);
        return id.longValue();
    }
}
