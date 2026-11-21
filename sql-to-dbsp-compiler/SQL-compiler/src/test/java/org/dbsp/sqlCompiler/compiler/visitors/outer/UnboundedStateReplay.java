package org.dbsp.sqlCompiler.compiler.visitors.outer;

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/** Reuse the compiler's source-level warning renderer for Rust state findings. */
public final class UnboundedStateReplay extends FindUnboundedState {
    public UnboundedStateReplay(DBSPCompiler compiler) {
        super(compiler);
    }

    public DBSPCircuit report(DBSPCircuit circuit, JsonNode findings) {
        if (!findings.isArray())
            throw new IllegalArgumentException("Invalid unbounded-state findings");
        if (findings.isEmpty())
            return circuit;
        Map<Long, DBSPOperator> operators = new HashMap<>();
        for (DBSPOperator operator : circuit.allOperators)
            operators.put(operator.getId(), operator);
        Set<Long> seen = new HashSet<>();
        this.unbounded.clear();
        for (JsonNode finding : findings) {
            JsonNode id = finding.required("operator");
            JsonNode inputs = finding.required("inputs");
            if (!id.isIntegralNumber() || !id.canConvertToLong() || !seen.add(id.longValue()) || !inputs.isArray())
                throw new IllegalArgumentException("Invalid unbounded-state selection");
            DBSPOperator operator = operators.get(id.longValue());
            if (operator == null)
                throw new IllegalArgumentException("Unknown unbounded operator " + id);
            var inputNumbers = new ArrayList<Integer>();
            for (JsonNode input : inputs) {
                if (!input.isIntegralNumber() || !input.canConvertToInt()
                        || input.intValue() < 0 || input.intValue() >= operator.inputs.size())
                    throw new IllegalArgumentException("Invalid unbounded input " + input);
                inputNumbers.add(input.intValue());
            }
            this.unbounded.add(new UnboundedOperator(operator, circuit, inputNumbers));
        }
        this.streaming = true;
        var graph = new Graph(this.compiler());
        graph.apply(circuit);
        return new ReportUnbounded(graph.getGraphs()).apply(circuit);
    }
}
