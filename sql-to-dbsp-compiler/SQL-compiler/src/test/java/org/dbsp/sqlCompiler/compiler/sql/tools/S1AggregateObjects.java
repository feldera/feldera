package org.dbsp.sqlCompiler.compiler.sql.tools;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.functions.AggregateFunctionDescription;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.functions.SqlUserDefinedAggregationFunction;
import org.dbsp.util.Utilities;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.dbsp.sqlCompiler.compiler.sql.tools.S1MetadataWire.*;

/** Resolved external aggregate declarations. Construction restores the trusted
 * return/operand inference rules from declared types without parsing SQL. */
final class S1AggregateObjects {
    private S1AggregateObjects() { }

    static ObjectNode encode(SqlUserDefinedAggregationFunction function, Map<RelDataType, Integer> types,
                             S1ExpressionSources sources, int source) throws IOException {
        var description = function.description;
        var wire = Utilities.deterministicObjectMapper().createObjectNode().put("kind", "aggregate_function")
                .put("linear", description.linear);
        wire.set("name", S1SqlIdentifier.encode(description.name, sources, source));
        wire.put("return_type", type(types, description.returnType));
        var parameters = wire.putArray("parameters");
        for (var parameter: description.parameterList)
            parameters.addObject().put("name", parameter.getName()).put("index", parameter.getIndex())
                    .put("ty", type(types, parameter.getType()));
        return wire;
    }

    static SqlUserDefinedAggregationFunction decode(JsonNode wire, List<RelDataType> types,
                                                     S1ExpressionSources sources) throws IOException {
        fields(wire, "kind", "name", "return_type", "parameters", "linear");
        check(text(wire.path("kind")).equals("aggregate_function"), "Unknown aggregate declaration kind");
        var name = S1SqlIdentifier.decode(wire.path("name"), sources);
        check(name.names.size() == 1, "Aggregate declaration requires a simple name");
        var result = types.get(reference(wire.path("return_type"), types.size()));
        array(wire.path("parameters"));
        var parameters = new ArrayList<org.apache.calcite.rel.type.RelDataTypeField>();
        for (var parameter: wire.path("parameters")) {
            fields(parameter, "name", "index", "ty");
            String parameterName = text(parameter.path("name"));
            check(!parameterName.isEmpty(), "Empty aggregate parameter name");
            parameters.add(new RelDataTypeFieldImpl(parameterName, unsigned(parameter.path("index")),
                    types.get(reference(parameter.path("ty"), types.size()))));
        }
        return new SqlUserDefinedAggregationFunction(new AggregateFunctionDescription(name, result, parameters, bool(wire.path("linear"))));
    }

    static S1FunctionObjects.Usage usage(SqlUserDefinedAggregationFunction function) {
        var description = function.description;
        long bytes = description.name.getSimple().getBytes(StandardCharsets.UTF_8).length;
        for (var parameter: description.parameterList) bytes += parameter.getName().getBytes(StandardCharsets.UTF_8).length;
        var components = org.apache.calcite.sql.FelderaS1Identifiers.components(description.name);
        return new S1FunctionObjects.Usage(3L + description.parameterList.size() + (components == null ? 0 : components.size()), bytes);
    }

    private static int type(Map<RelDataType, Integer> types, RelDataType type) throws IOException {
        Integer id = types.get(type); check(id != null, "Aggregate type is outside its arena"); return id;
    }
}
