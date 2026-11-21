package org.dbsp.sqlCompiler.compiler.backend;

import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.circuit.operator.DBSPFlatMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPNestedOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.compiler.visitors.outer.LowerCircuitVisitor;
import org.dbsp.util.Logger;
import org.dbsp.sqlCompiler.ir.expression.DBSPFlatmap;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTuple;

import java.nio.charset.StandardCharsets;

/** Types cached outside the circuit but shared with the lowered closures. */
public final class CircuitLoweringTypes {
    private CircuitLoweringTypes() {}

    public static byte[] encode(ToJsonOuterVisitor snapshot, DBSPCircuit circuit, boolean gen2) {
        var writer = snapshot.additionalInnerNodes();
        writer.liveNodes = snapshot.innerVisitor.liveNodes;
        writer.stream.beginObject().label("keep_aggregate_lists").append(gen2)
                .label("instrument_apply").append(Logger.INSTANCE.getLoggingLevel(LowerCircuitVisitor.class) >= 1)
                .label("empty_tuple");
        DBSPTypeTuple.EMPTY.accept(writer);
        writer.stream.label("elements").beginArray();
        writeElements(writer, circuit.getAllOperators());
        writer.stream.endArray().endObject();
        return writer.stream.toString().getBytes(StandardCharsets.UTF_8);
    }

    private static void writeElements(ToJsonInnerVisitor writer, Iterable<DBSPOperator> operators) {
        for (var operator : operators) {
            if (operator instanceof DBSPNestedOperator nested)
                writeElements(writer, nested.getAllOperators());
            else if (operator instanceof DBSPFlatMapOperator flat && flat.function instanceof DBSPFlatmap unnest) {
                writer.stream.beginArray();
                writer.stream.append(unnest.id);
                unnest.getCollectionElementType().accept(writer);
                writer.stream.endArray();
            }
        }
    }
}
