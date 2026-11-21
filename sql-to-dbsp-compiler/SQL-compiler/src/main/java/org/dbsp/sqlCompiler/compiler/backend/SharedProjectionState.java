package org.dbsp.sqlCompiler.compiler.backend;

import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.compiler.visitors.unusedFields.FindCommonProjections;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.util.IJson;

import java.nio.charset.StandardCharsets;
import java.util.Comparator;
import java.util.Map;

/** Closures produced by FindCommonProjections share node identities with the circuit. */
public final class SharedProjectionState {
    private SharedProjectionState() {}

    public static byte[] encode(ToJsonOuterVisitor circuit, FindCommonProjections analysis) {
        var writer = circuit.additionalInnerNodes();
        writer.stream.beginObject().label("outputs");
        writeClosures(writer, analysis.outputProjection);
        writer.stream.label("inputs");
        writeClosures(writer, analysis.inputProjection);
        if (!analysis.outputProjection.isEmpty()) {
            writer.stream.label("origin");
            IJson.toJsonStream(circuit.emptyRelOrigin(), writer.stream);
        }
        writer.stream.endObject();
        return writer.stream.toString().getBytes(StandardCharsets.UTF_8);
    }

    private static void writeClosures(ToJsonInnerVisitor writer, Map<DBSPOperator, DBSPClosureExpression> closures) {
        writer.stream.beginObject();
        closures.entrySet().stream().sorted(Comparator.comparingLong(entry -> entry.getKey().id))
                .forEach(entry -> {
                    writer.stream.label(Long.toString(entry.getKey().id));
                    entry.getValue().accept(writer);
                });
        writer.stream.endObject();
    }
}
