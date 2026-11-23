package org.dbsp.sqlCompiler.compiler.backend;

import org.dbsp.sqlCompiler.compiler.visitors.outer.FuseExpensiveMaps;

import java.nio.charset.StandardCharsets;

/** Encode simplified sibling-map functions in the input circuit's identity space. */
public final class MapFusionState {
    private MapFusionState() {}

    public static byte[] encode(ToJsonOuterVisitor circuit, FuseExpensiveMaps.Fuse pass) {
        var writer = circuit.additionalOptimizerExpressions();
        writer.stream.beginArray();
        for (var cluster : pass.clusters()) {
            writer.stream.beginArray();
            for (var member : cluster) {
                writer.stream.beginObject().label("operator").append(member.operator().id).label("function");
                member.function().accept(writer);
                writer.writeDeferredDefinitions();
                writer.stream.endObject();
            }
            writer.stream.endArray();
        }
        writer.stream.endArray();
        return writer.stream.toString().getBytes(StandardCharsets.UTF_8);
    }
}
