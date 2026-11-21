package org.dbsp.sqlCompiler.compiler.backend;

import org.dbsp.sqlCompiler.compiler.visitors.outer.CreateStarJoins.Replacement;

import java.nio.charset.StandardCharsets;
import java.util.List;

/** Serialize the join-tree closures in the identity space of the input circuit. */
public final class StarJoinState {
    private StarJoinState() {}

    public static byte[] encode(ToJsonOuterVisitor circuit, List<Replacement> replacements) {
        var writer = circuit.additionalInnerNodes();
        writer.stream.beginArray();
        for (var tree : replacements) {
            writer.stream.beginObject();
            writer.stream.label("root").append(tree.root()).label("inputs").beginArray();
            for (var input : tree.inputs()) {
                writer.stream.beginArray();
                writer.stream.append(input.operator.id).append(input.outputNumber).endArray();
            }
            writer.stream.endArray().label("function");
            tree.function().accept(writer);
            writer.stream.endObject();
        }
        writer.stream.endArray();
        return writer.stream.toString().getBytes(StandardCharsets.UTF_8);
    }
}
