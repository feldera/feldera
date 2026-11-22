package org.dbsp.sqlCompiler.compiler.backend;

import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.util.JsonStream;

import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.HashMap;

/** Send deep closure bodies as a batch of shallow definitions in one node-ID space. */
final class WaterlineArguments extends ToJsonInnerVisitor {
    // Node bodies can also contain JSON arrays and source metadata. Leave room
    // for those containers within the decoder's 128-level document limit.
    private static final int MAX_INLINE_NODES = 32;
    private final ArrayDeque<IDBSPInnerNode> deferred = new ArrayDeque<>();

    WaterlineArguments(DBSPCompiler compiler) {
        super(compiler, JsonStream.compactCircuit(), 1, true);
        this.liveNodes = new HashMap<>();
    }

    @Override
    public VisitDecision preorder(IDBSPInnerNode node) {
        if (this.context.size() < MAX_INLINE_NODES || this.serialized.contains(node.getId()))
            return super.preorder(node);
        this.stream.beginObject().label("node").append(node.getId()).endObject();
        this.deferred.addLast(node);
        return VisitDecision.STOP;
    }

    byte[] encode(IDBSPInnerNode[] arguments) {
        this.stream.beginArray();
        for (var node : arguments)
            node.accept(this);
        if (this.deferred.isEmpty())
            return this.stream.endArray().toString().getBytes(StandardCharsets.UTF_8);

        while (!this.deferred.isEmpty()) {
            var node = this.deferred.removeFirst();
            if (!this.serialized.contains(node.getId()))
                node.accept(this);
        }
        String batch = "{\"arguments\":" + arguments.length + ",\"nodes\":" + this.stream.endArray() + "}";
        return batch.getBytes(StandardCharsets.UTF_8);
    }
}
