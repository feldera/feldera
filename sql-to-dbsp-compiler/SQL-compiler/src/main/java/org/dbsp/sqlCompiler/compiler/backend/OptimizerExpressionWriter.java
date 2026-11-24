package org.dbsp.sqlCompiler.compiler.backend;

import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.expression.DBSPForExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPTimestampTzLiteral;
import org.dbsp.util.JsonStream;

import java.nio.charset.StandardCharsets;
import java.time.DateTimeException;
import java.util.ArrayDeque;
import java.util.HashMap;

/** Encodes optimizer expressions using shallow definitions that share one node-ID space. */
class OptimizerExpressionWriter extends ToJsonInnerVisitor {
    // Node bodies can also contain JSON arrays and source metadata. Leave room
    // for those containers within the decoder's 128-level document limit.
    private static final int MAX_INLINE_NODES = 32;
    private final ArrayDeque<IDBSPInnerNode> deferred = new ArrayDeque<>();

    OptimizerExpressionWriter(DBSPCompiler compiler) {
        this(compiler, true);
    }

    OptimizerExpressionWriter(DBSPCompiler compiler, boolean replayMetadata) {
        this(compiler, JsonStream.compactCircuit(), replayMetadata ? 1 : 0, replayMetadata);
        this.liveNodes = new HashMap<>();
    }

    OptimizerExpressionWriter(DBSPCompiler compiler, JsonStream stream, int verbosity, boolean replayMetadata) {
        super(compiler, stream, verbosity, replayMetadata);
        this.includeShuffleIdentities();
    }

    @Override
    public void postorder(DBSPForExpression loop) {
        this.stream.label("variable").append(loop.variable);
        super.postorder(loop);
    }

    @Override
    public void postorder(DBSPTimestampTzLiteral literal) {
        if (literal.value != null) {
            // DBSPTimestampTzLiteral.sameValue compares the instant resolved by this JDK's tzdata.
            // Retain the original spelling as well for SQL printing and artifact emission.
            try {
                long micros = java.util.Objects.requireNonNull(literal.getMicros());
                this.stream.label("runtimeMicros").append(micros);
            } catch (DateTimeException invalidZone) {
                // A custom GMT offset can exceed java.time's range. Walking the
                // graph does not demand its instant until an operation compares it.
                this.stream.label("runtimeMicros").appendNull();
            }
        }
        super.postorder(literal);
    }

    @Override
    public VisitDecision preorder(IDBSPInnerNode node) {
        if (this.context.size() < MAX_INLINE_NODES || this.serialized.contains(node.getId()))
            return super.preorder(node);
        this.stream.beginObject().label("node").append(node.getId()).endObject();
        this.deferred.addLast(node);
        return VisitDecision.STOP;
    }

    byte[] encode(IDBSPInnerNode... arguments) {
        this.stream.beginArray();
        int count = 0;
        for (var node : arguments) {
            if (node != null) {
                node.accept(this);
                count++;
            }
        }
        if (this.deferred.isEmpty())
            return this.stream.endArray().toString().getBytes(StandardCharsets.UTF_8);

        this.writeDeferred();
        String batch = "{\"arguments\":" + count + ",\"nodes\":" + this.stream.endArray() + "}";
        return batch.getBytes(StandardCharsets.UTF_8);
    }

    /** Append definitions for deep operands after all roots have been written. */
    void writeDeferredDefinitions() {
        if (this.deferred.isEmpty())
            return;
        this.stream.label("expressionDefinitions").beginArray();
        this.writeDeferred();
        this.stream.endArray();
    }

    private void writeDeferred() {
        while (!this.deferred.isEmpty()) {
            var node = this.deferred.removeFirst();
            if (!this.serialized.contains(node.getId()))
                node.accept(this);
        }
    }
}
