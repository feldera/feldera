package org.dbsp.sqlCompiler.compiler.sql.tools;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.errors.SourcePositionRange;
import org.dbsp.util.Utilities;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.dbsp.sqlCompiler.compiler.sql.tools.S1MetadataWire.*;

/** Frontend-only accepted diagnostics between lowering calls. Lowering output
 * is never restored: the imported compiler must reproduce it independently. */
final class S1DiagnosticTimeline {
    record Message(SourcePositionRange context, SourcePositionRange range, boolean continuation,
                   String type, String message) { }
    record Boundary(SourcePositionRange context, List<Message> messages) {
        void apply(DBSPCompiler compiler) {
            for (var message: this.messages) {
                compiler.messages.setErrorContext(message.context);
                compiler.messages.reportProblem(message.range, true, message.continuation, message.type, message.message);
                compiler.hasWarnings = true;
            }
            compiler.messages.setErrorContext(this.context);
        }
    }
    record Decoded(List<Boundary> before, Boundary after) { }

    static final class Capture {
        final List<Boundary> before = new ArrayList<>();
        Boundary after;
        private int consumed;

        void before(DBSPCompiler compiler) { this.before.add(this.take(compiler)); }
        void lowered(DBSPCompiler compiler) { this.consumed = compiler.messages.messages.size(); }
        void finished(DBSPCompiler compiler) {
            if (this.after != null) throw new IllegalStateException("Multiple binding batches require explicit context");
            this.after = this.take(compiler);
        }
        private Boundary take(DBSPCompiler compiler) {
            var messages = new ArrayList<Message>();
            var all = compiler.messages.messages;
            if (all.size() < this.consumed) throw new IllegalStateException("Cleared diagnostic history");
            for (int index = this.consumed; index < all.size(); index++) {
                var message = all.get(index);
                if (!message.warning) throw new IllegalStateException("Cannot resume failed binding");
                messages.add(new Message(message.context, message.range, message.continuation, message.errorType, message.message));
            }
            this.consumed = all.size();
            return new Boundary(compiler.messages.errorContext, List.copyOf(messages));
        }
        ObjectNode encode(S1ExpressionSources sources) throws IOException {
            check(this.after != null, "Missing binding completion boundary");
            var wire = Utilities.deterministicObjectMapper().createObjectNode();
            var before = wire.putArray("before");
            for (var boundary: this.before) before.add(encodeBoundary(boundary, sources));
            wire.set("after", encodeBoundary(this.after, sources));
            return wire;
        }
    }

    private static ObjectNode encodeBoundary(Boundary boundary, S1ExpressionSources sources) throws IOException {
        var wire = Utilities.deterministicObjectMapper().createObjectNode();
        wire.set("context", origin(boundary.context, sources));
        var messages = wire.putArray("messages");
        for (var message: boundary.messages) {
            var entry = messages.addObject().put("continuation", message.continuation)
                    .put("type", message.type).put("message", message.message);
            entry.set("context", origin(message.context, sources));
            entry.set("range", origin(message.range, sources));
        }
        return wire;
    }
    private static ObjectNode origin(SourcePositionRange range, S1ExpressionSources sources) throws IOException {
        return sources.origin(new SqlParserPos(range.start.line, range.start.column, range.end.line, range.end.column), 0);
    }

    static Decoded decode(JsonNode wire, S1ExpressionSources sources, int statements) throws IOException {
        fields(wire, "before", "after"); array(wire.path("before"));
        check(wire.path("before").size() == statements, "Diagnostic boundaries differ from statement count");
        var before = new ArrayList<Boundary>();
        long[] budget = {100_000, 16 * 1024 * 1024};
        for (var boundary: wire.path("before")) before.add(decodeBoundary(boundary, sources, budget));
        return new Decoded(List.copyOf(before), decodeBoundary(wire.path("after"), sources, budget));
    }
    private static Boundary decodeBoundary(JsonNode wire, S1ExpressionSources sources, long[] budget) throws IOException {
        fields(wire, "context", "messages"); array(wire.path("messages"));
        var context = range(wire.path("context"), sources);
        var messages = new ArrayList<Message>();
        for (var entry: wire.path("messages")) {
            check(--budget[0] >= 0, "Too many frontend diagnostics");
            fields(entry, "context", "range", "continuation", "type", "message");
            String type = text(entry.path("type")), message = text(entry.path("message"));
            budget[1] -= type.getBytes(java.nio.charset.StandardCharsets.UTF_8).length
                    + message.getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
            check(budget[1] >= 0, "Frontend diagnostic text exceeds limit");
            messages.add(new Message(range(entry.path("context"), sources), range(entry.path("range"), sources),
                    bool(entry.path("continuation")), type, message));
        }
        return new Boundary(context, List.copyOf(messages));
    }
    private static SourcePositionRange range(JsonNode wire, S1ExpressionSources sources) throws IOException {
        check(wire.path("kind").asText().equals("absent") || unsigned(wire.path("source")) == 0,
                "Diagnostic range must refer to user source");
        return new SourcePositionRange(sources.position(wire));
    }
}
