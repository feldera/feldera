package org.dbsp.sqlCompiler.compiler.sql.tools;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.dbsp.sqlCompiler.compiler.errors.SourcePositionRange;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.util.Utilities;

import java.io.IOException;

/** Frozen diagnostic presentation, with no dependency on a parsed SQL tree. */
final class S1DiagnosticSource {
    private S1DiagnosticSource() { }

    static CalciteObject snapshot(CalciteObject original) {
        var range = original.getPositionRange();
        String text = original.toString(), internal = original.toInternalString();
        boolean empty = original.isEmpty();
        return fixed(range, text, internal, empty);
    }

    private static CalciteObject fixed(SourcePositionRange range, String text, String internal, boolean empty) {
        return new CalciteObject(range) {
            @Override public boolean isEmpty() { return empty; }
            @Override public String toString() { return text; }
            @Override public String toInternalString() { return internal; }
        };
    }

    static ObjectNode encode(CalciteObject node, S1ExpressionSources sources) throws IOException {
        var result = Utilities.deterministicObjectMapper().createObjectNode();
        result.set("origin", origin(node.getPositionRange(), sources));
        return result.put("text", node.toString()).put("internal_text", node.toInternalString()).put("empty", node.isEmpty());
    }

    static ObjectNode origin(SourcePositionRange range, S1ExpressionSources sources) throws IOException {
        return sources.origin(new SqlParserPos(range.start.line, range.start.column, range.end.line, range.end.column));
    }

    static CalciteObject decode(JsonNode node, S1ExpressionSources sources) throws IOException {
        S1MetadataWire.fields(node, "origin", "text", "internal_text", "empty");
        return fixed(new SourcePositionRange(sources.position(node.path("origin"))),
                S1MetadataWire.text(node.path("text")), S1MetadataWire.text(node.path("internal_text")),
                S1MetadataWire.bool(node.path("empty")));
    }
}
