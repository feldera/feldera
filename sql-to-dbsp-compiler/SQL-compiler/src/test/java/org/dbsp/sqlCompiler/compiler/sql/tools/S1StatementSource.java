package org.dbsp.sqlCompiler.compiler.sql.tools;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.calcite.sql.FelderaS1StatementSource;
import org.apache.calcite.sql.SqlKind;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.ParsedStatement;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.util.Utilities;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;

/** Presentation metadata only. Decoding never parses or validates SQL text. */
final class S1StatementSource {
    private S1StatementSource() { }

    static ObjectNode encode(ParsedStatement statement) {
        return encode(statement, 0);
    }

    static ObjectNode encode(ParsedStatement statement, int source) {
        var result = Utilities.deterministicObjectMapper().createObjectNode();
        result.put("source", source).put("kind", statement.statement().getKind().name()).put("visible", statement.visible());
        result.put("statement_text", statement.toString());
        result.put("diagnostic_text", CalciteObject.create(statement.statement()).toString());
        var position = statement.statement().getParserPosition();
        result.set("position", S1RelationMetadata.position(position));
        if (position instanceof org.dbsp.sqlCompiler.compiler.frontend.ExtendedSqlParserPos extended)
            result.put("position_internal", extended.internal);
        else result.putNull("position_internal");
        return result;
    }
    static ParsedStatement decode(JsonNode wire) throws IOException {
        var fields = new HashSet<String>(); wire.fieldNames().forEachRemaining(fields::add);
        check(wire.isObject() && fields.equals(Set.of("source", "kind", "visible", "statement_text", "diagnostic_text", "position", "position_internal")),
                "Unknown or missing statement source fields");
        int source = S1MetadataWire.unsigned(wire.path("source"));
        check(wire.path("visible").isBoolean(), "Invalid statement visibility");
        long bytes = 0;
        for (String name: Set.of("kind", "statement_text", "diagnostic_text")) {
            check(wire.path(name).isTextual(), "Invalid statement source text");
            bytes += wire.path(name).textValue().getBytes(StandardCharsets.UTF_8).length;
        }
        check(bytes <= 1024 * 1024, "Statement source text exceeds limit");
        SqlKind kind;
        try { kind = SqlKind.valueOf(wire.path("kind").textValue()); }
        catch (IllegalArgumentException error) { throw new IOException("Unknown statement source kind", error); }
        var position = S1RelationMetadata.position(wire.path("position"));
        if (!wire.path("position_internal").isNull()) {
            check(wire.path("position_internal").isBoolean() && !position.isQuoted(), "Invalid extended source position");
            position = new org.dbsp.sqlCompiler.compiler.frontend.ExtendedSqlParserPos(position, wire.path("position_internal").booleanValue());
        }
        var node = new FelderaS1StatementSource(position, kind,
                wire.path("statement_text").textValue(), wire.path("diagnostic_text").textValue());
        var result = new ParsedStatement(node, wire.path("visible").booleanValue());
        check(encode(result, source).equals(wire), "Statement source metadata changed during reconstruction");
        return result;
    }
    static ParsedStatement decode(JsonNode wire, S1ExpressionSources sources) throws IOException {
        var statement = decode(wire);
        var context = sources.inSource(S1MetadataWire.unsigned(wire.path("source")));
        check(statement.visible() == (context.source == 0), "Statement visibility differs from source context");
        return new ParsedStatement(statement.statement(), statement.visible(),
                statement.visible() ? null : context.text(context.source));
    }

    private static void check(boolean condition, String message) throws IOException { if (!condition) throw new IOException(message); }
}
