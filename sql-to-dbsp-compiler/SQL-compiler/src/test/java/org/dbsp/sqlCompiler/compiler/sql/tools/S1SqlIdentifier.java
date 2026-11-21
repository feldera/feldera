package org.dbsp.sqlCompiler.compiler.sql.tools;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.FelderaS1Identifiers;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.dbsp.util.Utilities;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.dbsp.sqlCompiler.compiler.sql.tools.S1MetadataWire.*;

/** Exact identifier components, quoting, and source positions. */
final class S1SqlIdentifier {
    private S1SqlIdentifier() { }
    static ObjectNode encode(SqlIdentifier identifier, S1ExpressionSources sources, int source) throws IOException {
        check(identifier != null && identifier.getClass() == SqlIdentifier.class, "Unsupported SQL identifier identifier class");
        check(identifier.getCollation() == null, "Collated SQL identifier names require a separate contract");
        check(!identifier.names.isEmpty(), "Empty SQL identifier name");
        var result = Utilities.deterministicObjectMapper().createObjectNode();
        var names = result.putArray("names");
        for (String part: identifier.names) { check(!part.isEmpty(), "Empty SQL identifier name component"); names.add(part); }
        result.set("position", position(identifier.getParserPosition(), sources, source));
        var components = result.putObject("components");
        List<SqlParserPos> positions = FelderaS1Identifiers.components(identifier);
        if (positions == null) components.put("kind", "absent");
        else {
            check(positions.size() == identifier.names.size(), "SQL identifier name position arity differs");
            components.put("kind", "present"); var items = components.putArray("positions");
            for (SqlParserPos position: positions) items.add(position(position, sources, source));
        }
        return result;
    }

    static SqlIdentifier decode(JsonNode wire, S1ExpressionSources sources) throws IOException {
        fields(wire, "names", "position", "components");
        JsonNode names = wire.path("names");
        check(names.isArray() && !names.isEmpty() && names.size() <= 100_000, "Invalid SQL identifier name");
        var parts = new ArrayList<String>();
        for (JsonNode part: names) {
            check(part.isTextual() && !part.textValue().isEmpty(), "Invalid SQL identifier name component"); parts.add(part.textValue());
        }
        SqlParserPos position = position(wire.path("position"), sources);
        JsonNode components = wire.path("components"); List<SqlParserPos> positions = null;
        if (text(components.path("kind")).equals("absent")) fields(components, "kind");
        else {
            fields(components, "kind", "positions");
            check(text(components.path("kind")).equals("present"), "Unknown SQL identifier component positions");
            check(components.path("positions").isArray() && components.path("positions").size() == parts.size(), "SQL identifier name position arity differs");
            positions = new ArrayList<>();
            for (JsonNode item: components.path("positions")) positions.add(position(item, sources));
        }
        return new SqlIdentifier(parts, null, position, positions);
    }

    private static ObjectNode position(SqlParserPos position, S1ExpressionSources sources, int source) throws IOException {
        var result = Utilities.deterministicObjectMapper().createObjectNode();
        result.set("origin", sources.origin(position.withQuoting(false), source)); result.put("quoted", position.isQuoted());
        return result;
    }

    private static SqlParserPos position(JsonNode position, S1ExpressionSources sources) throws IOException {
        fields(position, "origin", "quoted"); check(position.path("quoted").isBoolean(), "Invalid SQL identifier name quoting");
        return sources.position(position.path("origin")).withQuoting(position.path("quoted").booleanValue());
    }

}
