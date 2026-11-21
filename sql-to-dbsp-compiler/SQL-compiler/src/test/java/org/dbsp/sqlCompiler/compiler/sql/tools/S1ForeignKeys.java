package org.dbsp.sqlCompiler.compiler.sql.tools;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.dbsp.sqlCompiler.compiler.errors.SourcePositionRange;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.ForeignKey;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.S1ForeignKeyFactory;
import org.dbsp.sqlCompiler.compiler.frontend.parser.S1ForeignKeyIdentifier;
import org.dbsp.sqlCompiler.compiler.frontend.parser.SqlFragmentIdentifier;
import org.dbsp.util.Utilities;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.dbsp.sqlCompiler.compiler.sql.tools.S1MetadataWire.*;

/** Preserve source and destination order, identifier components, and positions. */
final class S1ForeignKeys {
    private S1ForeignKeys() { }
    static ArrayNode encode(List<ForeignKey> keys, S1ExpressionSources sources) throws IOException {
        var result = Utilities.deterministicObjectMapper().createArrayNode();
        for (var key: keys) {
            var entry = result.addObject(); entry.set("source", encode(key.thisTable, sources));
            entry.set("target", encode(key.otherTable, sources));
        }
        return result;
    }
    private static ObjectNode encode(ForeignKey.TableAndColumns side, S1ExpressionSources sources) throws IOException {
        var result = Utilities.deterministicObjectMapper().createObjectNode();
        result.set("position", S1DiagnosticSource.origin(side.listPos, sources));
        result.set("table", S1SqlIdentifier.encode(S1ForeignKeyIdentifier.get(side.tableName), sources, 0));
        var columns = result.putArray("columns");
        for (var column: side.columnNames) columns.add(S1SqlIdentifier.encode(S1ForeignKeyIdentifier.get(column), sources, 0));
        return result;
    }
    static List<ForeignKey> decode(JsonNode wire, S1ExpressionSources sources) throws IOException {
        array(wire); var keys = new ArrayList<ForeignKey>();
        for (var key: wire) {
            fields(key, "source", "target");
            var source = side(key.path("source"), sources); var target = side(key.path("target"), sources);
            check(source.columnNames.size() == target.columnNames.size(), "Foreign key column counts differ");
            keys.add(new ForeignKey(source, target));
        }
        return List.copyOf(keys);
    }
    private static ForeignKey.TableAndColumns side(JsonNode wire, S1ExpressionSources sources) throws IOException {
        fields(wire, "position", "table", "columns"); array(wire.path("columns"));
        var columns = new ArrayList<SqlFragmentIdentifier>();
        for (var column: wire.path("columns")) columns.add(new SqlFragmentIdentifier(S1SqlIdentifier.decode(column, sources)));
        return S1ForeignKeyFactory.create(new SourcePositionRange(sources.position(wire.path("position"))),
                new SqlFragmentIdentifier(S1SqlIdentifier.decode(wire.path("table"), sources)), columns);
    }
}
