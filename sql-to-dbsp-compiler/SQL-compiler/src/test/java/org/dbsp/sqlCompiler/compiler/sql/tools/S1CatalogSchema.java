package org.dbsp.sqlCompiler.compiler.sql.tools;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rex.RexNode;
import org.dbsp.sqlCompiler.compiler.errors.SourcePositionRange;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.ProgramIdentifier;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.RelColumnMetadata;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.frontend.statements.IHasSchema;
import org.dbsp.util.Properties;
import org.dbsp.util.Utilities;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;

import static org.dbsp.sqlCompiler.compiler.sql.tools.S1MetadataWire.*;

/** Catalog schema captured before lowering mutates columns or their unused flags.
 * Value slots refer to one group in the shared expression arena. */
final class S1CatalogSchema {
    private S1CatalogSchema() { }

    record Snapshot(ProgramIdentifier name, CalciteObject node, List<RelColumnMetadata> columns,
                    JsonNode properties, int source) {
        List<RexNode> values() {
            var values = new ArrayList<RexNode>();
            for (var column: this.columns) {
                if (column.lateness != null) values.add(column.lateness);
                if (column.defaultValue != null) values.add(column.defaultValue);
            }
            return List.copyOf(values);
        }
        List<RelDataType> types() { return this.columns.stream().map(RelColumnMetadata::getType).toList(); }
    }

    record Decoded(ProgramIdentifier name, CalciteObject node, List<RelColumnMetadata> columns,
                   Properties properties, int source) implements IHasSchema {
        @Override public ProgramIdentifier getName() { return this.name; }
        @Override public CalciteObject getNode() { return this.node; }
        @Override public List<RelColumnMetadata> getColumns() { return this.columns; }
        @Override public Properties getProperties() { return this.properties; }
    }

    static Snapshot capture(IHasSchema schema) {
        return capture(schema, schema instanceof Decoded decoded ? decoded.source() : 0);
    }

    static Snapshot capture(IHasSchema schema, int source) {
        var columns = new ArrayList<RelColumnMetadata>();
        for (var column: schema.getColumns()) {
            var copy = new RelColumnMetadata(S1DiagnosticSource.snapshot(column.node), column.field,
                    column.isPrimaryKey, column.nameIsQuoted, column.lateness, column.defaultValue, column.defaultValuePosition);
            copy.unused = column.unused;
            columns.add(copy);
        }
        return new Snapshot(schema.getName(), S1DiagnosticSource.snapshot(schema.getNode()), List.copyOf(columns),
                schema.getProperties() == null ? null : schema.getProperties().asJson().deepCopy(), source);
    }

    static ObjectNode encode(Snapshot schema, S1TypeTable.Encoded types, S1ExpressionSources sources,
                             int values) throws IOException {
        sources = sources.inSource(schema.source());
        var wire = Utilities.deterministicObjectMapper().createObjectNode().put("source", schema.source());
        wire.set("name", name(schema.name())); wire.set("diagnostic", S1DiagnosticSource.encode(schema.node(), sources));
        wire.put("values", values);
        var columns = wire.putArray("columns"); int slot = 0;
        for (var column: schema.columns()) {
            var entry = columns.addObject(); entry.set("name", name(column.getName()));
            entry.put("index", column.field.getIndex()).put("ty", types.id(column.getType()))
                    .put("primary_key", column.isPrimaryKey).put("unused", column.unused);
            entry.set("diagnostic", S1DiagnosticSource.encode(column.node, sources));
            if (column.lateness == null) entry.putNull("lateness"); else entry.put("lateness", slot++);
            if (column.defaultValue == null) entry.putNull("default_value"); else entry.put("default_value", slot++);
            if (column.defaultValuePosition == null) entry.putNull("default_position");
            else entry.set("default_position", S1DiagnosticSource.origin(column.defaultValuePosition, sources));
        }
        if (schema.properties() == null) wire.putNull("properties");
        else {
            var properties = wire.putArray("properties");
            var entries = new ArrayList<>(schema.properties().properties());
            entries.sort(Comparator.comparing(java.util.Map.Entry::getKey));
            for (var property: entries) {
                var entry = properties.addObject().put("name", property.getKey()).put("value", property.getValue().path("value").textValue());
                for (String position: List.of("key_position", "value_position"))
                    entry.set(position, S1DiagnosticSource.origin(SourcePositionRange.fromJson(property.getValue().path(position)), sources));
            }
        }
        return wire;
    }

    static Decoded decode(JsonNode wire, List<RelDataType> types, S1ExpressionSources sources,
                          JsonNode valueUses, List<RexNode> expressions) throws IOException {
        fields(wire, "name", "diagnostic", "columns", "properties", "values", "source");
        int source = unsigned(wire.path("source"));
        sources = sources.inSource(source);
        var name = name(wire.path("name")); var node = S1DiagnosticSource.decode(wire.path("diagnostic"), sources);
        array(valueUses);
        var roots = valueUses.get(reference(wire.path("values"), valueUses.size())).path("roots"); array(roots);
        var values = new ArrayList<RexNode>();
        for (var root: roots) values.add(expressions.get(reference(root, expressions.size())));
        array(wire.path("columns")); var columns = new ArrayList<RelColumnMetadata>(); int slot = 0;
        for (var entry: wire.path("columns")) {
            fields(entry, "name", "index", "ty", "primary_key", "unused", "diagnostic", "lateness", "default_value", "default_position");
            int index = unsigned(entry.path("index"));
            check(index == columns.size(), "Catalog column index differs from position");
            var columnName = name(entry.path("name"));
            RexNode lateness = null, defaultValue = null;
            if (!entry.path("lateness").isNull()) {
                check(reference(entry.path("lateness"), values.size()) == slot, "Lateness value slot is out of order"); lateness = values.get(slot++);
            }
            if (!entry.path("default_value").isNull()) {
                check(reference(entry.path("default_value"), values.size()) == slot, "Default value slot is out of order"); defaultValue = values.get(slot++);
            }
            var column = new RelColumnMetadata(S1DiagnosticSource.decode(entry.path("diagnostic"), sources),
                    new RelDataTypeFieldImpl(columnName.name(), index, types.get(reference(entry.path("ty"), types.size()))),
                    bool(entry.path("primary_key")), columnName.isQuoted(), lateness, defaultValue,
                    entry.path("default_position").isNull() ? null : new SourcePositionRange(sources.position(entry.path("default_position"))));
            column.unused = bool(entry.path("unused")); columns.add(column);
        }
        check(slot == values.size(), "Catalog has unclaimed value slots");
        Properties properties = null;
        if (!wire.path("properties").isNull()) {
            array(wire.path("properties")); var names = new HashSet<String>();
            var positioned = Utilities.deterministicObjectMapper().createObjectNode();
            for (var property: wire.path("properties")) {
                fields(property, "name", "value", "key_position", "value_position");
                String key = text(property.path("name")); check(names.add(key), "Duplicate catalog property");
                var entry = positioned.putObject(key).put("value", text(property.path("value")));
                for (String position: List.of("key_position", "value_position"))
                    entry.set(position, new SourcePositionRange(sources.position(property.path(position))).asJson());
            }
            properties = Properties.fromJson(positioned);
        }
        return new Decoded(name, node, columns, properties, source);
    }
}
