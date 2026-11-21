package org.dbsp.sqlCompiler.compiler.sql.tools;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.dbsp.sqlCompiler.compiler.frontend.statements.CalciteTableDescription;
import org.dbsp.sqlCompiler.compiler.frontend.statements.S1CatalogAccess;
import org.dbsp.util.Utilities;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;

import static org.dbsp.sqlCompiler.compiler.sql.tools.S1MetadataWire.*;

/** Catalog table identity and exact scan-to-table edges. Schema identity is
 * separate: Calcite can create multiple table wrappers for the same schema. */
final class S1Catalog {
    private S1Catalog() { }
    record Table(int schema, RelOptTable original) { }
    record Scan(int relation, int table) { }
    record Snapshot(List<S1CatalogSchema.Snapshot> schemas, List<Table> tables, List<Scan> scans) {
        List<List<RexNode>> values() { return this.schemas.stream().map(S1CatalogSchema.Snapshot::values).toList(); }
        List<RelDataType> types() {
            var types = new ArrayList<RelDataType>();
            this.schemas.forEach(schema -> types.addAll(schema.types()));
            this.tables.forEach(table -> types.add(table.original().getRowType()));
            return types;
        }
    }

    static Snapshot capture(S1ExpressionSharingProbe.References captured) {
        return capture(captured, List.of());
    }

    static Snapshot capture(S1ExpressionSharingProbe.References captured, List<S1Statements.Capture> statements) {
        return capture(captured, statements, schema -> 0);
    }

    static Snapshot capture(S1ExpressionSharingProbe.References captured, List<S1Statements.Capture> statements,
                            java.util.function.ToIntFunction<org.dbsp.sqlCompiler.compiler.frontend.statements.IHasSchema> source) {
        var schemas = new ArrayList<S1CatalogSchema.Snapshot>();
        var tables = new ArrayList<Table>(); var scans = new ArrayList<Scan>();
        var schemaIds = new IdentityHashMap<org.dbsp.sqlCompiler.compiler.frontend.statements.IHasSchema, Integer>();
        var tableIds = new IdentityHashMap<RelOptTable, Integer>();
        for (var statement: statements) {
            if (statement.schema() == null) continue;
            var schema = (org.dbsp.sqlCompiler.compiler.frontend.statements.IHasSchema) statement.statement();
            if (!schemaIds.containsKey(schema)) {
                schemaIds.put(schema, schemas.size()); schemas.add(statement.schema());
            }
        }
        for (int relation = 0; relation < captured.relationNodes.size(); relation++) {
            if (!(captured.relationNodes.get(relation) instanceof TableScan scan)) continue;
            var table = scan.getTable();
            Integer tableId = tableIds.get(table);
            if (tableId == null) {
                var schema = S1CatalogAccess.schema(table); Integer schemaId = schemaIds.get(schema);
                if (schemaId == null) {
                    schemaId = schemas.size(); schemaIds.put(schema, schemaId); schemas.add(S1CatalogSchema.capture(schema, source.applyAsInt(schema)));
                }
                tableId = tables.size(); tableIds.put(table, tableId); tables.add(new Table(schemaId, table));
            }
            scans.add(new Scan(relation, tableId));
        }
        return new Snapshot(List.copyOf(schemas), List.copyOf(tables), List.copyOf(scans));
    }

    static ObjectNode encode(Snapshot catalog, S1ExpressionObjectsEncoder.Encoded graph) throws IOException {
        var wire = Utilities.deterministicObjectMapper().createObjectNode().put("schema_version", "catalog-v1");
        var sources = S1ExpressionSources.decode(graph.inputs().table().at("/type_table/sources"));
        var schemas = wire.putArray("schemas");
        for (int id = 0; id < catalog.schemas().size(); id++)
            schemas.add(S1CatalogSchema.encode(catalog.schemas().get(id), graph.inputs().types(), sources, id));
        var tables = wire.putArray("tables");
        for (var table: catalog.tables()) {
            var entry = tables.addObject().put("schema", table.schema()).put("row", graph.inputs().types().id(table.original().getRowType()));
            var name = entry.putArray("name"); table.original().getQualifiedName().forEach(name::add);
        }
        var scans = wire.putArray("scans");
        for (var scan: catalog.scans()) scans.addObject().put("relation", scan.relation()).put("table", scan.table());
        return wire;
    }

    record Decoded(List<S1CatalogSchema.Decoded> schemas, S1RelNodes.Tables resolver) { }

    static Decoded decode(JsonNode record, S1ExpressionObjectsTable.Decoded expressions) throws IOException {
        var wire = record.path("catalog"); fields(wire, "schema_version", "schemas", "tables", "scans");
        check(text(wire.path("schema_version")).equals("catalog-v1"), "Unknown catalog version");
        var sources = S1ExpressionSources.decode(record.at("/table/type_table/sources"));
        var schemas = new ArrayList<S1CatalogSchema.Decoded>(); array(wire.path("schemas"));
        for (var schema: wire.path("schemas")) schemas.add(S1CatalogSchema.decode(schema, expressions.types(), sources,
                record.at("/expressions/value_uses"), expressions.expressions()));
        var tables = new ArrayList<RelOptTable>(); array(wire.path("tables"));
        for (var table: wire.path("tables")) {
            fields(table, "schema", "row", "name");
            var schema = schemas.get(reference(table.path("schema"), schemas.size()));
            var row = expressions.types().get(reference(table.path("row"), expressions.types().size()));
            check(row.isStruct() && row.getFieldCount() == schema.columns().size(), "Catalog row differs from schema width");
            for (int column = 0; column < row.getFieldCount(); column++) {
                var field = row.getFieldList().get(column); var declared = schema.columns().get(column);
                check(field.getType() == declared.getType() && field.getName().equals(declared.field.getName()),
                        "Catalog row differs from schema fields");
            }
            array(table.path("name")); var name = new ArrayList<String>();
            for (var component: table.path("name")) name.add(text(component));
            check(!name.isEmpty(), "Catalog table requires a qualified name");
            tables.add(RelOptTableImpl.create(null, row, name, new CalciteTableDescription(schema),
                    (org.apache.calcite.linq4j.tree.Expression) null));
        }
        var scans = new HashMap<Integer, RelOptTable>(); array(wire.path("scans"));
        for (var scan: wire.path("scans")) {
            fields(scan, "relation", "table");
            int relation = reference(scan.path("relation"), record.at("/producers/relations").size());
            check(record.at("/producers/relations").get(relation).path("form").asText().equals("LogicalTableScan"),
                    "Catalog edge requires a logical table scan");
            var table = tables.get(reference(scan.path("table"), tables.size()));
            check(scans.put(relation, table) == null, "Duplicate catalog scan edge");
        }
        for (int relation = 0; relation < record.at("/producers/relations").size(); relation++)
            if (record.at("/producers/relations").get(relation).path("form").asText().equals("LogicalTableScan"))
                check(scans.containsKey(relation), "Table scan has no catalog edge");
        S1RelNodes.Tables resolver = (relation, name, row) -> {
            var table = scans.get(relation); check(table != null, "Missing catalog table");
            check(table.getRowType() == row && table.getQualifiedName().equals(name), "Scan and catalog declaration differ");
            return table;
        };
        return new Decoded(List.copyOf(schemas), resolver);
    }
}
