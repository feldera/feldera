package org.dbsp.sqlCompiler.compiler.sql.tools;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.dbsp.sqlCompiler.compiler.errors.SourcePositionRange;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.ProgramIdentifier;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.functions.ExternalFunction;
import org.dbsp.sqlCompiler.compiler.frontend.parser.SqlCreateView;
import org.dbsp.sqlCompiler.compiler.frontend.statements.*;
import org.dbsp.util.Utilities;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.dbsp.sqlCompiler.compiler.sql.tools.S1MetadataWire.*;

/** Typed statement fields. SQL text is presentation metadata and is never parsed
 * on import. Catalog and expression references use the enclosing program arenas. */
final class S1Statements {
    private S1Statements() { }
    record Capture(RelStatement statement, ObjectNode source, S1CatalogSchema.Snapshot schema,
                   ObjectNode nameDiagnostic, JsonNode foreignKeys) { }

    static Capture capture(RelStatement statement, S1ExpressionSources sources) throws IOException {
        sources = sources.forStatement(statement.parsedStatement);
        var schema = statement instanceof CreateRelationStatement relation ? S1CatalogSchema.capture(relation, sources.source) : null;
        var name = statement instanceof CreateTableStatement table ? table.nameNode
                : statement instanceof CreateTypeStatement type ? type.nameNode : null;
        return new Capture(statement, S1StatementSource.encode(statement.parsedStatement, sources.source), schema,
                name == null ? null : S1DiagnosticSource.encode(name, sources),
                statement instanceof CreateTableStatement table ? S1ForeignKeys.encode(table.foreignKeys, sources) : null);
    }

    static List<RexNode> values(Capture capture) {
        if (capture.statement() instanceof SetOptionStatement option) return List.of(option.value);
        return capture.schema() == null ? List.of() : capture.schema().values();
    }
    static List<RelDataType> types(Capture capture) {
        var result = new ArrayList<RelDataType>();
        if (capture.schema() != null) result.addAll(capture.schema().types());
        if (capture.statement() instanceof CreateTypeStatement type) result.add(type.relDataType);
        if (capture.statement() instanceof CreateViewStatement view) result.add(view.getRoot().validatedRowType);
        return result;
    }
    static RelNode relation(Capture capture) {
        if (capture.statement() instanceof CreateViewStatement view) return view.getRel();
        if (capture.statement() instanceof TableModifyStatement modify) return modify.rel;
        return null;
    }

    static ObjectNode encode(Capture capture, Map<RelNode, Integer> relations,
                             S1TypeTable.Encoded types, int schema, int values,
                             Integer function) throws IOException {
        var wire = Utilities.deterministicObjectMapper().createObjectNode(); wire.set("source", capture.source());
        var statement = capture.statement();
        if (statement instanceof CreateTableStatement table) {
            wire.put("kind", "create_table").put("schema", schema); wire.set("name", name(table.relationName));
            wire.set("name_diagnostic", capture.nameDiagnostic()); wire.set("foreign_keys", capture.foreignKeys());
        } else if (statement instanceof CreateViewStatement view) {
            wire.put("kind", "create_view").put("schema", schema).put("view_kind", view.getViewKind().name())
                    .put("emit_final", view.emitFinalColumn());
            wire.set("name", name(view.relationName)); wire.set("root", S1RelRoot.encode(view.getRoot(), relations, types));
        } else if (statement instanceof DeclareViewStatement view) {
            wire.put("kind", "declare_view").put("schema", schema); wire.set("name", name(view.relationName));
        } else if (statement instanceof DropTableStatement drop) {
            wire.put("kind", "drop_table").put("if_exists", drop.ifExists); wire.set("name", name(drop.tableName));
        } else if (statement instanceof CreateFunctionStatement) {
            check(function != null, "Function statement has no operator declaration");
            wire.put("kind", "create_function").put("function", function);
        } else if (statement instanceof CreateAggregateStatement) {
            check(function != null, "Aggregate statement has no operator declaration");
            wire.put("kind", "create_aggregate").put("function", function);
        } else if (statement instanceof CreateTypeStatement type) {
            wire.put("kind", "create_type").put("ty", types.id(type.relDataType)); wire.set("name", name(type.typeName));
            wire.set("name_diagnostic", capture.nameDiagnostic());
            if (type.attributes == null) wire.putNull("attributes");
            else {
                var attributes = wire.putArray("attributes");
                for (var attribute: type.attributes) {
                    var entry = attributes.addObject(); entry.set("name", name(attribute.name()));
                    if (attribute.namedType() == null) entry.putNull("named_type"); else entry.set("named_type", name(attribute.namedType()));
                    if (attribute.nullable() == null) entry.putNull("nullable"); else entry.put("nullable", attribute.nullable());
                }
            }
        } else if (statement instanceof CreateIndexStatement index) {
            wire.put("kind", "create_index"); wire.set("name", name(index.indexName)); wire.set("target", name(index.refersTo));
            wire.set("name_position", S1RelationMetadata.position(index.namePosition));
            wire.set("target_position", S1RelationMetadata.position(index.targetPosition));
            var columns = wire.putArray("columns");
            for (int id = 0; id < index.columns.size(); id++) {
                var entry = columns.addObject(); entry.set("name", name(index.columns.get(id)));
                entry.set("position", S1RelationMetadata.position(index.columnParserPosition(id)));
            }
        } else if (statement instanceof SetOptionStatement option) {
            wire.put("kind", "set_option").put("values", values); wire.set("name", name(option.identifier));
        } else if (statement instanceof TableModifyStatement modify) {
            wire.put("kind", "modify_table").put("insert", modify.insert); wire.set("name", name(modify.tableName));
            if (modify.rel == null) wire.putNull("relation");
            else { Integer id = relations.get(modify.rel); check(id != null, "Modification relation outside arena"); wire.put("relation", id); }
            if (modify.targetColumns == null) wire.putNull("columns");
            else {
                var columns = wire.putArray("columns");
                for (var column: modify.targetColumns) {
                    var entry = columns.addObject().put("display_name", column.displayName()); entry.set("name", name(column.name()));
                    var range = column.position();
                    entry.set("position", S1RelationMetadata.position(new org.apache.calcite.sql.parser.SqlParserPos(
                            range.start.line, range.start.column, range.end.line, range.end.column)));
                }
            }
        } else throw new IOException("Statement adapter is missing for " + statement.getClass().getSimpleName());
        return wire;
    }

    static RelStatement decode(JsonNode wire, List<S1CatalogSchema.Decoded> schemas,
                               List<RelNode> relations, S1ExpressionObjectsTable.Decoded expressions,
                               S1ExpressionSources sources, JsonNode valueUses) throws IOException {
        var source = S1StatementSource.decode(wire.path("source"), sources);
        switch (text(wire.path("kind"))) {
            case "create_table" -> {
                fields(wire, "kind", "source", "schema", "name", "name_diagnostic", "foreign_keys");
                var schema = schemas.get(reference(wire.path("schema"), schemas.size()));
                return new CreateTableStatement(source, name(wire.path("name")), schema.columns(),
                        S1ForeignKeys.decode(wire.path("foreign_keys"), sources), schema.properties(),
                        S1DiagnosticSource.decode(wire.path("name_diagnostic"), sources));
            }
            case "create_view" -> {
                fields(wire, "kind", "source", "schema", "name", "view_kind", "emit_final", "root");
                var schema = schemas.get(reference(wire.path("schema"), schemas.size()));
                SqlCreateView.ViewKind kind;
                try { kind = SqlCreateView.ViewKind.valueOf(text(wire.path("view_kind"))); }
                catch (IllegalArgumentException error) { throw new IOException("Unknown view kind", error); }
                var emit = wire.path("emit_final");
                check(emit.isIntegralNumber() && emit.canConvertToInt() && emit.intValue() >= -1
                        && emit.intValue() < schema.columns().size(), "Invalid emit_final column");
                return new CreateViewStatement(source, name(wire.path("name")), schema.columns(), kind,
                        S1RelRoot.decode(wire.path("root"), relations, expressions.types()), emit.intValue(), schema.properties());
            }
            case "declare_view" -> {
                fields(wire, "kind", "source", "schema", "name");
                var schema = schemas.get(reference(wire.path("schema"), schemas.size()));
                check(schema.properties() == null, "Declared view cannot have properties");
                return new DeclareViewStatement(source, name(wire.path("name")), schema.columns());
            }
            case "drop_table" -> {
                fields(wire, "kind", "source", "name", "if_exists");
                return new DropTableStatement(source, name(wire.path("name")), bool(wire.path("if_exists")));
            }
            case "create_function" -> {
                fields(wire, "kind", "source", "function");
                SqlOperator function = expressions.operators().get(reference(wire.path("function"), expressions.operators().size()));
                check(function instanceof ExternalFunction, "Function declaration requires a resolved function");
                return new CreateFunctionStatement(source, (ExternalFunction) function);
            }
            case "create_aggregate" -> {
                fields(wire, "kind", "source", "function");
                var function = expressions.operators().get(reference(wire.path("function"), expressions.operators().size()));
                check(function instanceof org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.functions.SqlUserDefinedAggregationFunction,
                        "Aggregate declaration requires a resolved aggregate");
                return new CreateAggregateStatement(source, (org.apache.calcite.sql.validate.SqlUserDefinedAggFunction) function);
            }
            case "create_type" -> {
                fields(wire, "kind", "source", "name", "name_diagnostic", "ty", "attributes");
                List<CreateTypeStatement.Attribute> attributes = null;
                if (!wire.path("attributes").isNull()) {
                    array(wire.path("attributes")); attributes = new ArrayList<>();
                    for (var attribute: wire.path("attributes")) {
                        fields(attribute, "name", "named_type", "nullable");
                        attributes.add(new CreateTypeStatement.Attribute(name(attribute.path("name")),
                                attribute.path("named_type").isNull() ? null : name(attribute.path("named_type")),
                                attribute.path("nullable").isNull() ? null : bool(attribute.path("nullable"))));
                    }
                }
                var type = expressions.types().get(reference(wire.path("ty"), expressions.types().size()));
                check(attributes == null || (type.isStruct() && type.getFieldCount() == attributes.size()), "Type attribute count differs");
                return new CreateTypeStatement(source, S1DiagnosticSource.decode(wire.path("name_diagnostic"), sources),
                        name(wire.path("name")), type, attributes);
            }
            case "create_index" -> {
                fields(wire, "kind", "source", "name", "target", "name_position", "target_position", "columns");
                array(wire.path("columns")); var names = new ArrayList<ProgramIdentifier>();
                var positions = new ArrayList<org.apache.calcite.sql.parser.SqlParserPos>();
                for (var column: wire.path("columns")) {
                    fields(column, "name", "position"); names.add(name(column.path("name"))); positions.add(S1RelationMetadata.position(column.path("position")));
                }
                return new CreateIndexStatement(source, name(wire.path("name")), name(wire.path("target")), names, positions,
                        S1RelationMetadata.position(wire.path("name_position")), S1RelationMetadata.position(wire.path("target_position")));
            }
            case "set_option" -> {
                fields(wire, "kind", "source", "name", "values");
                var use = valueUses.get(reference(wire.path("values"), valueUses.size())); array(use.path("roots"));
                check(use.path("roots").size() == 1, "SET requires exactly one value");
                var value = expressions.expressions().get(reference(use.path("roots").get(0), expressions.expressions().size()));
                return new SetOptionStatement(source, name(wire.path("name")), value);
            }
            case "modify_table" -> {
                fields(wire, "kind", "source", "name", "insert", "relation", "columns");
                List<TableModifyStatement.TargetColumn> columns = null;
                if (!wire.path("columns").isNull()) {
                    array(wire.path("columns")); columns = new ArrayList<>();
                    for (var column: wire.path("columns")) {
                        fields(column, "name", "position", "display_name");
                        columns.add(new TableModifyStatement.TargetColumn(name(column.path("name")),
                                new SourcePositionRange(S1RelationMetadata.position(column.path("position"))), text(column.path("display_name"))));
                    }
                }
                var statement = new TableModifyStatement(source, bool(wire.path("insert")), name(wire.path("name")), columns);
                if (!wire.path("relation").isNull()) statement.setTranslation(relations.get(reference(wire.path("relation"), relations.size())));
                return statement;
            }
            default -> throw new IOException("Unknown S1 statement kind");
        }
    }
}
