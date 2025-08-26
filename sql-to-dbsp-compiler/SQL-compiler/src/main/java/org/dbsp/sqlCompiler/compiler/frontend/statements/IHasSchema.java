package org.dbsp.sqlCompiler.compiler.frontend.statements;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.calcite.rel.externalize.RelJson;
import org.apache.calcite.rel.externalize.RelJsonReader;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.util.JsonBuilder;
import org.dbsp.sqlCompiler.compiler.IHasCalciteObject;
import org.dbsp.sqlCompiler.compiler.errors.SourcePositionRange;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.ProgramIdentifier;
import org.dbsp.sqlCompiler.compiler.frontend.TypeCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.RelColumnMetadata;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteRelNode;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeStruct;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTuple;
import org.dbsp.util.ICastable;
import org.dbsp.util.Properties;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** An interface implemented by objects which have a name and a schema */
public interface IHasSchema extends IHasCalciteObject, ICastable {
    /** The name of this object */
    ProgramIdentifier getName();
    /** The list of columns of this object */
    List<RelColumnMetadata> getColumns();
    /** Properties describing the connector attached to this object */
    @Nullable
    Properties getProperties();

    default int getColumnIndex(ProgramIdentifier ident) {
        List<RelColumnMetadata> columns = this.getColumns();
        for (int i = 0; i < columns.size(); i++) {
            if (columns.get(i).getName().equals(ident))
                return i;
        }
        return -1;
    }

    /** Return the index of the specified column; -1 if columns is not found */
    default int getColumnIndex(SqlIdentifier id) {
        ProgramIdentifier ident = Utilities.toIdentifier(id);
        return this.getColumnIndex(ident);
    }

    /** A reduced version of IHasSchema which contains only information that can be obtained from
     * deserializing JSON */
    class AbstractIHasSchema implements IHasSchema {
        final ProgramIdentifier name;
        final List<RelColumnMetadata> columns;
        @Nullable final Properties properties;

        public AbstractIHasSchema(ProgramIdentifier name, List<RelColumnMetadata> columns, @Nullable Properties properties) {
            this.name = name;
            this.columns = columns;
            this.properties = properties;
            for (RelColumnMetadata c : columns)
                Utilities.enforce(c != null);
        }

        @Override
        public ProgramIdentifier getName() {
            return this.name;
        }

        @Override
        public List<RelColumnMetadata> getColumns() {
            return this.columns;
        }

        @Nullable
        @Override
        public Properties getProperties() {
            return this.properties;
        }

        @Override
        public CalciteObject getNode() {
            return CalciteObject.EMPTY;
        }

        public static AbstractIHasSchema fromJson(JsonNode node, RelDataTypeFactory typeFactory) {
            // The inverse of the asJson function
            String name = Utilities.getStringProperty(node, "name");
            boolean caseSensitive = Utilities.getBooleanProperty(node, "case_sensitive");
            ProgramIdentifier pi = new ProgramIdentifier(name, caseSensitive);
            Properties properties = null;
            if (node.has("properties")) {
                properties = Properties.fromJson(Utilities.getProperty(node, "properties"));
            }
            List<RelColumnMetadata> columns = new ArrayList<>();
            var it = Utilities.getProperty(node, "fields").elements();
            int index = 0;
            while (it.hasNext()) {
                var col = IHasSchema.relMetaFromJson(it.next(), index, typeFactory);
                index++;
                columns.add(col);
            }
            return new AbstractIHasSchema(pi, columns, properties);
        }
    }

    // Inverse of asJson below
    static RelColumnMetadata relMetaFromJson(JsonNode node, int index, RelDataTypeFactory typeFactory) {
        try {
            String name = Utilities.getStringProperty(node, "name");
            boolean caseSensitive = Utilities.getBooleanProperty(node, "case_sensitive");
            boolean isPrimaryKey = node.has("primary_key");
            JsonNode jsonType = Utilities.getProperty(node, "columntype");
            String json = Utilities.deterministicObjectMapper().writeValueAsString(jsonType);
            RelDataType type = RelJsonReader.readType(typeFactory, json);
            RelDataTypeField field = new RelDataTypeFieldImpl(name, index, type);
            // Do we need lateness, watermark, etc.?
            boolean interned = node.has("interned");
            return new RelColumnMetadata(CalciteObject.EMPTY, field, isPrimaryKey, caseSensitive,
                    null, null, null, SourcePositionRange.INVALID, interned);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    /** The schema as a JSON objects.
     *
     * @param addColumnCaseSensitivity If true, include case sensitivity information
     *                                 about the fields of nested structures.
     */
    default JsonNode asJson(boolean addColumnCaseSensitivity) {
        ObjectMapper mapper = Utilities.deterministicObjectMapper();
        ObjectNode result = mapper.createObjectNode();
        result.put("name", this.getName().name());
        result.put("case_sensitive", this.getName().isQuoted());
        ArrayNode fields = result.putArray("fields");
        ArrayNode keyFields = mapper.createArrayNode();
        boolean hasKey = false;
        for (RelColumnMetadata col: this.getColumns()) {
            ObjectNode column = fields.addObject();
            column.put("name", col.getName().name());
            column.put("case_sensitive", col.getName().isQuoted());
            if (col.isPrimaryKey) {
                keyFields.add(col.getName().name());
                hasKey = true;
            }
            Object object = RelJson.create().withJsonBuilder(new JsonBuilder())
                    .toJson(col.getType());
            try {
                String json = mapper.writeValueAsString(object);
                JsonNode repr = mapper.readTree(json);
                if (addColumnCaseSensitivity)
                    this.addColumnCaseSensitivity(repr);
                column.set("columntype", repr);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
            if (col.defaultValue != null) {
                column.put("default", CalciteRelNode.toSqlString(col.defaultValue));
            }
            if (col.lateness != null) {
                column.put("lateness", CalciteRelNode.toSqlString(col.lateness));
            }
            if (col.watermark != null) {
                column.put("watermark", CalciteRelNode.toSqlString(col.watermark));
            }
            if (col.interned) {
                column.put("interned", true);
            }
            column.put("unused", col.unused);
        }
        if (hasKey)
            result.set("primary_key", keyFields);
        Properties props = this.getProperties();
        if (props != null) {
            result.set("properties", props.asJson());
        }
        return result;
    }

    default void addColumnCaseSensitivity(JsonNode repr) {
        JsonNode component = repr.get("component");
        if (component != null) {
            this.addColumnCaseSensitivity(component);
            return;
        }
        JsonNode key = repr.get("key");
        if (key != null) {
            this.addColumnCaseSensitivity(key);
            return;
        }
        JsonNode value = repr.get("value");
        if (value != null) {
            this.addColumnCaseSensitivity(value);
            return;
        }
        JsonNode fields = repr.get("fields");
        if (fields != null) {
            ArrayNode fieldsArray = (ArrayNode) fields;
            for (JsonNode field: fieldsArray) {
                ObjectNode obj = (ObjectNode) field;
                String name = field.get("name").asText();
                boolean caseSensitive = ProgramIdentifier.needsQuotes(name);
                obj.put("case_sensitive", caseSensitive);
                JsonNode type = field.get("type");
                if (type instanceof ObjectNode) {
                    this.addColumnCaseSensitivity(type);
                }
            }
        }
    }

    default DBSPTypeStruct getRowTypeAsStruct(TypeCompiler compiler) {
        return compiler.convertType(this.getNode(), this.getName(), this.getColumns(), true, false)
                .to(DBSPTypeStruct.class);
    }

    default DBSPTypeTuple getRowTypeAsTuple(TypeCompiler compiler) {
        DBSPType type = compiler.convertType(this.getNode(), this.getName(), this.getColumns(), false, false);
        return type.to(DBSPTypeTuple.class);
    }
}
