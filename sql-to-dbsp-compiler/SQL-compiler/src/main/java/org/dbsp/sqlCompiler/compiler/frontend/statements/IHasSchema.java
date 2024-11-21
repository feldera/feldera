package org.dbsp.sqlCompiler.compiler.frontend.statements;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.calcite.rel.externalize.RelJson;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.util.JsonBuilder;
import org.dbsp.sqlCompiler.compiler.IHasCalciteObject;
import org.dbsp.sqlCompiler.compiler.errors.CompilationError;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.frontend.TypeCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.RelColumnMetadata;
import org.dbsp.sqlCompiler.compiler.frontend.parser.PropertyList;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeStruct;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTuple;
import org.dbsp.util.ICastable;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.util.List;

/** An interface implemented by objects which have a name and a schema */
public interface IHasSchema extends IHasCalciteObject, ICastable {
    /** The name of this object */
    String getName();
    /** True if the name is quoted */
    boolean nameIsQuoted();
    /** The list of columns of this object */
    List<RelColumnMetadata> getColumns();
    /** Properties describing the connector attached to this object */
    @Nullable
    PropertyList getProperties();

    /** Return the index of the specified column. */
    default int getColumnIndex(SqlIdentifier id) {
        List<RelColumnMetadata> columns = this.getColumns();
        for (int i = 0; i < columns.size(); i++) {
            if (columns.get(i).getName().equals(id.toString()))
                return i;
        }
        throw new CompilationError("Column " + Utilities.singleQuote(id.toString()) +
                " not found in " + Utilities.singleQuote(this.getName()), CalciteObject.create(id));
    }

    default JsonNode asJson() {
        ObjectMapper mapper = Utilities.deterministicObjectMapper();
        ObjectNode result = mapper.createObjectNode();
        result.put("name", this.getName());
        result.put("case_sensitive", this.nameIsQuoted());
        ArrayNode fields = result.putArray("fields");
        ArrayNode keyFields = mapper.createArrayNode();
        boolean hasKey = false;
        for (RelColumnMetadata col: this.getColumns()) {
            ObjectNode column = fields.addObject();
            column.put("name", col.getName());
            column.put("case_sensitive", col.nameIsQuoted);
            if (col.isPrimaryKey) {
                keyFields.add(col.getName());
                hasKey = true;
            }
            Object object = RelJson.create().withJsonBuilder(new JsonBuilder())
                    .toJson(col.getType());
            try {
                // Is there a better way to do this?
                String json = mapper.writeValueAsString(object);
                JsonNode repr = mapper.readTree(json);
                column.set("columntype", repr);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        }
        if (hasKey)
            result.set("primary_key", keyFields);
        PropertyList props = this.getProperties();
        if (props != null) {
            result.set("properties", props.asJson());
        }
        return result;
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
