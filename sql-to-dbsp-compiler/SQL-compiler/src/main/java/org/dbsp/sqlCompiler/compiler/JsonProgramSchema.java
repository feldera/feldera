package org.dbsp.sqlCompiler.compiler;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.ProgramIdentifier;
import org.dbsp.util.Properties;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;

/** The front end owns schema derivation. A resumed mid-end preserves its result
 * verbatim and changes only the column-usage flags. Both constructor and accessor
 * copy JSON so callers cannot change this metadata accidentally. */
public final class JsonProgramSchema implements ProgramSchema {
    private final ObjectNode schema;
    private final ProgramIdentifier name;

    public JsonProgramSchema(JsonNode schema) {
        if (!(schema instanceof ObjectNode object) || !schema.path("fields").isArray())
            throw new IllegalArgumentException("A program schema must be an object with a fields array");
        this.schema = object.deepCopy();
        this.name = new ProgramIdentifier(Utilities.getStringProperty(schema, "name"),
                Utilities.getBooleanProperty(schema, "case_sensitive"));
        for (JsonNode column: this.schema.withArray("fields")) {
            if (!column.isObject() || !column.path("columntype").isObject())
                throw new IllegalArgumentException("A schema column must have a columntype object");
        }
    }

    @Override
    public ProgramIdentifier getName() {
        return this.name;
    }

    @Nullable @Override
    public Properties getProperties() {
        JsonNode properties = this.schema.get("properties");
        return properties == null ? null : Properties.fromJson(properties);
    }

    @Override
    public JsonNode asJson(boolean addColumnCaseSensitivity) {
        ObjectNode result = this.schema.deepCopy();
        if (addColumnCaseSensitivity) {
            for (JsonNode column: result.withArray("fields"))
                ProgramSchema.addColumnCaseSensitivity(column.get("columntype"));
        }
        return result;
    }

    @Override
    public void markColumnUnused(int index) {
        ArrayNode columns = this.schema.withArray("fields");
        if (index < 0 || index >= columns.size())
            throw new IndexOutOfBoundsException("No column " + index + " in " + this.name);
        ((ObjectNode) columns.get(index)).put("unused", true);
    }
}
