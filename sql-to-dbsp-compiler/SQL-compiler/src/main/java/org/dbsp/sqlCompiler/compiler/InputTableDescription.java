package org.dbsp.sqlCompiler.compiler;

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.compiler.frontend.statements.CreateTableStatement;

/**
 * Information about an input table.
 */
public class InputTableDescription {
    // TODO: this representation should probably be more abstract
    final CreateTableStatement createTableStatement;

    public InputTableDescription(CreateTableStatement createTableStatement) {
        this.createTableStatement = createTableStatement;
    }

    public JsonNode asJson() {
        return this.createTableStatement.getDefinedObjectSchema();
    }

    public String getName() {
        return this.createTableStatement.relationName;
    }
}
