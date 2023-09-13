package org.dbsp.sqlCompiler.compiler;

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.compiler.frontend.statements.CreateViewStatement;

/**
 * Information about an output view.
 */
public class OutputViewDescription {
    final CreateViewStatement view;

    public OutputViewDescription(CreateViewStatement view) {
        this.view = view;
    }

    JsonNode asJson() {
        return this.view.getDefinedObjectSchema();
    }
}
