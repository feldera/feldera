package org.dbsp.sqlCompiler.compiler;

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.compiler.backend.jit.JitJsonOutputDescription;
import org.dbsp.sqlCompiler.compiler.backend.jit.JitOutputDescription;
import org.dbsp.sqlCompiler.compiler.backend.jit.JitSerializationKind;
import org.dbsp.sqlCompiler.compiler.errors.UnimplementedException;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.RelColumnMetadata;
import org.dbsp.sqlCompiler.compiler.frontend.statements.CreateViewStatement;

/**
 * Information about an output view.
 */
public class OutputViewDescription {
    final CreateViewStatement view;

    public OutputViewDescription(CreateViewStatement view) {
        this.view = view;
    }

    public JitOutputDescription getDescription(JitSerializationKind kind) {
        switch (kind) {
            case Json: {
                JitJsonOutputDescription result = new JitJsonOutputDescription();
                for (RelColumnMetadata column: this.view.columns)
                    result.addColumn(column.getName());
                return result;
            }
            default:
            case Csv:
                throw new UnimplementedException();
        }
    }

    public JsonNode asJson() {
        return this.view.getDefinedObjectSchema();
    }

    public String getName() {
        return this.view.relationName;
    }
}
