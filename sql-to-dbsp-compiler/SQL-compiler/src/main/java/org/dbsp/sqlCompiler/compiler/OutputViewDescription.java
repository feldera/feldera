package org.dbsp.sqlCompiler.compiler;

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.compiler.backend.jit.JitFileAndSerialization;
import org.dbsp.sqlCompiler.compiler.backend.jit.JitIODescription;
import org.dbsp.sqlCompiler.compiler.backend.jit.JitJsonOutputDescription;
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

    public JitIODescription getDescription(JitFileAndSerialization fas) {
        switch (fas.kind) {
            case Json: {
                JitJsonOutputDescription result = new JitJsonOutputDescription(this.getName(), fas.path);
                for (RelColumnMetadata column: this.view.columns)
                    result.addColumn(column.getName(), column.getType());
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
