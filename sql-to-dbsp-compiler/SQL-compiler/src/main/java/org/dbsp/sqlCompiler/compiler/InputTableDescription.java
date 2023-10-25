package org.dbsp.sqlCompiler.compiler;

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.compiler.backend.jit.JitCsvInputDescription;
import org.dbsp.sqlCompiler.compiler.backend.jit.JitFileAndSerialization;
import org.dbsp.sqlCompiler.compiler.backend.jit.JitIODescription;
import org.dbsp.sqlCompiler.compiler.errors.UnimplementedException;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.RelColumnMetadata;
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

    JsonNode asJson() {
        return this.createTableStatement.getDefinedObjectSchema();
    }

    JitIODescription getJitDescription(JitFileAndSerialization fas) {
        switch (fas.kind) {
            default:
            case Json:
                throw new UnimplementedException();
            case Csv: {
                JitCsvInputDescription result = new JitCsvInputDescription(this.getName(), fas.path);
                int index = 0;
                for (RelColumnMetadata columnMeta: createTableStatement.columns) {
                    JitCsvInputDescription.Column column = new JitCsvInputDescription.Column(index, index, columnMeta.getType());
                    result.addColumn(column);
                    if (columnMeta.isPrimaryKey)
                        result.addKeyColumn(column);
                    index++;
                }
                return result;
            }
        }
    }

    public String getName() {
        return this.createTableStatement.relationName;
    }
}
