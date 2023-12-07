package org.dbsp.sqlCompiler.compiler;

import org.dbsp.util.Utilities;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;

/**
 * Metadata describing an input table.
 */
public class InputTableMetadata {
    final LinkedHashMap<String, InputColumnMetadata> columnMetadata;

    public InputTableMetadata(List<InputColumnMetadata> columns) {
        this.columnMetadata = new LinkedHashMap<>();
        for (InputColumnMetadata meta: columns) {
            Utilities.putNew(this.columnMetadata, meta.name, meta);
        }
    }

    public InputColumnMetadata getColumnMetadata(String column) {
        return this.columnMetadata.get(column);
    }

    public int getColumnCount() { return this.columnMetadata.size(); }

    public Collection<InputColumnMetadata> getColumns() { return this.columnMetadata.values(); }
}
