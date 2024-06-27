package org.dbsp.sqlCompiler.compiler;

import org.dbsp.util.Utilities;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;

/** Metadata describing an input table. */
public class TableMetadata {
    final LinkedHashMap<String, InputColumnMetadata> columnMetadata;
    public final boolean materialized;

    public TableMetadata(List<InputColumnMetadata> columns, boolean materialized) {
        this.columnMetadata = new LinkedHashMap<>();
        this.materialized = materialized;
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
