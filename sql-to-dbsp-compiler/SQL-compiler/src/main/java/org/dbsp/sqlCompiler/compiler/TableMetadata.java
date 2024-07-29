package org.dbsp.sqlCompiler.compiler;

import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.ForeignKey;
import org.dbsp.util.Linq;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;

/** Metadata describing an input table. */
public class TableMetadata {
    final LinkedHashMap<String, InputColumnMetadata> columnMetadata;
    final List<ForeignKey> foreignKeys;
    public final boolean materialized;

    public TableMetadata(List<InputColumnMetadata> columns, List<ForeignKey> foreignKeys,
                         boolean materialized) {
        this.columnMetadata = new LinkedHashMap<>();
        this.materialized = materialized;
        this.foreignKeys = foreignKeys;
        for (InputColumnMetadata meta: columns) {
            Utilities.putNew(this.columnMetadata, meta.name, meta);
        }
    }

    List<InputColumnMetadata> getPrimaryKeys() {
        return Linq.where(this.getColumns(), c -> c.isPrimaryKey);
    }

    @Nullable
    public InputColumnMetadata getColumnMetadata(String column) {
        return this.columnMetadata.get(column);
    }

    public int getColumnCount() { return this.columnMetadata.size(); }

    public Collection<InputColumnMetadata> getColumns() { return this.columnMetadata.values(); }
}
