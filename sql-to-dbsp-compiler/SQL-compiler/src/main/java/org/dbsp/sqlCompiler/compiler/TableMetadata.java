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
    final String tableName;
    final LinkedHashMap<String, InputColumnMetadata> columnMetadata;
    final List<ForeignKey> foreignKeys;
    public final boolean materialized;
    final TableChanges changes;

    /** Describes the kind of changes that can be applied to the table */
    enum TableChanges {
        /** Table supports inserts, deletes, updates */
        Unrestricted,
        /** Table only supports inserts */
        AppendOnly
    }

    public TableMetadata(String tableName,
                         List<InputColumnMetadata> columns, List<ForeignKey> foreignKeys,
                         boolean materialized, boolean streaming) {
        this.tableName = tableName;
        this.columnMetadata = new LinkedHashMap<>();
        this.materialized = materialized;
        this.foreignKeys = foreignKeys;
        this.changes = streaming ? TableChanges.AppendOnly : TableChanges.Unrestricted;
        for (InputColumnMetadata meta: columns) {
            Utilities.putNew(this.columnMetadata, meta.name, meta);
        }
    }

    public List<ForeignKey> getForeignKeys() {
        return this.foreignKeys;
    }

    public List<InputColumnMetadata> getPrimaryKeys() {
        return Linq.where(this.getColumns(), c -> c.isPrimaryKey);
    }

    public int getColumnIndex(String columnName) {
        int index = 0;
        for (String colName: this.columnMetadata.keySet()) {
            if (colName.equals(columnName))
                return index;
            index++;
        }
        throw new RuntimeException("Column " + Utilities.singleQuote(columnName) +
                " not found in table " + Utilities.singleQuote(this.tableName));
    }

    @Nullable
    public InputColumnMetadata getColumnMetadata(String column) {
        return this.columnMetadata.get(column);
    }

    public int getColumnCount() { return this.columnMetadata.size(); }

    public Collection<InputColumnMetadata> getColumns() { return this.columnMetadata.values(); }

    public boolean isAppendOnly() {
        return this.changes == TableChanges.AppendOnly;
    }
}
