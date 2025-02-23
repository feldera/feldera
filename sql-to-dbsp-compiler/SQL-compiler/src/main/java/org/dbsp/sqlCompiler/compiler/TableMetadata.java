package org.dbsp.sqlCompiler.compiler;

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.compiler.backend.JsonDecoder;
import org.dbsp.sqlCompiler.compiler.backend.ToJsonInnerVisitor;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.ForeignKey;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.ProgramIdentifier;
import org.dbsp.util.IJson;
import org.dbsp.util.JsonStream;
import org.dbsp.util.Linq;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;

/** Metadata describing an input table. */
public class TableMetadata implements IJson {
    public final ProgramIdentifier tableName;
    final LinkedHashMap<ProgramIdentifier, InputColumnMetadata> columnMetadata;
    final List<ProgramIdentifier> columnNames;
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

    public TableMetadata(ProgramIdentifier tableName,
                         List<InputColumnMetadata> columns, List<ForeignKey> foreignKeys,
                         boolean materialized, boolean streaming) {
        this.tableName = tableName;
        this.columnMetadata = new LinkedHashMap<>();
        this.materialized = materialized;
        this.foreignKeys = foreignKeys;
        this.changes = streaming ? TableChanges.AppendOnly : TableChanges.Unrestricted;
        this.columnNames = new ArrayList<>();
        for (InputColumnMetadata meta: columns) {
            Utilities.putNew(this.columnMetadata, meta.name, meta);
            this.columnNames.add(meta.name);
        }
    }

    public boolean isStreaming() {
        return this.changes == TableChanges.AppendOnly;
    }

    public List<ForeignKey> getForeignKeys() {
        return this.foreignKeys;
    }

    public List<InputColumnMetadata> getPrimaryKeys() {
        return Linq.where(this.getColumns(), c -> c.isPrimaryKey);
    }

    public int getColumnIndex(ProgramIdentifier columnName) {
        int index = 0;
        for (ProgramIdentifier colName: this.columnMetadata.keySet()) {
            if (colName.equals(columnName))
                return index;
            index++;
        }
        throw new RuntimeException("Column " + columnName.singleQuote() +
                " not found in table " + this.tableName.singleQuote());
    }

    @Nullable
    public InputColumnMetadata getColumnMetadata(ProgramIdentifier column) {
        return this.columnMetadata.get(column);
    }

    public InputColumnMetadata getColumnMetadata(int index) {
        ProgramIdentifier id = this.columnNames.get(index);
        return this.columnMetadata.get(id);
    }

    public int getColumnCount() { return this.columnMetadata.size(); }

    public Collection<InputColumnMetadata> getColumns() { return this.columnMetadata.values(); }

    public boolean isAppendOnly() {
        return this.changes == TableChanges.AppendOnly;
    }

    @Override
    public void asJson(ToJsonInnerVisitor visitor) {
        JsonStream stream = visitor.stream;
        stream.beginObject();
        stream.label("tableName");
        this.tableName.asJson(visitor);
        stream.label("materialized").append(this.materialized);
        stream.label("changes").append(this.changes.toString());
        stream.label("foreignKeys");
        stream.beginArray();
        for (ForeignKey key: this.foreignKeys)
            key.asJson(visitor);
        stream.endArray();
        stream.label("columnMetadata");
        stream.beginArray();
        for (InputColumnMetadata col: this.columnMetadata.values())
            col.asJson(visitor);
        stream.endArray();
        stream.endObject();
    }

    public static TableMetadata fromJson(JsonNode node, JsonDecoder decoder) {
        ProgramIdentifier tableName = ProgramIdentifier.fromJson(
                Utilities.getProperty(node, "tableName"));
        boolean materialized = Utilities.getBooleanProperty(node, "materialized");
        String changesS = Utilities.getStringProperty(node, "changes");
        TableChanges changes = TableChanges.valueOf(changesS);
        JsonNode fk = Utilities.getProperty(node, "foreignKeys");
        List<ForeignKey> foreignKeys = Linq.list(Linq.map(fk.elements(), ForeignKey::fromJson));
        JsonNode cm = Utilities.getProperty(node, "columnMetadata");
        List<InputColumnMetadata> columnMetadata =
                Linq.list(Linq.map(cm.elements(), e -> InputColumnMetadata.fromJson(e, decoder)));
        return new TableMetadata(
                tableName, columnMetadata, foreignKeys, materialized, changes == TableChanges.AppendOnly);
    }
}
