package org.dbsp.sqlCompiler.ir.type;

import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeIndexedZSet;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.util.Utilities;

import java.util.ArrayList;
import java.util.List;

/** The shape of an indexed Z-set whose index tuple has {@code indexFields} fields and whose value
 * tuple has {@code valueFields}; its columns are {@link CollectionShape.Part#INDEX} and
 * {@link CollectionShape.Part#VALUE}. */
public record IndexedShape(int indexFields, int valueFields) implements CollectionShape {
    @Override
    public boolean contains(Column column) {
        return switch (column.part()) {
            case NONE -> false;
            case INDEX -> column.field() < this.indexFields;
            case VALUE -> column.field() < this.valueFields;
        };
    }

    @Override
    public List<Column> columns() {
        List<Column> columns = this.indexColumns();
        for (int i = 0; i < this.valueFields; i++)
            columns.add(Column.value(i));
        return columns;
    }

    /** The columns of the index */
    public List<Column> indexColumns() {
        List<Column> columns = new ArrayList<>();
        for (int i = 0; i < this.indexFields; i++)
            columns.add(Column.index(i));
        return columns;
    }

    @Override
    public Column output(int position) {
        Utilities.enforce(position >= 0 && position < this.width(),
                () -> "Position " + position + " out of bounds for " + this);
        if (position < this.indexFields)
            return Column.index(position);
        return Column.value(position - this.indexFields);
    }

    @Override
    public int width() {
        return this.indexFields + this.valueFields;
    }

    /** An indexed collection reaches a function as an (index, value) pair of references. */
    @Override
    public DBSPVariablePath rowVariable(DBSPType type) {
        return type.to(DBSPTypeIndexedZSet.class).getKVRefType().var();
    }

    @Override
    public DBSPExpression field(DBSPVariablePath row, Column column) {
        DBSPExpression part = row.field(column.part() == Part.INDEX ? 0 : 1).deref();
        return part.field(column.field()).applyCloneIfNeeded();
    }
}
