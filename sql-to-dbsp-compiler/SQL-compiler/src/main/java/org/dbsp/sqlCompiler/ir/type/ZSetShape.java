package org.dbsp.sqlCompiler.ir.type;

import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeZSet;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.util.Utilities;

import java.util.ArrayList;
import java.util.List;

/** The shape of a Z-set of tuples with {@code fields} fields; its columns are {@link CollectionShape.Part#NONE}. */
public record ZSetShape(int fields) implements CollectionShape {
    @Override
    public boolean contains(Column column) {
        return column.part() == Part.NONE && column.field() < this.fields;
    }

    @Override
    public List<Column> columns() {
        List<Column> columns = new ArrayList<>();
        for (int i = 0; i < this.fields; i++)
            columns.add(Column.none(i));
        return columns;
    }

    @Override
    public Column output(int position) {
        Utilities.enforce(position >= 0 && position < this.fields,
                () -> "Position " + position + " out of bounds for " + this);
        return Column.none(position);
    }

    @Override
    public int width() {
        return this.fields;
    }

    @Override
    public DBSPVariablePath rowVariable(DBSPType type) {
        return type.to(DBSPTypeZSet.class).elementType.ref().var();
    }

    @Override
    public DBSPExpression field(DBSPVariablePath row, Column column) {
        return row.deref().field(column.field()).applyCloneIfNeeded();
    }
}
