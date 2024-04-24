package org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;

import javax.annotation.Nullable;

/** Stores metadata for a column produced by an operator at the level
 * of Calcite Rel objects. */
public class RelColumnMetadata {
    public final CalciteObject node;
    /** Column name and type. */
    public final RelDataTypeField field;
    /** True if the column is a primary key. */
    public final boolean isPrimaryKey;
    /** Lateness, if declared. */
    @Nullable
    public final RexNode lateness;
    /** Default value, if declared */
    @Nullable
    public final RexNode defaultValue;
    /** True if the column name was quoted. */
    public final boolean nameIsQuoted;

    @Override
    public String toString() {
        return "RelColumnMetadata{" +
                "field=" + field +
                ", isPrimaryKey=" + isPrimaryKey +
                ", lateness=" + lateness +
                ", defaultValue=" + defaultValue +
                ", nameIsQuoted=" + nameIsQuoted +
                '}';
    }

    public RelColumnMetadata(
            CalciteObject node, RelDataTypeField field, boolean isPrimaryKey, boolean nameIsQuoted,
            @Nullable RexNode lateness, @Nullable RexNode defaultValue) {
        this.node = node;
        this.isPrimaryKey = isPrimaryKey;
        this.nameIsQuoted = nameIsQuoted;
        this.field = field;
        this.lateness = lateness;
        this.defaultValue = defaultValue;
    }

    public String getName() {
        return this.field.getName();
    }

    public RelDataType getType() {
        return this.field.getType();
    }

    public CalciteObject getNode() { return this.node; }
}
