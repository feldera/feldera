package org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;
import org.dbsp.sqlCompiler.compiler.errors.SourcePositionRange;
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
    /** Lateness, if declared. */
    @Nullable
    public final RexNode watermark;
    /** Default value, if declared */
    @Nullable
    public final RexNode defaultValue;
    @Nullable
    public final SourcePositionRange defaultValuePosition;
    public final boolean nameIsQuoted;
    // Set during compilation
    public boolean unused;

    public RelColumnMetadata(
            CalciteObject node, RelDataTypeField field, boolean isPrimaryKey, boolean nameIsQuoted,
            @Nullable RexNode lateness, @Nullable RexNode watermark, @Nullable RexNode defaultValue,
            @Nullable SourcePositionRange defaultValuePosition) {
        this.node = node;
        this.isPrimaryKey = isPrimaryKey;
        this.nameIsQuoted = nameIsQuoted;
        this.field = field;
        this.lateness = lateness;
        this.watermark = watermark;
        this.defaultValue = defaultValue;
        this.defaultValuePosition = defaultValuePosition;
        this.unused = false;
    }

    public void setUnused() {
        this.unused = true;
    }

    @Override
    public String toString() {
        return "RelColumnMetadata{" +
                "field=" + this.field +
                ", isPrimaryKey=" + this.isPrimaryKey +
                ", lateness=" + this.lateness +
                ", watermark=" + this.watermark +
                ", defaultValue=" + this.defaultValue +
                ", nameIsQuoted=" + this.nameIsQuoted +
                ", unused=" + this.unused +
                '}';
    }

    public ProgramIdentifier getName() {
        return new ProgramIdentifier(this.field.getName(), this.nameIsQuoted);
    }

    public RelDataType getType() {
        return this.field.getType();
    }

    public CalciteObject getNode() { return this.node; }
}
