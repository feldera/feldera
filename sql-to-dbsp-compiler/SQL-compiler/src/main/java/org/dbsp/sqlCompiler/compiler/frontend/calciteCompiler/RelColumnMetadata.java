package org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.dbsp.sqlCompiler.circuit.ForeignKeyReference;

import javax.annotation.Nullable;

/**
 * Stores metadata for a column produced by an operator at the level
 * of Calcite Rel objects.
 */
public class RelColumnMetadata {
    /**
     * Column name and type.
     */
    public final RelDataTypeField field;
    /**
     * True if the column is a primary key.
     */
    public final boolean isPrimaryKey;
    /**
     * Lateness, if declared.
     */
    @Nullable
    public final RexNode lateness;
    /**
     * If non-empty this is a foreign key reference to a different column.
     */
    @Nullable
    public final ForeignKeyReference foreignKeyReference;
    /**
     * True if the column name was quoted.
     */
    public final boolean nameIsQuoted;

    public RelColumnMetadata(RelDataTypeField field, boolean isPrimaryKey, boolean nameIsQuoted,
                             @Nullable RexNode lateness,
                             @Nullable ForeignKeyReference foreignKeyReference) {
        this.isPrimaryKey = isPrimaryKey;
        this.nameIsQuoted = nameIsQuoted;
        this.field = field;
        this.lateness = lateness;
        this.foreignKeyReference = foreignKeyReference;
    }

    public String getName() {
        return this.field.getName();
    }

    public RelDataType getType() {
        return this.field.getType();
    }
}
