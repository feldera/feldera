package org.dbsp.sqlCompiler.circuit.operator;

import org.dbsp.sqlCompiler.circuit.ForeignKeyReference;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;

import javax.annotation.Nullable;

/**
 * Metadata describing an input table column.
 */
public class InputColumnMetadata {
    /**
     * Column name.
     */
    public final String name;
    /**
     * Column type.
     */
    public final DBSPType type;
    /**
     * True if the column is a primary key.
     */
    public final boolean isPrimaryKey;
    /**
     * Lateness, if declared.  Should be a constant expression.
     */
    @Nullable
    public final DBSPExpression lateness;
    /**
     * If non-empty this is a foreign key reference to a different column.
     */
    @Nullable
    public final ForeignKeyReference foreignKeyReference;

    public InputColumnMetadata(String name, DBSPType type, boolean isPrimaryKey,
                               @Nullable DBSPExpression lateness,
                               @Nullable ForeignKeyReference foreignKeyReference) {
        this.name = name;
        this.type = type;
        this.isPrimaryKey = isPrimaryKey;
        this.lateness = lateness;
        this.foreignKeyReference = foreignKeyReference;
    }

    public String getName() {
        return this.name;
    }

    public DBSPType getType() {
        return this.type;
    }
}
