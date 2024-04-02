package org.dbsp.sqlCompiler.compiler;

import org.dbsp.sqlCompiler.compiler.frontend.CalciteObject;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;

import javax.annotation.Nullable;

/** Metadata describing an input table column. */
public class InputColumnMetadata {
    public final CalciteObject node;
    /** Column name. */
    public final String name;
    /** Column type. */
    public final DBSPType type;
    /** True if the column is part of a primary key. */
    public final boolean isPrimaryKey;
    /** Lateness, if declared.  Should be a constant expression. */
    @Nullable
    public final DBSPExpression lateness;
    /** Default value, if declared.  Should be a constant expression */
    @Nullable
    public final DBSPExpression defaultValue;

    public InputColumnMetadata(CalciteObject node, String name, DBSPType type, boolean isPrimaryKey,
                               @Nullable DBSPExpression lateness, @Nullable DBSPExpression defaultValue) {
        this.node = node;
        this.name = name;
        this.type = type;
        this.isPrimaryKey = isPrimaryKey;
        this.lateness = lateness;
        this.defaultValue = defaultValue;
    }

    public String getName() {
        return this.name;
    }

    public DBSPType getType() {
        return this.type;
    }

    public CalciteObject getNode() { return this.node; }
}
