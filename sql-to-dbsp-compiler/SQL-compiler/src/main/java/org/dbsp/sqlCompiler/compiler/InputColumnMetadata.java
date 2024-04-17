package org.dbsp.sqlCompiler.compiler;

import org.dbsp.sqlCompiler.compiler.errors.SourcePositionRange;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.IHasType;
import org.dbsp.util.IHasName;

import javax.annotation.Nullable;

/** Metadata describing an input table column. */
public class InputColumnMetadata
        implements IHasLateness, IHasName, IHasSourcePositionRange, IHasType {
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

    @Override
    public String getName() {
        return this.name;
    }

    @Override
    public DBSPType getType() {
        return this.type;
    }

    public CalciteObject getNode() { return this.node; }

    @Nullable
    @Override
    public DBSPExpression getLateness() {
        return this.lateness;
    }

    @Override
    public SourcePositionRange getPositionRange() {
        return this.getNode().getPositionRange();
    }
}
