package org.dbsp.sqlCompiler.compiler;

import org.dbsp.sqlCompiler.compiler.errors.SourcePositionRange;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.ProgramIdentifier;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.IHasType;

import javax.annotation.Nullable;
import java.util.Objects;

/** Metadata for a column belonging to a view */
public class ViewColumnMetadata
        implements IHasLateness, IHasSourcePositionRange, IHasType {
    public final ProgramIdentifier viewName;
    /** Initially the column type is unknown, but it is filled in later */
    @Nullable
    public final DBSPType type;
    public final CalciteObject node;
    public final ProgramIdentifier columnName;
    @Nullable
    public final DBSPExpression lateness;

    public ViewColumnMetadata(CalciteObject node, ProgramIdentifier viewName, ProgramIdentifier columnName,
                              @Nullable DBSPType type, @Nullable DBSPExpression lateness) {
        this.node = node;
        this.type = type;
        this.viewName = viewName;
        this.columnName = columnName;
        this.lateness = lateness;
    }

    public CalciteObject getNode() { return this.node; }

    @Nullable
    @Override
    public DBSPExpression getLateness() {
        return this.lateness;
    }

    public ProgramIdentifier getName() {
        return this.columnName;
    }

    @Override
    public SourcePositionRange getPositionRange() {
        return this.getNode().getPositionRange();
    }

    public ViewColumnMetadata withType(DBSPType type) {
        assert this.type == null;
        return new ViewColumnMetadata(this.node, this.viewName, this.columnName, type, this.lateness);
    }

    @Override
    public String toString() {
        return this.viewName + "." + this.columnName +
                (this.lateness != null ? (" LATENESS " + this.lateness) : "");
    }

    @Override
    public DBSPType getType() {
        return Objects.requireNonNull(this.type);
    }
}
