package org.dbsp.sqlCompiler.compiler;

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.compiler.backend.JsonDecoder;
import org.dbsp.sqlCompiler.compiler.backend.ToJsonInnerVisitor;
import org.dbsp.sqlCompiler.compiler.errors.SourcePositionRange;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.ProgramIdentifier;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.ir.DBSPNode;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.IHasType;
import org.dbsp.util.IJson;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.util.Objects;

/** Metadata for a column belonging to a view */
public class ViewColumnMetadata
        implements IHasLateness, IHasSourcePositionRange, IHasType, IJson {
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

    @Override
    public String toString() {
        return this.viewName + "." + this.columnName +
                (this.lateness != null ? (" LATENESS " + this.lateness) : "");
    }

    @Override
    public DBSPType getType() {
        return Objects.requireNonNull(this.type);
    }

    public static ViewColumnMetadata fromJson(JsonNode node, JsonDecoder decoder) {
        ProgramIdentifier viewName = ProgramIdentifier.fromJson(Utilities.getProperty(node, "viewName"));
        DBSPType type = DBSPNode.fromJsonInner(node, "type", decoder, DBSPType.class);
        ProgramIdentifier columnName = ProgramIdentifier.fromJson(Utilities.getProperty(node, "columnName"));
        DBSPExpression lateness = null;
        if (node.has("lateness"))
            lateness = DBSPNode.fromJsonInner(node, "lateness", decoder, DBSPExpression.class);
        return new ViewColumnMetadata(CalciteObject.EMPTY, viewName, columnName, type, lateness);
    }

    @Override
    public void asJson(ToJsonInnerVisitor visitor) {
        // TODO
    }
}
