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
import org.dbsp.util.IJson;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;

/** Metadata describing an input table column. */
public class InputColumnMetadata
        implements IColumnMetadata, IJson {
    public final CalciteObject node;
    /** Column name. */
    public final ProgramIdentifier name;
    /** Column type. */
    public final DBSPType type;
    /** True if the column is part of a primary key. */
    public final boolean isPrimaryKey;
    /** Lateness, if declared.  Should be a constant expression. */
    @Nullable
    public final DBSPExpression lateness;
    /** Watermark, if declared.  Should be a constant expression. */
    @Nullable
    public final DBSPExpression watermark;
    /** Default value, if declared.  Should be a constant expression */
    @Nullable
    public final DBSPExpression defaultValue;
    @Nullable
    public final SourcePositionRange defaultValuePosition;
    public final boolean interned;

    public InputColumnMetadata(CalciteObject node, ProgramIdentifier name, DBSPType type, boolean isPrimaryKey,
                               @Nullable DBSPExpression lateness, @Nullable DBSPExpression watermark,
                               @Nullable DBSPExpression defaultValue,
                               @Nullable SourcePositionRange defaultValuePosition,
                               boolean interned) {
        this.node = node;
        this.name = name;
        this.type = type;
        this.isPrimaryKey = isPrimaryKey;
        this.lateness = lateness;
        this.watermark = watermark;
        this.defaultValue = defaultValue;
        this.defaultValuePosition = defaultValuePosition;
        this.interned = interned;
    }

    public ProgramIdentifier getName() {
        return this.name;
    }

    @Override
    public DBSPType getType() {
        return this.type;
    }

    public CalciteObject getNode() { return this.node; }

    @Override @Nullable
    public DBSPExpression getLateness() {
        return this.lateness;
    }

    @Override
    public SourcePositionRange getPositionRange() {
        return this.getNode().getPositionRange();
    }

    @Override @Nullable
    public DBSPExpression getWatermark() {
        return this.watermark;
    }

    @Nullable @Override
    public DBSPExpression getDefaultValue() {
        return this.defaultValue;
    }

    @Override public ProgramIdentifier getColumnName() {
        return this.name;
    }

    @Override
    public boolean isPrimaryKey() {
        return this.isPrimaryKey;
    }

    @Override
    public boolean isInterned() {
        return this.interned;
    }

    @Override
    public void asJson(ToJsonInnerVisitor visitor) {
        visitor.stream.beginObject()
                .label("name");
        this.name.asJson(visitor);
        visitor.stream.label("type");
        this.type.accept(visitor);
        visitor.stream.label("isPrimaryKey")
                        .append(this.isPrimaryKey);
        if (this.lateness != null) {
            visitor.stream.label("lateness");
            this.lateness.accept(visitor);
        }
        if (this.watermark != null) {
            visitor.stream.label("watermark");
            this.watermark.accept(visitor);
        }
        if (this.defaultValue != null) {
            visitor.stream.label("defaultValue");
            this.defaultValue.accept(visitor);
        }
        visitor.stream.label("interned")
                        .append(this.interned);
        visitor.stream.endObject();
    }

    public static InputColumnMetadata fromJson(JsonNode node, JsonDecoder decoder) {
        ProgramIdentifier name = ProgramIdentifier.fromJson(Utilities.getProperty(node, "name"));
        DBSPType type = DBSPNode.fromJsonInner(node, "type", decoder, DBSPType.class);
        boolean isPrimaryKey = Utilities.getBooleanProperty(node, "isPrimaryKey");
        DBSPExpression lateness = null;
        if (node.has("lateness"))
            lateness = DBSPNode.fromJsonInner(node, "lateness", decoder, DBSPExpression.class);
        DBSPExpression watermark = null;
        if (node.has("watermark"))
            watermark = DBSPNode.fromJsonInner(node, "watermark", decoder, DBSPExpression.class);
        DBSPExpression defaultValue = null;
        if (node.has("defaultValue"))
            defaultValue = DBSPNode.fromJsonInner(node, "defaultValue", decoder, DBSPExpression.class);
        boolean interned = Utilities.getBooleanProperty(node, "interned");
        return new InputColumnMetadata(CalciteObject.EMPTY, name, type, isPrimaryKey,
                lateness, watermark, defaultValue, SourcePositionRange.INVALID, interned);
    }
}
