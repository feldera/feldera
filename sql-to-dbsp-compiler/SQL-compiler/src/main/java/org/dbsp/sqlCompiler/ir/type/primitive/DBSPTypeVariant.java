package org.dbsp.sqlCompiler.ir.type.primitive;

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.compiler.backend.JsonDecoder;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariantExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeCode;
import org.dbsp.util.IIndentStream;

/** Represents a SQL VARIANT type: dynamically-typed values. */
public class DBSPTypeVariant extends DBSPTypeBaseType {
    /** True when VARIANT values use {@code LegacyVariant} rather than
     * {@code SqlVariant}.  This will go away. */
    private static boolean legacyRepresentation = false;

    /** True when VARIANT should be emitted as the enum {@code Variant}. */
    public static boolean legacyRepresentation() {
        return legacyRepresentation;
    }

    /** Selects the representation for the program being compiled. */
    public static void useLegacyRepresentation(boolean value) {
        legacyRepresentation = value;
    }

    /** Runs {@code codeGenerator} generating code with the legacy Variant representation.
     *
     * <p>Connector-metadata DEFAULT expressions need this: the adapters build
     * record metadata using the LegacyVariant type. */
    public static void withLegacyRepresentation(Runnable codeGenerator) {
        boolean saved = legacyRepresentation;
        legacyRepresentation = true;
        try {
            codeGenerator.run();
        } finally {
            legacyRepresentation = saved;
        }
    }

    DBSPTypeVariant(CalciteObject node, boolean mayBeNull) {
        super(node, DBSPTypeCode.VARIANT, mayBeNull);
    }

    @Override
    public String shortName() {
        return legacyRepresentation ? this.code.shortName : "FV";
    }

    @Override
    public String getRustString() {
        return legacyRepresentation ? this.code.rustName : "SqlVariant";
    }

    public static final DBSPTypeVariant INSTANCE = new DBSPTypeVariant(false);
    public static final DBSPTypeVariant INSTANCE_NULLABLE = new DBSPTypeVariant(true);

    DBSPTypeVariant(boolean mayBeNull) {
        this(CalciteObject.EMPTY, mayBeNull);
    }

    public static DBSPTypeVariant create(CalciteObject node, boolean mayBeNull) {
        if (node.isEmpty()) {
            return mayBeNull ? INSTANCE_NULLABLE : INSTANCE;
        }
        return new DBSPTypeVariant(node, mayBeNull);
    }

    public static DBSPTypeVariant create(boolean mayBeNull) {
        return mayBeNull ? INSTANCE_NULLABLE : INSTANCE;
    }

    @Override
    public boolean sameType(DBSPType other) {
        if (!super.sameNullability(other))
            return false;
        return other.is(DBSPTypeVariant.class);
    }

    @Override
    public DBSPType withMayBeNull(boolean mayBeNull) {
        if (this.mayBeNull == mayBeNull)
            return this;
        return new DBSPTypeVariant(this.getNode(), mayBeNull);
    }

    @Override
    public DBSPExpression defaultValue() {
        return DBSPVariantExpression.sqlNull(this.mayBeNull);
    }

    @Override
    public void accept(InnerVisitor visitor) {
        VisitDecision decision = visitor.preorder(this);
        if (decision.stop()) return;
        visitor.push(this);
        visitor.pop(this);
        visitor.postorder(this);
    }

    @Override
    public IIndentStream toString(IIndentStream builder) {
        return builder.append("VARIANT").append(this.mayBeNull ? "?" : "");
    }

    @Override
    public boolean hasCopy() {
        return false;
    }

    @SuppressWarnings("unused")
    public static DBSPTypeVariant fromJson(JsonNode node, JsonDecoder decoder) {
        boolean mayBeNull = fromJsonMayBeNull(node);
        return DBSPTypeVariant.create(mayBeNull);
    }
}
