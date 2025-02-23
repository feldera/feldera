package org.dbsp.sqlCompiler.ir.expression.literal;

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.compiler.backend.JsonDecoder;
import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.ISameValue;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeVariant;
import org.dbsp.util.IIndentStream;

import javax.annotation.Nullable;

/** The Variant.null value, the only value of native type VARIANT */
public class DBSPVariantNullLiteral extends DBSPLiteral {
    private DBSPVariantNullLiteral() {
        super(CalciteObject.EMPTY, DBSPTypeVariant.INSTANCE, false);
        assert type.is(DBSPTypeVariant.class);
    }

    public static final DBSPVariantNullLiteral INSTANCE = new DBSPVariantNullLiteral();

    @Override
    public void accept(InnerVisitor visitor) {
        VisitDecision decision = visitor.preorder(this);
        if (decision.stop()) return;
        visitor.push(this);
        visitor.pop(this);
        visitor.postorder(this);
    }

    @Override
    public boolean sameValue(@Nullable ISameValue o) {
        if (this == o) return true;
        return o != null && getClass() == o.getClass();
    }

    @Override
    public DBSPLiteral getWithNullable(boolean mayBeNull) {
        throw new InternalCompilerError("getWithNullable for VariantNullLiteral");
    }

    @Override
    public String toSqlString() {
        return "null";
    }

    @Override
    public DBSPExpression deepCopy() {
        return this;
    }

    @Override
    public IIndentStream toString(IIndentStream builder) {
        return builder.append("Variant::VariantNull");
    }

    /** The value representing the VARIANT null  */
    public static DBSPLiteral variantNull() {
        return DBSPVariantNullLiteral.INSTANCE;
    }

    /** The variant object representing the null literal */
    public static DBSPLiteral nullVariant() {
        return DBSPVariantNullLiteral.INSTANCE;
    }

    @SuppressWarnings("unused")
    public static DBSPVariantNullLiteral fromJson(JsonNode node, JsonDecoder decoder) {
        return INSTANCE;
    }
}
