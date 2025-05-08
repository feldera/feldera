package org.dbsp.sqlCompiler.ir.type.primitive;

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.compiler.backend.JsonDecoder;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPDecimalLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPLiteral;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeCode;
import org.dbsp.sqlCompiler.ir.type.IsNumericType;

import javax.annotation.Nullable;
import java.math.BigDecimal;
import java.util.Objects;

public class DBSPTypeRuntimeDecimal extends DBSPTypeBaseType implements IsNumericType {
    public DBSPTypeRuntimeDecimal(CalciteObject node, boolean mayBeNull) {
        super(node, DBSPTypeCode.DECIMAL_RUNTIME, mayBeNull);
    }

    public static DBSPTypeRuntimeDecimal getDefault() {
        return new DBSPTypeRuntimeDecimal(CalciteObject.EMPTY, false);
    }

    @Override
    public int getPrecision() {
        return DBSPTypeDecimal.MAX_PRECISION;
    }

    @Override
    public DBSPLiteral getZero() {
        return new DBSPDecimalLiteral(this.getNode(), this, new BigDecimal(0));
    }

    @Override
    public DBSPLiteral getOne() {
        return new DBSPDecimalLiteral(this.getNode(), this, new BigDecimal(1));
    }

    @Override
    public DBSPType withMayBeNull(boolean mayBeNull) {
        if (mayBeNull == this.mayBeNull)
            return this;
        return new DBSPTypeRuntimeDecimal(this.getNode(), mayBeNull);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode());
    }

    @Override
    public DBSPExpression defaultValue() {
        if (this.mayBeNull)
            return this.none();
        return this.getZero();
    }

    @Override
    public boolean sameType(DBSPType type) {
        if (!super.sameNullability(type))
            return false;
        return type.is(DBSPTypeRuntimeDecimal.class);
    }

    @Nullable
    @Override
    public String asSqlString() {
        return "DECIMAL";
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
    public String toString() {
        return "DECIMAL"+ (this.mayBeNull ? "?" : "");
    }

    @Override
    public DBSPExpression getMaxValue() {
        return DBSPTypeDecimal.getDefault().getMaxValue();
    }

    @Override
    public DBSPExpression getMinValue() {
        return DBSPTypeDecimal.getDefault().getMinValue();
    }

    @SuppressWarnings("unused")
    public static DBSPTypeRuntimeDecimal fromJson(JsonNode node, JsonDecoder decoder) {
        boolean mayBeNull = DBSPTypeUSize.fromJsonMayBeNull(node);
        return new DBSPTypeRuntimeDecimal(CalciteObject.EMPTY, mayBeNull);
    }
}
