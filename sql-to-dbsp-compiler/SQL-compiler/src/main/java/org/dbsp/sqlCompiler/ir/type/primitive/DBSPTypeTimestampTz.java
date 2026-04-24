package org.dbsp.sqlCompiler.ir.type.primitive;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.calcite.util.TimestampWithTimeZoneString;
import org.dbsp.sqlCompiler.compiler.backend.JsonDecoder;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPTimestampLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPTimestampTzLiteral;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeCode;
import org.dbsp.sqlCompiler.ir.type.IsDateType;
import org.dbsp.sqlCompiler.ir.type.IsTimeRelatedType;

import java.util.Objects;

public class DBSPTypeTimestampTz extends DBSPTypeBaseType
        implements IsDateType, IsTimeRelatedType {

    /** Number of digits after decimal point (microseconds) */
    public static final int PRECISION = 6;

    DBSPTypeTimestampTz(CalciteObject node, boolean mayBeNull) {
        super(node, DBSPTypeCode.TIMESTAMP_TZ, mayBeNull);
    }

    public static final DBSPTypeTimestampTz INSTANCE = new DBSPTypeTimestampTz(CalciteObject.EMPTY, false);
    public static final DBSPTypeTimestampTz NULLABLE_INSTANCE = new DBSPTypeTimestampTz(CalciteObject.EMPTY, true);

    public static DBSPTypeTimestampTz create(CalciteObject node, boolean mayBeNull) {
        if (node.isEmpty())
            return mayBeNull ? NULLABLE_INSTANCE : INSTANCE;
        return new DBSPTypeTimestampTz(node, mayBeNull);
    }

    public static DBSPTypeTimestampTz create(boolean mayBeNull) {
        return create(CalciteObject.EMPTY, mayBeNull);
    }

    @Override
    public DBSPExpression defaultValue() {
        if (this.mayBeNull)
            return this.none();
        return new DBSPTimestampTzLiteral(CalciteObject.EMPTY, this,
                new TimestampWithTimeZoneString("1970-01-01 00:00:00+0000"));
    }

    @Override
    public DBSPExpression getMaxValue() {
        return new DBSPTimestampTzLiteral(CalciteObject.EMPTY, this,
                new TimestampWithTimeZoneString("9999-12-31 23:59:59.999999+0000"));
    }

    @Override
    public DBSPExpression getMinValue() {
        return new DBSPTimestampTzLiteral(CalciteObject.EMPTY, this,
                new TimestampWithTimeZoneString("0001-01-01 00:00:00+0000"));
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
    public DBSPType withMayBeNull(boolean mayBeNull) {
        if (this.mayBeNull == mayBeNull)
            return this;
        return DBSPTypeTimestampTz.create(this.getNode(), mayBeNull);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.mayBeNull, 16);
    }

    @Override
    public boolean sameType(DBSPType other) {
        if (!super.sameNullability(other))
            return false;
        return other.is(DBSPTypeTimestampTz.class);
    }

    @SuppressWarnings("unused")
    public static DBSPTypeTimestampTz fromJson(JsonNode node, JsonDecoder decoder) {
        boolean mayBeNull = DBSPType.fromJsonMayBeNull(node);
        return DBSPTypeTimestampTz.create(mayBeNull);
    }
}
