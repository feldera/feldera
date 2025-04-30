package org.dbsp.sqlCompiler.ir.expression.literal;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.compiler.backend.JsonDecoder;
import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.ISameValue;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeUuid;
import org.dbsp.util.IIndentStream;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.UUID;

public final class DBSPUuidLiteral extends DBSPLiteral {
    @Nullable
    public final UUID value;

    public DBSPUuidLiteral(CalciteObject node, DBSPType type, @Nullable UUID value) {
        super(node, type, value == null);
        Utilities.enforce(type.is(DBSPTypeUuid.class));
        this.value = value;
    }

    @Nullable
    public byte[] getByteArray() {
        if (this.value == null)
            return null;
        ByteBuffer byteBuffer = ByteBuffer.wrap(new byte[16]);
        byteBuffer.putLong(this.value.getMostSignificantBits());
        byteBuffer.putLong(this.value.getLeastSignificantBits());
        return byteBuffer.array();
    }

    public DBSPUuidLiteral() {
        this(null, true);
    }

    public DBSPUuidLiteral(@Nullable UUID u, boolean nullable) {
        this(CalciteObject.EMPTY, new DBSPTypeUuid(CalciteObject.EMPTY, nullable), u);
        if (u == null && !nullable)
            throw new InternalCompilerError("Null value with non-nullable type", this);
    }

    @Override
    public DBSPExpression deepCopy() {
        return new DBSPUuidLiteral(this.getNode(), this.type, this.value);
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
    public DBSPLiteral getWithNullable(boolean mayBeNull) {
        return new DBSPUuidLiteral(this.checkIfNull(this.value, mayBeNull), mayBeNull);
    }

    @Override
    public String toSqlString() {
        if (this.value == null)
            return DBSPNullLiteral.NULL;
        return this.value.toString();
    }

    @Override
    public boolean sameValue(@Nullable ISameValue o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DBSPUuidLiteral that = (DBSPUuidLiteral) o;
        return Objects.equals(this.value, that.value);
    }

    @Override
    public IIndentStream toString(IIndentStream builder) {
        if (this.value == null)
            return builder.append("(")
                    .append(this.type)
                    .append(")null");
        else
            return builder.append(this.value.toString());
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), this.value);
    }

    @SuppressWarnings("unused")
    public static DBSPUuidLiteral fromJson(JsonNode node, JsonDecoder decoder) {
        UUID value = null;
        if (node.has("value")) {
            value = UUID.fromString(Utilities.getStringProperty(node, "value"));
        }
        DBSPType type = getJsonType(node, decoder);
        return new DBSPUuidLiteral(CalciteObject.EMPTY, type, value);
    }
}