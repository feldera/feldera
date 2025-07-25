package org.dbsp.sqlCompiler.ir.expression;

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.compiler.IConstructor;
import org.dbsp.sqlCompiler.compiler.backend.JsonDecoder;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.EquivalenceContext;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.ISameValue;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeVariant;
import org.dbsp.util.IIndentStream;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;

public class DBSPVariantExpression extends DBSPExpression implements ISameValue, IConstructor {
    // this is usually a literal; if null, this is a NULL VARIANT literal.
    @Nullable public final DBSPExpression value;
    // If true this VARIANT has the value NULL inside.
    // But it's a VARIANT object, not a NULL
    public final boolean isSqlNull;

    public DBSPVariantExpression(@Nullable DBSPExpression value, DBSPType type) {
        super(CalciteObject.EMPTY, type);
        this.value = value;
        this.isSqlNull = false;
        Utilities.enforce(type.is(DBSPTypeVariant.class));
        Utilities.enforce(value == null || value.is(ISameValue.class));
    }

    public DBSPVariantExpression(boolean mayBeNull) {
        super(CalciteObject.EMPTY, DBSPTypeVariant.create(mayBeNull));
        this.isSqlNull = true;
        this.value = null;
    }

    public DBSPVariantExpression(@Nullable DBSPExpression value, boolean mayBeNull) {
        this(value, DBSPTypeVariant.create(mayBeNull));
    }

    public DBSPVariantExpression(@Nullable DBSPExpression value) {
        this(value, DBSPTypeVariant.create(false));
    }

    /** The value representing the SQL NULL */
    public static DBSPExpression sqlNull(boolean mayBeNull) {
        return new DBSPVariantExpression(mayBeNull);
    }

    public boolean isConstant() {
        return this.isSqlNull || this.value == null || this.value.isCompileTimeConstant();
    }

    @Override
    public void accept(InnerVisitor visitor) {
        VisitDecision decision = visitor.preorder(this);
        if (decision.stop()) return;
        visitor.property("type");
        this.type.accept(visitor);
        visitor.push(this);
        if (this.value != null) {
            visitor.property("value");
            this.value.accept(visitor);
        }
        visitor.pop(this);
        visitor.postorder(this);
    }

    @Override
    public boolean sameFields(IDBSPInnerNode other) {
        DBSPVariantExpression otherVariant = other.as(DBSPVariantExpression.class);
        if (otherVariant == null)
            return false;
        if (this.isSqlNull)
            return otherVariant.isSqlNull;
        return this.value == otherVariant.value;
    }

    @Override
    public boolean sameValue(@Nullable ISameValue o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DBSPVariantExpression that = (DBSPVariantExpression) o;
        if (this.isSqlNull)
            return that.isSqlNull && this.value == that.value;
        if (this.value == null)
            return that.value == null;
        if (that.value == null)
            return false;
        return this.value.to(ISameValue.class).sameValue(that.value.to(ISameValue.class));
    }

    @Override
    public DBSPExpression deepCopy() {
        if (this.isSqlNull)
            return this;
        return new DBSPVariantExpression(this.value, this.getType().mayBeNull);
    }

    @Override
    public boolean equivalent(EquivalenceContext context, DBSPExpression other) {
        DBSPVariantExpression otherVariant = other.as(DBSPVariantExpression.class);
        if (otherVariant == null)
            return false;
        if (this.isSqlNull)
            return otherVariant.isSqlNull;
        return context.equivalent(this.value, otherVariant.value);
    }

    @Override
    public IIndentStream toString(IIndentStream builder) {
        if (this.value == null)
            return builder.append("(VARIANT)null");
        if (this.isSqlNull)
            return builder.append("VARIANT(NULL)");
        return builder.append("VARIANT(")
                .append(this.value)
                .append(")");
    }

    @SuppressWarnings("unused")
    public static DBSPVariantExpression fromJson(JsonNode node, JsonDecoder decoder) {
        boolean isSqlNull = Utilities.getBooleanProperty(node, "isSqlNull");
        DBSPType type = getJsonType(node, decoder);
        if (isSqlNull)
            return new DBSPVariantExpression(type.mayBeNull);
        DBSPExpression value = null;
        if (node.has("value")) {
            value = fromJsonInner(node, "value", decoder, DBSPExpression.class);
        }
        return new DBSPVariantExpression(value, type);
    }
}
