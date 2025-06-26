package org.dbsp.sqlCompiler.ir.type;

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.compiler.backend.JsonDecoder;
import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeBaseType;
import org.dbsp.util.Utilities;

/** Abstract type representing an interned string representation.  Always nullable. */
public class DBSPTypeInterned extends DBSPTypeBaseType {
    public static final DBSPTypeInterned INSTANCE = new DBSPTypeInterned(true);

    protected DBSPTypeInterned(boolean mayBeNull) {
        super(CalciteObject.EMPTY, DBSPTypeCode.INTERNED_STRING, mayBeNull);
    }

    @Override
    public boolean sameType(DBSPType other) {
        if (!super.sameNullability(other))
            return false;
        return other.is(DBSPTypeInterned.class);
    }

    @Override
    public DBSPType withMayBeNull(boolean mayBeNull) {
        Utilities.enforce(mayBeNull);
        return INSTANCE;
    }

    @Override
    public boolean hasCopy() {
        return false;
    }

    @Override
    public DBSPExpression defaultValue() {
        throw new InternalCompilerError("Default value of interned string");
    }

    @Override
    public void accept(InnerVisitor visitor) {
        VisitDecision decision = visitor.preorder(this);
        if (decision.stop()) return;
        visitor.push(this);
        visitor.pop(this);
        visitor.postorder(this);
    }

    @SuppressWarnings("unused")
    public static DBSPTypeInterned fromJson(JsonNode node, JsonDecoder decoder) {
        return INSTANCE;
    }
}
