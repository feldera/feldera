package org.dbsp.sqlCompiler.ir.expression.literal;

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.compiler.backend.JsonDecoder;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.ir.ISameValue;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeVoid;
import org.dbsp.util.IIndentStream;

/** In literal the () value has type void, but that's confusing.
 * There should exist an empty tuple.  This is a literal of type void. */
public class DBSPVoidLiteral extends DBSPLiteral {
    public static final DBSPVoidLiteral INSTANCE = new DBSPVoidLiteral();

    DBSPVoidLiteral() {
        super(CalciteObject.EMPTY, DBSPTypeVoid.INSTANCE, false);
    }

    @Override
    public DBSPLiteral getWithNullable(boolean mayBeNull) {
        if (mayBeNull)
            throw new RuntimeException("nullable void literal does not exist");
        return this;
    }

    @Override
    public String toSqlString() {
        return "";
    }

    @Override
    public boolean sameValue(ISameValue expression) {
        return expression.is(DBSPVoidLiteral.class);
    }

    @Override
    public DBSPExpression deepCopy() {
        return this;
    }

    @Override
    public IIndentStream toString(IIndentStream builder) {
        return builder.append("()");
    }

    @SuppressWarnings("unused")
    public static DBSPVoidLiteral fromJson(JsonNode node, JsonDecoder decoder) {
        return INSTANCE;
    }

}
