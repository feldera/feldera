package org.dbsp.sqlCompiler.ir.expression;

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.compiler.backend.JsonDecoder;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.EquivalenceContext;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.util.IIndentStream;
import org.dbsp.util.Utilities;

/** A Fail expression is the equivalent of panic(); it can return any type. */
public class DBSPFailExpression extends DBSPExpression {
    public final String message;

    public DBSPFailExpression(CalciteObject node, DBSPType type, String message) {
        super(node, type);
        this.message = message;
    }

    @Override
    public DBSPExpression deepCopy() {
        return new DBSPFailExpression(this.node, this.type, this.message);
    }

    @Override
    public boolean equivalent(EquivalenceContext context, DBSPExpression other) {
        DBSPFailExpression o = other.as(DBSPFailExpression.class);
        return o != null;
    }

    @Override
    public void accept(InnerVisitor visitor) {
        VisitDecision decision = visitor.preorder(this);
        if (decision.stop()) return;
        visitor.push(this);
        visitor.property("type");
        this.type.accept(visitor);
        visitor.pop(this);
        visitor.postorder(this);
    }

    @Override
    public boolean sameFields(IDBSPInnerNode other) {
        DBSPFailExpression o = other.as(DBSPFailExpression.class);
        if (o == null)
            return false;
        return this.message.equals(o.message);
    }

    @Override
    public IIndentStream toString(IIndentStream builder) {
        return builder.append("panic!(")
                .append(Utilities.doubleQuote(this.message, false))
                .append(")");
    }

    @SuppressWarnings("unused")
    public static DBSPFailExpression fromJson(JsonNode node, JsonDecoder decoder) {
        DBSPType type = getJsonType(node, decoder);
        String message = Utilities.getStringProperty(node, "message");
        return new DBSPFailExpression(CalciteObject.EMPTY, type, message);
    }
}
