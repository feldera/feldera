package org.dbsp.sqlCompiler.ir.expression;

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.compiler.backend.JsonDecoder;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.EquivalenceContext;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeVoid;
import org.dbsp.util.IIndentStream;

public class DBSPReturnExpression extends DBSPExpression {
    public final DBSPExpression argument;

    public DBSPReturnExpression(CalciteObject node, DBSPExpression argument) {
        super(node, DBSPTypeVoid.INSTANCE);
        this.argument = argument;
    }

    @Override
    public DBSPExpression deepCopy() {
        return new DBSPReturnExpression(this.getNode(), this.argument);
    }

    @Override
    public boolean equivalent(EquivalenceContext context, DBSPExpression other) {
        DBSPReturnExpression otherExpression = other.as(DBSPReturnExpression.class);
        if (otherExpression == null)
            return false;
        return context.equivalent(this.argument, otherExpression.argument);
    }

    @Override
    public void accept(InnerVisitor visitor) {
        VisitDecision decision = visitor.preorder(this);
        if (decision.stop()) return;
        visitor.push(this);
        visitor.property("type");
        this.type.accept(visitor);
        visitor.property("argument");
        this.argument.accept(visitor);
        visitor.pop(this);
        visitor.postorder(this);
    }

    @Override
    public boolean sameFields(IDBSPInnerNode other) {
        DBSPReturnExpression o = other.as(DBSPReturnExpression.class);
        if (o == null)
            return false;
        return this.argument == o.argument;
    }

    @Override
    public IIndentStream toString(IIndentStream builder) {
        return builder.append("return ")
                .append(this.argument)
                .append(";");
    }

    @SuppressWarnings("unused")
    public static DBSPReturnExpression fromJson(JsonNode node, JsonDecoder decoder) {
        DBSPExpression argument = fromJsonInner(node, "argument", decoder, DBSPExpression.class);
        return new DBSPReturnExpression(CalciteObject.EMPTY, argument);
    }
}
