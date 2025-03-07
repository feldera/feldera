package org.dbsp.sqlCompiler.ir.statement;

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.compiler.backend.JsonDecoder;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.EquivalenceContext;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.NonCoreIR;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.IHasType;
import org.dbsp.util.IIndentStream;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;

import static org.dbsp.sqlCompiler.ir.expression.DBSPExpression.getJsonType;

/** <a href="https://doc.rust-lang.org/reference/items/constant-items.html">Constant item</a> */
@NonCoreIR
public final class DBSPConstItem extends DBSPItem implements IHasType {
    public final String name;
    public final DBSPType type;
    @Nullable
    public final DBSPExpression expression;

    public DBSPConstItem(String name, DBSPType type, @Nullable DBSPExpression expression) {
        this.name = name;
        this.type = type;
        this.expression = expression;
    }

    @Override
    public DBSPStatement deepCopy() {
        return new DBSPConstItem(this.name, this.type, DBSPExpression.nullableDeepCopy(this.expression));
    }

    @Override
    public void accept(InnerVisitor visitor) {
        VisitDecision decision = visitor.preorder(this);
        if (decision.stop()) return;
        visitor.push(this);
        visitor.property("type");
        this.type.accept(visitor);
        if (this.expression != null) {
            visitor.property("expression");
            this.expression.accept(visitor);
        }
        visitor.pop(this);
        visitor.postorder(this);
    }

    public DBSPVariablePath getVariable() {
        return new DBSPVariablePath(this.name, this.type);
    }

    @Override
    public DBSPType getType() {
        return this.type;
    }

    @Override
    public boolean sameFields(IDBSPInnerNode other) {
        DBSPConstItem o = other.as(DBSPConstItem.class);
        if (o == null)
            return false;
        return this.name.equals(o.name) &&
                this.type.sameType(o.type) &&
                this.expression == o.expression;
    }

    @Override
    public IIndentStream toString(IIndentStream builder) {
        builder.append("const ")
                .append(this.name)
                .append(": ")
                .append(this.type);
        if (this.expression != null)
            builder.append(" = ")
                    .append(this.expression);
        return builder;
    }

    @Override
    public EquivalenceResult equivalent(EquivalenceContext context, DBSPStatement other) {
        // Since this is NonCoreIR we leave this for later
        return new EquivalenceResult(false, context);
    }

    @SuppressWarnings("unused")
    public static DBSPConstItem fromJson(JsonNode node, JsonDecoder decoder) {
        String name = Utilities.getStringProperty(node, "name");
        DBSPType type = getJsonType(node, decoder);
        DBSPExpression expression = fromJsonInner(node, "expression", decoder, DBSPExpression.class);
        return new DBSPConstItem(name, type, expression);
    }

    @Override
    public String getName() {
        return this.name;
    }
}
