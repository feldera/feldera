package org.dbsp.sqlCompiler.ir.expression;

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.compiler.backend.JsonDecoder;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.EquivalenceContext;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.IDBSPDeclaration;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.util.IIndentStream;
import org.dbsp.util.Utilities;

/** An expression that introduces a unique name
 * that exists only locally and is bound to an immutable value.
 * Can be implemented as a BlockExpression with a let statement.
 * In some languages this is written as
 * let name = initializer in consumer. */
public class DBSPLetExpression extends DBSPExpression implements IDBSPDeclaration {
    public final DBSPVariablePath variable;
    public final DBSPExpression initializer;
    public final DBSPExpression consumer;

    public DBSPLetExpression(DBSPVariablePath var, DBSPExpression initializer, DBSPExpression consumer) {
        super(initializer.getNode(), consumer.getType());
        Utilities.enforce(var.getType().sameType(initializer.getType()), "Variable has type " + var.getType() + " and initializer has type " + initializer.getType());
        this.variable = var;
        this.initializer = initializer;
        this.consumer = consumer;
    }

    @Override
    public DBSPExpression deepCopy() {
        return new DBSPLetExpression(
                this.variable.deepCopy().to(DBSPVariablePath.class),
                this.initializer.deepCopy(), this.consumer.deepCopy());
    }

    @Override
    public boolean equivalent(EquivalenceContext context, DBSPExpression other) {
        DBSPLetExpression otherExpression = other.as(DBSPLetExpression.class);
        if (otherExpression == null)
            return false;
        if (!context.equivalent(this.initializer, otherExpression.initializer))
            return false;
        context.leftDeclaration.newContext();
        context.rightDeclaration.newContext();
        context.leftDeclaration.substitute(this.variable.variable, this);
        context.rightDeclaration.substitute(otherExpression.variable.variable, otherExpression);
        context.leftToRight.substitute(this, otherExpression);
        return context.equivalent(this.consumer, otherExpression.consumer);
    }

    @Override
    public void accept(InnerVisitor visitor) {
        VisitDecision decision = visitor.preorder(this);
        if (decision.stop()) return;
        visitor.push(this);
        visitor.property("variable");
        this.variable.accept(visitor);
        visitor.property("initializer");
        this.initializer.accept(visitor);
        visitor.property("consumer");
        this.consumer.accept(visitor);
        visitor.pop(this);
        visitor.postorder(this);
    }

    @Override
    public boolean sameFields(IDBSPInnerNode other) {
        DBSPLetExpression let = other.as(DBSPLetExpression.class);
        if (let == null)
            return false;
        return this.variable == let.variable &&
                this.initializer == let.initializer &&
                this.consumer == let.consumer;
    }

    @Override
    public IIndentStream toString(IIndentStream builder) {
        return builder
                .append("{let ")
                .append(this.variable)
                .append(" = ")
                .append(this.initializer)
                .append(";")
                .newline()
                .append(this.consumer)
                .append("}");
    }

    @Override
    public String getName() {
        return this.variable.variable;
    }

    @SuppressWarnings("unused")
    public static DBSPLetExpression fromJson(JsonNode node, JsonDecoder decoder) {
        DBSPVariablePath variable = fromJsonInner(node, "variable", decoder, DBSPVariablePath.class);
        DBSPExpression initializer = fromJsonInner(node, "initializer", decoder, DBSPExpression.class);
        DBSPExpression consumer = fromJsonInner(node, "consumer", decoder, DBSPExpression.class);
        return new DBSPLetExpression(variable, initializer, consumer);
    }
}
