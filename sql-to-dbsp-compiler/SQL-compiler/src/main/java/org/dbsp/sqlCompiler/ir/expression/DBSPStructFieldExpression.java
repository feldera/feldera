package org.dbsp.sqlCompiler.ir.expression;

import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.EquivalenceContext;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.IDBSPNode;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeAny;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeStruct;
import org.dbsp.util.IIndentStream;

/** Struct field reference expression */
public class DBSPStructFieldExpression extends DBSPExpression {
    public final DBSPExpression expression;
    public final String fieldName;

    protected DBSPStructFieldExpression(CalciteObject node, DBSPExpression expression, String fieldName, DBSPType type) {
        super(node, type);
        this.expression = expression;
        this.fieldName = fieldName;
    }

    protected DBSPStructFieldExpression(DBSPExpression expression, String fieldName, DBSPType type) {
        this(CalciteObject.EMPTY, expression, fieldName, type);
    }

    static DBSPType getFieldType(DBSPType type, String fieldName) {
        if (type.is(DBSPTypeAny.class))
            return type;
        DBSPTypeStruct struct = type.to(DBSPTypeStruct.class);
        return struct.getFieldType(fieldName);
    }

    public DBSPStructFieldExpression(CalciteObject node, DBSPExpression expression, String fieldName) {
        this(node, expression, fieldName, getFieldType(expression.getType(), fieldName));
    }

    DBSPStructFieldExpression(DBSPExpression expression, String fieldName) {
        this(CalciteObject.EMPTY, expression, fieldName);
    }

    @Override
    public void accept(InnerVisitor visitor) {
        VisitDecision decision = visitor.preorder(this);
        if (decision.stop()) return;
        visitor.push(this);
        this.type.accept(visitor);
        this.expression.accept(visitor);
        visitor.pop(this);
        visitor.postorder(this);
    }

    @Override
    public boolean sameFields(IDBSPNode other) {
        DBSPStructFieldExpression o = other.as(DBSPStructFieldExpression.class);
        if (o == null)
            return false;
        return this.expression == o.expression &&
                this.fieldName.equals(o.fieldName) &&
                this.hasSameType(o);
    }

    @Override
    public IIndentStream toString(IIndentStream builder) {
        return builder.append("(")
                .append(this.expression)
                .append(".")
                .append(this.fieldName)
                .append(")");
    }

    @Override
    public DBSPExpression deepCopy() {
        return new DBSPStructFieldExpression(this.expression.deepCopy(), this.fieldName);
    }

    @Override
    public boolean equivalent(EquivalenceContext context, DBSPExpression other) {
        DBSPStructFieldExpression otherExpression = other.as(DBSPStructFieldExpression.class);
        if (otherExpression == null)
            return false;
        return this.fieldName.equals(otherExpression.fieldName) &&
                context.equivalent(this.expression, otherExpression.expression);
    }
}
