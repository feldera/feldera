package org.dbsp.sqlCompiler.ir.type.user;

import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPOpcode;
import org.dbsp.sqlCompiler.ir.expression.DBSPUnaryExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.IsBoundedType;

import static org.dbsp.sqlCompiler.ir.type.DBSPTypeCode.TYPEDBOX;
import static org.dbsp.sqlCompiler.ir.type.DBSPTypeCode.USER;

public class DBSPTypeTypedBox extends DBSPTypeUser implements IsBoundedType {
    static DBSPType[] makeTypeArgs(DBSPType arg, boolean typed) {
        DBSPType[] result = new DBSPType[2];
        result[0] = arg;
        if (typed) {
            result[1] = new DBSPTypeUser(arg.getNode(), USER, "DynDataTyped", false, arg);
        } else {
            result[1] = new DBSPTypeUser(arg.getNode(), USER, "DynData", false);
        }
        return result;
    }

    /** @param typed If true the type is TypedBox<T, DynData>,
     *              else it is TypedBox<T, DynDataTyped<T>> */
    public DBSPTypeTypedBox(DBSPType argType, boolean typed) {
        super(argType.getNode(), TYPEDBOX, "TypedBox", false,
                makeTypeArgs(argType, typed));
    }

    @Override
    public void accept(InnerVisitor visitor) {
        VisitDecision decision = visitor.preorder(this);
        if (decision.stop()) return;
        visitor.push(this);
        visitor.startArrayProperty("typeArgs");
        int index = 0;
        for (DBSPType type: this.typeArgs) {
            visitor.propertyIndex(index);
            index++;
            type.accept(visitor);
        }
        visitor.endArrayProperty("typeArgs");
        visitor.pop(this);
        visitor.postorder(this);
    }

    @Override
    public DBSPExpression getMaxValue() {
        DBSPExpression max = this.typeArgs[0].to(IsBoundedType.class).getMaxValue();
        return new DBSPUnaryExpression(this.getNode(), this, DBSPOpcode.TYPEDBOX, max);
    }

    @Override
    public DBSPExpression getMinValue() {
        DBSPExpression min = this.typeArgs[0].to(IsBoundedType.class).getMinValue();
        return new DBSPUnaryExpression(this.getNode(), this, DBSPOpcode.TYPEDBOX, min);
    }

    public static DBSPExpression wrapTypedBox(DBSPExpression expression, boolean typed) {
        DBSPType type = new DBSPTypeTypedBox(expression.getType(), typed);
        return new DBSPUnaryExpression(expression.getNode(), type, DBSPOpcode.TYPEDBOX, expression);
    }

    // sameType and hashCode inherited from TypeUser.
}
