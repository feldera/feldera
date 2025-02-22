package org.dbsp.sqlCompiler.ir.type.user;

import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPArrayExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.ICollectionType;

import static org.dbsp.sqlCompiler.ir.type.DBSPTypeCode.ARRAY;

/** Representation for a SQL ARRAY type. */
public class DBSPTypeArray extends DBSPTypeUser implements ICollectionType {
    public DBSPTypeArray(DBSPType vectorElementType, boolean mayBeNull) {
        super(vectorElementType.getNode(), ARRAY, "Array", mayBeNull, vectorElementType);
    }

    public DBSPType getElementType() {
        return this.getTypeArg(0);
    }

    public DBSPTypeVec innerType() {
        return new DBSPTypeVec(this.getElementType(), this.mayBeNull);
    }

    @Override
    public DBSPType deref() {
        return this.innerType();
    }

    @Override
    public DBSPExpression defaultValue() {
        if (this.mayBeNull)
            return this.none();
        return new DBSPArrayExpression(this, false);
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
    public DBSPType withMayBeNull(boolean mayBeNull) {
        if (mayBeNull == this.mayBeNull)
            return this;
        return new DBSPTypeArray(this.getElementType(), mayBeNull);
    }

    // sameType and hashCode inherited from TypeUser.
}
