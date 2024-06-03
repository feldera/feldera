package org.dbsp.sqlCompiler.ir.expression.literal;

import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.errors.UnimplementedException;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.IDBSPContainer;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeIndexedZSet;
import org.dbsp.util.IIndentStream;

import javax.annotation.Nullable;

/**
 * Represents a (constant) IndexedZSet described by its elements.
 * An IndexedZSet is a map from keys to tuples to integer weights.
 * Currently, we only support empty indexed zsets since we found no
 * need for other constants yet.
 */
public final class DBSPIndexedZSetLiteral extends DBSPLiteral implements IDBSPContainer {
    public final DBSPTypeIndexedZSet indexedZSetType;

    public DBSPIndexedZSetLiteral(CalciteObject node, DBSPType type) {
        super(node, type, false);
        this.indexedZSetType = this.getType().to(DBSPTypeIndexedZSet.class);
    }

    @Override
    public DBSPExpression deepCopy() {
        return new DBSPIndexedZSetLiteral(this.getNode(), this.type);
    }

    @Override
    public void accept(InnerVisitor visitor) {
        VisitDecision decision = visitor.preorder(this);
        if (decision.stop()) return;
        visitor.push(this);
        visitor.pop(this);
        visitor.postorder(this);
    }

    @Override
    public DBSPLiteral getWithNullable(boolean mayBeNull) {
        if (mayBeNull)
            throw new InternalCompilerError("Null indexed zset");
        return this;
    }

    @SuppressWarnings("SameReturnValue")
    public int size() {
        return 0;
    }

    public DBSPType getElementType() {
        return this.indexedZSetType.elementType;
    }

    @Override
    public boolean sameValue(@Nullable DBSPLiteral o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DBSPIndexedZSetLiteral that = (DBSPIndexedZSetLiteral) o;
        return this.indexedZSetType.sameType(that.indexedZSetType);
    }

    @Override
    public IDBSPContainer add(DBSPExpression expression) {
        throw new UnimplementedException(expression);
    }

    @Override
    public IIndentStream toString(IIndentStream builder) {
        return builder.append("indexed_zset!()");
    }
}
