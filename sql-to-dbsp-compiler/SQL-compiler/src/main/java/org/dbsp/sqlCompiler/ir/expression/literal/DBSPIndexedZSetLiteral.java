package org.dbsp.sqlCompiler.ir.expression.literal;

import org.dbsp.sqlCompiler.ir.InnerVisitor;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.IDBSPContainer;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeIndexedZSet;
import org.dbsp.util.Unimplemented;

import javax.annotation.Nullable;

/**
 * Represents a (constant) IndexedZSet described by its elements.
 * An IndexedZSet is a map from keys to tuples to integer weights.
 * Currently, we only support empty indexed zsets since we found no
 * need for other constants yet.
 */
public class DBSPIndexedZSetLiteral extends DBSPLiteral implements IDBSPContainer {
    public final DBSPTypeIndexedZSet indexedZSetType;

    public DBSPIndexedZSetLiteral(@Nullable Object node, DBSPType type) {
        super(node, type, false);
        this.indexedZSetType = this.getNonVoidType().to(DBSPTypeIndexedZSet.class);
    }

    @Override
    public void accept(InnerVisitor visitor) {
        if (!visitor.preorder(this)) return;
        visitor.push(this);
        visitor.pop(this);
        visitor.postorder(this);
    }

    @Override
    public DBSPLiteral getNonNullable() {
        return this;
    }

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
        if (!this.indexedZSetType.sameType(that.indexedZSetType)) return false;
        return true;
    }

    @Override
    public void add(DBSPExpression expression) {
        throw new Unimplemented();
    }
}
