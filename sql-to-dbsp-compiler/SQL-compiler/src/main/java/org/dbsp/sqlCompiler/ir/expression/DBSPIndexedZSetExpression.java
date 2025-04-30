package org.dbsp.sqlCompiler.ir.expression;

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.compiler.IConstructor;
import org.dbsp.sqlCompiler.compiler.backend.JsonDecoder;
import org.dbsp.sqlCompiler.compiler.errors.UnimplementedException;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.EquivalenceContext;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.ISameValue;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeIndexedZSet;
import org.dbsp.util.IIndentStream;

import javax.annotation.Nullable;

/** Represents an IndexedZSet described by its elements.
 * An IndexedZSet is a map from keys to tuples to integer weights.
 * Currently, we only support empty indexed zsets since we found no
 * need for other constants yet. */
public final class DBSPIndexedZSetExpression extends DBSPExpression
        implements IDBSPContainer, ISameValue, IConstructor {
    public final DBSPTypeIndexedZSet indexedZSetType;

    public DBSPIndexedZSetExpression(CalciteObject node, DBSPType type) {
        super(node, type);
        this.indexedZSetType = this.getType().to(DBSPTypeIndexedZSet.class);
    }

    public boolean isConstant() {
        return true;
    }

    @Override
    public DBSPExpression deepCopy() {
        return new DBSPIndexedZSetExpression(this.getNode(), this.type);
    }

    @Override
    public boolean equivalent(EquivalenceContext context, DBSPExpression other) {
        throw new UnimplementedException();
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
        return other.is(DBSPIndexedZSetExpression.class);
    }

    public boolean isEmpty() {
        return true;
    }

    @SuppressWarnings("SameReturnValue")
    public int size() {
        return 0;
    }

    public DBSPType getElementType() {
        return this.indexedZSetType.elementType;
    }

    @Override
    public boolean sameValue(@Nullable ISameValue o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DBSPIndexedZSetExpression that = (DBSPIndexedZSetExpression) o;
        return this.indexedZSetType.sameType(that.indexedZSetType);
    }

    @Override
    public IDBSPContainer add(DBSPExpression expression) {
        throw new UnimplementedException("Not yet implemented: IndexedZSet literals", expression);
    }

    @Override
    public IIndentStream toString(IIndentStream builder) {
        return builder.append("indexed_zset!()");
    }

    @SuppressWarnings("unused")
    public static DBSPIndexedZSetExpression fromJson(JsonNode node, JsonDecoder decoder) {
        DBSPType type = getJsonType(node, decoder);
        return new DBSPIndexedZSetExpression(CalciteObject.EMPTY, type);
    }
}
