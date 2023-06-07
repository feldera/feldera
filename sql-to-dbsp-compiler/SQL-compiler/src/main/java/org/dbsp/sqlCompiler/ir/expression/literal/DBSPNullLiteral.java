package org.dbsp.sqlCompiler.ir.expression.literal;

import org.dbsp.sqlCompiler.ir.InnerVisitor;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeNull;
import org.dbsp.util.UnsupportedException;

import javax.annotation.Nullable;

/**
 * A literal with type NULL, the only value of this type.
 */
public class DBSPNullLiteral extends DBSPLiteral {
    public static final DBSPNullLiteral INSTANCE = new DBSPNullLiteral();
    @Nullable
    public final Object value = null;

    public DBSPNullLiteral(@Nullable Object node, DBSPType type, @Nullable Object value) {
        super(node,  type, true);
        if (value != null)
            throw new RuntimeException("Value must be null");
        if (!this.getNonVoidType().sameType(DBSPTypeNull.INSTANCE))
            throw new RuntimeException("Type must be NULL");
    }

    public DBSPNullLiteral() {
        this(null, DBSPTypeNull.INSTANCE, null);
    }

    @Override
    public void accept(InnerVisitor visitor) {
        if (!visitor.preorder(this)) return;
        visitor.push(this);
        visitor.pop(this);
        visitor.postorder(this);
    }

    @Override
    public boolean sameValue(@Nullable DBSPLiteral o) {
        if (this == o) return true;
        return o != null && getClass() == o.getClass();
    }

    @Override
    public DBSPLiteral getNonNullable() {
        throw new UnsupportedException(this);
    }
}
