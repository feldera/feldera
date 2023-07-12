package org.dbsp.sqlCompiler.ir.expression.literal;

import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.frontend.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeNull;
import org.dbsp.util.IIndentStream;
import org.dbsp.sqlCompiler.compiler.errors.UnsupportedException;

import javax.annotation.Nullable;

/**
 * A literal with type NULL, the only value of this type.
 */
public class DBSPNullLiteral extends DBSPLiteral {
    public static final DBSPNullLiteral INSTANCE = new DBSPNullLiteral();

    public DBSPNullLiteral(CalciteObject node, DBSPType type, @Nullable Object value) {
        super(node,  type, true);
        if (value != null)
            throw new InternalCompilerError("Value must be null", this);
        if (!this.getType().sameType(DBSPTypeNull.INSTANCE))
            throw new InternalCompilerError("Type must be NULL", this);
    }

    public DBSPNullLiteral() {
        this(CalciteObject.EMPTY, DBSPTypeNull.INSTANCE, null);
    }

    @Override
    public void accept(InnerVisitor visitor) {
        if (visitor.preorder(this).stop()) return;
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
    public DBSPLiteral getWithNullable(boolean mayBeNull) {
        if (!mayBeNull)
            throw new UnsupportedException(this.getNode());
        return this;
    }

    @Override
    public IIndentStream toString(IIndentStream builder) {
        return builder.append("null");
    }
}
