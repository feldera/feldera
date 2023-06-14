package org.dbsp.sqlCompiler.ir.type.primitive;

import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPLiteral;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.util.Unimplemented;

import javax.annotation.Nullable;

/**
 * This type stands for the weights of an element in a collection.
 * The compiler decides how this is implemented.
 */
public class DBSPTypeWeight extends DBSPTypeBaseType {
    public final String name = "Weight";
    public static final DBSPTypeWeight INSTANCE = new DBSPTypeWeight();

    protected DBSPTypeWeight() {
        super(null, false);
    }

    @Override
    public void accept(InnerVisitor visitor) {
        if (visitor.preorder(this).stop()) return;
        visitor.push(this);
        visitor.pop(this);
        visitor.postorder(this);
    }

    @Override
    public boolean sameType(@Nullable DBSPType other) {
        return other != null && other.is(DBSPTypeWeight.class);
    }

    @Override
    public DBSPType setMayBeNull(boolean mayBeNull) {
        if (mayBeNull)
            throw new UnsupportedOperationException();
        return this;
    }

    @Override
    public String shortName() {
        return "Weight";
    }

    @Override
    public DBSPLiteral defaultValue() {
        throw new Unimplemented();
    }
}
