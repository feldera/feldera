package org.dbsp.sqlCompiler.ir.type.primitive;

import org.dbsp.sqlCompiler.ir.InnerVisitor;
import org.dbsp.sqlCompiler.ir.type.DBSPType;

/**
 * This type stands for the weights of an element in a collection.
 * The compiler decides how this is implemented.
 */
public class DBSPTypeWeight extends DBSPType {
    public final String name = "Weight";
    public static final DBSPTypeWeight INSTANCE = new DBSPTypeWeight();

    protected DBSPTypeWeight() {
        super(null, false);
    }

    @Override
    public void accept(InnerVisitor visitor) {
        if (!visitor.preorder(this)) return;
        visitor.push(this);
        visitor.pop(this);
        visitor.postorder(this);
    }

    @Override
    public DBSPType setMayBeNull(boolean mayBeNull) {
        if (mayBeNull)
            throw new UnsupportedOperationException();
        return this;
    }
}
