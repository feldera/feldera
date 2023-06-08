package org.dbsp.sqlCompiler.ir.statement;

import org.dbsp.sqlCompiler.ir.InnerVisitor;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.IHasType;

import javax.annotation.Nullable;

/**
 * <a href="https://doc.rust-lang.org/reference/items/constant-items.html">Constant item</a>
 */
public class DBSPConstItem extends DBSPItem implements IHasType {
    public final String name;
    public final DBSPType type;
    @Nullable
    public final DBSPExpression expression;

    public DBSPConstItem(String name, DBSPType type, @Nullable DBSPExpression expression) {
        this.name = name;
        this.type = type;
        this.expression = expression;
    }

    @Override
    public void accept(InnerVisitor visitor) {
        if (!visitor.preorder(this)) return;
        visitor.push(this);
        this.type.accept(visitor);
        if (this.expression != null)
            this.expression.accept(visitor);
        visitor.pop(this);
        visitor.postorder(this);
    }

    public DBSPVariablePath getVariable() {
        return new DBSPVariablePath(this.name, this.type);
    }

    @Nullable
    @Override
    public DBSPType getType() {
        return this.type;
    }
}
