package org.dbsp.sqlCompiler.ir.statement;

import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.DBSPFunction;
import org.dbsp.sqlCompiler.ir.IDBSPNode;
import org.dbsp.sqlCompiler.ir.NonCoreIR;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.IHasType;
import org.dbsp.util.IIndentStream;

import javax.annotation.Nullable;

/**
 * A function declaration.
 */
@NonCoreIR
public class DBSPFunctionItem extends DBSPItem {
    public final DBSPFunction function;

    public DBSPFunctionItem(DBSPFunction function) {
        this.function = function;
    }

    @Override
    public DBSPStatement deepCopy() {
        return new DBSPFunctionItem(this.function);
    }

    @Override
    public void accept(InnerVisitor visitor) {
        if (visitor.preorder(this).stop()) return;
        visitor.push(this);
        this.function.accept(visitor);
        visitor.pop(this);
        visitor.postorder(this);
    }

    @Override
    public boolean sameFields(IDBSPNode other) {
        DBSPFunctionItem o = other.as(DBSPFunctionItem.class);
        if (o == null)
            return false;
        return this.function == o.function;
    }

    @Override
    public IIndentStream toString(IIndentStream builder) {
        return builder.append(this.function);
    }
}
