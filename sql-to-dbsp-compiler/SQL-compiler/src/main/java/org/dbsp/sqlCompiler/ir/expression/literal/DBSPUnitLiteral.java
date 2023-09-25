package org.dbsp.sqlCompiler.ir.expression.literal;

import org.dbsp.sqlCompiler.compiler.frontend.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.NonCoreIR;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.util.IIndentStream;

import javax.annotation.Nullable;

/**
 * A literal of value ().
 * Normally we don't need such literals, because we use DBSPTupleExpressions instead.
 * But the JIT backend cannot use expressions, it really needs a literal.
 */
@NonCoreIR
public class DBSPUnitLiteral extends DBSPLiteral {
    public DBSPUnitLiteral(CalciteObject node, DBSPType type, boolean isNull) {
        super(node, type, isNull);
    }

    @Override
    public DBSPExpression deepCopy() {
        return new DBSPUnitLiteral(this.getNode(), this.type, this.isNull);
    }

    @Override
    public boolean sameValue(@Nullable DBSPLiteral other) {
        if (other == null)
            return false;
        DBSPUnitLiteral ol = other.as(DBSPUnitLiteral.class);
        if (ol == null)
            return false;
        return this.isNull == ol.isNull;
    }

    @Override
    public DBSPLiteral getWithNullable(boolean mayBeNull) {
        return new DBSPUnitLiteral(this.getNode(), this.type.setMayBeNull(mayBeNull), this.isNull);
    }

    @Override
    public IIndentStream toString(IIndentStream builder) {
        return builder.append("()");
    }

    @Override
    public void accept(InnerVisitor visitor) {
        if (visitor.preorder(this).stop()) return;
        visitor.push(this);
        visitor.pop(this);
        visitor.postorder(this);
    }
}
