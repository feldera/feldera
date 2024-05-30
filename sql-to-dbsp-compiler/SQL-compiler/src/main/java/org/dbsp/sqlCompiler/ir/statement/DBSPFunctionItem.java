package org.dbsp.sqlCompiler.ir.statement;

import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.EquivalenceContext;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.DBSPFunction;
import org.dbsp.sqlCompiler.ir.IDBSPNode;
import org.dbsp.sqlCompiler.ir.NonCoreIR;
import org.dbsp.util.IIndentStream;

/** A function declaration. */
@NonCoreIR
public final class DBSPFunctionItem extends DBSPItem {
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
        VisitDecision decision = visitor.preorder(this);
        if (decision.stop()) return;
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

    @Override
    public EquivalenceResult equivalent(EquivalenceContext context, DBSPStatement other) {
        // Since this is NonCoreIR we leave this for later
        return new EquivalenceResult(false, context);
    }
}
