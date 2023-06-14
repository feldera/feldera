package org.dbsp.sqlCompiler.compiler.visitors.inner;

import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.util.IWritesLogs;
import org.dbsp.util.Logger;

/**
 * Repeats another IRTransform until no changes happen anymore.
 */
public class InnerRepeat implements IWritesLogs, IRTransform {
    protected final IRTransform visitor;

    public InnerRepeat(IRTransform visitor) {
        this.visitor = visitor;
    }

    @Override
    public IDBSPInnerNode apply(IDBSPInnerNode node) {
        while (true) {
            IDBSPInnerNode result = this.visitor.apply(node);
            Logger.INSTANCE.belowLevel(this, 3)
                    .append("After ")
                    .append(this.visitor.toString())
                    .newline()
                    .appendSupplier(result::toString)
                    .newline();
            if (result == node)
                return result;
        }
    }
}
