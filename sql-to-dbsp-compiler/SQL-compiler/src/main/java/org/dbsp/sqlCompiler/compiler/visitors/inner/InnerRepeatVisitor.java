package org.dbsp.sqlCompiler.compiler.visitors.inner;

import org.dbsp.sqlCompiler.circuit.IDBSPInnerNode;
import org.dbsp.sqlCompiler.compiler.IErrorReporter;
import org.dbsp.util.IWritesLogs;
import org.dbsp.util.Logger;

import java.util.function.Supplier;

/**
 * Repeats another pass until no changes happen anymore.
 */
public class InnerRepeatVisitor extends InnerRewriteVisitor implements IWritesLogs {
    protected final InnerRewriteVisitor visitor;

    public InnerRepeatVisitor(IErrorReporter reporter, InnerRewriteVisitor visitor) {
        super(reporter);
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
                    .append((Supplier<String>) result::toString)
                    .newline();
            if (result == node)
                return result;
        }
    }
}
