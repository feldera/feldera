package org.dbsp.sqlCompiler.compiler.visitors.inner;

import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.compiler.IErrorReporter;
import org.dbsp.util.IWritesLogs;
import org.dbsp.util.Linq;
import org.dbsp.util.Logger;

import java.util.List;

/**
 * Applies multiple other inner visitors in sequence.
 */
public class InnerPassesVisitor implements IWritesLogs, IRTransform {
    final IErrorReporter errorReporter;
    public final List<IRTransform> passes;

    public InnerPassesVisitor(IErrorReporter reporter, IRTransform... passes) {
        this(reporter, Linq.list(passes));
    }

    public InnerPassesVisitor(IErrorReporter reporter, List<IRTransform> passes) {
        this.errorReporter = reporter;
        this.passes = passes;
    }

    public void add(InnerRewriteVisitor pass) {
        this.passes.add(pass);
    }

    @Override
    public IDBSPInnerNode apply(IDBSPInnerNode node) {
        for (IRTransform pass: this.passes) {
            Logger.INSTANCE.belowLevel(this, 1)
                    .append("Executing ")
                    .append(pass.toString())
                    .newline();
            node = pass.apply(node);
            Logger.INSTANCE.belowLevel(this, 3)
                    .append("After ")
                    .append(pass.toString())
                    .newline()
                    .appendSupplier(node::toString)
                    .newline();
        }
        return node;
    }

    @Override
    public String toString() {
        return super.toString() + this.passes;
    }
}
