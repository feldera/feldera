package org.dbsp.sqlCompiler.compiler.visitors.inner;

import org.dbsp.sqlCompiler.circuit.IDBSPInnerNode;
import org.dbsp.sqlCompiler.compiler.IErrorReporter;
import org.dbsp.util.IWritesLogs;
import org.dbsp.util.Linq;
import org.dbsp.util.Logger;

import java.util.List;
import java.util.function.Supplier;

/**
 * Applies multiple other inner visitors in sequence.
 */
public class InnerPassesVisitor extends InnerRewriteVisitor implements IWritesLogs {
    public final List<InnerRewriteVisitor> passes;

    public InnerPassesVisitor(IErrorReporter reporter, InnerRewriteVisitor... passes) {
        this(reporter, Linq.list(passes));
    }

    public InnerPassesVisitor(IErrorReporter reporter, List<InnerRewriteVisitor> passes) {
        super(reporter);
        this.passes = passes;
    }

    public void add(InnerRewriteVisitor pass) {
        this.passes.add(pass);
    }

    @Override
    public IDBSPInnerNode apply(IDBSPInnerNode node) {
        for (InnerRewriteVisitor pass: this.passes) {
            Logger.INSTANCE.belowLevel(this, 1)
                    .append("Executing ")
                    .append(pass.toString())
                    .newline();
            node = pass.apply(node);
            Logger.INSTANCE.belowLevel(this, 3)
                    .append("After ")
                    .append(pass.toString())
                    .newline()
                    .append((Supplier<String>) node::toString)
                    .newline();
        }
        return node;
    }

    @Override
    public String toString() {
        return super.toString() + this.passes;
    }
}
