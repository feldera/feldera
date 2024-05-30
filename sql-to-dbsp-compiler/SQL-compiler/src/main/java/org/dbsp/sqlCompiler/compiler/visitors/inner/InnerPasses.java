package org.dbsp.sqlCompiler.compiler.visitors.inner;

import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.util.IWritesLogs;
import org.dbsp.util.Linq;
import org.dbsp.util.Logger;

import java.util.List;

/** Applies multiple other inner visitors in sequence. */
public class InnerPasses implements IWritesLogs, IRTransform {
    public final List<IRTransform> passes;

    public InnerPasses(IRTransform... passes) {
        this(Linq.list(passes));
    }

    public InnerPasses(List<IRTransform> passes) {
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
