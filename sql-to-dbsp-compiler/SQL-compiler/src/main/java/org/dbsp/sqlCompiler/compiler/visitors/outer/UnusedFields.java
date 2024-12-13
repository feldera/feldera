package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.inner.unusedFields.RemoveUnusedFields;

/** Find and remove unused fields. */
public class UnusedFields extends Repeat {
    public UnusedFields(DBSPCompiler compiler) {
        super(compiler, new OnePass(compiler));
    }

    static class OnePass extends Passes {
        OnePass(DBSPCompiler compiler) {
            super("UnusedFields", compiler);
            this.add(new RemoveUnusedFields(compiler));
            this.add(new DeadCode(compiler, true, false));
            this.add(new OptimizeWithGraph(compiler, g -> new OptimizeMaps(compiler, true, g)));
        }
    }
}
