package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.outer.keys.KeyAnalysis;

/** Removes LEFT JOINs whose right input contributes nothing. */
public class RemoveUselessLeftJoins extends Passes {
    public RemoveUselessLeftJoins(DBSPCompiler compiler) {
        super("RemoveUselessLeftJoins", compiler);
        KeyAnalysis keys = new KeyAnalysis(compiler);
        this.add(keys);
        this.add(new RemoveUselessLeftJoinsVisitor(compiler, keys));
    }
}
