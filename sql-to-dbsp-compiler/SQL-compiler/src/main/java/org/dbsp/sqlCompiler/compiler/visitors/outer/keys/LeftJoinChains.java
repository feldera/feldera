package org.dbsp.sqlCompiler.compiler.visitors.outer.keys;

import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.outer.Conditional;
import org.dbsp.sqlCompiler.compiler.visitors.outer.Passes;
import org.dbsp.util.Logger;

/** Defers the columns a chain of LEFT JOINs only passes along: {@link LeftJoinChainsVisitor}
 * finds the chains and measures what each carries, and {@link DeferCarriedColumns} rewrites
 * the ones worth it. */
public class LeftJoinChains extends Passes {
    public final LeftJoinChainsVisitor chains;

    public LeftJoinChains(DBSPCompiler compiler) {
        super("LeftJoinChains", compiler);
        KeyAnalysis keys = new KeyAnalysis(compiler);
        this.chains = new LeftJoinChainsVisitor(compiler, keys);
        this.add(keys);
        this.add(this.chains);
        this.add(new DeferCarriedColumns(compiler, this.chains.chainEnd));
    }
}
