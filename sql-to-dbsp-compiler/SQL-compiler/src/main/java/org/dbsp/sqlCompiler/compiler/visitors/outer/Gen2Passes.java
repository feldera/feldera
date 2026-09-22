package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.compiler.DBSPCompiler;

/** Rewrites the optimized circuit into the form the Gen-2 engine reads.  The Gen-2 engine
 * consumes the circuit IR instead of Rust code, so these passes remove the constructs that only
 * the Rust backend needs. */
public class Gen2Passes extends Passes {
    public Gen2Passes(DBSPCompiler compiler) {
        super("Gen2", compiler);
        this.add(new RemoveTypedBox(compiler));
    }
}
