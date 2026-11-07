package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.outer.merkle.CheckPersistentIds;

/** Replaces operators with the primitives that the Gen-2 engine runs, removes the operators
 * that the primitives leave without consumers, merges the primitives that compute the same
 * stream, names the streams again, and checks that the persistent ids are still unique. */
public class Gen2Lowering extends Passes {
    public Gen2Lowering(DBSPCompiler compiler) {
        super("Gen2Lowering", compiler);
        this.add(new LowerToPrimitives(compiler));
        this.add(new DeadCode(compiler, true));
        this.add(new CSE(compiler));
        this.add(new CompactNames(compiler));
        this.add(new CheckPersistentIds(compiler, false));
    }
}
