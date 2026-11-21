package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.compiler.DBSPCompiler;

/** Removes operators whose output is not used. */
public class DeadCode extends Passes {
    public final boolean keepAllSources;

    /** Create a circuit visitor which removes unused operators.
     * @param keepAllSources  If true keep source operators that have no users. */
    public DeadCode(DBSPCompiler compiler, boolean keepAllSources) {
        super("DeadCode", compiler);
        this.keepAllSources = keepAllSources;
        FindDeadCode finder = new FindDeadCode(compiler, keepAllSources);
        super.add(finder);
        super.add(new RemoveOperators(compiler, finder.toKeep));
    }
}
