package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.compiler.DBSPCompiler;

/** Removes operators whose output is not used. */
public class DeadCode extends Passes {
    /**
     * Create a circuit visitor which removes unused operators.
     * @param keepAllSources  If true keep source operators that have no users.
     * @param warn      If true warn about unused inputs.
     */
    public DeadCode(DBSPCompiler compiler, boolean keepAllSources, boolean warn) {
        super("DeadCode", compiler);
        FindDeadCode finder = new FindDeadCode(compiler, keepAllSources, warn);
        super.add(finder);
        super.add(new RemoveOperatorsVisitor(compiler, finder.toKeep));
    }
}
