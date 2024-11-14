package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.compiler.IErrorReporter;

/** Removes operators whose output is not used. */
public class DeadCode extends Passes {
    /**
     * Create a circuit visitor which removes unused operators.
     * @param reporter  Used to report errors.
     * @param keepAllSources  If true keep source operators that have no users.
     * @param warn      If true warn about unused inputs.
     */
    public DeadCode(IErrorReporter reporter, boolean keepAllSources, boolean warn) {
        super("DeadCode", reporter);
        FindDeadCode finder = new FindDeadCode(reporter, keepAllSources, warn);
        super.add(finder);
        super.add(new RemoveOperatorsVisitor(reporter, finder.toKeep));
    }
}
