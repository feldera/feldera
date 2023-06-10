package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.compiler.IErrorReporter;

/**
 * Removes operators whose output is not used.
 */
public class DeadCode extends PassesVisitor {
    /**
     * Create a circuit visitor which removes unused operators.
     * @param reporter  Used to report errors.
     * @param warn      If true warn about unused inputs.
     */
    public DeadCode(IErrorReporter reporter, boolean warn) {
        super(reporter);
        FindDeadCode finder = new FindDeadCode(reporter, warn);
        super.add(finder);
        super.add(new RemoveOperatorsVisitor(reporter, finder.toKeep));
    }
}
