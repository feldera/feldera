package org.dbsp.sqlCompiler.compiler;

import org.dbsp.sqlCompiler.compiler.errors.SourcePositionRange;

/** Simple error reporter which reports data to stderr. */
public class StderrErrorReporter implements IErrorReporter {
    int errorCount = 0;

    @Override
    public void setErrorContext(SourcePositionRange range) {}

    @Override
    public void reportProblem(SourcePositionRange range, boolean warning, boolean continuation,
                              String errorType, String message) {
        System.out.println("ERROR " + errorType + ": " + message);
        if (!warning)
            this.errorCount++;
    }

    @Override
    public boolean hasErrors() {
        return this.errorCount > 0;
    }
}
