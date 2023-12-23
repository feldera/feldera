package org.dbsp.sqlCompiler.compiler;

import org.dbsp.sqlCompiler.compiler.errors.SourcePositionRange;

/**
 * Interface that knows how to report errors.
 */
public interface IErrorReporter {
    void reportProblem(SourcePositionRange range, boolean warning,
                       String errorType, String message);

    default void reportError(SourcePositionRange range, String errorType, String message) {
        reportProblem(range, false, errorType, message);
    }

    default void reportWarning(SourcePositionRange range, String errorType, String message) {
        reportProblem(range, true, errorType, message);
    }

    /**
     * True if any error (but not a warning) has been reported.
     */
    boolean hasErrors();
}
