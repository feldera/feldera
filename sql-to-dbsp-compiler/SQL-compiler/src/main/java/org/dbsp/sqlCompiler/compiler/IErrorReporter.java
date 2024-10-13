package org.dbsp.sqlCompiler.compiler;

import org.dbsp.sqlCompiler.compiler.errors.SourcePositionRange;

/** Interface for reporting errors. */
public interface IErrorReporter {
    /** Report a problem.
     *
     * @param range     Source position where the problem occurred.
     * @param warning   If true, this is a warning.
     * @param continuation If true, this is the continuation of a multi-line message.
     * @param errorType Type of error.
     * @param message   Message to report.
     */
    void reportProblem(SourcePositionRange range, boolean warning, boolean continuation,
                       String errorType, String message);

    default void reportError(SourcePositionRange range, String errorType, String message) {
        reportProblem(range, false, false, errorType, message);
    }

    default void reportError(SourcePositionRange range, String errorType, String message, boolean continued) {
        reportProblem(range, false, continued, errorType, message);
    }

    default void reportWarning(SourcePositionRange range, String errorType, String message) {
        reportProblem(range, true, false, errorType, message);
    }

    /** True if any error (but not a warning) has been reported. */
    boolean hasErrors();
}
