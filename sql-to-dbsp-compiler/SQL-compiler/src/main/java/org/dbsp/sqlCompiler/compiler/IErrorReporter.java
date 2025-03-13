package org.dbsp.sqlCompiler.compiler;

import org.dbsp.sqlCompiler.compiler.errors.SourcePositionRange;

/** Interface for reporting errors. */
public interface IErrorReporter {
    /** Specify the source position of the code unit currently under compilation.  Usually
     * corresponds to a view that is being compiled. */
    void setErrorContext(SourcePositionRange range);

    /** Report a problem (error or warning).
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
        this.reportError(range, errorType, message, false);
    }

    default void reportError(SourcePositionRange range, String errorType, String message, boolean continuation) {
        this.reportProblem(range, false, continuation, errorType, message);
    }

    default void reportWarning(SourcePositionRange range, String errorType, String message) {
        this.reportWarning(range, errorType, message, false);
    }

    default void reportWarning(SourcePositionRange range, String errorType, String message, boolean continuation) {
        this.reportProblem(range, true, continuation, errorType, message);
    }

    /** True if any error (but not a warning) has been reported. */
    boolean hasErrors();
}
