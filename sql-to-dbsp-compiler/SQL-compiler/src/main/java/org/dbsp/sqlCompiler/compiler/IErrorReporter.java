package org.dbsp.sqlCompiler.compiler;

import org.dbsp.sqlCompiler.compiler.errors.SourcePositionRange;

/**
 * Interface that knows how to report errors.
 */
public interface IErrorReporter {
    void reportError(SourcePositionRange range, boolean warning,
                     String errorType, String message);

    /**
     * True if any error (but not a warning) has been reported.
     */
    boolean hasErrors();
}
